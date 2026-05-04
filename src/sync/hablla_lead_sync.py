"""
Sync Hablla → tabela `leads` (Postgres).

Para cada lead que tem email, busca a pessoa correspondente no Hablla,
seus cards (deals/oportunidades) e services (atendimentos multicanal),
e materializa os campos consumidos pelo card do Kanban:

    - consultor              ← nome do responsável do card aberto mais recente (ou owner da pessoa)
    - matricula_curso        ← nome da lista do card (= curso/programa) mais recente
    - hablla_card_status     ← "open" | "won" | "lost" | None  (status do card mais recente)
    - hablla_em_atendimento  ← True se há card "open" (ou service status in (in_attendance, in_bot, pending))
    - s3_canal_preferido     ← canal mais usado nos services ("whatsapp" | "email" | ...)
    - s3_ultima_msg_em       ← max(updated_at) dos services
    - s3_ultima_resposta_em  ← max(updated_at) dos services com canal != email (heurística — Hablla
                               via token de integração não expõe direção da mensagem; documentado abaixo)
    - hablla_synced_at       ← timestamp do sync

NÃO INVENTA NADA: se a pessoa não existe no Hablla ou o card/service não têm
um campo, deixa NULL. O card só renderiza o que veio.

Uso:
    # Dry-run de 5 leads (sem escrever no DB)
    python -m src.sync.hablla_lead_sync --limit 5 --dry-run

    # Backfill incremental (leads sem hablla_synced_at OU sincronizados há > 24h)
    python -m src.sync.hablla_lead_sync --limit 200

    # Forçar sync de um email específico
    python -m src.sync.hablla_lead_sync --email lead@email.com

Como uso programático (e.g. dentro do cron diário):
    from src.sync.hablla_lead_sync import run_incremental_sync
    stats = await run_incremental_sync(limit=200)
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy import text

from src.database.connection import get_session
from src.integrations.hablla.client import HabllaClient

logger = logging.getLogger("sync.hablla_lead_sync")

TZ_BRT = timezone(timedelta(hours=-3))

# Janela depois da qual um lead já sincronizado entra de novo na fila
DEFAULT_RESYNC_AFTER_HOURS = 24


# ----------------------------------------------------------------------
# Migration (idempotente) — colunas novas que precisamos
# ----------------------------------------------------------------------

# Algumas colunas já existem (consultor, matricula_curso, s3_*) via migrations
# 002 e 004. Aqui adicionamos só as auxiliares novas.
SQL_ENSURE_COLUMNS = text("""
ALTER TABLE leads
    ADD COLUMN IF NOT EXISTS hablla_person_id     VARCHAR(64),
    ADD COLUMN IF NOT EXISTS hablla_card_status   VARCHAR(16),
    ADD COLUMN IF NOT EXISTS hablla_em_atendimento BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS hablla_synced_at     TIMESTAMPTZ
""")


# ----------------------------------------------------------------------
# Resultado
# ----------------------------------------------------------------------

@dataclass
class LeadSyncResult:
    lead_id: int
    email: str
    status: str  # "updated" | "no_match" | "no_email" | "error" | "dry_run"
    fields: dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None


@dataclass
class SyncStats:
    total: int = 0
    updated: int = 0
    no_match: int = 0
    no_email: int = 0
    errors: int = 0
    started_at: datetime = field(default_factory=lambda: datetime.now(TZ_BRT))

    def add(self, r: LeadSyncResult) -> None:
        self.total += 1
        if r.status == "updated":
            self.updated += 1
        elif r.status == "no_match":
            self.no_match += 1
        elif r.status == "no_email":
            self.no_email += 1
        elif r.status == "error":
            self.errors += 1

    def as_dict(self) -> dict:
        return {
            "total": self.total,
            "updated": self.updated,
            "no_match": self.no_match,
            "no_email": self.no_email,
            "errors": self.errors,
            "started_at": self.started_at.isoformat(),
            "duration_s": (datetime.now(TZ_BRT) - self.started_at).total_seconds(),
        }


# ----------------------------------------------------------------------
# Extração de campos do Hablla
# ----------------------------------------------------------------------

def _extract_consultor(card: dict, person: dict) -> Optional[str]:
    """Tenta extrair o nome do consultor responsável. Defensivo — Hablla
    expõe o owner em vários formatos dependendo da versão do endpoint."""
    for key in ("responsible", "owner", "assigned_user", "user"):
        v = card.get(key)
        if isinstance(v, dict):
            name = v.get("name") or v.get("full_name") or v.get("email")
            if name:
                return str(name).strip()
        elif isinstance(v, str) and v.strip():
            return v.strip()
    # Fallback: owner da pessoa
    for key in ("owner", "responsible"):
        v = person.get(key)
        if isinstance(v, dict):
            name = v.get("name") or v.get("full_name") or v.get("email")
            if name:
                return str(name).strip()
        elif isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _extract_curso(card: dict) -> Optional[str]:
    """O 'curso/programa' costuma ser o nome do board/list ao qual o card
    pertence (cada curso tem seu pipeline na Hablla)."""
    for key in ("list", "board", "stage", "pipeline"):
        v = card.get(key)
        if isinstance(v, dict):
            name = v.get("name") or v.get("title")
            if name:
                return str(name).strip()
        elif isinstance(v, str) and v.strip():
            return v.strip()
    # Última tentativa: o próprio nome do card
    name = card.get("name") or card.get("title")
    if name:
        return str(name).strip()
    return None


def _normalize_card_status(raw: str) -> Optional[str]:
    s = (raw or "").lower().strip()
    if s in ("open", "aberto", "em_andamento", "in_progress"):
        return "open"
    if s in ("won", "ganho", "convertido"):
        return "won"
    if s in ("lost", "perdido"):
        return "lost"
    if s:
        return s[:16]
    return None


def _pick_latest(items: list[dict]) -> Optional[dict]:
    """Pega o item mais recentemente atualizado — usa updated_at e cai pra
    created_at, defendendo de strings vazias."""
    if not items:
        return None
    def keyf(x: dict) -> str:
        return x.get("updated_at") or x.get("created_at") or ""
    return max(items, key=keyf)


def _parse_iso(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, AttributeError):
        return None


def _build_update_payload(person: dict, cards: list[dict], services: list[dict]) -> dict[str, Any]:
    """Traduz a resposta da Hablla para os campos da tabela `leads`."""

    payload: dict[str, Any] = {
        "hablla_person_id": str(person.get("id") or person.get("_id") or "")[:64] or None,
        "consultor": None,
        "matricula_curso": None,
        "hablla_card_status": None,
        "hablla_em_atendimento": False,
        "s3_estagio": None,
        "s3_canal_preferido": None,
        "s3_ultima_msg_em": None,
        "s3_ultima_resposta_em": None,
    }

    # ---- CARDS ----
    if cards:
        latest_card = _pick_latest(cards)
        if latest_card:
            payload["consultor"] = _extract_consultor(latest_card, person)
            payload["matricula_curso"] = _extract_curso(latest_card)
            payload["hablla_card_status"] = _normalize_card_status(
                latest_card.get("status", "")
            )
            # s3_estagio = nome do estágio (list.name) se existir
            stage = latest_card.get("list") or latest_card.get("stage")
            if isinstance(stage, dict):
                stage_name = stage.get("name")
                if stage_name:
                    payload["s3_estagio"] = str(stage_name)[:64]
            elif isinstance(stage, str):
                payload["s3_estagio"] = stage[:64]

        # Há card aberto?
        for c in cards:
            if _normalize_card_status(c.get("status", "")) == "open":
                payload["hablla_em_atendimento"] = True
                break

    # ---- SERVICES ----
    if services:
        # canal mais frequente (ignora vazios)
        canais = [
            (s.get("type") or "").lower().strip()
            for s in services
            if (s.get("type") or "").strip()
        ]
        if canais:
            most_common = Counter(canais).most_common(1)[0][0]
            payload["s3_canal_preferido"] = most_common[:16]

        # última atividade (qualquer canal)
        latest_svc = _pick_latest(services)
        if latest_svc:
            payload["s3_ultima_msg_em"] = _parse_iso(
                latest_svc.get("updated_at") or latest_svc.get("created_at")
            )

        # heurística "última resposta": services não-email costumam ser
        # conversas síncronas (whatsapp/instagram/telegram) — usamos como
        # proxy; quando Squad 3 implementar leitura de mensagens isso fica
        # mais preciso.
        non_email = [s for s in services if (s.get("type") or "").lower() != "email"]
        latest_chat = _pick_latest(non_email)
        if latest_chat:
            payload["s3_ultima_resposta_em"] = _parse_iso(
                latest_chat.get("updated_at") or latest_chat.get("created_at")
            )

        # Em atendimento também se há service ativo (status in_attendance/pending)
        for s in services:
            st = (s.get("status") or "").lower().strip()
            if st in ("in_attendance", "in_bot", "pending", "open", "aberto"):
                payload["hablla_em_atendimento"] = True
                break

    return payload


# ----------------------------------------------------------------------
# Operações de DB
# ----------------------------------------------------------------------

SQL_PICK_PENDING = text("""
SELECT id, email
FROM leads
WHERE email IS NOT NULL AND email <> ''
  AND (
       hablla_synced_at IS NULL
    OR hablla_synced_at < :resync_before
  )
ORDER BY
  -- prioriza score alto (eles aparecem antes no kanban)
  COALESCE(s2_score, 0) DESC,
  hablla_synced_at NULLS FIRST,
  id ASC
LIMIT :limit
""")


SQL_UPDATE_LEAD = text("""
UPDATE leads SET
    hablla_person_id      = :hablla_person_id,
    consultor             = :consultor,
    matricula_curso       = :matricula_curso,
    hablla_card_status    = :hablla_card_status,
    hablla_em_atendimento = :hablla_em_atendimento,
    s3_estagio            = :s3_estagio,
    s3_canal_preferido    = :s3_canal_preferido,
    s3_ultima_msg_em      = :s3_ultima_msg_em,
    s3_ultima_resposta_em = :s3_ultima_resposta_em,
    hablla_synced_at      = :hablla_synced_at
WHERE id = :id
""")


async def _ensure_columns() -> None:
    """Garante que as colunas auxiliares existem (idempotente)."""
    async with get_session() as session:
        async with session.begin():
            await session.execute(SQL_ENSURE_COLUMNS)


# ----------------------------------------------------------------------
# Sync de um lead
# ----------------------------------------------------------------------

async def sync_one_lead(
    hablla: HabllaClient,
    lead_id: int,
    email: str,
    *,
    dry_run: bool = False,
) -> LeadSyncResult:
    """Sincroniza um lead. Idempotente — sempre atualiza com o estado
    atual da Hablla."""

    if not email:
        return LeadSyncResult(lead_id=lead_id, email="", status="no_email")

    try:
        person = await hablla.search_person_by_email(email)
        if not person:
            # Marca como sincronizado de qualquer forma pra não tentar de novo no
            # próximo run (até a janela de resync passar). Não preenche outros
            # campos — fica null mesmo.
            if not dry_run:
                async with get_session() as session:
                    async with session.begin():
                        await session.execute(SQL_UPDATE_LEAD, {
                            "id": lead_id,
                            "hablla_person_id": None,
                            "consultor": None,
                            "matricula_curso": None,
                            "hablla_card_status": None,
                            "hablla_em_atendimento": False,
                            "s3_estagio": None,
                            "s3_canal_preferido": None,
                            "s3_ultima_msg_em": None,
                            "s3_ultima_resposta_em": None,
                            "hablla_synced_at": datetime.now(timezone.utc),
                        })
            return LeadSyncResult(lead_id=lead_id, email=email, status="no_match")

        person_id = str(person.get("id") or person.get("_id") or "")
        if not person_id:
            return LeadSyncResult(
                lead_id=lead_id, email=email, status="error",
                error="pessoa sem id",
            )

        # Cards e services em paralelo
        cards_data, svcs_data = await asyncio.gather(
            hablla.list_cards(person_id=person_id, limit=50),
            hablla.list_services(person_id=person_id, limit=50),
            return_exceptions=True,
        )

        cards = []
        services = []
        if isinstance(cards_data, dict):
            cards = cards_data.get("results", []) or []
        elif isinstance(cards_data, Exception):
            logger.warning("Cards fail %s: %s", email, cards_data)

        if isinstance(svcs_data, dict):
            services = svcs_data.get("results", []) or []
        elif isinstance(svcs_data, Exception):
            logger.warning("Services fail %s: %s", email, svcs_data)

        payload = _build_update_payload(person, cards, services)
        payload["hablla_synced_at"] = datetime.now(timezone.utc)
        payload["id"] = lead_id

        if dry_run:
            return LeadSyncResult(
                lead_id=lead_id, email=email, status="dry_run",
                fields={k: v for k, v in payload.items() if k != "id"},
            )

        async with get_session() as session:
            async with session.begin():
                await session.execute(SQL_UPDATE_LEAD, payload)

        return LeadSyncResult(
            lead_id=lead_id, email=email, status="updated",
            fields={k: v for k, v in payload.items() if k != "id"},
        )

    except Exception as e:
        logger.exception("sync falhou pro lead %s (%s): %s", lead_id, email, e)
        return LeadSyncResult(
            lead_id=lead_id, email=email, status="error", error=str(e),
        )


# ----------------------------------------------------------------------
# Sync incremental
# ----------------------------------------------------------------------

async def run_incremental_sync(
    *,
    limit: int = 200,
    dry_run: bool = False,
    resync_after_hours: int = DEFAULT_RESYNC_AFTER_HOURS,
) -> SyncStats:
    """Loop principal: pega N leads pendentes e sincroniza.
    Respeita o rate limit do client da Hablla (5 req/s)."""

    token = os.getenv("HABLLA_API_TOKEN", "")
    workspace = os.getenv("HABLLA_WORKSPACE_ID", "")
    if not token or not workspace:
        raise RuntimeError(
            "HABLLA_API_TOKEN e/ou HABLLA_WORKSPACE_ID não configurados",
        )

    if not dry_run:
        await _ensure_columns()

    resync_before = datetime.now(timezone.utc) - timedelta(hours=resync_after_hours)
    stats = SyncStats()

    async with get_session() as session:
        result = await session.execute(SQL_PICK_PENDING, {
            "resync_before": resync_before, "limit": limit,
        })
        rows = [dict(r) for r in result.mappings().all()]

    if not rows:
        logger.info("Nada a sincronizar (todos com hablla_synced_at recente)")
        return stats

    logger.info(
        "Iniciando sync de %d leads (dry_run=%s, resync_after=%dh)",
        len(rows), dry_run, resync_after_hours,
    )

    hablla = HabllaClient(api_token=token, workspace_id=workspace)
    try:
        for row in rows:
            r = await sync_one_lead(
                hablla, row["id"], row["email"], dry_run=dry_run,
            )
            stats.add(r)
            if r.status == "error":
                logger.warning("erro lead %s: %s", r.lead_id, r.error)
            elif r.status == "updated":
                # Log compacto pros campos preenchidos
                preenchidos = {
                    k: v for k, v in r.fields.items()
                    if v not in (None, False, "")
                }
                logger.info("lead %s ok — %s", r.lead_id, list(preenchidos.keys()))
    finally:
        await hablla.close()

    logger.info("Sync concluído: %s", stats.as_dict())
    return stats


async def run_for_email(email: str, dry_run: bool = False) -> Optional[LeadSyncResult]:
    """Útil pra debugar: sincroniza apenas um email específico."""
    token = os.getenv("HABLLA_API_TOKEN", "")
    workspace = os.getenv("HABLLA_WORKSPACE_ID", "")
    if not token or not workspace:
        raise RuntimeError(
            "HABLLA_API_TOKEN e/ou HABLLA_WORKSPACE_ID não configurados",
        )

    if not dry_run:
        await _ensure_columns()

    async with get_session() as session:
        result = await session.execute(
            text("SELECT id, email FROM leads WHERE LOWER(email) = LOWER(:e) LIMIT 1"),
            {"e": email},
        )
        row = result.mappings().first()
        if not row:
            logger.error("email %s não encontrado em leads", email)
            return None

    hablla = HabllaClient(api_token=token, workspace_id=workspace)
    try:
        return await sync_one_lead(
            hablla, row["id"], row["email"], dry_run=dry_run,
        )
    finally:
        await hablla.close()


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------

def _setup_logging() -> None:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


async def _main() -> None:
    _setup_logging()
    parser = argparse.ArgumentParser(description="Sync Hablla → leads")
    parser.add_argument("--limit", type=int, default=50,
                        help="Máximo de leads (default 50)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Não escreve no DB; só mostra o que faria")
    parser.add_argument("--email", type=str, default=None,
                        help="Sincroniza apenas o lead com esse email")
    parser.add_argument("--resync-after", type=int,
                        default=DEFAULT_RESYNC_AFTER_HOURS,
                        help="Horas mínimas pra re-sincronizar um lead")
    args = parser.parse_args()

    if args.email:
        r = await run_for_email(args.email, dry_run=args.dry_run)
        if r:
            print(json.dumps({
                "lead_id": r.lead_id, "email": r.email,
                "status": r.status, "error": r.error,
                "fields": {
                    k: (v.isoformat() if isinstance(v, datetime) else v)
                    for k, v in r.fields.items()
                },
            }, indent=2, ensure_ascii=False, default=str))
        return

    stats = await run_incremental_sync(
        limit=args.limit,
        dry_run=args.dry_run,
        resync_after_hours=args.resync_after,
    )
    print(json.dumps(stats.as_dict(), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(_main())
