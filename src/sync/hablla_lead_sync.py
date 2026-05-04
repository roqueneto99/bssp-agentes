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
    dry_run: int = 0
    no_match: int = 0
    no_email: int = 0
    errors: int = 0
    sample_fields: list[dict] = field(default_factory=list)
    started_at: datetime = field(default_factory=lambda: datetime.now(TZ_BRT))

    def add(self, r: LeadSyncResult) -> None:
        self.total += 1
        if r.status == "updated":
            self.updated += 1
        elif r.status == "dry_run":
            self.dry_run += 1
            # guarda os primeiros 3 pra inspeção rápida
            if len(self.sample_fields) < 3:
                self.sample_fields.append({
                    "lead_id": r.lead_id, "email": r.email, **{
                        k: (v.isoformat() if isinstance(v, datetime) else v)
                        for k, v in r.fields.items()
                    },
                })
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
            "dry_run": self.dry_run,
            "no_match": self.no_match,
            "no_email": self.no_email,
            "errors": self.errors,
            "sample_fields": self.sample_fields,
            "started_at": self.started_at.isoformat(),
            "duration_s": (datetime.now(TZ_BRT) - self.started_at).total_seconds(),
        }


# ----------------------------------------------------------------------
# Extração de campos do Hablla
# ----------------------------------------------------------------------

def _extract_consultor(
    card: dict, person: dict, users_map: dict[str, str],
) -> Optional[str]:
    """Resolve o nome do consultor responsável. Hablla retorna apenas IDs
    em card.user/user_id e person.users[]; resolvemos via users_map
    (id → name) que vem de list_users())."""
    # 1) consultor é o user dono do card
    for key in ("user_id", "user"):
        v = card.get(key)
        if isinstance(v, str) and v.strip():
            name = users_map.get(v.strip())
            if name:
                return name
        if isinstance(v, dict):
            inner = v.get("id") or v.get("_id")
            if inner and users_map.get(inner):
                return users_map[inner]
            n = v.get("name") or v.get("full_name") or v.get("email")
            if n:
                return str(n).strip()

    # 2) fallback: primeiro user atribuído à pessoa
    raw_users = person.get("users") or []
    if isinstance(raw_users, list):
        for u in raw_users:
            uid = u if isinstance(u, str) else (u.get("id") if isinstance(u, dict) else None)
            if uid and users_map.get(uid):
                return users_map[uid]

    return None


def _extract_curso(card: dict, boards_map: dict[str, str]) -> Optional[str]:
    """O 'curso/programa' costuma ser o nome do BOARD ao qual o card
    pertence (cada curso tem seu pipeline na Hablla)."""
    for key in ("board_id", "board"):
        v = card.get(key)
        if isinstance(v, str) and v.strip():
            name = boards_map.get(v.strip())
            if name:
                return name
        if isinstance(v, dict):
            inner = v.get("id") or v.get("_id")
            if inner and boards_map.get(inner):
                return boards_map[inner]
            n = v.get("name") or v.get("title")
            if n:
                return str(n).strip()
    return None


def _extract_estagio(card: dict, lists_map: dict[str, str]) -> Optional[str]:
    """A 'etapa' do funil é o nome da LIST a que o card pertence."""
    for key in ("list_id", "list"):
        v = card.get(key)
        if isinstance(v, str) and v.strip():
            name = lists_map.get(v.strip())
            if name:
                return name[:64]
        if isinstance(v, dict):
            inner = v.get("id") or v.get("_id")
            if inner and lists_map.get(inner):
                return lists_map[inner][:64]
            n = v.get("name") or v.get("title")
            if n:
                return str(n).strip()[:64]
    return None


def _normalize_card_status(raw: str) -> Optional[str]:
    """Hablla usa statuses customizáveis. Mantém o valor literal mas
    enxuga os mais comuns pra poder filtrar com confiança."""
    s = (raw or "").lower().strip()
    if not s:
        return None
    # apertando vocabulário pra os que importam pro card aberto/atendendo
    if s in ("open", "aberto", "em_andamento", "in_progress", "in_attendance",
             "in_bot", "pending", "atendendo", "active", "ativo"):
        return "open"
    if s in ("won", "ganho", "convertido", "ganhou", "ganha"):
        return "won"
    if s in ("lost", "perdido", "perdida", "cancelled", "cancelado"):
        return "lost"
    if s in ("finished", "concluido", "concluído"):
        return "finished"
    return s[:16]


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


def _build_update_payload(
    person: dict,
    cards: list[dict],
    services: list[dict],
    *,
    users_map: dict[str, str],
    boards_map: dict[str, str],
    lists_map: dict[str, str],
) -> dict[str, Any]:
    """Traduz a resposta da Hablla para os campos da tabela `leads`.
    Os 3 maps são caches id→name carregados antes do batch."""

    payload: dict[str, Any] = {
        "hablla_person_id": str(person.get("id") or person.get("_id") or "")[:64] or None,
        "consultor": None,
        "matricula_curso": None,
        "hablla_board_id": None,
        "hablla_list_id": None,
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
            payload["consultor"] = _extract_consultor(latest_card, person, users_map)
            payload["matricula_curso"] = _extract_curso(latest_card, boards_map)
            payload["hablla_card_status"] = _normalize_card_status(
                latest_card.get("status", "")
            )
            payload["s3_estagio"] = _extract_estagio(latest_card, lists_map)
            # Persiste IDs crus pra permitir JOIN com cursos / hablla_id_map
            bid = latest_card.get("board_id") or latest_card.get("board")
            lid = latest_card.get("list_id") or latest_card.get("list")
            if isinstance(bid, dict):
                bid = bid.get("id") or bid.get("_id")
            if isinstance(lid, dict):
                lid = lid.get("id") or lid.get("_id")
            payload["hablla_board_id"] = str(bid)[:64] if bid else None
            payload["hablla_list_id"] = str(lid)[:64] if lid else None

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
    hablla_board_id       = :hablla_board_id,
    hablla_list_id        = :hablla_list_id,
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

async def _load_manual_maps() -> dict[str, dict[str, str]]:
    """Lê hablla_id_map (cadastro manual feito pelo admin) e separa por type.
    Tabela existe a partir da migration 006."""
    out = {"board": {}, "list": {}, "user": {}, "sector": {}}
    try:
        async with get_session() as session:
            result = await session.execute(text(
                "SELECT hablla_id, type, name FROM hablla_id_map"
            ))
            for row in result.mappings().all():
                t = row["type"]
                if t in out:
                    out[t][row["hablla_id"]] = row["name"]
    except Exception as e:
        # se a migration 006 ainda não rodou, segue sem manual map
        logger.info("hablla_id_map indisponível: %s", e)
    return out


async def _build_resolution_maps(hablla: HabllaClient) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    """Carrega users do Hablla + boards/lists da tabela manual hablla_id_map.
    Devolve 3 dicts id→name. Falhas individuais não derrubam o sync."""
    users_map: dict[str, str] = {}
    boards_map: dict[str, str] = {}
    lists_map: dict[str, str] = {}

    # 1) Users — direto do Hablla (já paginado)
    try:
        users = await hablla.list_users()
        for member in users or []:
            inner = member.get("user") if isinstance(member, dict) else None
            if isinstance(inner, dict):
                uid = str(inner.get("id") or inner.get("_id") or "")
                name = inner.get("name") or inner.get("full_name") or inner.get("email")
                if uid and name:
                    users_map[uid] = str(name).strip()
            uid_raw = str(member.get("id") or "") if isinstance(member, dict) else ""
            name_raw = (member.get("name") if isinstance(member, dict) else None)
            if uid_raw and name_raw and uid_raw not in users_map:
                users_map[uid_raw] = str(name_raw).strip()
    except Exception as e:
        logger.warning("Falha ao carregar users do Hablla: %s", e)

    # 2) Boards e lists — tenta Hablla primeiro (provavelmente vai dar 401),
    #    mas começa com tabela manual.
    manual = await _load_manual_maps()
    boards_map.update(manual.get("board", {}))
    lists_map.update(manual.get("list", {}))
    # users também pode receber overrides manuais (ex: caso o user_id do
    # card não esteja no list_users)
    for k, v in (manual.get("user") or {}).items():
        users_map[k] = v

    try:
        boards = await hablla.list_boards(limit=200)
        for b in boards or []:
            bid = str(b.get("id") or b.get("_id") or "")
            name = b.get("name") or b.get("title")
            if bid and name and bid not in boards_map:
                boards_map[bid] = str(name).strip()
    except Exception as e:
        logger.info("list_boards Hablla falhou (ok, usando manual): %s", e)

    try:
        lists = await hablla.list_lists(limit=500)
        for l in lists or []:
            lid = str(l.get("id") or l.get("_id") or "")
            name = l.get("name") or l.get("title")
            if lid and name and lid not in lists_map:
                lists_map[lid] = str(name).strip()
    except Exception as e:
        logger.info("list_lists Hablla falhou (ok, usando manual): %s", e)

    logger.info(
        "Maps carregados: users=%d boards=%d lists=%d (manual: b=%d l=%d u=%d)",
        len(users_map), len(boards_map), len(lists_map),
        len(manual.get("board", {})), len(manual.get("list", {})),
        len(manual.get("user", {})),
    )
    return users_map, boards_map, lists_map


async def sync_one_lead(
    hablla: HabllaClient,
    lead_id: int,
    email: str,
    *,
    dry_run: bool = False,
    users_map: Optional[dict[str, str]] = None,
    boards_map: Optional[dict[str, str]] = None,
    lists_map: Optional[dict[str, str]] = None,
) -> LeadSyncResult:
    """Sincroniza um lead. Idempotente — sempre atualiza com o estado
    atual da Hablla."""

    # Carrega maps localmente se não vieram (ex.: chamada single-shot)
    if users_map is None or boards_map is None or lists_map is None:
        users_map, boards_map, lists_map = await _build_resolution_maps(hablla)

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
                            "hablla_board_id": None,
                            "hablla_list_id": None,
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

        payload = _build_update_payload(
            person, cards, services,
            users_map=users_map, boards_map=boards_map, lists_map=lists_map,
        )
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
        # Carrega maps id→name uma vez por batch (rate-limit friendly)
        users_map, boards_map, lists_map = await _build_resolution_maps(hablla)

        for row in rows:
            r = await sync_one_lead(
                hablla, row["id"], row["email"],
                dry_run=dry_run,
                users_map=users_map,
                boards_map=boards_map,
                lists_map=lists_map,
            )
            stats.add(r)
            if r.status == "error":
                logger.warning("erro lead %s: %s", r.lead_id, r.error)
            elif r.status in ("updated", "dry_run"):
                preenchidos = {
                    k: v for k, v in r.fields.items()
                    if v not in (None, False, "")
                }
                logger.info("lead %s %s — %s",
                            r.lead_id, r.status, list(preenchidos.keys()))
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
