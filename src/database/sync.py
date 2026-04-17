"""
Sincronização RD Station → PostgreSQL.

Dois modos:
  1. full_sync()  — Carga inicial de todos os ~363K leads
  2. incremental_sync() — Apenas leads novos/atualizados (por last_conversion_date)

Ambos respeitam rate limits do RD Station (burst 120 + 2 req/s no plano Pro).

Uso:
    python -m src.database.sync --mode full
    python -m src.database.sync --mode incremental
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

# Setup path
sys.path.insert(0, str(Path(__file__).parents[2]))
from dotenv import load_dotenv

load_dotenv(Path(__file__).parents[2] / ".env")

from sqlalchemy import select, func, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.integrations.rdstation.client import RDStationClient
from src.database.connection import get_engine, get_session, init_db, close_db
from src.database.models import Lead, Execucao

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
logger = logging.getLogger("sync")

# --- Constantes ---
PAGE_SIZE = 125  # Máximo da API de segmentação
BATCH_SIZE = 20  # Páginas em paralelo por batch
MAX_UPSERT_BATCH = 500  # Leads por batch de upsert no PostgreSQL


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    """Converte string ISO para datetime, retorna None se inválido."""
    if not value:
        return None
    try:
        # Remove milissegundos extras e normaliza
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _contact_to_lead_dict(contact: dict) -> dict:
    """Mapeia um contato da API RD Station para um dict de colunas do Lead."""
    return {
        "uuid": contact.get("uuid"),
        "email": (contact.get("email") or "").lower().strip(),
        "name": contact.get("name"),
        "personal_phone": contact.get("personal_phone"),
        "mobile_phone": contact.get("mobile_phone"),
        "job_title": contact.get("job_title"),
        "company_name": contact.get("company_name") or contact.get("company"),
        "city": contact.get("city"),
        "state": contact.get("state"),
        "country": contact.get("country"),
        "linkedin": contact.get("linkedin"),
        "website": contact.get("website"),
        "rd_created_at": _parse_dt(contact.get("created_at")),
        "last_conversion_date": _parse_dt(contact.get("last_conversion_date")),
        "first_conversion_date": _parse_dt(contact.get("first_conversion_date")),
        "tags": contact.get("tags") or [],
        "legal_bases": [
            lb if isinstance(lb, dict) else {"type": str(lb)}
            for lb in (contact.get("legal_bases") or [])
        ],
        "lifecycle_stage": contact.get("lifecycle_stage"),
        "fit_score": contact.get("fit_score"),
        "interest_score": contact.get("interest_score"),
        "raw_data": contact,
        "synced_at": datetime.now(timezone.utc),
    }


async def _upsert_leads(session, leads_data: list[dict]) -> tuple[int, int]:
    """
    Upsert em batch no PostgreSQL usando INSERT ... ON CONFLICT.
    Retorna (novos, atualizados).
    """
    if not leads_data:
        return 0, 0

    # Filtrar leads sem email
    valid = [d for d in leads_data if d.get("email")]
    if not valid:
        return 0, 0

    stmt = pg_insert(Lead).values(valid)

    # ON CONFLICT (email) DO UPDATE — atualiza todos os campos relevantes
    update_cols = {
        col: stmt.excluded[col]
        for col in valid[0].keys()
        if col not in ("id", "email", "s1_temperatura", "s1_prioridade",
                        "s1_area_principal", "s1_compliance", "s1_duplicados",
                        "s1_pode_seguir_squad2", "s1_processado_em",
                        "s2_score", "s2_classificacao", "s2_rota", "s2_acoes",
                        "s2_dimensoes", "s2_briefing", "s2_tags",
                        "s2_pode_seguir_squad3", "s2_processado_em")
    }

    stmt = stmt.on_conflict_do_update(
        index_elements=["email"],
        set_=update_cols,
    )

    result = await session.execute(stmt)

    # PostgreSQL não dá rowcount confiável para upserts em batch,
    # mas o total afetado = novos + atualizados
    affected = result.rowcount if result.rowcount else len(valid)
    return affected, 0  # Sem como distinguir precisamente


async def _fetch_page(rd: RDStationClient, seg_id: int, page: int) -> list[dict]:
    """Busca uma página de contatos. Retorna lista vazia em caso de erro."""
    try:
        data = await rd.get_segmentation_contacts(
            seg_id, page=page, page_size=PAGE_SIZE,
        )
        if isinstance(data, dict):
            return data.get("contacts", [])
        elif isinstance(data, list):
            return data
        return []
    except Exception as e:
        logger.warning("Erro ao buscar página %d: %s", page, e)
        return []


async def full_sync(seg_id: Optional[int] = None) -> dict:
    """
    Carga completa: busca TODOS os leads da segmentação e insere no PostgreSQL.

    Se seg_id não for informado, usa a primeira segmentação disponível.

    Retorna estatísticas da sincronização.
    """
    logger.info("=" * 60)
    logger.info("INÍCIO DA CARGA COMPLETA (full_sync)")
    logger.info("=" * 60)

    rd = RDStationClient(
        client_id=os.getenv("RDSTATION_CLIENT_ID", ""),
        client_secret=os.getenv("RDSTATION_CLIENT_SECRET", ""),
        refresh_token=os.getenv("RDSTATION_REFRESH_TOKEN", ""),
        plan=os.getenv("RDSTATION_PLAN", "pro"),
    )

    await init_db()

    stats = {
        "tipo": "full",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "total_pages": 0,
        "total_contacts": 0,
        "new_contacts": 0,
        "updated_contacts": 0,
        "errors": 0,
    }

    try:
        # Resolver segmentação
        if seg_id is None:
            segs = await rd.list_segmentations()
            if not segs:
                raise RuntimeError("Nenhuma segmentação encontrada no RD Station")
            seg_id = segs[0].get("id")
            logger.info("Usando segmentação: %s (%s)", seg_id, segs[0].get("name"))

        # Buscar primeira página + total
        first_data, total_rows = await rd.get_segmentation_contacts_with_total(
            seg_id, page=1, page_size=PAGE_SIZE,
        )
        first_contacts = []
        if isinstance(first_data, dict):
            first_contacts = first_data.get("contacts", [])
        elif isinstance(first_data, list):
            first_contacts = first_data

        total_pages = math.ceil(total_rows / PAGE_SIZE) if total_rows > 0 else 1
        logger.info("Total de leads na base: %d (%d páginas)", total_rows, total_pages)

        # Upsert primeira página
        all_leads = [_contact_to_lead_dict(c) for c in first_contacts]

        async with get_session() as session:
            affected, _ = await _upsert_leads(session, all_leads)
            stats["new_contacts"] += affected

        stats["total_contacts"] += len(first_contacts)
        stats["total_pages"] = 1

        logger.info("Página 1/%d: %d leads salvos", total_pages, len(first_contacts))

        # Buscar páginas restantes em batches paralelos
        page_num = 2
        t0 = time.time()

        while page_num <= total_pages:
            end_page = min(page_num + BATCH_SIZE, total_pages + 1)

            tasks = [
                _fetch_page(rd, seg_id, p)
                for p in range(page_num, end_page)
            ]
            results = await asyncio.gather(*tasks)

            batch_leads = []
            batch_done = False
            for i, contacts in enumerate(results):
                if not contacts:
                    if i == 0:  # Primeira página vazia = fim
                        batch_done = True
                    stats["errors"] += 1
                    continue
                batch_leads.extend(_contact_to_lead_dict(c) for c in contacts)
                stats["total_contacts"] += len(contacts)
                if len(contacts) < PAGE_SIZE:
                    batch_done = True

            # Upsert batch inteiro
            if batch_leads:
                for chunk_start in range(0, len(batch_leads), MAX_UPSERT_BATCH):
                    chunk = batch_leads[chunk_start:chunk_start + MAX_UPSERT_BATCH]
                    try:
                        async with get_session() as session:
                            affected, _ = await _upsert_leads(session, chunk)
                            stats["new_contacts"] += affected
                    except Exception as e:
                        logger.error("Erro ao salvar batch: %s", e)
                        stats["errors"] += 1

            current_page = min(end_page - 1, total_pages)
            stats["total_pages"] = current_page
            elapsed = time.time() - t0
            rate = stats["total_contacts"] / elapsed if elapsed > 0 else 0

            logger.info(
                "Páginas %d-%d/%d | %d leads total | %.1f leads/s | %.0fs",
                page_num, current_page, total_pages,
                stats["total_contacts"], rate, elapsed,
            )

            if batch_done:
                break

            page_num = end_page

        stats["finished_at"] = datetime.now(timezone.utc).isoformat()
        stats["status"] = "completed"
        total_time = time.time() - t0

        logger.info("=" * 60)
        logger.info("CARGA COMPLETA FINALIZADA")
        logger.info("  Leads processados: %d", stats["total_contacts"])
        logger.info("  Páginas: %d/%d", stats["total_pages"], total_pages)
        logger.info("  Erros: %d", stats["errors"])
        logger.info("  Tempo total: %.1fs", total_time)
        logger.info("=" * 60)

        # Salvar log de sync
        async with get_session() as session:
            await session.execute(
                text("""
                    INSERT INTO sync_log (tipo, started_at, finished_at,
                        total_pages, total_contacts, new_contacts,
                        updated_contacts, errors, status)
                    VALUES (:tipo, :started_at, :finished_at,
                        :total_pages, :total_contacts, :new_contacts,
                        :updated_contacts, :errors, :status)
                """),
                {
                    "tipo": "full",
                    "started_at": stats["started_at"],
                    "finished_at": stats["finished_at"],
                    "total_pages": stats["total_pages"],
                    "total_contacts": stats["total_contacts"],
                    "new_contacts": stats["new_contacts"],
                    "updated_contacts": stats["updated_contacts"],
                    "errors": stats["errors"],
                    "status": "completed",
                },
            )

        return stats

    except Exception as e:
        stats["status"] = "failed"
        stats["error_message"] = str(e)
        logger.error("ERRO NA CARGA COMPLETA: %s", e, exc_info=True)
        raise
    finally:
        await rd.close()


async def incremental_sync(
    seg_id: Optional[int] = None,
    since_hours: int = 24,
) -> dict:
    """
    Sincronização incremental: busca leads com last_conversion_date
    recente e atualiza no banco.

    Como a API ordena por last_conversion_date:desc, basta
    percorrer as primeiras páginas até encontrar leads mais
    antigos que o threshold.
    """
    logger.info("=" * 60)
    logger.info("INÍCIO DO SYNC INCREMENTAL (últimas %dh)", since_hours)
    logger.info("=" * 60)

    rd = RDStationClient(
        client_id=os.getenv("RDSTATION_CLIENT_ID", ""),
        client_secret=os.getenv("RDSTATION_CLIENT_SECRET", ""),
        refresh_token=os.getenv("RDSTATION_REFRESH_TOKEN", ""),
        plan=os.getenv("RDSTATION_PLAN", "pro"),
    )

    await init_db()

    cutoff = datetime.now(timezone.utc) - timedelta(hours=since_hours)
    cutoff_str = cutoff.isoformat()

    stats = {
        "tipo": "incremental",
        "since_hours": since_hours,
        "cutoff": cutoff_str,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "total_pages": 0,
        "total_contacts": 0,
        "new_contacts": 0,
        "errors": 0,
    }

    try:
        # Resolver segmentação
        if seg_id is None:
            segs = await rd.list_segmentations()
            if not segs:
                raise RuntimeError("Nenhuma segmentação encontrada")
            seg_id = segs[0].get("id")

        page = 1
        keep_going = True

        while keep_going:
            contacts = await _fetch_page(rd, seg_id, page)
            if not contacts:
                break

            stats["total_pages"] = page

            # Filtrar: manter apenas leads com last_conversion_date >= cutoff
            batch_leads = []
            for c in contacts:
                lcd = c.get("last_conversion_date", "")
                if lcd and lcd < cutoff_str:
                    # API retorna ordenado por last_conversion_date desc,
                    # então se encontramos um anterior ao cutoff, paramos
                    keep_going = False
                    break
                batch_leads.append(_contact_to_lead_dict(c))

            if batch_leads:
                async with get_session() as session:
                    affected, _ = await _upsert_leads(session, batch_leads)
                    stats["new_contacts"] += affected
                stats["total_contacts"] += len(batch_leads)

            logger.info(
                "Página %d: %d leads recentes de %d na página",
                page, len(batch_leads), len(contacts),
            )

            if len(contacts) < PAGE_SIZE:
                break

            page += 1

        stats["finished_at"] = datetime.now(timezone.utc).isoformat()
        stats["status"] = "completed"

        logger.info("=" * 60)
        logger.info("SYNC INCREMENTAL FINALIZADO")
        logger.info("  Leads atualizados: %d", stats["total_contacts"])
        logger.info("  Páginas: %d", stats["total_pages"])
        logger.info("=" * 60)

        # Log
        async with get_session() as session:
            await session.execute(
                text("""
                    INSERT INTO sync_log (tipo, started_at, finished_at,
                        total_pages, total_contacts, new_contacts, errors, status)
                    VALUES (:tipo, :started_at, :finished_at,
                        :total_pages, :total_contacts, :new_contacts, :errors, :status)
                """),
                {
                    "tipo": "incremental",
                    "started_at": stats["started_at"],
                    "finished_at": stats["finished_at"],
                    "total_pages": stats["total_pages"],
                    "total_contacts": stats["total_contacts"],
                    "new_contacts": stats["new_contacts"],
                    "errors": stats["errors"],
                    "status": "completed",
                },
            )

        return stats

    except Exception as e:
        stats["status"] = "failed"
        logger.error("ERRO NO SYNC INCREMENTAL: %s", e, exc_info=True)
        raise
    finally:
        await rd.close()


# --- CLI ---
async def main():
    import argparse

    parser = argparse.ArgumentParser(description="Sync RD Station → PostgreSQL")
    parser.add_argument("--mode", choices=["full", "incremental"], default="incremental")
    parser.add_argument("--seg-id", type=int, default=None, help="ID da segmentação")
    parser.add_argument("--since-hours", type=int, default=24, help="Horas para trás (incremental)")
    args = parser.parse_args()

    try:
        if args.mode == "full":
            result = await full_sync(seg_id=args.seg_id)
        else:
            result = await incremental_sync(seg_id=args.seg_id, since_hours=args.since_hours)

        print("\n✓ Sync finalizado:")
        print(json.dumps(result, indent=2, default=str))
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())
