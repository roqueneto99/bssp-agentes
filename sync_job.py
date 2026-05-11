#!/usr/bin/env python3
"""
Job periódico de sincronização RD Station → PostgreSQL.

Roda como processo standalone — ideal para Railway cron jobs
ou um scheduler externo.

Modos:
  python sync_job.py                   # incremental (últimas 24h)
  python sync_job.py --full            # carga completa
  python sync_job.py --hours 6         # incremental últimas 6h
  python sync_job.py --skip-scoring    # pula batch de scoring
  python sync_job.py --skip-comparativo  # pula reconciliação RD x Hablla

Em produção (Railway), configure como cron job:
  Schedule: 0 3 * * *   (todo dia às 03:00 BRT)
  Command:  python sync_job.py --hours 24
"""

import asyncio
import logging
import os
import sys
from pathlib import Path

# Garantir imports do projeto
sys.path.insert(0, str(Path(__file__).parent))
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [sync_job] %(levelname)s %(message)s",
)
logger = logging.getLogger("sync_job")


async def _run_scoring_safe(skip: bool) -> None:
    """Roda batch scoring isolado em try/except — não derruba sync."""
    if skip:
        logger.info("Scoring pulado (--skip-scoring).")
        return
    try:
        import asyncpg
        from scripts.squad2_batch_scoring import run_batch_scoring
        dsn = os.getenv("DATABASE_URL", "").replace(
            "postgresql+asyncpg://", "postgresql://"
        )
        if not dsn:
            logger.warning("DATABASE_URL ausente — scoring pulado.")
            return
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
        try:
            metrics = await run_batch_scoring(
                pool=pool,
                max_leads=int(os.environ.get("MAX_SCORING_PER_RUN", "500")),
                dry_run=os.environ.get("SCORING_DRY_RUN", "false").lower() == "true",
            )
            logger.info("Scoring metrics: %s", metrics)
        finally:
            await pool.close()
    except Exception:
        logger.exception("Falha no batch scoring (sync seguiu OK)")


async def _run_comparativo_safe(skip: bool) -> None:
    """Reconcilia RD x Hablla (delta 24h). Isolado em try/except."""
    if skip:
        logger.info("Comparativo pulado (--skip-comparativo).")
        return
    try:
        from src.sync.lead_comparison import run_daily_comparison
        comp = await run_daily_comparison(snapshot_date=None, dry_run=False)
        logger.info(
            "Comparativo: total=%s divergentes=%s só-RD=%s só-Hablla=%s",
            comp["total"], comp["matched_divergent"],
            comp["only_rd"], comp["only_hablla"],
        )
    except Exception:
        logger.exception("Falha no comparativo (sync seguiu OK)")


async def run() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="BSSP Sync Job")
    parser.add_argument("--full", action="store_true", help="Carga completa")
    parser.add_argument("--hours", type=int, default=24, help="Horas para trás")
    parser.add_argument("--seg-id", type=int, default=None)
    parser.add_argument("--skip-scoring", action="store_true",
                        help="Pula batch de scoring após o sync")
    parser.add_argument("--skip-comparativo", action="store_true",
                        help="Pula reconciliação RD x Hablla")
    args = parser.parse_args()

    from src.database.sync import full_sync, incremental_sync
    from src.database.connection import close_db

    try:
        if args.full:
            result = await full_sync(seg_id=args.seg_id)
        else:
            result = await incremental_sync(
                seg_id=args.seg_id,
                since_hours=args.hours,
            )

        status = result.get("status", "unknown")
        contacts = result.get("total_contacts", 0)
        logger.info("Sync %s finalizado: %s (%s leads)",
                    result.get("tipo", "?"), status, contacts)

        # --- Passos pós-sync (cada um isolado) ---
        await _run_scoring_safe(args.skip_scoring)
        await _run_comparativo_safe(args.skip_comparativo)

        return 0 if status == "completed" else 1
    finally:
        await close_db()


if __name__ == "__main__":
    code = asyncio.run(run())
    sys.exit(code)
