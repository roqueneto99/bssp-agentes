#!/usr/bin/env python3
"""
Job periódico de sincronização RD Station → PostgreSQL.

Roda como processo standalone — ideal para Railway cron jobs
ou um scheduler externo.

Modos:
  python sync_job.py                   # incremental (últimas 24h)
  python sync_job.py --full            # carga completa
  python sync_job.py --hours 6         # incremental últimas 6h

Em produção (Railway), configure como cron job:
  Schedule: 0 */4 * * *   (a cada 4 horas)
  Command:  python sync_job.py --hours 6
"""

import asyncio
import os
import sys
from pathlib import Path

# Garantir imports do projeto
sys.path.insert(0, str(Path(__file__).parent))
from dotenv import load_dotenv
from scripts.squad2_batch_scoring import run_batch_scoring

load_dotenv(Path(__file__).parent / ".env")


async def run():
    import argparse

    parser = argparse.ArgumentParser(description="BSSP Sync Job")
    parser.add_argument("--full", action="store_true", help="Carga completa")
    parser.add_argument("--hours", type=int, default=24, help="Horas para trás")
    parser.add_argument("--seg-id", type=int, default=None)
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
        print(f"Sync {result['tipo']} finalizado: {status} ({contacts} leads)")
        return 0 if status == "completed" else 1
    finally:
        await close_db()
    # === Squad 2 — scoring automático em batch (auto-deploy 2026-05) ===
    if not getattr(args, 'skip_scoring', False):
        try:
            scoring_metrics = await run_batch_scoring(
                pool=db_pool,
                max_leads=int(os.environ.get('MAX_SCORING_PER_RUN', '500')),
                dry_run=os.environ.get('SCORING_DRY_RUN','false').lower()=='true',
            )
            logger.info('Scoring metrics: %s', scoring_metrics)
        except Exception:
            logger.exception('falha no batch scoring (sync seguiu OK)')



if __name__ == "__main__":
    code = asyncio.run(run())
    sys.exit(code)