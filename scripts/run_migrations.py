#!/usr/bin/env python3
"""
Aplica migrations SQL no banco configurado em DATABASE_URL.

Executado pelo Railway via `deploy.preDeployCommand` (railway.json).
Idempotente — CREATEs usam IF NOT EXISTS.

Variáveis de ambiente:
    DATABASE_URL              (obrigatória)
    MIGRATION_MIN_VERSION     (opcional) — pula arquivos com número < N.
                              Útil pra desbloquear o deploy quando uma
                              migration antiga tem bug latente e o banco
                              prod já está num estado avançado.

Uso manual:
    python -m scripts.run_migrations
    MIGRATION_MIN_VERSION=9 python scripts/run_migrations.py   # só >= 009
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [migrations] %(levelname)s %(message)s",
)
logger = logging.getLogger("migrations")

MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "migrations"
MIN_VERSION = int(os.getenv("MIGRATION_MIN_VERSION", "0"))


def _version_of(path: Path) -> int:
    m = re.match(r"^(\d+)", path.name)
    return int(m.group(1)) if m else 0


async def _apply_one(sql_path: Path) -> None:
    """Aplica um arquivo SQL via asyncpg (executa em bloco, não statement-a-statement)."""
    import asyncpg

    url = os.getenv("DATABASE_URL", "")
    if not url:
        logger.warning("DATABASE_URL ausente — pulando migrations.")
        return

    dsn = url.replace("postgresql+asyncpg://", "postgresql://")
    sql = sql_path.read_text(encoding="utf-8")
    conn = await asyncpg.connect(dsn)
    try:
        logger.info(f"Aplicando {sql_path.name}…")
        await conn.execute(sql)
        logger.info(f"OK — {sql_path.name}")
    finally:
        await conn.close()


async def main() -> int:
    if not MIGRATIONS_DIR.exists():
        logger.warning("Pasta migrations/ não encontrada — nada a fazer.")
        return 0

    files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    if not files:
        logger.info("Nenhum arquivo .sql em migrations/.")
        return 0

    skipped = 0
    applied = 0
    for f in files:
        if _version_of(f) < MIN_VERSION:
            logger.info(f"Pulando {f.name} (versão < MIGRATION_MIN_VERSION={MIN_VERSION})")
            skipped += 1
            continue
        try:
            await _apply_one(f)
            applied += 1
        except Exception as exc:  # noqa: BLE001
            logger.exception(f"Falha na migration {f.name}: {exc}")
            return 1

    logger.info(f"Migrations: {applied} aplicada(s), {skipped} pulada(s).")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
