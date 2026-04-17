#!/usr/bin/env python3
"""
Aplica migrations SQL no banco configurado em DATABASE_URL.

Executado automaticamente pelo Railway no `release` phase (ver Procfile).
Idempotente — todos os CREATEs usam IF NOT EXISTS.

Uso manual:
    python -m scripts.run_migrations
"""

from __future__ import annotations

import asyncio
import logging
import os
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


async def _apply_one(sql_path: Path) -> None:
    """Aplica um arquivo SQL via asyncpg (executa em bloco, não statement-a-statement)."""
    import asyncpg

    url = os.getenv("DATABASE_URL", "")
    if not url:
        logger.warning("DATABASE_URL ausente — pulando migrations.")
        return

    # asyncpg não aceita o prefixo +asyncpg; normalizar.
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

    for f in files:
        try:
            await _apply_one(f)
        except Exception as exc:  # noqa: BLE001
            logger.exception(f"Falha na migration {f.name}: {exc}")
            return 1

    logger.info(f"{len(files)} migration(s) aplicada(s) com sucesso.")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
