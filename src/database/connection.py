"""
Conexão PostgreSQL via SQLAlchemy async.

Lê DATABASE_URL do .env. Suporta tanto o formato Railway
(postgresql://...) quanto o asyncpg (postgresql+asyncpg://...).
"""

from __future__ import annotations

import os
import logging
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from .models import Base

logger = logging.getLogger("database")

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def _normalize_url(url: str) -> str:
    """Converte postgresql:// para postgresql+asyncpg:// se necessário."""
    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
    elif url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+asyncpg://", 1)
    return url


def get_engine() -> AsyncEngine:
    """Retorna o engine singleton. Cria na primeira chamada."""
    global _engine
    if _engine is None:
        raw_url = os.getenv("DATABASE_URL", "")
        if not raw_url:
            raise RuntimeError(
                "DATABASE_URL não configurada. "
                "Defina no .env: DATABASE_URL=postgresql+asyncpg://user:pass@host:5432/bssp_agentes"
            )
        url = _normalize_url(raw_url)
        _engine = create_async_engine(
            url,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            echo=os.getenv("DB_ECHO", "").lower() in ("1", "true"),
        )
        logger.info("Engine PostgreSQL criado: %s", url.split("@")[-1] if "@" in url else "localhost")
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """Retorna a session factory singleton."""
    global _session_factory
    if _session_factory is None:
        _session_factory = async_sessionmaker(
            get_engine(), expire_on_commit=False
        )
    return _session_factory


@asynccontextmanager
async def get_session():
    """Context manager para obter uma sessão async."""
    factory = get_session_factory()
    async with factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def init_db():
    """Cria todas as tabelas (idempotente — ignora se já existem)."""
    engine = get_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Tabelas criadas/verificadas com sucesso")


async def close_db():
    """Fecha o engine e libera conexões."""
    global _engine, _session_factory
    if _engine:
        await _engine.dispose()
        _engine = None
        _session_factory = None
        logger.info("Engine PostgreSQL fechado")
