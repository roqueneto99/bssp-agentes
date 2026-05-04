"""
Endpoints administrativos pra disparar syncs sob demanda.

Protegido por header X-Admin-Token (bate com env ADMIN_SYNC_TOKEN).
Existe pra rodar dry-runs/backfills do sandbox/dashboard sem precisar
de Railway CLI ou shell no container.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Header, HTTPException, Query

from src.sync.hablla_lead_sync import (
    DEFAULT_RESYNC_AFTER_HOURS,
    run_for_email,
    run_incremental_sync,
)

logger = logging.getLogger("admin_sync")
router = APIRouter(prefix="/api/admin/sync", tags=["admin-sync"])


def _check_token(x_admin_token: Optional[str]) -> None:
    expected = os.getenv("ADMIN_SYNC_TOKEN", "")
    if not expected:
        raise HTTPException(
            500, "ADMIN_SYNC_TOKEN não configurado no servidor",
        )
    if not x_admin_token or x_admin_token != expected:
        raise HTTPException(401, "token inválido")


def _serialize(obj):
    """JSON-friendly: datetime → ISO string."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_serialize(v) for v in obj]
    return obj


@router.post("/hablla")
async def trigger_hablla_sync(
    limit: int = Query(default=5, ge=1, le=200),
    dry_run: bool = Query(default=True),
    resync_after_hours: int = Query(default=DEFAULT_RESYNC_AFTER_HOURS, ge=0, le=720),
    x_admin_token: Optional[str] = Header(default=None),
):
    """Dispara o sync Hablla→leads. Por padrão dry_run=true."""
    _check_token(x_admin_token)
    logger.info(
        "trigger_hablla_sync limit=%d dry_run=%s resync_after=%dh",
        limit, dry_run, resync_after_hours,
    )
    stats = await run_incremental_sync(
        limit=limit, dry_run=dry_run, resync_after_hours=resync_after_hours,
    )
    return _serialize(stats.as_dict())


@router.post("/hablla/email/{email}")
async def trigger_hablla_sync_email(
    email: str,
    dry_run: bool = Query(default=True),
    x_admin_token: Optional[str] = Header(default=None),
):
    """Dispara o sync Hablla→leads pra um email específico (debug)."""
    _check_token(x_admin_token)
    r = await run_for_email(email, dry_run=dry_run)
    if r is None:
        raise HTTPException(404, f"email {email} não encontrado em leads")
    return {
        "lead_id": r.lead_id,
        "email": r.email,
        "status": r.status,
        "error": r.error,
        "fields": _serialize(r.fields),
    }
