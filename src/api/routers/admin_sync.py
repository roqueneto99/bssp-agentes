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


@router.get("/hablla/raw/{email}")
async def debug_hablla_raw(
    email: str,
    x_admin_token: Optional[str] = Header(default=None),
):
    """Retorna o JSON cru de person + 1 card + 1 service da Hablla. Apenas
    pra calibrar o mapeamento — não toca no DB."""
    _check_token(x_admin_token)
    from src.integrations.hablla.client import HabllaClient
    token = os.getenv("HABLLA_API_TOKEN", "")
    workspace = os.getenv("HABLLA_WORKSPACE_ID", "")
    if not token or not workspace:
        raise HTTPException(500, "HABLLA_API_TOKEN/HABLLA_WORKSPACE_ID ausentes")

    h = HabllaClient(api_token=token, workspace_id=workspace)
    try:
        person = await h.search_person_by_email(email)
        if not person:
            return {"person": None, "note": "email não encontrado no Hablla"}
        person_id = str(person.get("id") or person.get("_id") or "")
        cards = (await h.list_cards(person_id=person_id, limit=3)).get("results", [])
        services = (await h.list_services(person_id=person_id, limit=3)).get("results", [])
        users = await h.list_users()
        return {
            "person_keys": list(person.keys()),
            "person": person,
            "cards_count": len(cards),
            "first_card": cards[0] if cards else None,
            "first_card_keys": list(cards[0].keys()) if cards else [],
            "services_count": len(services),
            "first_service": services[0] if services else None,
            "first_service_keys": list(services[0].keys()) if services else [],
            "users_sample": users[:3] if users else [],
        }
    finally:
        await h.close()
