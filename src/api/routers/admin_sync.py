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


@router.get("/hablla/check-user/{user_id}")
async def check_user_in_map(
    user_id: str,
    x_admin_token: Optional[str] = Header(default=None),
):
    """Confirma se um user_id (vindo de card.user_id) bate com alguma chave
    do users_map. Retorna nome se sim; lista chaves próximas se não."""
    _check_token(x_admin_token)
    from src.integrations.hablla.client import HabllaClient
    from src.sync.hablla_lead_sync import _build_resolution_maps
    token = os.getenv("HABLLA_API_TOKEN", "")
    workspace = os.getenv("HABLLA_WORKSPACE_ID", "")
    h = HabllaClient(api_token=token, workspace_id=workspace)
    try:
        users_raw = await h.list_users()
        users_map, _, _ = await _build_resolution_maps(h)
        # vasculha em todos os shapes possíveis dos members
        found_in: list[str] = []
        for member in users_raw or []:
            if not isinstance(member, dict):
                continue
            inner = member.get("user") if isinstance(member.get("user"), dict) else {}
            if member.get("id") == user_id:
                found_in.append("member.id")
            if inner.get("id") == user_id:
                found_in.append("member.user.id")
            if inner.get("_id") == user_id:
                found_in.append("member.user._id")
            if member.get("user_id") == user_id:
                found_in.append("member.user_id")
            if inner.get("firebase_id") == user_id:
                found_in.append("member.user.firebase_id")
        return {
            "user_id_searched": user_id,
            "found_in_map": user_id in users_map,
            "name_if_found": users_map.get(user_id),
            "matched_in_member_paths": found_in,
            "users_count": len(users_raw),
            "users_map_size": len(users_map),
            "first_5_keys_in_map": list(users_map.keys())[:5],
        }
    finally:
        await h.close()


@router.get("/hablla/probe-single/{email}")
async def probe_single_resources(
    email: str,
    x_admin_token: Optional[str] = Header(default=None),
):
    """Tenta GET single em board, list, card, user — pra confirmar se token
    tem permissão de ler recursos individuais (mesmo sem listar)."""
    _check_token(x_admin_token)
    from src.integrations.hablla.client import HabllaClient, HabllaError
    token = os.getenv("HABLLA_API_TOKEN", "")
    workspace = os.getenv("HABLLA_WORKSPACE_ID", "")
    h = HabllaClient(api_token=token, workspace_id=workspace)
    out: dict = {}
    try:
        person = await h.search_person_by_email(email)
        if not person:
            return {"error": "email não encontrado"}
        person_id = str(person.get("id") or "")
        cards = (await h.list_cards(person_id=person_id, limit=1)).get("results", [])
        if not cards:
            return {"error": "sem card"}
        card = cards[0]
        card_id = str(card.get("id") or "")
        board_id = str(card.get("board") or card.get("board_id") or "")
        list_id = str(card.get("list") or card.get("list_id") or "")
        user_id = str(card.get("user") or card.get("user_id") or "")
        out["ids"] = {"card_id": card_id, "board_id": board_id, "list_id": list_id, "user_id": user_id}
        attempts = [
            ("v1", f"boards/{board_id}", "board"),
            ("v2", f"boards/{board_id}", "board"),
            ("v1", f"lists/{list_id}", "list"),
            ("v2", f"lists/{list_id}", "list"),
            ("v1", f"boards/{board_id}/lists/{list_id}", "board>list"),
            ("v1", f"cards/{card_id}", "card_full"),
            ("v2", f"cards/{card_id}", "card_full_v2"),
            ("v1", f"users/{user_id}", "user"),
            ("v2", f"users/{user_id}", "user_v2"),
        ]
        for version, resource, label in attempts:
            path = h._ws_path(version, resource)
            try:
                data = await h._request("GET", path)
                if isinstance(data, dict):
                    out[f"{label} ({version})"] = {
                        "ok": True,
                        "keys": list(data.keys())[:12],
                        "name": data.get("name") or data.get("title"),
                    }
                else:
                    out[f"{label} ({version})"] = {"ok": True, "shape": type(data).__name__}
            except HabllaError as e:
                out[f"{label} ({version})"] = {"ok": False, "status": e.status_code}
            except Exception as e:
                out[f"{label} ({version})"] = {"ok": False, "exc": str(e)[:80]}
    finally:
        await h.close()
    return out


@router.get("/hablla/probe-boards")
async def probe_boards_endpoints(
    x_admin_token: Optional[str] = Header(default=None),
):
    """Tenta múltiplos endpoints de boards/lists e retorna qual funcionou."""
    _check_token(x_admin_token)
    from src.integrations.hablla.client import HabllaClient, HabllaError
    token = os.getenv("HABLLA_API_TOKEN", "")
    workspace = os.getenv("HABLLA_WORKSPACE_ID", "")
    if not token or not workspace:
        raise HTTPException(500, "env ausentes")

    h = HabllaClient(api_token=token, workspace_id=workspace)
    candidatos = [
        ("v1", "boards"),
        ("v2", "boards"),
        ("v1", "deals"),
        ("v2", "deals"),
        ("v1", "pipelines"),
        ("v2", "pipelines"),
    ]
    out = {}
    try:
        for version, resource in candidatos:
            path = h._ws_path(version, resource)
            try:
                data = await h._request("GET", path, params={"page": 1, "limit": 5})
                shape = "list" if isinstance(data, list) else type(data).__name__
                if isinstance(data, dict):
                    keys = list(data.keys())[:8]
                    results = data.get("results", [])
                    out[f"{version}/{resource}"] = {
                        "ok": True, "keys": keys, "results_count": len(results),
                        "first_result_keys": list(results[0].keys())[:15] if results else [],
                        "first_result_name": results[0].get("name") if results else None,
                    }
                else:
                    out[f"{version}/{resource}"] = {"ok": True, "shape": shape, "count": len(data) if isinstance(data, list) else None}
            except HabllaError as e:
                out[f"{version}/{resource}"] = {"ok": False, "status": e.status_code, "msg": (e.message or "")[:120]}
            except Exception as e:
                out[f"{version}/{resource}"] = {"ok": False, "exc": str(e)[:120]}
    finally:
        await h.close()
    return out


@router.get("/hablla/maps")
async def debug_hablla_maps(
    x_admin_token: Optional[str] = Header(default=None),
):
    """Mostra status dos 3 maps id→name (users, boards, lists) que o sync usa."""
    _check_token(x_admin_token)
    from src.integrations.hablla.client import HabllaClient
    from src.sync.hablla_lead_sync import _build_resolution_maps
    token = os.getenv("HABLLA_API_TOKEN", "")
    workspace = os.getenv("HABLLA_WORKSPACE_ID", "")
    if not token or not workspace:
        raise HTTPException(500, "HABLLA_API_TOKEN/HABLLA_WORKSPACE_ID ausentes")

    h = HabllaClient(api_token=token, workspace_id=workspace)
    try:
        # Tenta cada endpoint individualmente pra reportar erros separados
        result = {}
        try:
            users_raw = await h.list_users()
            result["users_raw_count"] = len(users_raw or [])
            result["users_raw_sample"] = users_raw[:2] if users_raw else []
        except Exception as e:
            result["users_error"] = str(e)

        try:
            boards_raw = await h.list_boards(limit=200)
            result["boards_raw_count"] = len(boards_raw or [])
            result["boards_raw_sample"] = boards_raw[:3] if boards_raw else []
        except Exception as e:
            result["boards_error"] = str(e)

        try:
            lists_raw = await h.list_lists(limit=500)
            result["lists_raw_count"] = len(lists_raw or [])
            result["lists_raw_sample"] = lists_raw[:3] if lists_raw else []
        except Exception as e:
            result["lists_error"] = str(e)

        users_map, boards_map, lists_map = await _build_resolution_maps(h)
        result["users_map_size"] = len(users_map)
        result["boards_map_size"] = len(boards_map)
        result["lists_map_size"] = len(lists_map)
        # mostra alguns pares pra inspeção
        result["users_map_sample"] = dict(list(users_map.items())[:5])
        result["boards_map_sample"] = dict(list(boards_map.items())[:5])
        result["lists_map_sample"] = dict(list(lists_map.items())[:5])
        return result
    finally:
        await h.close()
