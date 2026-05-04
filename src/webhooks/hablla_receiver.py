"""
Receiver de webhooks do Hablla.

POST /api/webhooks/hablla
Recebe payload de evento (mensagem, service, etc), persiste em
`mensagens_hablla`. Usa hablla_message_id pra idempotência.

Auth: header `X-Hablla-Token` deve bater com env HABLLA_WEBHOOK_TOKEN.

Como o shape exato do payload do Hablla pode variar entre eventos
(message.created, service.opened, etc), o receiver é defensivo:
salva raw_payload sempre e tenta extrair os campos conhecidos.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Header, HTTPException, Request
from sqlalchemy import text

from src.database.connection import get_session

logger = logging.getLogger("hablla_receiver")
router = APIRouter(prefix="/api/webhooks/hablla", tags=["webhooks"])


def _check_token(x_hablla_token: Optional[str]) -> None:
    expected = os.getenv("HABLLA_WEBHOOK_TOKEN", "")
    if not expected:
        # se ainda não configurado, deixa passar mas loga (pra setup inicial)
        logger.warning("HABLLA_WEBHOOK_TOKEN não configurado — aceitando sem auth")
        return
    if not x_hablla_token or x_hablla_token != expected:
        raise HTTPException(401, "token de webhook inválido")


def _parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, AttributeError):
        return None


def _extract_message_fields(payload: dict) -> dict[str, Any]:
    """Extrai campos da mensagem de um payload Hablla. Defensivo: payload
    pode ser nested (event/data) ou flat dependendo do tipo de evento."""

    # Tenta acessar payload.data primeiro (eventos costumam vir aninhados)
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        data = payload  # fallback flat

    msg_id = str(data.get("id") or data.get("_id") or data.get("message_id") or "")[:64] or None

    # Service & Person
    svc_obj = data.get("service")
    svc_id = (
        str(data.get("service_id"))
        if data.get("service_id")
        else (str(svc_obj.get("id")) if isinstance(svc_obj, dict) and svc_obj.get("id") else None)
    )
    if svc_id: svc_id = svc_id[:64]

    person_obj = data.get("person")
    person_id = (
        str(data.get("person_id"))
        if data.get("person_id")
        else (str(person_obj.get("id")) if isinstance(person_obj, dict) and person_obj.get("id") else None)
    )
    if person_id: person_id = person_id[:64]

    # Canal
    canal = (
        data.get("type") or data.get("channel") or
        (svc_obj.get("type") if isinstance(svc_obj, dict) else None) or ""
    )
    canal = str(canal).lower()[:16] if canal else None

    # Direção
    # Heurística: presença de user_id (consultor) → out (BSSP enviou).
    # Sem user_id mas com sender=person/lead → in.
    direcao = None
    user_obj = data.get("user")
    user_id = (
        str(data.get("user_id"))
        if data.get("user_id")
        else (str(user_obj.get("id")) if isinstance(user_obj, dict) and user_obj.get("id") else None)
    )
    if user_id:
        direcao = "out"
    elif data.get("from_lead") or data.get("from_person") or data.get("inbound"):
        direcao = "in"
    else:
        # Fallback: se vem com flag explicit ou direction, usa
        d_raw = (data.get("direction") or data.get("direcao") or "").lower()
        if "in" in d_raw or "incoming" in d_raw:
            direcao = "in"
        elif "out" in d_raw or "outgoing" in d_raw:
            direcao = "out"

    autor_nome = None
    if isinstance(user_obj, dict):
        inner = user_obj.get("user") if isinstance(user_obj.get("user"), dict) else user_obj
        autor_nome = inner.get("name") or inner.get("full_name")
    if not autor_nome and direcao == "in" and isinstance(person_obj, dict):
        autor_nome = person_obj.get("name")

    conteudo = (
        data.get("content") or data.get("text") or data.get("body") or
        data.get("message") or ""
    )
    if isinstance(conteudo, dict):
        conteudo = conteudo.get("text") or conteudo.get("body") or json.dumps(conteudo)[:5000]
    conteudo = str(conteudo)[:10000] if conteudo else None

    midia_tipo = data.get("media_type") or data.get("type_of_message") or "text"
    midia_url = data.get("media_url") or data.get("url")

    enviado_em = _parse_iso(
        data.get("created_at") or data.get("sent_at") or data.get("timestamp")
    )

    return {
        "hablla_message_id": msg_id,
        "hablla_service_id": svc_id,
        "hablla_person_id": person_id,
        "canal": canal,
        "direcao": direcao,
        "autor_user_id": user_id[:64] if user_id else None,
        "autor_nome": str(autor_nome)[:255] if autor_nome else None,
        "conteudo": conteudo,
        "midia_tipo": str(midia_tipo)[:32] if midia_tipo else "text",
        "midia_url": str(midia_url) if midia_url else None,
        "enviado_em": enviado_em,
    }


SQL_INSERT_MSG = text("""
INSERT INTO mensagens_hablla (
    hablla_message_id, hablla_service_id, hablla_person_id, lead_id,
    canal, direcao, autor_user_id, autor_nome,
    conteudo, midia_tipo, midia_url, enviado_em, raw_payload,
    received_at, processed_ok, error_msg
)
VALUES (
    :hablla_message_id, :hablla_service_id, :hablla_person_id, :lead_id,
    :canal, :direcao, :autor_user_id, :autor_nome,
    :conteudo, :midia_tipo, :midia_url, :enviado_em, CAST(:raw_payload AS JSONB),
    NOW(), :processed_ok, :error_msg
)
ON CONFLICT (hablla_message_id) DO NOTHING
RETURNING id
""")


SQL_RESOLVE_LEAD_BY_PERSON = text(
    "SELECT id FROM leads WHERE hablla_person_id = :pid LIMIT 1"
)


@router.get("/health")
async def webhook_health():
    """Health-check pra usar como teste no painel admin do Hablla."""
    return {
        "ok": True,
        "service": "hablla-webhook-receiver",
        "auth_required": bool(os.getenv("HABLLA_WEBHOOK_TOKEN", "")),
    }


@router.post("")
async def receive_hablla_event(
    request: Request,
    x_hablla_token: Optional[str] = Header(default=None),
):
    """Recebe um evento do Hablla. Retorna 200 mesmo em payloads desconhecidos
    pra evitar retry infinito; falhas ficam registradas com processed_ok=false."""
    _check_token(x_hablla_token)

    try:
        payload = await request.json()
    except Exception as e:
        logger.warning("hablla webhook: JSON inválido: %s", e)
        return {"ok": False, "error": "invalid_json"}

    if not isinstance(payload, dict):
        # alguns providers enviam list de eventos; trata cada um
        if isinstance(payload, list):
            results = []
            for ev in payload:
                if isinstance(ev, dict):
                    r = await _process_one(ev)
                    results.append(r)
            return {"ok": True, "processed": len(results), "results": results}
        return {"ok": False, "error": "unexpected_payload_shape"}

    return await _process_one(payload)


async def _process_one(payload: dict) -> dict:
    fields = _extract_message_fields(payload)

    # Resolve lead_id pelo hablla_person_id
    lead_id = None
    if fields["hablla_person_id"]:
        try:
            async with get_session() as session:
                r = await session.execute(
                    SQL_RESOLVE_LEAD_BY_PERSON,
                    {"pid": fields["hablla_person_id"]},
                )
                row = r.first()
                if row:
                    lead_id = int(row[0])
        except Exception as e:
            logger.warning("falha resolvendo lead_id: %s", e)

    error_msg = None
    processed_ok = True
    if not fields["hablla_message_id"]:
        error_msg = "payload sem id de mensagem reconhecivel"
        processed_ok = False
        logger.info("hablla webhook: %s — keys=%s", error_msg, list(payload.keys())[:10])

    try:
        async with get_session() as session:
            async with session.begin():
                result = await session.execute(SQL_INSERT_MSG, {
                    **fields,
                    "lead_id": lead_id,
                    "raw_payload": json.dumps(payload, default=str)[:200000],
                    "processed_ok": processed_ok,
                    "error_msg": error_msg,
                })
                row = result.mappings().first()
                inserted_id = row["id"] if row else None
    except Exception as e:
        logger.exception("hablla webhook: falha persistindo mensagem: %s", e)
        return {"ok": False, "error": str(e)[:200]}

    return {
        "ok": True,
        "inserted": inserted_id is not None,
        "duplicate": inserted_id is None and bool(fields["hablla_message_id"]),
        "lead_id": lead_id,
        "message_id": fields["hablla_message_id"],
    }
