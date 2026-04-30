"""
Webhook receiver para a Event Webhook da SendGrid.

Endpoint:
    POST /webhooks/sendgrid

Recebe um array de eventos JSON. Para cada evento, identifica a
mensagem em mensagens_squad3 pelo external_id (= sg_message_id) e
atualiza timestamps + status conforme o tipo de evento.

Eventos tratados:
    delivered  → delivered_at + status='delivered'
    open       → opened_at    + status='opened'
    click      → clicked_at   + status='clicked'
    bounce     → bounced_at   + status='bounced'
    dropped    → bounced_at   + status='bounced'
    spamreport → bounced_at   + status='bounced'
    deferred   → ignorado (transitório)
    processed  → ignorado (já registramos como 'sent')
    unsubscribe → status='skipped'

Resposta:
    SendGrid exige resposta rápida (preferencialmente < 1s) com status 200,
    senão entra em retry exponencial. Esta rota retorna 200 mesmo se
    parte dos eventos falhar — apenas loga e segue, conforme orientação
    da própria SendGrid.

Autenticação:
    Usa assinatura Ed25519 da SendGrid. Headers esperados:
        X-Twilio-Email-Event-Webhook-Signature
        X-Twilio-Email-Event-Webhook-Timestamp
    Se SENDGRID_WEBHOOK_PUBLIC_KEY não estiver configurada na conta,
    a verificação é pulada (rota fica aberta — usar apenas em ambientes
    de teste com URL não pública).
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from fastapi import APIRouter, Header, HTTPException, Request

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhooks/sendgrid", tags=["webhooks"])


# Singletons preenchidos na inicialização do painel.py
_sendgrid_client: Any = None
_mensagens_repo: Any = None


def configure(*, sendgrid_client: Any, mensagens_repo: Any) -> None:
    """
    Configura as dependências do receiver. Chamar uma vez no startup
    do painel.py após instanciar SendGridClient e MensagensRepository.
    """
    global _sendgrid_client, _mensagens_repo
    _sendgrid_client = sendgrid_client
    _mensagens_repo = mensagens_repo
    logger.info(
        "SendGrid webhook configurado: client=%s repo=%s",
        bool(sendgrid_client), bool(mensagens_repo),
    )


# ---------------------------------------------------------------------------
# Rotas
# ---------------------------------------------------------------------------

@router.get("/health")
async def health() -> dict:
    """Health-check para configurar o webhook na console SendGrid."""
    return {
        "status": "ok",
        "configured": bool(_sendgrid_client and _mensagens_repo),
    }


@router.post("")
async def receive_events(
    request: Request,
    x_twilio_email_event_webhook_signature: Optional[str] = Header(default=None),
    x_twilio_email_event_webhook_timestamp: Optional[str] = Header(default=None),
) -> dict:
    """Recebe um array de eventos da SendGrid."""
    if _sendgrid_client is None or _mensagens_repo is None:
        logger.warning("SendGrid webhook recebido mas client/repo não configurados")
        # Retorna 200 mesmo assim para não disparar retry da SendGrid
        return {"received": 0, "status": "not_configured"}

    raw_body = await request.body()

    # 1. Verifica assinatura (se a conta SendGrid tiver assinatura habilitada)
    if x_twilio_email_event_webhook_signature and x_twilio_email_event_webhook_timestamp:
        ok = _sendgrid_client.verify_webhook_signature(
            signature_b64=x_twilio_email_event_webhook_signature,
            timestamp=x_twilio_email_event_webhook_timestamp,
            request_body=raw_body,
        )
        if not ok:
            logger.warning("SendGrid webhook: assinatura inválida — request rejeitado")
            raise HTTPException(status_code=401, detail="invalid_signature")

    # 2. Parseia eventos
    try:
        raw_events = await request.json()
    except Exception as e:
        logger.warning("SendGrid webhook: JSON inválido: %s", e)
        raise HTTPException(status_code=400, detail="invalid_json")

    if not isinstance(raw_events, list):
        raw_events = [raw_events]

    events = _sendgrid_client.parse_webhook_events(raw_events)

    # 3. Aplica cada evento
    aplicados = 0
    ignorados = 0
    sem_match = 0
    erros = 0

    for ev in events:
        if not ev.sg_message_id:
            ignorados += 1
            continue
        try:
            updated = await _mensagens_repo.aplicar_evento_externo(
                external_id=ev.sg_message_id,
                evento=ev.event,
                ocorrido_em=ev.timestamp,
                razao=ev.reason,
            )
            if updated:
                aplicados += 1
            else:
                # Evento ignorado pela política (ex.: 'processed') OU sem match.
                # Se o evento é mapeável mas não achou linha, registramos como sem_match.
                sem_match += 1
        except Exception as e:
            erros += 1
            logger.error(
                "Falha ao aplicar evento SendGrid msg_id=%s evento=%s: %s",
                ev.sg_message_id, ev.event, e,
            )

    logger.info(
        "SendGrid webhook: total=%d aplicados=%d ignorados=%d sem_match=%d erros=%d",
        len(events), aplicados, ignorados, sem_match, erros,
    )
    return {
        "received": len(events),
        "applied": aplicados,
        "ignored": ignorados,
        "no_match": sem_match,
        "errors": erros,
    }
