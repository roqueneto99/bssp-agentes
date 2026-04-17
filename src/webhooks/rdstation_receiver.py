"""
Webhook Receiver para eventos do RD Station Marketing.

Endpoints FastAPI que recebem os webhooks de:
- WEBHOOK.CONVERTED — lead converteu (formulário, landing page, etc.)
- WEBHOOK.MARKED_OPPORTUNITY — lead marcado como oportunidade

Fluxo:
1. Recebe POST do RD Station
2. Valida autenticação (header customizado)
3. Parseia payload
4. Publica evento na fila (SQS/Redis) para processamento assíncrono
5. Retorna 200 imediatamente (RD Station exige resposta rápida)

Os squads de agentes consomem da fila, não deste endpoint.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from pydantic import BaseModel, Field

from src.integrations.rdstation.models import WebhookEntityType, WebhookEvent

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhooks/rdstation", tags=["webhooks"])


# ---------------------------------------------------------------------------
# Config (em produção, vem de variáveis de ambiente)
# ---------------------------------------------------------------------------

class WebhookConfig:
    """Configuração de autenticação do webhook."""
    # Header e chave definidos ao criar o webhook no RD Station
    AUTH_HEADER: str = "X-RDStation-Webhook-Secret"
    AUTH_KEY: str = ""  # Definir via env var RDSTATION_WEBHOOK_SECRET

    @classmethod
    def load_from_env(cls) -> None:
        import os
        cls.AUTH_KEY = os.getenv("RDSTATION_WEBHOOK_SECRET", "")


# ---------------------------------------------------------------------------
# Modelos de request
# ---------------------------------------------------------------------------

class LeadPayload(BaseModel):
    """Dados de um lead no payload do webhook."""
    email: str = ""
    uuid: Optional[str] = None
    name: Optional[str] = None
    job_title: Optional[str] = None
    company: Optional[str] = None  # Campo empresa — só vem no webhook!
    personal_phone: Optional[str] = None
    mobile_phone: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    tags: list[str] = Field(default_factory=list)
    conversion_identifier: Optional[str] = None
    # Campos personalizados dos agentes
    cf_score_ia: Optional[float] = None
    cf_temperatura: Optional[str] = None
    cf_motivo_interesse: Optional[str] = None

    class Config:
        extra = "allow"  # Aceita campos extras sem erro


class WebhookPayload(BaseModel):
    """Payload completo do webhook do RD Station."""
    entity_type: str = "WEBHOOK.CONVERTED"
    event_timestamp: Optional[str] = None
    leads: list[LeadPayload] = Field(default_factory=list)

    class Config:
        extra = "allow"


# ---------------------------------------------------------------------------
# Dependências
# ---------------------------------------------------------------------------

async def verify_webhook_auth(request: Request) -> None:
    """
    Verifica o header de autenticação do webhook.
    Se AUTH_KEY estiver vazio, aceita qualquer request (dev mode).
    """
    if not WebhookConfig.AUTH_KEY:
        logger.warning("Webhook auth desabilitada (AUTH_KEY vazia). Apenas para dev!")
        return

    received_key = request.headers.get(WebhookConfig.AUTH_HEADER, "")
    if received_key != WebhookConfig.AUTH_KEY:
        logger.warning(
            "Webhook auth falhou. Header '%s' com valor inválido.",
            WebhookConfig.AUTH_HEADER,
        )
        raise HTTPException(status_code=401, detail="Unauthorized")


# ---------------------------------------------------------------------------
# Event dispatcher (interface para a fila)
# ---------------------------------------------------------------------------

class EventDispatcher:
    """
    Interface para despachar eventos para processamento assíncrono.

    Em produção: publica em SQS ou Redis.
    Em dev: processa inline ou loga.
    """

    async def dispatch_conversion(self, event: WebhookEvent) -> None:
        """Lead converteu — aciona Squad 1 (Captura/Enriquecimento)."""
        logger.info(
            "CONVERSION: email=%s conversion=%s timestamp=%s",
            event.contact_email,
            event.conversion_identifier,
            event.event_timestamp,
        )
        # TODO: Publicar na fila SQS para o Squad 1
        # await sqs_client.send_message(
        #     queue_url=CONVERSION_QUEUE_URL,
        #     message_body=json.dumps(event.raw_payload),
        # )

    async def dispatch_opportunity(self, event: WebhookEvent) -> None:
        """Lead marcado como oportunidade — aciona Squad 4 (Handoff Comercial)."""
        logger.info(
            "OPPORTUNITY: email=%s timestamp=%s",
            event.contact_email,
            event.event_timestamp,
        )
        # TODO: Publicar na fila SQS para o Squad 4
        # await sqs_client.send_message(
        #     queue_url=OPPORTUNITY_QUEUE_URL,
        #     message_body=json.dumps(event.raw_payload),
        # )


# Singleton (substituir por dependency injection em produção)
_dispatcher = EventDispatcher()


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/converted", status_code=200)
async def handle_conversion(
    payload: WebhookPayload,
    _auth: None = Depends(verify_webhook_auth),
) -> dict[str, str]:
    """
    Recebe webhook de conversão do RD Station.

    Chamado quando um lead preenche formulário, baixa material,
    se inscreve em webinar, etc.

    Aciona o pipeline:
    Squad 1 (Captura) → Squad 2 (Qualificação) → Squad 3 (Comunicação)
    """
    try:
        event = WebhookEvent.from_webhook_payload(payload.model_dump())
        logger.info(
            "Webhook CONVERTED recebido: email=%s, conversion=%s, leads_count=%d",
            event.contact_email,
            event.conversion_identifier,
            len(payload.leads),
        )
        await _dispatcher.dispatch_conversion(event)
        return {"status": "ok", "event": "conversion_queued"}

    except Exception as e:
        # Sempre retorna 200 pro RD Station não ficar retentando
        logger.exception("Erro ao processar webhook de conversão: %s", e)
        return {"status": "error", "message": str(e)}


@router.post("/opportunity", status_code=200)
async def handle_opportunity(
    payload: WebhookPayload,
    _auth: None = Depends(verify_webhook_auth),
) -> dict[str, str]:
    """
    Recebe webhook de marcação de oportunidade do RD Station.

    Chamado quando um lead é marcado como oportunidade
    (por automação do RD, pelo scoring dos agentes, ou manualmente).

    Aciona diretamente o Squad 4 (Handoff Comercial).
    """
    try:
        event = WebhookEvent.from_webhook_payload(payload.model_dump())
        logger.info(
            "Webhook OPPORTUNITY recebido: email=%s, leads_count=%d",
            event.contact_email,
            len(payload.leads),
        )
        await _dispatcher.dispatch_opportunity(event)
        return {"status": "ok", "event": "opportunity_queued"}

    except Exception as e:
        logger.exception("Erro ao processar webhook de oportunidade: %s", e)
        return {"status": "error", "message": str(e)}


@router.get("/health")
async def health_check() -> dict[str, str]:
    """Health check para o RD Station e monitoramento."""
    return {"status": "healthy", "service": "rdstation-webhook-receiver"}
