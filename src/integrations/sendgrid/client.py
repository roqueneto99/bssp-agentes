"""
Cliente SendGrid v3 — async wrapper sobre POST /v3/mail/send.

Mínimo necessário para o Squad 3 — Sprint 2:
    - Envio individual com from-name/from-email configuráveis
    - sandbox_mode toggle (não dispara em produção, mas valida payload)
    - custom_args para correlacionar com mensagens_squad3 via webhook
    - parser de eventos do webhook (delivered, open, click, bounce, spam, unsubscribe)
    - verificação opcional de assinatura Ed25519 do webhook

Não escopo desta sprint:
    - Listas e contatos (Marketing API) — não usamos
    - Templates dinâmicos — usamos templates inline da Personalização
    - Subusers — não aplicável
    - Categorias e batch sends — pode entrar em S8 (hardening)
"""

from __future__ import annotations

import json
import logging
import os
from base64 import b64decode
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass
class SendGridConfig:
    """Configuração do cliente. Carregada do ambiente em produção."""

    api_key: str = ""
    base_url: str = "https://api.sendgrid.com"
    from_email: str = "no-reply@bssp.com.br"
    from_name: str = "BSSP Centro Educacional"
    reply_to_email: Optional[str] = None
    reply_to_name: Optional[str] = None
    sandbox_mode: bool = True
    webhook_public_key: Optional[str] = None  # base64 Ed25519 pública (opcional)
    timeout_seconds: float = 15.0

    @classmethod
    def from_env(cls) -> "SendGridConfig":
        return cls(
            api_key=os.getenv("SENDGRID_API_KEY", ""),
            base_url=os.getenv("SENDGRID_BASE_URL", "https://api.sendgrid.com"),
            from_email=os.getenv("SENDGRID_FROM_EMAIL", "no-reply@bssp.com.br"),
            from_name=os.getenv("SENDGRID_FROM_NAME", "BSSP Centro Educacional"),
            reply_to_email=os.getenv("SENDGRID_REPLY_TO_EMAIL") or None,
            reply_to_name=os.getenv("SENDGRID_REPLY_TO_NAME") or None,
            sandbox_mode=os.getenv("SENDGRID_SANDBOX_MODE", "true").lower() in ("1", "true", "yes"),
            webhook_public_key=os.getenv("SENDGRID_WEBHOOK_PUBLIC_KEY") or None,
            timeout_seconds=float(os.getenv("SENDGRID_TIMEOUT_S", "15")),
        )

    @property
    def is_configured(self) -> bool:
        return bool(self.api_key)


# ---------------------------------------------------------------------------
# Tipos de retorno
# ---------------------------------------------------------------------------

@dataclass
class SendGridResponse:
    """Resposta de uma chamada a /v3/mail/send."""

    success: bool
    status_code: int
    message_id: Optional[str] = None       # X-Message-Id (header)
    sandbox_mode: bool = False
    raw_body: Optional[str] = None
    error: Optional[str] = None


@dataclass
class SendGridEvent:
    """Evento individual normalizado do webhook."""

    email: str
    event: str                  # 'delivered'|'open'|'click'|'bounce'|'spamreport'|'unsubscribe'|'dropped'|'deferred'|'processed'
    timestamp: datetime
    sg_message_id: Optional[str] = None    # = external_id na nossa tabela
    sg_event_id: Optional[str] = None
    reason: Optional[str] = None           # presente em bounce/dropped
    custom_args: dict = field(default_factory=dict)
    raw: dict = field(default_factory=dict)


class SendGridError(Exception):
    """Erro lançado em falhas operacionais (4xx/5xx) que merecem fallback."""


# ---------------------------------------------------------------------------
# Cliente
# ---------------------------------------------------------------------------

class SendGridClient:
    """Cliente assíncrono para a API v3 do SendGrid."""

    def __init__(self, config: SendGridConfig) -> None:
        self.config = config
        self._http: Optional[httpx.AsyncClient] = None

    @property
    def http(self) -> httpx.AsyncClient:
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(
                base_url=self.config.base_url,
                timeout=self.config.timeout_seconds,
                headers={
                    "Authorization": f"Bearer {self.config.api_key}",
                    "Content-Type": "application/json",
                },
            )
        return self._http

    async def close(self) -> None:
        if self._http and not self._http.is_closed:
            await self._http.aclose()

    # ------------------------------------------------------------------
    # Envio
    # ------------------------------------------------------------------

    async def send_mail(
        self,
        *,
        to_email: str,
        to_name: Optional[str] = None,
        subject: str,
        body_text: str,
        body_html: Optional[str] = None,
        custom_args: Optional[dict[str, str]] = None,
        categories: Optional[list[str]] = None,
        reply_to_email: Optional[str] = None,
        reply_to_name: Optional[str] = None,
    ) -> SendGridResponse:
        """
        Dispara um e-mail individual.

        custom_args fica em cada evento de webhook — usamos para
        correlacionar com mensagens_squad3 (passa email + cadencia + passo).
        """
        if not self.config.is_configured:
            raise SendGridError("SENDGRID_API_KEY não configurada")

        payload = self._build_payload(
            to_email=to_email,
            to_name=to_name,
            subject=subject,
            body_text=body_text,
            body_html=body_html,
            custom_args=custom_args or {},
            categories=categories or [],
            reply_to_email=reply_to_email,
            reply_to_name=reply_to_name,
        )

        try:
            resp = await self.http.post("/v3/mail/send", json=payload)
        except httpx.HTTPError as e:
            logger.error("SendGrid HTTP error: %s", e)
            raise SendGridError(f"http_error:{e}") from e

        message_id = resp.headers.get("X-Message-Id")

        if 200 <= resp.status_code < 300:
            logger.info(
                "SendGrid OK to=%s status=%d message_id=%s sandbox=%s",
                to_email, resp.status_code, message_id, self.config.sandbox_mode,
            )
            return SendGridResponse(
                success=True,
                status_code=resp.status_code,
                message_id=message_id,
                sandbox_mode=self.config.sandbox_mode,
            )

        # 4xx/5xx: erro lógico ou da API
        body = resp.text[:1000]
        logger.warning(
            "SendGrid FAIL to=%s status=%d body=%s",
            to_email, resp.status_code, body,
        )
        return SendGridResponse(
            success=False,
            status_code=resp.status_code,
            message_id=message_id,
            sandbox_mode=self.config.sandbox_mode,
            raw_body=body,
            error=f"sendgrid_{resp.status_code}",
        )

    def _build_payload(
        self,
        *,
        to_email: str,
        to_name: Optional[str],
        subject: str,
        body_text: str,
        body_html: Optional[str],
        custom_args: dict[str, str],
        categories: list[str],
        reply_to_email: Optional[str],
        reply_to_name: Optional[str],
    ) -> dict[str, Any]:
        """Monta o payload conforme o schema v3 da SendGrid."""
        to: dict[str, Any] = {"email": to_email}
        if to_name:
            to["name"] = to_name

        contents: list[dict[str, str]] = [{"type": "text/plain", "value": body_text}]
        if body_html:
            contents.append({"type": "text/html", "value": body_html})

        body: dict[str, Any] = {
            "personalizations": [{
                "to": [to],
                "subject": subject,
                "custom_args": {k: str(v) for k, v in custom_args.items()},
            }],
            "from": {
                "email": self.config.from_email,
                "name": self.config.from_name,
            },
            "content": contents,
        }
        if categories:
            body["categories"] = categories[:10]  # SendGrid limita a 10

        reply_email = reply_to_email or self.config.reply_to_email
        reply_name = reply_to_name or self.config.reply_to_name
        if reply_email:
            body["reply_to"] = {"email": reply_email}
            if reply_name:
                body["reply_to"]["name"] = reply_name

        if self.config.sandbox_mode:
            body["mail_settings"] = {"sandbox_mode": {"enable": True}}

        return body

    # ------------------------------------------------------------------
    # Webhook
    # ------------------------------------------------------------------

    @staticmethod
    def parse_webhook_events(raw_events: list[dict]) -> list[SendGridEvent]:
        """
        Normaliza o array de eventos da SendGrid em SendGridEvent.

        SendGrid envia uma lista de objetos JSON. Campos relevantes:
            - email, event, timestamp (epoch seconds)
            - sg_message_id, sg_event_id
            - reason (em bounce/dropped)
            - custom args adicionais que enviamos no send (squad3_email,
              squad3_cadencia, squad3_passo, etc.)
        """
        events: list[SendGridEvent] = []
        for raw in raw_events:
            ts = raw.get("timestamp", 0)
            try:
                dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
            except Exception:
                dt = datetime.now(tz=timezone.utc)

            # Tudo que não é campo padrão é custom_args
            standard_keys = {
                "email", "event", "timestamp", "sg_message_id", "sg_event_id",
                "smtp-id", "useragent", "ip", "url", "url_offset", "category",
                "asm_group_id", "reason", "status", "type", "tls", "cert_err",
                "send_at",
            }
            custom = {k: v for k, v in raw.items() if k not in standard_keys}

            events.append(SendGridEvent(
                email=raw.get("email", ""),
                event=raw.get("event", "unknown"),
                timestamp=dt,
                sg_message_id=raw.get("sg_message_id"),
                sg_event_id=raw.get("sg_event_id"),
                reason=raw.get("reason"),
                custom_args=custom,
                raw=raw,
            ))
        return events

    def verify_webhook_signature(
        self,
        *,
        signature_b64: str,
        timestamp: str,
        request_body: bytes,
    ) -> bool:
        """
        Verifica a assinatura Ed25519 do webhook.
        Headers enviados pela SendGrid:
            - X-Twilio-Email-Event-Webhook-Signature
            - X-Twilio-Email-Event-Webhook-Timestamp

        Retorna True se a chave pública não estiver configurada
        (assinatura opcional na config — assumimos rede confiável).
        """
        if not self.config.webhook_public_key:
            logger.debug("SendGrid webhook public key não configurada — assinatura não verificada")
            return True

        try:
            from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
            from cryptography.exceptions import InvalidSignature
        except ImportError:
            logger.warning("cryptography não instalada — assinatura webhook não verificada")
            return True

        try:
            public_key_der = b64decode(self.config.webhook_public_key)
            public_key = Ed25519PublicKey.from_public_bytes(public_key_der[-32:])
            signature = b64decode(signature_b64)
            payload = (timestamp + request_body.decode("utf-8")).encode("utf-8")
            public_key.verify(signature, payload)
            return True
        except InvalidSignature:
            logger.warning("SendGrid webhook: assinatura inválida")
            return False
        except Exception as e:
            logger.warning("SendGrid webhook: falha ao verificar assinatura: %s", e)
            return False
