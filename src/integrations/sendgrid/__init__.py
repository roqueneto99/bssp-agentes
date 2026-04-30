"""
Integração com SendGrid (e-mail transacional).

Documentação: https://docs.sendgrid.com/api-reference/mail-send/mail-send

Uso:
    config = SendGridConfig.from_env()
    client = SendGridClient(config)
    response = await client.send_mail(
        to_email="lead@empresa.com.br",
        to_name="João",
        subject="Bem-vindo, João!",
        body_text="...",
        body_html="...",
        custom_args={"squad3_email": "lead@empresa.com.br", "squad3_passo": "0"},
    )
    external_id = response.message_id
"""

from .client import (
    SendGridClient,
    SendGridConfig,
    SendGridResponse,
    SendGridEvent,
    SendGridError,
)

__all__ = [
    "SendGridClient",
    "SendGridConfig",
    "SendGridResponse",
    "SendGridEvent",
    "SendGridError",
]
