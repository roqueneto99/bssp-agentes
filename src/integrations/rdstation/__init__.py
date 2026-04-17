"""
RD Station Marketing API Integration
Cliente Python para integração com a API do RD Station Marketing.
Usado pelos squads de agentes da BSSP.
"""

from .client import RDStationClient
from .auth import RDStationAuth
from .rate_limiter import RateLimiter
from .models import Contact, FunnelStage, WebhookEvent, TagUpdate

__all__ = [
    "RDStationClient",
    "RDStationAuth",
    "RateLimiter",
    "Contact",
    "FunnelStage",
    "WebhookEvent",
    "TagUpdate",
]
