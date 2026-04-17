"""
Modelos de dados para a integração com RD Station.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class FunnelStageEnum(str, Enum):
    LEAD = "Lead"
    QUALIFIED_LEAD = "Lead Qualificado"
    CLIENT = "Cliente"


class LegalBaseCategory(str, Enum):
    COMMUNICATIONS = "communications"


class LegalBaseType(str, Enum):
    PRE_EXISTENT_CONTRACT = "pre_existent_contract"
    CONSENT = "consent"
    LEGITIMATE_INTEREST = "legitimate_interest"
    JUDICIAL_PROCESS = "judicial_process"
    VITAL_INTEREST = "vital_interest"
    PUBLIC_INTEREST = "public_interest"


class LegalBaseStatus(str, Enum):
    GRANTED = "granted"
    DECLINED = "declined"


class WebhookEntityType(str, Enum):
    CONVERTED = "WEBHOOK.CONVERTED"
    MARKED_OPPORTUNITY = "WEBHOOK.MARKED_OPPORTUNITY"
    CRM_DEAL_CREATED = "crm_deal_created"
    CRM_DEAL_UPDATED = "crm_deal_updated"
    CRM_DEAL_DELETED = "crm_deal_deleted"


class ContactIdentifier(str, Enum):
    UUID = "uuid"
    EMAIL = "email"


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class LegalBase:
    category: LegalBaseCategory
    type: LegalBaseType
    status: LegalBaseStatus

    def to_dict(self) -> dict:
        return {
            "category": self.category.value,
            "type": self.type.value,
            "status": self.status.value,
        }


@dataclass
class Contact:
    """Representa um contato do RD Station."""
    email: str
    uuid: Optional[str] = None
    name: Optional[str] = None
    job_title: Optional[str] = None
    birthdate: Optional[str] = None
    bio: Optional[str] = None
    website: Optional[str] = None
    personal_phone: Optional[str] = None
    mobile_phone: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    twitter: Optional[str] = None
    facebook: Optional[str] = None
    linkedin: Optional[str] = None
    tags: list[str] = field(default_factory=list)
    extra_emails: list[str] = field(default_factory=list)
    legal_bases: list[LegalBase] = field(default_factory=list)
    # Campos personalizados dos agentes BSSP
    cf_score_ia: Optional[float] = None
    cf_temperatura: Optional[str] = None          # frio / morno / quente
    cf_motivo_interesse: Optional[str] = None      # curso ou área
    cf_ultima_interacao_agente: Optional[str] = None
    cf_segmento_ia: Optional[str] = None           # cluster comportamental

    def to_api_payload(self, *, exclude_email: bool = False) -> dict:
        """Converte para payload da API, ignorando campos None."""
        standard_fields = [
            "name", "job_title", "birthdate", "bio", "website",
            "personal_phone", "mobile_phone", "city", "state", "country",
            "twitter", "facebook", "linkedin",
        ]
        payload: dict = {}
        if not exclude_email and self.email:
            payload["email"] = self.email
        for f in standard_fields:
            val = getattr(self, f, None)
            if val is not None:
                payload[f] = val
        if self.tags:
            payload["tags"] = self.tags
        if self.legal_bases:
            payload["legal_bases"] = [lb.to_dict() for lb in self.legal_bases]
        # Campos customizados (prefixo cf_)
        custom = {}
        for attr in ("cf_score_ia", "cf_temperatura", "cf_motivo_interesse",
                      "cf_ultima_interacao_agente", "cf_segmento_ia"):
            val = getattr(self, attr, None)
            if val is not None:
                custom[attr] = val
        if custom:
            payload.update(custom)
        return payload

    @classmethod
    def from_api_response(cls, data: dict) -> "Contact":
        """Cria Contact a partir da resposta da API."""
        legal_bases = []
        for lb in data.get("legal_bases", []):
            try:
                legal_bases.append(LegalBase(
                    category=LegalBaseCategory(lb["category"]),
                    type=LegalBaseType(lb["type"]),
                    status=LegalBaseStatus(lb["status"]),
                ))
            except (KeyError, ValueError):
                continue
        return cls(
            uuid=data.get("uuid"),
            email=data.get("email", ""),
            name=data.get("name"),
            job_title=data.get("job_title"),
            birthdate=data.get("birthdate"),
            bio=data.get("bio"),
            website=data.get("website"),
            personal_phone=data.get("personal_phone"),
            mobile_phone=data.get("mobile_phone"),
            city=data.get("city"),
            state=data.get("state"),
            country=data.get("country"),
            twitter=data.get("twitter"),
            facebook=data.get("facebook"),
            linkedin=data.get("linkedin"),
            tags=data.get("tags", []),
            extra_emails=data.get("extra_emails", []),
            legal_bases=legal_bases,
            cf_score_ia=data.get("cf_score_ia"),
            cf_temperatura=data.get("cf_temperatura"),
            cf_motivo_interesse=data.get("cf_motivo_interesse"),
            cf_ultima_interacao_agente=data.get("cf_ultima_interacao_agente"),
            cf_segmento_ia=data.get("cf_segmento_ia"),
        )


@dataclass
class FunnelStage:
    """Estágio do funil de um contato."""
    contact_email: str
    lifecycle_stage: FunnelStageEnum
    opportunity: bool = False
    contact_owner_email: Optional[str] = None
    fit_score: Optional[float] = None       # score de Perfil
    interest_score: Optional[float] = None  # score de Interesse


@dataclass
class TagUpdate:
    """Operação de adição de tags a um contato."""
    contact_identifier: ContactIdentifier
    contact_value: str
    tags: list[str]


@dataclass
class WebhookEvent:
    """Evento recebido via webhook do RD Station."""
    entity_type: WebhookEntityType
    event_timestamp: datetime
    contact_email: str
    contact_data: dict = field(default_factory=dict)
    conversion_identifier: Optional[str] = None  # nome do formulário/evento
    raw_payload: dict = field(default_factory=dict)

    @classmethod
    def from_webhook_payload(cls, payload: dict) -> "WebhookEvent":
        """Parse do JSON recebido no webhook."""
        leads = payload.get("leads", [{}])
        lead = leads[0] if leads else {}
        return cls(
            entity_type=WebhookEntityType(
                payload.get("entity_type", "WEBHOOK.CONVERTED")
            ),
            event_timestamp=datetime.fromisoformat(
                payload.get("event_timestamp", datetime.utcnow().isoformat())
            ),
            contact_email=lead.get("email", ""),
            contact_data=lead,
            conversion_identifier=lead.get("conversion_identifier"),
            raw_payload=payload,
        )
