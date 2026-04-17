"""
Rotas da API para interação com os agentes via dashboard.
"""

from __future__ import annotations

import os
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from src.agents.base import LLMProvider
from src.agents.squad1_enrichment import EnrichmentAgent
from src.integrations.rdstation.client import RDStationClient

router = APIRouter(prefix="/api", tags=["agents"])

# Singletons (inicializados no startup da app)
_rdstation: Optional[RDStationClient] = None
_llm: Optional[LLMProvider] = None
_agent: Optional[EnrichmentAgent] = None


def init_agent(rdstation: RDStationClient):
    """Chamado no startup da app para inicializar o agente."""
    global _rdstation, _llm, _agent
    _rdstation = rdstation
    _llm = LLMProvider(
        provider="anthropic",
        api_key=os.getenv("LLM_API_KEY", ""),
        model=os.getenv("LLM_MODEL", "claude-sonnet-4-20250514"),
    )
    _agent = EnrichmentAgent(llm=_llm, rdstation=_rdstation)


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class EnrichRequest(BaseModel):
    email: str


class ContactSearchRequest(BaseModel):
    email: str


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/contact/{email}")
async def get_contact(email: str):
    """Busca um contato no RD Station."""
    if not _rdstation:
        raise HTTPException(500, "RD Station client não inicializado")
    try:
        contact = await _rdstation.get_contact(email=email)
        data = contact.to_api_payload()
        data["uuid"] = contact.uuid
        data["email"] = contact.email
        # Busca funil
        try:
            funnel = await _rdstation.get_funnel_stage(email)
            data["funnel"] = funnel
        except Exception:
            data["funnel"] = {}
        return {"success": True, "contact": data}
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.post("/enrich")
async def enrich_contact(req: EnrichRequest):
    """Executa o Squad 1 (Enriquecimento) para um lead."""
    if not _agent:
        raise HTTPException(500, "Agente não inicializado")
    result = await _agent.run(req.email)
    return {
        "success": result.success,
        "agent": result.agent_name,
        "email": result.contact_email,
        "duration_ms": round(result.duration_ms),
        "data": result.data,
        "error": result.error,
    }


@router.get("/contact/{email}/events")
async def get_contact_events(email: str):
    """Busca histórico de eventos (conversões e oportunidades) do lead."""
    if not _rdstation:
        raise HTTPException(500, "RD Station client não inicializado")
    try:
        # Primeiro busca o contato para obter o UUID (endpoint exige UUID)
        contact = await _rdstation.get_contact(email=email)
        if not contact.uuid:
            return {"success": False, "error": "Contato sem UUID"}

        events = await _rdstation.get_contact_all_events(contact.uuid)

        # Enriquece com dados do funil (úteis para análise de engajamento)
        try:
            funnel = await _rdstation.get_funnel_stage(email)
        except Exception:
            funnel = {}

        events["funnel"] = funnel
        events["tags"] = contact.tags or []

        return {"success": True, "events": events}
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.get("/segmentations")
async def list_segmentations():
    """Lista segmentações do RD Station."""
    if not _rdstation:
        raise HTTPException(500, "RD Station client não inicializado")
    try:
        segs = await _rdstation.list_segmentations()
        return {"success": True, "segmentations": segs}
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.get("/fields")
async def list_fields():
    """Lista campos personalizados."""
    if not _rdstation:
        raise HTTPException(500, "RD Station client não inicializado")
    try:
        fields = await _rdstation.list_custom_fields()
        return {"success": True, "fields": fields}
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.get("/leads")
async def list_leads(
    page: int = 1,
    page_size: int = 50,
    segmentation_id: int = 0,
    search: str = "",
    order: str = "last_conversion_date:desc",
):
    """
    Lista leads de uma segmentação com filtros.

    Args:
        page: Página (começa em 1)
        page_size: Registros por página (max 125)
        segmentation_id: ID da segmentação (0 = busca "Todos")
        search: Busca por nome (server-side, via API RD)
        order: Ordenação (ex: "last_conversion_date:desc", "name:asc")
    """
    if not _rdstation:
        raise HTTPException(500, "RD Station client não inicializado")
    try:
        seg_id = segmentation_id
        segmentations = []
        if seg_id == 0:
            segmentations = await _rdstation.list_segmentations()
            if segmentations:
                for s in segmentations:
                    name = (s.get("name") or "").lower()
                    if "todos" in name or "all" in name:
                        seg_id = s.get("id", 0)
                        break
                if seg_id == 0:
                    seg_id = segmentations[0].get("id", 0)

        if seg_id == 0:
            return {"success": False, "error": "Nenhuma segmentação encontrada"}

        data = await _rdstation.get_segmentation_contacts(
            seg_id,
            page=page,
            page_size=min(page_size, 125),
            order=order,
            search=search or None,
        )

        # Normaliza resposta — extrai headers de paginação se disponíveis
        contacts = []
        total = 0
        if isinstance(data, dict):
            contacts = data.get("contacts", [])
            total = data.get("total", len(contacts))
        elif isinstance(data, list):
            contacts = data
            total = len(contacts)

        return {
            "success": True,
            "contacts": contacts,
            "page": page,
            "page_size": page_size,
            "total": total,
            "segmentation_id": seg_id,
            "segmentations": segmentations if segmentation_id == 0 else [],
        }
    except Exception as e:
        return {"success": False, "error": str(e)}
