"""
Queries do banco de dados para o painel.

Substitui as chamadas diretas à API do RD Station
por consultas ao PostgreSQL local — instantâneas e
sem rate limits.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from sqlalchemy import select, func, desc, or_, and_, cast, String, text
from sqlalchemy.ext.asyncio import AsyncSession

from .models import Lead, Execucao
from .connection import get_session

logger = logging.getLogger("db.queries")


async def count_leads() -> int:
    """Total de leads no banco."""
    async with get_session() as session:
        result = await session.execute(select(func.count(Lead.id)))
        return result.scalar() or 0


async def list_leads(
    page: int = 1,
    page_size: int = 50,
    search: str = "",
    date_from: str = "",
    date_to: str = "",
    date_field: str = "rd_created_at",
    temperatura: str = "",
    classificacao: str = "",
    order_by: str = "last_conversion_date",
    order_dir: str = "desc",
) -> dict:
    """
    Lista leads com paginação, filtros e busca.

    Retorna formato compatível com o frontend existente.
    """
    async with get_session() as session:
        # Base query
        query = select(Lead)
        count_query = select(func.count(Lead.id))

        # Filtros
        conditions = []

        if search:
            search_like = f"%{search.lower()}%"
            conditions.append(
                or_(
                    func.lower(Lead.email).like(search_like),
                    func.lower(Lead.name).like(search_like),
                )
            )

        # Datas: convertem string "YYYY-MM-DD" para datetime antes de comparar.
        # Sem isso o Postgres rejeita: "operator does not exist:
        # timestamp with time zone >= character varying".
        def _parse_date(s: str, end_of_day: bool = False) -> Optional[datetime]:
            try:
                # Aceita "YYYY-MM-DD" ou ISO completo
                s = s.strip()
                if not s:
                    return None
                if "T" in s:
                    return datetime.fromisoformat(s.replace("Z", "+00:00"))
                if end_of_day:
                    return datetime.fromisoformat(s + "T23:59:59+00:00")
                return datetime.fromisoformat(s + "T00:00:00+00:00")
            except Exception:
                return None

        if date_from:
            df = _parse_date(date_from)
            if df is not None:
                col = getattr(Lead, date_field, Lead.rd_created_at)
                conditions.append(col >= df)

        if date_to:
            dt = _parse_date(date_to, end_of_day=True)
            if dt is not None:
                col = getattr(Lead, date_field, Lead.rd_created_at)
                conditions.append(col <= dt)

        if temperatura:
            conditions.append(Lead.s1_temperatura == temperatura)

        if classificacao:
            conditions.append(Lead.s2_classificacao == classificacao)

        if conditions:
            query = query.where(and_(*conditions))
            count_query = count_query.where(and_(*conditions))

        # Total
        total_result = await session.execute(count_query)
        total = total_result.scalar() or 0

        # Ordenação
        order_col = getattr(Lead, order_by, Lead.last_conversion_date)
        if order_dir == "desc":
            query = query.order_by(desc(order_col))
        else:
            query = query.order_by(order_col)

        # Paginação
        offset = (page - 1) * page_size
        query = query.offset(offset).limit(page_size)

        result = await session.execute(query)
        leads = result.scalars().all()

        # Converter para formato da API (compatível com frontend)
        contacts = [_lead_to_contact(lead) for lead in leads]

        return {
            "contacts": contacts,
            "page": page,
            "page_size": page_size,
            "received": len(contacts),
            "total": total,
            "has_next": total > offset + page_size,
            "has_prev": page > 1,
            "mode": "database",
        }


def _lead_to_contact(lead: Lead) -> dict:
    """Converte Lead ORM → dict compatível com o frontend."""
    contact = {
        "uuid": lead.uuid,
        "email": lead.email,
        "name": lead.name,
        "personal_phone": lead.personal_phone,
        "mobile_phone": lead.mobile_phone,
        "job_title": lead.job_title,
        "company_name": lead.company_name,
        "city": lead.city,
        "state": lead.state,
        "country": lead.country,
        "linkedin": lead.linkedin,
        "website": lead.website,
        "created_at": lead.rd_created_at.isoformat() if lead.rd_created_at else None,
        "last_conversion_date": lead.last_conversion_date.isoformat() if lead.last_conversion_date else None,
        "first_conversion_date": lead.first_conversion_date.isoformat() if lead.first_conversion_date else None,
        "tags": lead.tags or [],
        "legal_bases": lead.legal_bases or [],
        "lifecycle_stage": lead.lifecycle_stage,
        "fit_score": lead.fit_score,
        "interest_score": lead.interest_score,
    }

    # Enriquecer com resultados dos squads (se existirem)
    if lead.s1_processado_em:
        contact["_last_exec"] = {
            "resumo": {
                "temperatura": lead.s1_temperatura,
                "prioridade": lead.s1_prioridade,
                "area": lead.s1_area_principal,
                "compliance": lead.s1_compliance,
                "pode_seguir": lead.s1_pode_seguir_squad2,
                "duplicados": lead.s1_duplicados,
            },
        }
        if lead.s2_processado_em:
            contact["_last_exec"]["resumo_squad2"] = {
                "score_total": lead.s2_score,
                "classificacao": lead.s2_classificacao,
                "rota": lead.s2_rota,
                "acoes_recomendadas": lead.s2_acoes or [],
                "dimensoes": lead.s2_dimensoes or {},
                "briefing_comercial": lead.s2_briefing,
                "tags_aplicadas": lead.s2_tags or [],
                "pode_seguir_squad3": lead.s2_pode_seguir_squad3,
            }

    contact["_squad1_running"] = False

    return contact


async def get_lead_detail(email: str) -> Optional[dict]:
    """Busca detalhes completos de um lead pelo email."""
    async with get_session() as session:
        result = await session.execute(
            select(Lead).where(Lead.email == email.lower().strip())
        )
        lead = result.scalar_one_or_none()
        if not lead:
            return None

        contact = _lead_to_contact(lead)

        # Buscar histórico de execuções
        exec_result = await session.execute(
            select(Execucao)
            .where(Execucao.email == email.lower().strip())
            .order_by(desc(Execucao.timestamp))
            .limit(20)
        )
        execs = exec_result.scalars().all()

        hist = []
        for ex in execs:
            hist.append({
                "email": ex.email,
                "tipo": ex.tipo,
                "timestamp": ex.timestamp.isoformat() if ex.timestamp else None,
                "total_ms": ex.total_ms,
                "agentes": ex.agentes or {},
                "resumo_squad1": ex.resumo_squad1 or {},
                "resumo_squad2": ex.resumo_squad2 or {},
                "temperatura": ex.temperatura,
                "score": ex.score,
                "classificacao": ex.classificacao,
                "success": ex.success,
            })

        return {"contact": contact, "execucoes": hist}


async def save_execution(resultado: dict) -> None:
    """
    Salva resultado de execução no banco e atualiza o lead.

    Chamado após pipeline.process_new_lead() ou squad1.execute().
    """
    email = (resultado.get("email") or "").lower().strip()
    if not email:
        return

    async with get_session() as session:
        # 1. Inserir execução
        exec_row = Execucao(
            email=email,
            tipo=resultado.get("tipo", "squad1"),
            total_ms=resultado.get("total_ms"),
            agentes=resultado.get("agentes"),
            resumo_squad1=resultado.get("resumo_squad1") or resultado.get("resumo"),
            resumo_squad2=resultado.get("resumo_squad2"),
            temperatura=(resultado.get("resumo_squad1") or resultado.get("resumo", {})).get("temperatura"),
            score=(resultado.get("resumo_squad2") or {}).get("score_total"),
            classificacao=(resultado.get("resumo_squad2") or {}).get("classificacao"),
            success=True,
        )
        session.add(exec_row)

        # 2. Atualizar lead com resultados
        lead_result = await session.execute(
            select(Lead).where(Lead.email == email)
        )
        lead = lead_result.scalar_one_or_none()

        if lead:
            resumo_s1 = resultado.get("resumo_squad1") or resultado.get("resumo", {})
            if resumo_s1:
                lead.s1_temperatura = resumo_s1.get("temperatura")
                lead.s1_prioridade = resumo_s1.get("prioridade")
                lead.s1_area_principal = resumo_s1.get("area")
                lead.s1_compliance = resumo_s1.get("compliance")
                lead.s1_duplicados = resumo_s1.get("duplicados", 0)
                lead.s1_pode_seguir_squad2 = resumo_s1.get("pode_seguir", False)
                lead.s1_processado_em = datetime.utcnow()

            resumo_s2 = resultado.get("resumo_squad2", {})
            if resumo_s2 and resumo_s2.get("classificacao"):
                lead.s2_score = resumo_s2.get("score_total")
                lead.s2_classificacao = resumo_s2.get("classificacao")
                lead.s2_rota = resumo_s2.get("rota")
                lead.s2_acoes = resumo_s2.get("acoes_recomendadas")
                lead.s2_dimensoes = resumo_s2.get("dimensoes")
                lead.s2_briefing = resumo_s2.get("briefing_comercial")
                lead.s2_tags = resumo_s2.get("tags_aplicadas")
                lead.s2_pode_seguir_squad3 = resumo_s2.get("pode_seguir_squad3", False)
                lead.s2_processado_em = datetime.utcnow()


async def get_stats() -> dict:
    """Estatísticas gerais para o painel."""
    async with get_session() as session:
        total = (await session.execute(select(func.count(Lead.id)))).scalar() or 0

        # Por temperatura Squad 1
        temp_result = await session.execute(
            select(Lead.s1_temperatura, func.count(Lead.id))
            .where(Lead.s1_temperatura.isnot(None))
            .group_by(Lead.s1_temperatura)
        )
        temperaturas = {row[0]: row[1] for row in temp_result}

        # Por classificação Squad 2
        class_result = await session.execute(
            select(Lead.s2_classificacao, func.count(Lead.id))
            .where(Lead.s2_classificacao.isnot(None))
            .group_by(Lead.s2_classificacao)
        )
        classificacoes = {row[0]: row[1] for row in class_result}

        # Processados
        s1_count = (await session.execute(
            select(func.count(Lead.id)).where(Lead.s1_processado_em.isnot(None))
        )).scalar() or 0

        s2_count = (await session.execute(
            select(func.count(Lead.id)).where(Lead.s2_processado_em.isnot(None))
        )).scalar() or 0

        # Último sync
        sync_result = await session.execute(
            text("SELECT finished_at, total_contacts, status FROM sync_log ORDER BY id DESC LIMIT 1")
        )
        last_sync = sync_result.first()

        return {
            "total_leads": total,
            "temperaturas": temperaturas,
            "classificacoes": classificacoes,
            "processados_s1": s1_count,
            "processados_s2": s2_count,
            "last_sync": {
                "finished_at": last_sync[0].isoformat() if last_sync and last_sync[0] else None,
                "total_contacts": last_sync[1] if last_sync else 0,
                "status": last_sync[2] if last_sync else None,
            } if last_sync else None,
        }
