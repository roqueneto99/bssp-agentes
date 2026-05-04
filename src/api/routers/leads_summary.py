"""
Endpoint /api/leads/summary — KPIs do dashboard novo do Pipeline.

Retorna:
- total_leads (no período filtrado)
- em_atendimento (count com hablla_em_atendimento=true)
- whatsapp_ativo (count com canal=whatsapp E última msg < 24h)
- valor_potencial (soma de valor_matricula dos leads SAL+MQL+SQL+HANDOFF)
- matriculas_periodo (count CONVERTIDO no período)
- taxa_conversao (matriculas_periodo / total_leads * 100, em %)
- por_classificacao (count por coluna do kanban)
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Literal, Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel
from sqlalchemy import text

from src.database.connection import get_session

router = APIRouter(prefix="/api/leads", tags=["leads-summary"])
TZ_BRT = timezone(timedelta(hours=-3))


class ClassificacaoCount(BaseModel):
    COLD: int = 0
    SAL: int = 0
    MQL: int = 0
    SQL: int = 0
    HANDOFF: int = 0
    CONVERTIDO: int = 0


class PipelineSummary(BaseModel):
    total_leads: int
    em_atendimento: int
    whatsapp_ativo: int
    valor_potencial: float                # R$ somados de SAL+MQL+SQL+HANDOFF
    matriculas_periodo: int               # count CONVERTIDO no período
    taxa_conversao_pct: float             # 0.0 a 100.0
    ticket_medio: Optional[float] = None  # valor médio de matrícula entre os com valor preenchido
    por_classificacao: ClassificacaoCount
    periodo: str                          # "30d" / "90d" / etc.
    desde: datetime


SQL_SUMMARY = text("""
WITH base AS (
    SELECT
        l.id,
        l.s2_score,
        COALESCE(l.hablla_em_atendimento, false) AS em_atendimento,
        (
            LOWER(COALESCE(l.s3_canal_preferido, '')) = 'whatsapp'
            AND l.s3_ultima_msg_em IS NOT NULL
            AND l.s3_ultima_msg_em >= NOW() - INTERVAL '24 hours'
        ) AS whatsapp_ativo,
        c.valor_matricula_brl,
        CASE
            WHEN UPPER(COALESCE(l.s2_classificacao, l.cf_classificacao, '')) = 'CLIENTE' THEN 'CONVERTIDO'
            WHEN UPPER(COALESCE(l.s2_classificacao, l.cf_classificacao, '')) IN ('SQL','MQL','SAL','COLD','HANDOFF','CONVERTIDO')
                THEN UPPER(COALESCE(l.s2_classificacao, l.cf_classificacao))
            ELSE 'COLD'
        END AS classificacao
    FROM leads l
    LEFT JOIN cursos c
        ON c.hablla_board_id = l.hablla_board_id
       AND c.ativo = true
    WHERE
        COALESCE(l.ultima_interacao_em, l.last_conversion_date, l.rd_created_at) >= :desde
        OR l.s2_processado_em >= :desde
)
SELECT
    COUNT(*) AS total_leads,
    COUNT(*) FILTER (WHERE em_atendimento) AS em_atendimento,
    COUNT(*) FILTER (WHERE whatsapp_ativo) AS whatsapp_ativo,
    COALESCE(SUM(valor_matricula_brl) FILTER (
        WHERE classificacao IN ('SAL', 'MQL', 'SQL', 'HANDOFF')
    ), 0)::float AS valor_potencial,
    COUNT(*) FILTER (WHERE classificacao = 'CONVERTIDO') AS matriculas_periodo,
    AVG(valor_matricula_brl) FILTER (
        WHERE valor_matricula_brl IS NOT NULL
    )::float AS ticket_medio,
    -- contagem por classificação
    COUNT(*) FILTER (WHERE classificacao = 'COLD')       AS cls_cold,
    COUNT(*) FILTER (WHERE classificacao = 'SAL')        AS cls_sal,
    COUNT(*) FILTER (WHERE classificacao = 'MQL')        AS cls_mql,
    COUNT(*) FILTER (WHERE classificacao = 'SQL')        AS cls_sql,
    COUNT(*) FILTER (WHERE classificacao = 'HANDOFF')    AS cls_handoff,
    COUNT(*) FILTER (WHERE classificacao = 'CONVERTIDO') AS cls_convertido
FROM base
""")


@router.get("/summary", response_model=PipelineSummary)
async def get_pipeline_summary(
    periodo: Literal["7d", "30d", "90d", "365d"] = Query(default="30d"),
) -> PipelineSummary:
    dias = {"7d": 7, "30d": 30, "90d": 90, "365d": 365}[periodo]
    desde = datetime.now(TZ_BRT) - timedelta(days=dias)

    async with get_session() as session:
        result = await session.execute(SQL_SUMMARY, {"desde": desde})
        row = result.mappings().first()

    if not row:
        return PipelineSummary(
            total_leads=0, em_atendimento=0, whatsapp_ativo=0,
            valor_potencial=0.0, matriculas_periodo=0, taxa_conversao_pct=0.0,
            por_classificacao=ClassificacaoCount(),
            periodo=periodo, desde=desde,
        )

    total = int(row["total_leads"] or 0)
    matriculas = int(row["matriculas_periodo"] or 0)
    taxa = round((matriculas / total * 100), 2) if total else 0.0

    return PipelineSummary(
        total_leads=total,
        em_atendimento=int(row["em_atendimento"] or 0),
        whatsapp_ativo=int(row["whatsapp_ativo"] or 0),
        valor_potencial=float(row["valor_potencial"] or 0),
        matriculas_periodo=matriculas,
        taxa_conversao_pct=taxa,
        ticket_medio=float(row["ticket_medio"]) if row["ticket_medio"] is not None else None,
        por_classificacao=ClassificacaoCount(
            COLD=int(row["cls_cold"] or 0),
            SAL=int(row["cls_sal"] or 0),
            MQL=int(row["cls_mql"] or 0),
            SQL=int(row["cls_sql"] or 0),
            HANDOFF=int(row["cls_handoff"] or 0),
            CONVERTIDO=int(row["cls_convertido"] or 0),
        ),
        periodo=periodo,
        desde=desde,
    )
