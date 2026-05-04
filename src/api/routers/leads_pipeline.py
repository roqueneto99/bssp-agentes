"""
Endpoints FastAPI pro Kanban — bssp-agentes/src/api/routers/leads_pipeline.py

v3 — usa SQLAlchemy async (padrão do projeto, não asyncpg direto)

Schema real da tabela `leads`:
- id INTEGER (não UUID)
- name (não nome)
- rd_created_at (não criado_em)
- last_conversion_date (não ultima_conversao_em)
- s2_score (DOUBLE PRECISION) — score do Squad 2
- s2_classificacao — classificação do Squad 2 (fonte preferida)
- cf_classificacao — coluna custom (backfill em 03/05/2026)

Endpoints:
  GET   /api/leads/pipeline?periodo=30d       — agrupa por classificação
  PATCH /api/leads/{lead_id}/classificacao    — move lead entre colunas
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Literal, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import text

from src.database.connection import get_session

router = APIRouter(prefix="/api/leads", tags=["leads-pipeline"])
TZ_BRT = timezone(timedelta(hours=-3))


# ---------- Enums e Schemas ----------

class Classificacao(str, Enum):
    COLD = "COLD"
    SAL = "SAL"
    MQL = "MQL"
    SQL = "SQL"
    HANDOFF = "HANDOFF"
    CONVERTIDO = "CONVERTIDO"


class CadenciaInline(BaseModel):
    nome: str
    etapa_atual: int
    total_etapas: int


class LeadCard(BaseModel):
    id: str
    nome: str
    email: str
    iniciais: str = Field(..., min_length=1, max_length=3)
    origem: Literal["landing", "indicacao", "organico", "antigo"]
    origem_label: str
    score: int = Field(..., ge=0, le=100)
    classificacao: Classificacao
    ultima_interacao_em: datetime
    lgpd_conforme: bool
    cadencia: Optional[CadenciaInline] = None
    consultor: Optional[str] = None
    matricula_curso: Optional[str] = None


class PipelineResponse(BaseModel):
    COLD: list[LeadCard] = []
    SAL: list[LeadCard] = []
    MQL: list[LeadCard] = []
    SQL: list[LeadCard] = []
    HANDOFF: list[LeadCard] = []
    CONVERTIDO: list[LeadCard] = []


class MoveClassificacaoBody(BaseModel):
    para: Classificacao
    motivo: Optional[str] = Field(default="manual_drag", max_length=255)


# ---------- SQL ----------

CLASSIFICACAO_NORMALIZE_SQL = """
CASE
    WHEN UPPER(COALESCE(s2_classificacao, cf_classificacao, '')) = 'CLIENTE' THEN 'CONVERTIDO'
    WHEN UPPER(COALESCE(s2_classificacao, cf_classificacao, '')) IN ('SQL','MQL','SAL','COLD','HANDOFF','CONVERTIDO')
        THEN UPPER(COALESCE(s2_classificacao, cf_classificacao))
    ELSE 'COLD'
END
"""

SQL_PIPELINE = f"""
WITH leads_filtrados AS (
    SELECT
        l.id,
        COALESCE(l.name, '(sem nome)') AS nome,
        l.email,
        UPPER(
            LEFT(SPLIT_PART(COALESCE(l.name, ''), ' ', 1), 1) ||
            COALESCE(LEFT(SPLIT_PART(COALESCE(l.name, ''), ' ', 2), 1), '')
        ) AS iniciais,
        COALESCE(l.origem,
            CASE
                WHEN l.lifecycle_stage ILIKE '%landing%' THEN 'landing'
                WHEN l.lifecycle_stage ILIKE '%refer%' OR l.lifecycle_stage ILIKE '%indica%' THEN 'indicacao'
                ELSE 'organico'
            END
        ) AS origem,
        COALESCE(l.origem_label, l.lifecycle_stage, 'orgânico') AS origem_label,
        GREATEST(0, LEAST(100, COALESCE(l.s2_score, 0)::int)) AS score,
        {CLASSIFICACAO_NORMALIZE_SQL} AS classificacao,
        COALESCE(l.ultima_interacao_em, l.last_conversion_date, l.rd_created_at, NOW()) AS ultima_interacao_em,
        COALESCE(l.lgpd_conforme, l.s1_compliance = 'conforme', false) AS lgpd_conforme,
        l.consultor,
        l.matricula_curso
    FROM leads l
    WHERE COALESCE(l.ultima_interacao_em, l.last_conversion_date, l.rd_created_at) >= :desde
       OR l.s2_processado_em >= :desde
)
SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY classificacao
           ORDER BY score DESC, ultima_interacao_em DESC NULLS LAST
       ) AS rn
FROM leads_filtrados
"""


# ---------- GET /api/leads/pipeline ----------

@router.get("/pipeline", response_model=PipelineResponse)
async def get_pipeline(
    periodo: Literal["7d", "30d", "90d", "365d"] = Query(default="30d"),
    limit_por_coluna: int = Query(default=50, ge=10, le=200),
) -> PipelineResponse:
    dias = {"7d": 7, "30d": 30, "90d": 90, "365d": 365}[periodo]
    desde = datetime.now(TZ_BRT) - timedelta(days=dias)

    async with get_session() as session:
        result = await session.execute(text(SQL_PIPELINE), {"desde": desde})
        rows = result.mappings().all()

    response = PipelineResponse()
    for row in rows:
        if row["rn"] > limit_por_coluna:
            continue
        try:
            card = LeadCard(
                id=str(row["id"]),
                nome=row["nome"] or "(sem nome)",
                email=row["email"] or "",
                iniciais=row["iniciais"] or "?",
                origem=row["origem"] or "organico",
                origem_label=row["origem_label"] or "orgânico",
                score=int(row["score"]),
                classificacao=row["classificacao"],
                ultima_interacao_em=row["ultima_interacao_em"],
                lgpd_conforme=bool(row["lgpd_conforme"]),
                cadencia=None,
                consultor=row["consultor"],
                matricula_curso=row["matricula_curso"],
            )
            getattr(response, row["classificacao"]).append(card)
        except Exception:
            continue

    return response


# ---------- PATCH /api/leads/{lead_id}/classificacao ----------

SQL_MOVE = text("""
UPDATE leads
SET
    cf_classificacao = :para,
    classificacao_origem = 'manual',
    classificacao_atualizada_em = :agora
WHERE id = :lead_id
RETURNING id, name, cf_classificacao
""")


@router.patch("/{lead_id}/classificacao", response_model=LeadCard)
async def move_lead_classificacao(
    lead_id: int,
    body: MoveClassificacaoBody,
) -> LeadCard:
    agora = datetime.now(TZ_BRT)

    async with get_session() as session:
        atual = await session.execute(
            text("SELECT cf_classificacao FROM leads WHERE id = :id"),
            {"id": lead_id},
        )
        row = atual.first()
        if not row:
            raise HTTPException(404, "lead não encontrado")

        await session.execute(SQL_MOVE, {"para": body.para.value, "agora": agora, "lead_id": lead_id})

    return await _carregar_lead_card(lead_id)


async def _carregar_lead_card(lead_id: int) -> LeadCard:
    sql = text(f"""
    SELECT
        l.id,
        COALESCE(l.name, '(sem nome)') AS nome,
        l.email,
        UPPER(
            LEFT(SPLIT_PART(COALESCE(l.name, ''), ' ', 1), 1) ||
            COALESCE(LEFT(SPLIT_PART(COALESCE(l.name, ''), ' ', 2), 1), '')
        ) AS iniciais,
        COALESCE(l.origem, 'organico') AS origem,
        COALESCE(l.origem_label, l.lifecycle_stage, 'orgânico') AS origem_label,
        GREATEST(0, LEAST(100, COALESCE(l.s2_score, 0)::int)) AS score,
        {CLASSIFICACAO_NORMALIZE_SQL} AS classificacao,
        COALESCE(l.ultima_interacao_em, l.last_conversion_date, l.rd_created_at, NOW()) AS ultima_interacao_em,
        COALESCE(l.lgpd_conforme, false) AS lgpd_conforme,
        l.consultor, l.matricula_curso
    FROM leads l
    WHERE l.id = :id
    """)
    async with get_session() as session:
        result = await session.execute(sql, {"id": lead_id})
        row = result.mappings().first()
        if not row:
            raise HTTPException(404, "lead não encontrado")
        return LeadCard(
            id=str(row["id"]),
            nome=row["nome"],
            email=row["email"] or "",
            iniciais=row["iniciais"] or "?",
            origem=row["origem"],
            origem_label=row["origem_label"],
            score=int(row["score"]),
            classificacao=row["classificacao"],
            ultima_interacao_em=row["ultima_interacao_em"],
            lgpd_conforme=bool(row["lgpd_conforme"]),
            cadencia=None,
            consultor=row["consultor"],
            matricula_curso=row["matricula_curso"],
        )
