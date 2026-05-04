"""
Endpoints FastAPI pro Kanban — v5

Adiciona filtros ao GET /api/leads/pipeline:
- q (search nome/email)
- origem, temperatura, prioridade (csv)
- score_min, score_max
- lgpd_only (apenas conformes)
- com_cadencia (Squad 3 ativo)
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


class LeadDetail(BaseModel):
    id: str
    nome: str
    email: str
    iniciais: str
    personal_phone: Optional[str] = None
    mobile_phone: Optional[str] = None
    job_title: Optional[str] = None
    company_name: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    linkedin: Optional[str] = None
    website: Optional[str] = None
    classificacao: Classificacao
    classificacao_origem: Optional[str] = None
    score: int
    fit_score: Optional[str] = None
    interest_score: Optional[str] = None
    origem: str
    origem_label: str
    lgpd_conforme: bool
    s1_temperatura: Optional[str] = None
    s1_prioridade: Optional[str] = None
    s1_area_principal: Optional[str] = None
    s1_compliance: Optional[str] = None
    s1_processado_em: Optional[datetime] = None
    s2_briefing: Optional[str] = None
    s2_rota: Optional[str] = None
    s2_acoes: Optional[list] = None
    s2_tags: Optional[list] = None
    s2_dimensoes: Optional[dict] = None
    s2_processado_em: Optional[datetime] = None
    s3_estagio: Optional[str] = None
    s3_cadencia_atual: Optional[str] = None
    s3_canal_preferido: Optional[str] = None
    s3_msgs_enviadas: Optional[int] = None
    s3_ultima_msg_em: Optional[datetime] = None
    s3_ultima_resposta_em: Optional[datetime] = None
    rd_created_at: Optional[datetime] = None
    last_conversion_date: Optional[datetime] = None
    first_conversion_date: Optional[datetime] = None
    lifecycle_stage: Optional[str] = None
    tags: Optional[list] = None
    consultor: Optional[str] = None
    matricula_curso: Optional[str] = None
    ultima_interacao_em: datetime


# ---------- SQL ----------

CLASSIFICACAO_NORMALIZE_SQL = """
CASE
    WHEN UPPER(COALESCE(s2_classificacao, cf_classificacao, '')) = 'CLIENTE' THEN 'CONVERTIDO'
    WHEN UPPER(COALESCE(s2_classificacao, cf_classificacao, '')) IN ('SQL','MQL','SAL','COLD','HANDOFF','CONVERTIDO')
        THEN UPPER(COALESCE(s2_classificacao, cf_classificacao))
    ELSE 'COLD'
END
"""

# Base de filtragem por período (sempre aplica). Outros filtros são append.
SQL_PIPELINE_BASE = f"""
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
        l.s1_temperatura,
        l.s1_prioridade,
        l.s3_cadencia_atual,
        l.consultor,
        l.matricula_curso
    FROM leads l
    WHERE
        (COALESCE(l.ultima_interacao_em, l.last_conversion_date, l.rd_created_at) >= :desde
         OR l.s2_processado_em >= :desde)
        {{EXTRA_FILTERS}}
)
SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY classificacao
           ORDER BY score DESC, ultima_interacao_em DESC NULLS LAST
       ) AS rn
FROM leads_filtrados
"""

SQL_DETAIL = text(f"""
SELECT
    l.id,
    COALESCE(l.name, '(sem nome)') AS nome,
    l.email,
    UPPER(
        LEFT(SPLIT_PART(COALESCE(l.name, ''), ' ', 1), 1) ||
        COALESCE(LEFT(SPLIT_PART(COALESCE(l.name, ''), ' ', 2), 1), '')
    ) AS iniciais,
    l.personal_phone, l.mobile_phone,
    l.job_title, l.company_name,
    l.city, l.state, l.country,
    l.linkedin, l.website,
    {CLASSIFICACAO_NORMALIZE_SQL} AS classificacao,
    l.classificacao_origem,
    GREATEST(0, LEAST(100, COALESCE(l.s2_score, 0)::int)) AS score,
    l.fit_score, l.interest_score,
    COALESCE(l.origem, 'organico') AS origem,
    COALESCE(l.origem_label, l.lifecycle_stage, 'orgânico') AS origem_label,
    COALESCE(l.lgpd_conforme, l.s1_compliance = 'conforme', false) AS lgpd_conforme,
    l.s1_temperatura, l.s1_prioridade, l.s1_area_principal, l.s1_compliance, l.s1_processado_em,
    l.s2_briefing, l.s2_rota, l.s2_acoes, l.s2_tags, l.s2_dimensoes, l.s2_processado_em,
    l.s3_estagio, l.s3_cadencia_atual, l.s3_canal_preferido,
    l.s3_msgs_enviadas, l.s3_ultima_msg_em, l.s3_ultima_resposta_em,
    l.rd_created_at, l.last_conversion_date, l.first_conversion_date,
    l.lifecycle_stage, l.tags,
    l.consultor, l.matricula_curso,
    COALESCE(l.ultima_interacao_em, l.last_conversion_date, l.rd_created_at, NOW()) AS ultima_interacao_em
FROM leads l
WHERE l.id = :id
""")


def _csv_to_list(s: Optional[str]) -> Optional[list[str]]:
    if not s:
        return None
    items = [x.strip().lower() for x in s.split(",") if x.strip()]
    return items or None


def _build_pipeline_filters(
    q: Optional[str],
    origem: Optional[list[str]],
    temperatura: Optional[list[str]],
    prioridade: Optional[list[str]],
    score_min: Optional[int],
    score_max: Optional[int],
    lgpd_only: bool,
    com_cadencia: Optional[bool],
) -> tuple[str, dict]:
    """Constrói cláusulas WHERE adicionais e dict de parâmetros, à prova de SQL injection
    (tudo via named params do SQLAlchemy)."""
    clauses: list[str] = []
    params: dict = {}

    if q:
        clauses.append("(l.name ILIKE :q OR l.email ILIKE :q)")
        params["q"] = f"%{q}%"

    if origem:
        # ANY com array literal
        clauses.append("LOWER(COALESCE(l.origem, 'organico')) = ANY(:origem_list)")
        params["origem_list"] = origem

    if temperatura:
        clauses.append("LOWER(l.s1_temperatura) = ANY(:temp_list)")
        params["temp_list"] = temperatura

    if prioridade:
        clauses.append("LOWER(l.s1_prioridade) = ANY(:prio_list)")
        params["prio_list"] = prioridade

    if score_min is not None:
        clauses.append("COALESCE(l.s2_score, 0) >= :score_min")
        params["score_min"] = score_min

    if score_max is not None:
        clauses.append("COALESCE(l.s2_score, 0) <= :score_max")
        params["score_max"] = score_max

    if lgpd_only:
        clauses.append("(COALESCE(l.lgpd_conforme, false) = true OR l.s1_compliance = 'conforme')")

    if com_cadencia is True:
        clauses.append("l.s3_cadencia_atual IS NOT NULL")
    elif com_cadencia is False:
        clauses.append("l.s3_cadencia_atual IS NULL")

    if not clauses:
        return "", params
    return "AND " + " AND ".join(clauses), params


# ---------- GET /api/leads/pipeline ----------

@router.get("/pipeline", response_model=PipelineResponse)
async def get_pipeline(
    periodo: Literal["7d", "30d", "90d", "365d"] = Query(default="30d"),
    limit_por_coluna: int = Query(default=50, ge=10, le=200),
    q: Optional[str] = Query(default=None, description="busca por nome ou email"),
    origem: Optional[str] = Query(default=None, description="csv: landing,indicacao,organico,antigo"),
    temperatura: Optional[str] = Query(default=None, description="csv: quente,morno,frio"),
    prioridade: Optional[str] = Query(default=None, description="csv: alta,media,baixa"),
    score_min: Optional[int] = Query(default=None, ge=0, le=100),
    score_max: Optional[int] = Query(default=None, ge=0, le=100),
    lgpd_only: bool = Query(default=False),
    com_cadencia: Optional[bool] = Query(default=None),
) -> PipelineResponse:
    dias = {"7d": 7, "30d": 30, "90d": 90, "365d": 365}[periodo]
    desde = datetime.now(TZ_BRT) - timedelta(days=dias)

    extra, extra_params = _build_pipeline_filters(
        q=q,
        origem=_csv_to_list(origem),
        temperatura=_csv_to_list(temperatura),
        prioridade=_csv_to_list(prioridade),
        score_min=score_min,
        score_max=score_max,
        lgpd_only=lgpd_only,
        com_cadencia=com_cadencia,
    )
    sql = SQL_PIPELINE_BASE.replace("{EXTRA_FILTERS}", extra)
    params = {"desde": desde, **extra_params}

    async with get_session() as session:
        result = await session.execute(text(sql), params)
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


# ---------- GET /api/leads/{lead_id} ----------

@router.get("/{lead_id}", response_model=LeadDetail)
async def get_lead_detail(lead_id: int) -> LeadDetail:
    async with get_session() as session:
        result = await session.execute(SQL_DETAIL, {"id": lead_id})
        row = result.mappings().first()
        if not row:
            raise HTTPException(404, "lead não encontrado")

    d = dict(row)
    d["id"] = str(d["id"])
    d["score"] = int(d["score"])
    d["lgpd_conforme"] = bool(d["lgpd_conforme"])
    return LeadDetail(**d)


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
        async with session.begin():
            atual = await session.execute(
                text("SELECT cf_classificacao FROM leads WHERE id = :id"),
                {"id": lead_id},
            )
            if not atual.first():
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
