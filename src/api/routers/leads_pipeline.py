"""
Endpoints FastAPI pro Kanban — bssp-agentes/src/api/routers/leads_pipeline.py
=============================================================================
2 endpoints novos consumidos pelo KanbanBoard do bssp-frontend:

  GET  /api/leads/pipeline?periodo=30d
       → retorna leads agrupados por classificação

  PATCH /api/leads/{lead_id}/classificacao
       → move lead entre colunas (com auditoria)

Schemas Pydantic compatíveis com Zod compartilhado (bssp-frontend/src/schemas/).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Literal, Optional
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from src.auth.dependencies import current_user, User
from src.database.pool import get_pool

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
    id: UUID
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
    """Mapa classificacao -> [LeadCard]. Ordem da lista importa (mais quente primeiro)."""
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

# Otimização: 1 query agrupa tudo. Limite por coluna pra não estourar payload.
SQL_PIPELINE = """
WITH leads_filtrados AS (
    SELECT
        l.id, l.nome, l.email,
        UPPER(LEFT(SPLIT_PART(l.nome, ' ', 1), 1) ||
              COALESCE(LEFT(SPLIT_PART(l.nome, ' ', 2), 1), '')) AS iniciais,
        l.origem, l.origem_label,
        COALESCE(l.cf_score_ia, 0)::int AS score,
        l.cf_classificacao,
        COALESCE(l.ultima_interacao_em, l.criado_em) AS ultima_interacao_em,
        COALESCE(l.lgpd_conforme, false) AS lgpd_conforme,
        c.nome AS cadencia_nome,
        c.etapa_atual,
        c.total_etapas,
        l.consultor,
        l.matricula_curso
    FROM leads l
    LEFT JOIN cadencias c ON c.lead_id = l.id AND c.ativa = true
    WHERE l.cf_classificacao IS NOT NULL
      AND COALESCE(l.ultima_interacao_em, l.criado_em) >= $1
)
SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY cf_classificacao
           ORDER BY score DESC, ultima_interacao_em DESC
       ) AS rn
FROM leads_filtrados
"""


# ---------- GET /api/leads/pipeline ----------

@router.get("/pipeline", response_model=PipelineResponse)
async def get_pipeline(
    periodo: Literal["7d", "30d", "90d"] = Query(default="30d"),
    limit_por_coluna: int = Query(default=50, ge=10, le=200),
    pool: asyncpg.Pool = Depends(get_pool),
    user: User = Depends(current_user),
) -> PipelineResponse:
    dias = {"7d": 7, "30d": 30, "90d": 90}[periodo]
    desde = datetime.now(TZ_BRT) - timedelta(days=dias)

    async with pool.acquire() as conn:
        rows = await conn.fetch(SQL_PIPELINE, desde)

    response = PipelineResponse()
    for row in rows:
        if row["rn"] > limit_por_coluna:
            continue
        cadencia = None
        if row["cadencia_nome"]:
            cadencia = CadenciaInline(
                nome=row["cadencia_nome"],
                etapa_atual=row["etapa_atual"] or 0,
                total_etapas=row["total_etapas"] or 0,
            )
        card = LeadCard(
            id=row["id"],
            nome=row["nome"],
            email=row["email"],
            iniciais=row["iniciais"],
            origem=row["origem"],
            origem_label=row["origem_label"],
            score=row["score"],
            classificacao=row["cf_classificacao"],
            ultima_interacao_em=row["ultima_interacao_em"],
            lgpd_conforme=row["lgpd_conforme"],
            cadencia=cadencia,
            consultor=row["consultor"],
            matricula_curso=row["matricula_curso"],
        )
        getattr(response, row["cf_classificacao"]).append(card)

    return response


# ---------- PATCH /api/leads/{lead_id}/classificacao ----------

SQL_MOVE = """
UPDATE leads
SET
    cf_classificacao = $2,
    classificacao_origem = 'manual',
    classificacao_atualizada_em = $3,
    classificacao_atualizada_por = $4
WHERE id = $1
RETURNING id, nome, cf_classificacao
"""

SQL_AUDITORIA = """
INSERT INTO leads_auditoria (
    lead_id, evento, dados_antes, dados_depois, motivo, autor_id, autor_email, ocorreu_em
) VALUES ($1, 'classificacao_movida', $2, $3, $4, $5, $6, $7)
"""


@router.patch("/{lead_id}/classificacao", response_model=LeadCard)
async def move_lead_classificacao(
    lead_id: UUID,
    body: MoveClassificacaoBody,
    pool: asyncpg.Pool = Depends(get_pool),
    user: User = Depends(current_user),
) -> LeadCard:
    """
    Move um lead pra outra coluna do Kanban.
    Sobrescreve o scoring automático até a próxima conversão.
    Sempre registra na tabela leads_auditoria.
    """
    if user.role not in ("admin", "sales"):
        raise HTTPException(403, "sem permissão pra mover leads")

    agora = datetime.now(TZ_BRT)

    async with pool.acquire() as conn:
        async with conn.transaction():
            # Pega estado atual
            atual = await conn.fetchrow(
                "SELECT cf_classificacao FROM leads WHERE id = $1 FOR UPDATE",
                lead_id,
            )
            if not atual:
                raise HTTPException(404, "lead não encontrado")

            classificacao_anterior = atual["cf_classificacao"]
            if classificacao_anterior == body.para.value:
                raise HTTPException(400, "lead já está nessa coluna")

            # Aplica
            await conn.execute(SQL_MOVE, lead_id, body.para.value, agora, user.id)

            # Audita (jsonb)
            import json
            await conn.execute(
                SQL_AUDITORIA,
                lead_id,
                json.dumps({"cf_classificacao": classificacao_anterior}),
                json.dumps({"cf_classificacao": body.para.value}),
                body.motivo,
                user.id,
                user.email,
                agora,
            )

    # Retorna o card completo (recarrega via /pipeline pra estado consistente)
    return await _carregar_lead_card(pool, lead_id)


async def _carregar_lead_card(pool: asyncpg.Pool, lead_id: UUID) -> LeadCard:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                l.id, l.nome, l.email,
                UPPER(LEFT(SPLIT_PART(l.nome, ' ', 1), 1) ||
                      COALESCE(LEFT(SPLIT_PART(l.nome, ' ', 2), 1), '')) AS iniciais,
                l.origem, l.origem_label,
                COALESCE(l.cf_score_ia, 0)::int AS score,
                l.cf_classificacao,
                COALESCE(l.ultima_interacao_em, l.criado_em) AS ultima_interacao_em,
                COALESCE(l.lgpd_conforme, false) AS lgpd_conforme,
                l.consultor, l.matricula_curso
            FROM leads l
            WHERE l.id = $1
            """,
            lead_id,
        )
        if not row:
            raise HTTPException(404, "lead não encontrado")
        return LeadCard(
            id=row["id"],
            nome=row["nome"],
            email=row["email"],
            iniciais=row["iniciais"],
            origem=row["origem"],
            origem_label=row["origem_label"],
            score=row["score"],
            classificacao=row["cf_classificacao"],
            ultima_interacao_em=row["ultima_interacao_em"],
            lgpd_conforme=row["lgpd_conforme"],
            cadencia=None,
            consultor=row["consultor"],
            matricula_curso=row["matricula_curso"],
        )


# =============================================================================
# Migration relacionada — migrations/004_kanban_audit.sql
# =============================================================================
"""
ALTER TABLE leads ADD COLUMN IF NOT EXISTS classificacao_origem TEXT
    DEFAULT 'automatico' CHECK (classificacao_origem IN ('automatico','manual'));
ALTER TABLE leads ADD COLUMN IF NOT EXISTS classificacao_atualizada_em TIMESTAMPTZ;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS classificacao_atualizada_por UUID;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS origem TEXT;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS origem_label TEXT;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS lgpd_conforme BOOLEAN DEFAULT false;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS consultor TEXT;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS matricula_curso TEXT;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS ultima_interacao_em TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS leads_auditoria (
    id BIGSERIAL PRIMARY KEY,
    lead_id UUID NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
    evento TEXT NOT NULL,
    dados_antes JSONB,
    dados_depois JSONB,
    motivo TEXT,
    autor_id UUID,
    autor_email TEXT,
    ocorreu_em TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_leads_auditoria_lead_id ON leads_auditoria(lead_id, ocorreu_em DESC);
CREATE INDEX IF NOT EXISTS idx_leads_classificacao_interacao
    ON leads (cf_classificacao, ultima_interacao_em DESC NULLS LAST);
"""
