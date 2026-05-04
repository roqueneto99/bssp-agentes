"""
Endpoints de leitura das mensagens persistidas pelo webhook do Hablla.

GET /api/conversas
    Inbox: lista as conversas mais recentes (1 linha por lead com a última msg).
GET /api/conversas/{lead_id}
    Histórico completo de mensagens de um lead.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text

from src.database.connection import get_session

router = APIRouter(prefix="/api/conversas", tags=["conversas"])


class Mensagem(BaseModel):
    id: int
    enviado_em: Optional[datetime] = None
    canal: Optional[str] = None
    direcao: Optional[str] = None
    autor_nome: Optional[str] = None
    conteudo: Optional[str] = None
    midia_tipo: Optional[str] = None
    midia_url: Optional[str] = None


class ConversaResumo(BaseModel):
    lead_id: int
    nome: str
    email: str
    iniciais: str
    consultor: Optional[str] = None
    canal_ultima: Optional[str] = None
    ultima_msg_em: Optional[datetime] = None
    ultima_msg_preview: Optional[str] = None
    direcao_ultima: Optional[str] = None
    nao_respondidas: int = 0   # nº de msgs in sem out depois (resposta pendente)


SQL_INBOX = text("""
WITH ultima_por_lead AS (
    SELECT DISTINCT ON (m.lead_id)
        m.lead_id, m.canal, m.direcao, m.conteudo, m.enviado_em
    FROM mensagens_hablla m
    WHERE m.lead_id IS NOT NULL
    ORDER BY m.lead_id, m.enviado_em DESC NULLS LAST
),
nao_respondidas AS (
    SELECT lead_id, COUNT(*) AS pendentes
    FROM mensagens_hablla m
    WHERE m.direcao = 'in'
      AND NOT EXISTS (
          SELECT 1 FROM mensagens_hablla m2
          WHERE m2.lead_id = m.lead_id
            AND m2.direcao = 'out'
            AND m2.enviado_em > m.enviado_em
      )
    GROUP BY lead_id
)
SELECT
    u.lead_id,
    COALESCE(l.name, '(sem nome)') AS nome,
    COALESCE(l.email, '') AS email,
    UPPER(
        LEFT(SPLIT_PART(COALESCE(l.name, ''), ' ', 1), 1) ||
        COALESCE(LEFT(SPLIT_PART(COALESCE(l.name, ''), ' ', 2), 1), '')
    ) AS iniciais,
    l.consultor,
    u.canal AS canal_ultima,
    u.enviado_em AS ultima_msg_em,
    LEFT(COALESCE(u.conteudo, ''), 120) AS ultima_msg_preview,
    u.direcao AS direcao_ultima,
    COALESCE(nr.pendentes, 0)::int AS nao_respondidas
FROM ultima_por_lead u
LEFT JOIN leads l ON l.id = u.lead_id
LEFT JOIN nao_respondidas nr ON nr.lead_id = u.lead_id
ORDER BY u.enviado_em DESC NULLS LAST
LIMIT :limit
""")


SQL_LISTAR_MENSAGENS = text("""
SELECT id, enviado_em, canal, direcao, autor_nome, conteudo, midia_tipo, midia_url
FROM mensagens_hablla
WHERE lead_id = :lead_id
ORDER BY enviado_em ASC NULLS LAST, id ASC
LIMIT :limit
""")


@router.get("", response_model=list[ConversaResumo])
async def listar_inbox(
    limit: int = Query(default=50, ge=1, le=200),
):
    async with get_session() as session:
        result = await session.execute(SQL_INBOX, {"limit": limit})
        rows = [dict(r) for r in result.mappings().all()]

    return [ConversaResumo(**r) for r in rows]


@router.get("/{lead_id}", response_model=list[Mensagem])
async def listar_mensagens_do_lead(
    lead_id: int,
    limit: int = Query(default=200, ge=1, le=500),
):
    async with get_session() as session:
        result = await session.execute(
            SQL_LISTAR_MENSAGENS, {"lead_id": lead_id, "limit": limit},
        )
        rows = [dict(r) for r in result.mappings().all()]

    if not rows:
        # Não erra: lead pode existir mas ainda não ter conversa
        return []

    return [Mensagem(**r) for r in rows]
