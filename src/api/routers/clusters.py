"""
src/api/routers/clusters.py
============================

Endpoints REST pra análise de leads perdidos (only_rd) clusterizados.

Rotas:
    POST /api/clusters/run                  → executa nova análise
    GET  /api/clusters/runs                 → histórico de execuções
    GET  /api/clusters/runs/{run_id}        → clusters da execução
    GET  /api/clusters/{cluster_id}/leads   → leads de um cluster (paginado)
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text

from src.database.connection import get_session

router = APIRouter(prefix="/api/clusters", tags=["clusters"])


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------
class ClusterStats(BaseModel):
    n: int
    score_medio: float
    score_mediano: float
    days_mediana: int
    lifecycle_dominante: str
    fit_dominante: Optional[str] = None
    interest_dominante: Optional[str] = None


class ClusterCard(BaseModel):
    id: int
    cluster_index: int
    label: str
    hipotese: Optional[str]
    copy_whatsapp: Optional[str]
    copy_email_assunto: Optional[str]
    copy_email_corpo: Optional[str]
    n_leads: int
    score_medio: float
    score_mediano: float
    lifecycle_dominante: Optional[str]
    dias_sem_interacao_mediana: Optional[int]
    metadados: Optional[dict] = None


class RunSummary(BaseModel):
    id: int
    since_date: Optional[date]
    n_leads: int
    n_clusters: int
    silhouette_score: Optional[float]
    executado_em: datetime


class LeadInCluster(BaseModel):
    id: int
    email: str
    name: Optional[str]
    s2_score: Optional[float]
    lifecycle_stage: Optional[str]
    last_conversion_date: Optional[datetime]
    distance: Optional[float]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@router.post("/run")
async def run_analysis(
    since: Optional[date] = Query(None, description="Data inicial (default: sem filtro)"),
    dry_run: bool = Query(False),
):
    """Roda uma nova análise de clusterização. Pode demorar 30s–2min."""
    from src.analytics.cluster_lost_leads import run_cluster_analysis
    try:
        result = await run_cluster_analysis(since_date=since, dry_run=dry_run)
    except Exception as e:
        raise HTTPException(500, f"Falha na análise: {e}")
    return result


@router.get("/runs", response_model=list[RunSummary])
async def list_runs(limit: int = Query(20, ge=1, le=100)):
    async with get_session() as session:
        rows = (await session.execute(text("""
            SELECT id, since_date, n_leads, n_clusters,
                   silhouette_score::float AS silhouette_score, executado_em
              FROM cluster_runs
             ORDER BY executado_em DESC
             LIMIT :limit
        """), {"limit": limit})).mappings().all()
    return [RunSummary(**r) for r in rows]


@router.get("/runs/latest")
async def latest_run():
    """Atalho: clusters da última execução."""
    async with get_session() as session:
        row = (await session.execute(text("""
            SELECT id FROM cluster_runs ORDER BY executado_em DESC LIMIT 1
        """))).first()
        if not row:
            raise HTTPException(404, "Nenhuma análise rodada ainda.")
        return await _clusters_for_run(row[0])


@router.get("/runs/{run_id}")
async def clusters_of_run(run_id: int):
    return await _clusters_for_run(run_id)


@router.get("/{cluster_id}/leads", response_model=list[LeadInCluster])
async def leads_of_cluster(
    cluster_id: int,
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1, le=200),
):
    async with get_session() as session:
        rows = (await session.execute(text("""
            SELECT l.id, l.email, l.name,
                   l.s2_score::float AS s2_score,
                   l.lifecycle_stage,
                   l.last_conversion_date,
                   a.distance::float AS distance
              FROM lead_cluster_assignments a
              JOIN leads l ON l.id = a.lead_id
             WHERE a.cluster_id = :cid
             ORDER BY l.s2_score DESC NULLS LAST,
                      l.last_conversion_date DESC NULLS LAST
             LIMIT :limit OFFSET :offset
        """), {
            "cid": cluster_id,
            "limit": size,
            "offset": (page - 1) * size,
        })).mappings().all()
    return [LeadInCluster(**r) for r in rows]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
async def _clusters_for_run(run_id: int):
    async with get_session() as session:
        run_row = (await session.execute(text("""
            SELECT id, since_date, n_leads, n_clusters,
                   silhouette_score::float AS silhouette_score, executado_em
              FROM cluster_runs WHERE id = :id
        """), {"id": run_id})).mappings().first()
        if not run_row:
            raise HTTPException(404, "Run não encontrado.")

        rows = (await session.execute(text("""
            SELECT id, cluster_index, label, hipotese,
                   copy_whatsapp, copy_email_assunto, copy_email_corpo,
                   n_leads,
                   score_medio::float AS score_medio,
                   score_mediano::float AS score_mediano,
                   lifecycle_dominante, dias_sem_interacao_mediana,
                   metadados
              FROM lead_clusters
             WHERE run_id = :run_id
             ORDER BY n_leads DESC
        """), {"run_id": run_id})).mappings().all()

    return {
        "run": dict(run_row),
        "clusters": [ClusterCard(**r) for r in rows],
    }
