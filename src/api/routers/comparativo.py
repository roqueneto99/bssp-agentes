"""
src/api/routers/comparativo.py
==============================

Endpoints REST para o comparativo diário RD x Hablla.
Consumido pela página /comparativo do frontend Next.js.

Rotas:
    GET /api/comparativo/resumo            ?date=YYYY-MM-DD
    GET /api/comparativo/itens             ?date=&bucket=&dim=&q=&page=&size=
    GET /api/comparativo/itens/{id}        → detalhe lead-a-lead
    POST /api/comparativo/rerun            (admin) força recomputo do dia

Bucket ∈ {matched_aligned, matched_divergent, only_rd, only_hablla}
Dim    ∈ {stage, classification, last_interaction, origin}

Para registrar em painel.py (mesmo arquivo onde leads_summary/leads_pipeline já entram):

    # ~linha 47
    from src.api.routers import (
        leads_pipeline, leads_summary, admin_sync, conversas, comparativo,
    )

    # ~linha 203 (depois do admin_sync)
    app.include_router(comparativo.router)
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Literal, Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
import logging

logger = logging.getLogger("comparativo")
from pydantic import BaseModel
from sqlalchemy import text

from src.database.connection import get_session

router = APIRouter(prefix="/api/comparativo", tags=["comparativo"])
TZ_BRT = timezone(timedelta(hours=-3))


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------
Bucket = Literal["matched_aligned", "matched_divergent", "only_rd", "only_hablla"]
Dim = Literal["stage", "classification", "last_interaction", "origin"]


class ResumoComparativo(BaseModel):
    snapshot_date: date
    total: int
    matched_aligned: int
    matched_divergent: int
    only_rd: int
    only_hablla: int
    divergent_stage: int
    divergent_classification: int
    divergent_last_interaction: int
    divergent_origin: int
    matched_by_external_id: int
    matched_by_email: int
    matched_by_phone: int
    matched_by_name: int
    novos_24h: int
    atualizados_24h: int
    computed_at: datetime


class LeadComparison(BaseModel):
    id: int
    lead_id: Optional[int]
    hablla_only_id: Optional[int]
    bucket: Bucket
    match_key: Optional[str]
    match_score: Optional[float]
    name: Optional[str]
    email: Optional[str]
    phone: Optional[str]
    diffs: list[Dim]
    rd_stage: Optional[str]
    hablla_stage: Optional[str]
    rd_classification: Optional[str]
    hablla_classification: Optional[str]
    rd_last_interaction: Optional[datetime]
    hablla_last_interaction: Optional[datetime]
    rd_origin: Optional[str]
    hablla_origin: Optional[str]
    updated_in_last_24h: bool


class ItensPage(BaseModel):
    items: list[LeadComparison]
    page: int
    size: int
    total: int


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------
SQL_RESUMO = text("""
    SELECT * FROM daily_comparison_summary
     WHERE snapshot_date = :snapshot_date
""")

SQL_LIST = text("""
    SELECT c.id, c.lead_id, c.hablla_only_id, c.bucket,
           c.match_key, c.match_score,
           COALESCE(l.name, h.name)               AS name,
           COALESCE(l.email, h.email)             AS email,
           COALESCE(l.mobile_phone, h.phone_e164) AS phone,
           c.diff_stage, c.diff_classification,
           c.diff_last_interaction, c.diff_origin,
           c.rd_stage, c.hablla_stage,
           c.rd_classification, c.hablla_classification,
           c.rd_last_interaction, c.hablla_last_interaction,
           c.rd_origin, c.hablla_origin,
           c.updated_in_last_24h
      FROM daily_lead_comparison c
      LEFT JOIN leads l            ON l.id = c.lead_id
      LEFT JOIN leads_hablla_only h ON h.id = c.hablla_only_id
     WHERE c.snapshot_date = :snapshot_date
       AND (:bucket IS NULL OR c.bucket = :bucket)
       AND (
            :dim IS NULL
            OR (:dim = 'stage'              AND c.diff_stage)
            OR (:dim = 'classification'     AND c.diff_classification)
            OR (:dim = 'last_interaction'   AND c.diff_last_interaction)
            OR (:dim = 'origin'             AND c.diff_origin)
       )
       AND (:q IS NULL OR (
            LOWER(COALESCE(l.name, h.name, ''))  LIKE :qlike
         OR LOWER(COALESCE(l.email, h.email, '')) LIKE :qlike
       ))
       AND (
            :since_date IS NOT NULL
              AND (l.updated_at >= :since_dt OR h.last_seen_at >= :since_dt)
            OR (:since_date IS NULL
                AND (NOT :only_updated_24h OR c.updated_in_last_24h))
       )
     ORDER BY c.updated_in_last_24h DESC,
              c.bucket,
              COALESCE(l.email, h.email)
     LIMIT :limit OFFSET :offset
""")

SQL_COUNT = text("""
    SELECT COUNT(*)
      FROM daily_lead_comparison c
      LEFT JOIN leads l            ON l.id = c.lead_id
      LEFT JOIN leads_hablla_only h ON h.id = c.hablla_only_id
     WHERE c.snapshot_date = :snapshot_date
       AND (:bucket IS NULL OR c.bucket = :bucket)
       AND (
            :dim IS NULL
            OR (:dim = 'stage'              AND c.diff_stage)
            OR (:dim = 'classification'     AND c.diff_classification)
            OR (:dim = 'last_interaction'   AND c.diff_last_interaction)
            OR (:dim = 'origin'             AND c.diff_origin)
       )
       AND (:q IS NULL OR (
            LOWER(COALESCE(l.name, h.name, ''))  LIKE :qlike
         OR LOWER(COALESCE(l.email, h.email, '')) LIKE :qlike
       ))
       AND (
            :since_date IS NOT NULL
              AND (l.updated_at >= :since_dt OR h.last_seen_at >= :since_dt)
            OR (:since_date IS NULL
                AND (NOT :only_updated_24h OR c.updated_in_last_24h))
       )
""")

SQL_DETAIL = text("""
    SELECT c.*,
           l.name AS rd_name, l.email AS rd_email,
           l.mobile_phone AS rd_phone, l.tags AS rd_tags,
           h.name AS hablla_name, h.email AS hablla_email,
           h.phone_e164 AS hablla_phone
      FROM daily_lead_comparison c
      LEFT JOIN leads l            ON l.id = c.lead_id
      LEFT JOIN leads_hablla_only h ON h.id = c.hablla_only_id
     WHERE c.id = :id
""")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _today_brt() -> date:
    return datetime.now(TZ_BRT).date()


def _diffs_from_row(row) -> list[Dim]:
    out: list[Dim] = []
    if row["diff_stage"]: out.append("stage")
    if row["diff_classification"]: out.append("classification")
    if row["diff_last_interaction"]: out.append("last_interaction")
    if row["diff_origin"]: out.append("origin")
    return out


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@router.get("/resumo", response_model=ResumoComparativo)
async def resumo(date_: Optional[date] = Query(None, alias="date")):
    snap = date_ or _today_brt()
    async with get_session() as session:
        row = (await session.execute(SQL_RESUMO,
                                     {"snapshot_date": snap})).mappings().first()
        if not row:
            raise HTTPException(404, f"Sem snapshot para {snap}. Rode o job ou troque a data.")
        return ResumoComparativo(**row)


@router.get("/itens", response_model=ItensPage)
async def itens(
    date_: Optional[date] = Query(None, alias="date"),
    bucket: Optional[Bucket] = None,
    dim: Optional[Dim] = None,
    q: Optional[str] = Query(None, min_length=2),
    only_updated_24h: bool = Query(False),
    since: Optional[date] = Query(None, description="Mostra leads tocados desde YYYY-MM-DD"),
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1, le=200),
):
    snap = date_ or _today_brt()
    qlike = f"%{q.lower()}%" if q else "%"
    since_dt = datetime.combine(since, datetime.min.time(), tzinfo=TZ_BRT) if since else None
    params = {
        "snapshot_date": snap,
        "bucket": bucket, "dim": dim,
        "q": q, "qlike": qlike,
        "only_updated_24h": only_updated_24h,
        "since_date": since,
        "since_dt": since_dt,
        "limit": size, "offset": (page - 1) * size,
    }
    async with get_session() as session:
        total = (await session.execute(SQL_COUNT, params)).scalar_one()
        rows = (await session.execute(SQL_LIST, params)).mappings().all()
    items = [
        LeadComparison(
            id=r["id"], lead_id=r["lead_id"], hablla_only_id=r["hablla_only_id"],
            bucket=r["bucket"], match_key=r["match_key"],
            match_score=float(r["match_score"]) if r["match_score"] is not None else None,
            name=r["name"], email=r["email"], phone=r["phone"],
            diffs=_diffs_from_row(r),
            rd_stage=r["rd_stage"], hablla_stage=r["hablla_stage"],
            rd_classification=r["rd_classification"],
            hablla_classification=r["hablla_classification"],
            rd_last_interaction=r["rd_last_interaction"],
            hablla_last_interaction=r["hablla_last_interaction"],
            rd_origin=r["rd_origin"], hablla_origin=r["hablla_origin"],
            updated_in_last_24h=r["updated_in_last_24h"],
        )
        for r in rows
    ]
    return ItensPage(items=items, page=page, size=size, total=total)


@router.get("/itens/{item_id}")
async def detalhe(item_id: int):
    async with get_session() as session:
        row = (await session.execute(SQL_DETAIL, {"id": item_id})).mappings().first()
    if not row:
        raise HTTPException(404, "Item não encontrado")
    return dict(row)


@router.post("/rerun")
async def rerun(date_: Optional[date] = Query(None, alias="date")):
    """Endpoint admin para reprocessar o comparativo de um dia."""
    from src.sync.lead_comparison import run_daily_comparison
    snap = date_ or _today_brt()
    summary = await run_daily_comparison(snapshot_date=snap, dry_run=False)
    return summary


async def _backfill_worker(from_: date, to: date) -> None:
    """Roda backfill em background — sem bloquear o request."""
    from src.sync.lead_comparison import run_daily_comparison
    cur = from_
    while cur <= to:
        try:
            s = await run_daily_comparison(snapshot_date=cur, dry_run=False)
            logger.info("backfill %s OK: total=%s divergentes=%s",
                        cur, s.get("total"), s.get("matched_divergent"))
        except Exception:
            logger.exception("backfill %s falhou", cur)
        cur += timedelta(days=1)


@router.post("/backfill", status_code=202)
async def backfill(
    bg: BackgroundTasks,
    from_: date = Query(..., alias="from", description="Data inicial (inclusiva)"),
    to: Optional[date] = Query(None, description="Data final (inclusiva). Default: hoje BRT"),
):
    """
    Gera snapshots retroativos de [from..to] em BACKGROUND.

    Retorna 202 imediatamente. O processamento pode levar alguns minutos
    (varia por número de dias × tamanho da base). Recarregue a página em
    1-3 minutos pra ver os snapshots aparecendo.

    Limitação: cada snapshot usa o estado ATUAL do banco; a flag
    `updated_in_last_24h` é calculada com a janela 24h do dia X (usando
    leads.updated_at). Leads deletados depois não aparecem nos dias antigos.
    """
    end = to or _today_brt()
    if from_ > end:
        raise HTTPException(400, "from > to")
    days = (end - from_).days + 1
    if days > 120:
        raise HTTPException(400, f"backfill de {days} dias — máx 120 por request")

    bg.add_task(_backfill_worker, from_, end)
    return {
        "status": "started",
        "from": from_.isoformat(),
        "to": end.isoformat(),
        "days": days,
        "message": "Backfill rodando em background. Recarregue em alguns minutos.",
    }
