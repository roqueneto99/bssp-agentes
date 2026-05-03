"""
Squad 2 — Batch Scoring automático
====================================
Roda Analisador → Scorer → Classificador em batch para os leads que
precisam de scoring novo, ao final do sync_job.py do bssp-agentes.

Idempotente. Inclui Fase A de estabilização do Scorer:
- temperature=0
- cache LLM em memória
- timing congelado por dia
- coluna metodo_scoring registrando llm | fallback_heuristico

Uso integrado no sync_job.py (ver patch ao final do arquivo).
Uso standalone:
    python -m scripts.squad2_batch_scoring --max-leads 500
    python -m scripts.squad2_batch_scoring --dry-run
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import asyncpg

# ATENÇÃO: ajuste estes imports conforme o caminho real no bssp-agentes/
from src.agents.squad2.analisador_engajamento import AnalisadorEngajamentoAgent
from src.agents.squad2.scorer import ScorerAgent
from src.agents.squad2.classificador_rota import ClassificadorRotaAgent

logger = logging.getLogger(__name__)
TZ_BRT = timezone(timedelta(hours=-3))


# ---------- SQL ----------

QUERY_LEADS_PARA_SCORAR = """
SELECT
    id,
    email,
    nome,
    criado_em,
    ultima_conversao_em,
    ultima_pontuacao_em,
    cf_score_ia
FROM leads
WHERE
    -- nunca foi scorado
    ultima_pontuacao_em IS NULL
    -- OU teve nova conversão depois do último scoring
    OR (ultima_conversao_em IS NOT NULL AND ultima_conversao_em > ultima_pontuacao_em)
    -- OU score expirou (>30 dias) — re-score periódico
    OR ultima_pontuacao_em < (now() AT TIME ZONE 'America/Sao_Paulo' - INTERVAL '30 days')
ORDER BY
    -- prioridade: leads com conversão recente
    COALESCE(ultima_conversao_em, criado_em) DESC NULLS LAST
LIMIT $1
"""

UPDATE_SCORING = """
UPDATE leads
SET
    cf_score_ia          = $2,
    cf_temperatura       = $3,
    cf_classificacao     = $4,
    cf_motivo_interesse  = $5,
    cf_segmento_ia       = $6,
    metodo_scoring       = $7,
    ultima_pontuacao_em  = $8
WHERE id = $1
"""

# Migration sugerida:
#   ALTER TABLE leads ADD COLUMN IF NOT EXISTS metodo_scoring TEXT;
#   ALTER TABLE leads ADD COLUMN IF NOT EXISTS ultima_pontuacao_em TIMESTAMPTZ;
#   ALTER TABLE leads ADD COLUMN IF NOT EXISTS cf_classificacao TEXT;
# (cf_score_ia, cf_temperatura, cf_motivo_interesse, cf_segmento_ia já existem
#  conforme RD Station setup de 15/04/2026.)


# ---------- Core ----------

async def run_batch_scoring(
    pool: asyncpg.Pool,
    max_leads: int = 500,
    dry_run: bool = False,
    timing_freeze_date: Optional[date] = None,
) -> dict:
    """
    Executa batch de scoring nos leads que precisam.

    Args:
        pool: pool asyncpg conectado ao Postgres do Railway
        max_leads: teto de leads por execução (segurança contra runaway)
        dry_run: se True, calcula mas não persiste — útil pra validar em staging
        timing_freeze_date: data de referência do Timing (Fase A — congela
                            timing por dia pra evitar drift entre runs)

    Returns:
        dict com métricas: lidos, scorados, erros, por_classificacao, latência média.
    """
    started_at = datetime.now(TZ_BRT)
    timing_freeze_date = timing_freeze_date or started_at.date()

    # Fase A — estabilização do Scorer
    analisador = AnalisadorEngajamentoAgent()
    scorer = ScorerAgent(temperature=0, cache_enabled=True)
    classificador = ClassificadorRotaAgent()

    metrics = {
        "started_at": started_at.isoformat(),
        "max_leads": max_leads,
        "dry_run": dry_run,
        "lidos": 0,
        "scorados": 0,
        "erros": 0,
        "por_classificacao": {"SQL": 0, "MQL": 0, "SAL": 0, "COLD": 0},
        "por_metodo": {"llm": 0, "fallback_heuristico": 0},
        "latencia_total_ms": 0,
    }

    async with pool.acquire() as conn:
        rows = await conn.fetch(QUERY_LEADS_PARA_SCORAR, max_leads)
        metrics["lidos"] = len(rows)
        logger.info(
            "Squad 2 batch: %d leads candidatos a scoring (max=%d, dry_run=%s)",
            len(rows), max_leads, dry_run,
        )

        for row in rows:
            try:
                t0 = datetime.now()

                # 1. Analisa engajamento (multicanal: RD + Hablla)
                engajamento = await analisador.analisar(
                    lead_id=row["id"],
                    conn=conn,
                )

                # 2. Scorer com timing congelado
                resultado = await scorer.calcular(
                    lead_id=row["id"],
                    engajamento=engajamento,
                    timing_date=timing_freeze_date,
                )

                # 3. Classifica rota (SQL/MQL/SAL/COLD)
                classificacao = classificador.classificar(resultado)

                if dry_run:
                    logger.debug(
                        "[dry-run] %s score=%s class=%s metodo=%s",
                        row["email"], resultado.score,
                        classificacao.rota, resultado.metodo,
                    )
                else:
                    await conn.execute(
                        UPDATE_SCORING,
                        row["id"],
                        resultado.score,
                        resultado.temperatura,
                        classificacao.rota,
                        resultado.motivo,
                        resultado.segmento,
                        resultado.metodo,
                        datetime.now(TZ_BRT),
                    )

                metrics["scorados"] += 1
                metrics["por_classificacao"][classificacao.rota] = (
                    metrics["por_classificacao"].get(classificacao.rota, 0) + 1
                )
                metrics["por_metodo"][resultado.metodo] = (
                    metrics["por_metodo"].get(resultado.metodo, 0) + 1
                )
                metrics["latencia_total_ms"] += int(
                    (datetime.now() - t0).total_seconds() * 1000
                )

            except Exception:
                logger.exception(
                    "Erro ao scorar lead %s", row.get("email") or row["id"]
                )
                metrics["erros"] += 1

    metrics["finished_at"] = datetime.now(TZ_BRT).isoformat()
    metrics["latencia_media_ms"] = (
        metrics["latencia_total_ms"] // max(metrics["scorados"], 1)
    )
    logger.info("Squad 2 batch concluído: %s", metrics)
    return metrics


# ---------- CLI ----------

async def _main_cli(args: argparse.Namespace) -> int:
    pool = await asyncpg.create_pool(
        dsn=os.environ["DATABASE_URL"],
        min_size=1, max_size=4,
    )
    try:
        metrics = await run_batch_scoring(
            pool=pool,
            max_leads=args.max_leads,
            dry_run=args.dry_run,
        )
        print(metrics)
        return 0 if metrics["erros"] == 0 else 1
    finally:
        await pool.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="Squad 2 batch scoring")
    parser.add_argument("--max-leads", type=int, default=500)
    parser.add_argument("--dry-run", action="store_true")
    raise SystemExit(asyncio.run(_main_cli(parser.parse_args())))


# =============================================================================
# PATCH para sync_job.py (aplicar no final, depois do sync existente)
# =============================================================================
#
# import asyncio
# from scripts.squad2_batch_scoring import run_batch_scoring
#
# # ... código existente do sync_job (sync RD Station) ...
#
# async def main():
#     # 1. Sync RD Station (já existente)
#     sync_metrics = await run_sync(hours=args.hours, ...)
#
#     # 2. Squad 2 — Scoring automático em batch (NOVO)
#     if not getattr(args, "skip_scoring", False):
#         scoring_metrics = await run_batch_scoring(
#             pool=db_pool,
#             max_leads=int(os.environ.get("MAX_SCORING_PER_RUN", 500)),
#             dry_run=getattr(args, "dry_run", False),
#         )
#         logger.info("Scoring metrics: %s", scoring_metrics)
#
# # Adicionar argumentos no parser:
# # parser.add_argument("--skip-scoring", action="store_true",
# #                     help="Pula a etapa de Squad 2 batch scoring")
#
# # Variáveis de ambiente novas no Railway:
# #   MAX_SCORING_PER_RUN=500     (teto de leads por run)
# #   SCORING_DRY_RUN=false       (toggle global de segurança)
