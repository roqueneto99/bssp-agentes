"""
Agente Orquestrador v2 — bssp-agentes/src/orquestrador.py

Diferenças vs v1:
- Aceita progress_callback opcional (atualizado a cada lead)
- Aceita lista emails opcional (override da query SQL — útil pro board do front)
"""
from __future__ import annotations

import logging
from typing import Awaitable, Callable, Optional

from sqlalchemy import text

logger = logging.getLogger("orquestrador")

_RANK = {
    "COLD": 0, "SAL": 1, "MQL": 2, "SQL": 3, "HANDOFF": 4, "CONVERTIDO": 5, "CLIENTE": 5,
}

ProgressCallback = Callable[[int, int, Optional[str], str], Awaitable[None]]


SQL_CANDIDATOS = text("""
    SELECT email, s2_classificacao, COALESCE(s2_score, 0)::int AS s2_score, s2_processado_em
    FROM leads
    WHERE email IS NOT NULL
      AND LENGTH(TRIM(email)) > 0
      AND (
        s2_processado_em IS NULL
        OR (synced_at IS NOT NULL AND synced_at > s2_processado_em)
        OR s2_processado_em < (NOW() - INTERVAL '14 days')
      )
    ORDER BY
        CASE WHEN s2_processado_em IS NULL THEN 0 ELSE 1 END,
        synced_at DESC NULLS LAST,
        rd_created_at DESC NULLS LAST
    LIMIT :max
""")

SQL_BY_EMAILS = text("""
    SELECT email, s2_classificacao, COALESCE(s2_score, 0)::int AS s2_score, s2_processado_em
    FROM leads
    WHERE email = ANY(:emails)
""")


async def run(
    pipeline_obj,
    running_set: set,
    data_mode: str,
    max_leads: int = 30,
    motivo: str = "scheduled",
    emails: Optional[list[str]] = None,
    progress_callback: Optional[ProgressCallback] = None,
) -> dict:
    """
    Executa orquestrador.

    Args:
        emails: se passado, processa SOMENTE esses (ignora critérios de mudança).
                Útil pra re-scorar leads visíveis num board específico.
        progress_callback: async fn (processed, total, current_email, step).
                Chamado antes de cada lead e ao final.

    Retorna summary igual ao v1.
    """
    from src.database.connection import get_session
    from src.database.queries import save_execution

    if not pipeline_obj or data_mode != "database":
        return {"skipped": True, "motivo": motivo}

    async with get_session() as session:
        if emails:
            result = await session.execute(SQL_BY_EMAILS, {"emails": emails})
        else:
            result = await session.execute(SQL_CANDIDATOS, {"max": max_leads})
        candidatos = [dict(row) for row in result.mappings().all()]

    total = len(candidatos)
    promovidos: list[dict] = []
    rebaixados: list[dict] = []
    novos = inalterados = falhas = 0

    if progress_callback:
        await progress_callback(0, total, None, "iniciando")

    for i, c in enumerate(candidatos):
        email = c["email"]
        before_class = c.get("s2_classificacao")
        before_score = int(c.get("s2_score") or 0)

        if progress_callback:
            await progress_callback(i, total, email, "processando")

        if email in running_set:
            if progress_callback:
                await progress_callback(i + 1, total, email, "skipped")
            continue
        running_set.add(email)
        try:
            results = await pipeline_obj.process_new_lead(
                email, conversion_identifier=f"orquestrador_{motivo}",
            )
            resultado = {"email": email, "tipo": "pipeline_completo", "agentes": {}}
            for r in results:
                resultado["agentes"][r.agent_name] = {
                    "success": r.success,
                    "duration_ms": round(r.duration_ms),
                    "error": r.error,
                    "data": r.data,
                }

            resumo_s1 = resultado["agentes"].get("squad1_resumo", {}).get("data", {})
            resultado["resumo_squad1"] = {
                "temperatura": resumo_s1.get("temperatura", "-"),
                "prioridade": resumo_s1.get("prioridade_contato", "-"),
                "area": resumo_s1.get("area_principal", "-"),
                "compliance": resumo_s1.get("compliance_status", "-"),
                "pode_seguir": resumo_s1.get("pode_seguir_squad2", False),
            }

            resumo_s2 = resultado["agentes"].get("squad2_resumo", {}).get("data", {})
            resultado["resumo_squad2"] = {
                "score_total": resumo_s2.get("score_total", 0),
                "classificacao": resumo_s2.get("classificacao", "-"),
                "rota": resumo_s2.get("rota", "-"),
                "acoes_recomendadas": resumo_s2.get("acoes_recomendadas", []),
                "briefing_comercial": resumo_s2.get("briefing_comercial"),
                "tags_aplicadas": resumo_s2.get("tags_aplicadas", []),
                "pode_seguir_squad3": resumo_s2.get("pode_seguir_squad3", False),
            }
            scorer_data = resultado["agentes"].get("squad2_scorer", {}).get("data", {})
            resultado["resumo_squad2"]["dimensoes"] = scorer_data.get("dimensoes", {})

            await save_execution(resultado)

            after_class = resultado["resumo_squad2"]["classificacao"]
            after_score = int(resultado["resumo_squad2"]["score_total"] or 0)

            if before_class is None:
                novos += 1
            elif after_class and after_class != before_class and after_class != "-":
                evento = {
                    "email": email,
                    "de": before_class,
                    "para": after_class,
                    "score_de": before_score,
                    "score_para": after_score,
                }
                if _RANK.get(after_class, 0) > _RANK.get(before_class, 0):
                    promovidos.append(evento)
                else:
                    rebaixados.append(evento)
            else:
                inalterados += 1
        except Exception as e:
            logger.warning("orquestrador: falha em %s: %s", email, e)
            falhas += 1
        finally:
            running_set.discard(email)
            if progress_callback:
                await progress_callback(i + 1, total, email, "concluido")

    if progress_callback:
        await progress_callback(total, total, None, "finalizado")

    summary = {
        "motivo": motivo,
        "candidates": total,
        "novos": novos,
        "promovidos": len(promovidos),
        "rebaixados": len(rebaixados),
        "inalterados": inalterados,
        "falhas": falhas,
        "promovidos_detalhe": promovidos[:50],
        "rebaixados_detalhe": rebaixados[:50],
    }
    logger.info(
        "orquestrador (%s): %d cand · novos=%d promovidos=%d rebaixados=%d inalterados=%d falhas=%d",
        motivo, total, novos, len(promovidos), len(rebaixados), inalterados, falhas,
    )
    for p in promovidos[:5]:
        logger.info(
            "  ↑ %s: %s → %s (%d → %d)",
            p["email"], p["de"], p["para"], p["score_de"], p["score_para"],
        )
    return summary
