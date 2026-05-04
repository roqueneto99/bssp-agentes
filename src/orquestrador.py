"""
Agente Orquestrador — bssp-agentes/src/orquestrador.py

Detecta leads que mudaram desde o último scoring, re-roda Squad 1+2,
atualiza score e identifica movimentações de coluna no Kanban.

Critérios de re-scoring (qualquer um dispara):
  1) nunca foi scorado (s2_processado_em IS NULL)
  2) sincronizado depois do último scoring (synced_at > s2_processado_em)
  3) score expirou (s2_processado_em < now - 14 days)
"""
from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy import text

logger = logging.getLogger("orquestrador")

# Ranking dos estágios do Kanban — pra detectar promoção vs rebaixamento
_RANK = {
    "COLD": 0,
    "SAL": 1,
    "MQL": 2,
    "SQL": 3,
    "HANDOFF": 4,
    "CONVERTIDO": 5,
    "CLIENTE": 5,
}


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


async def run(
    pipeline_obj,
    running_set: set,
    data_mode: str,
    max_leads: int = 30,
    motivo: str = "scheduled",
) -> dict:
    """
    Executa o orquestrador.

    Args:
        pipeline_obj: instância de pipeline (do painel) que tem .process_new_lead()
        running_set: set global do painel pra evitar processar mesmo email 2x
        data_mode: "database" ou outro — só roda se database
        max_leads: teto de leads por execução
        motivo: tag de origem (auto_sync | manual | etc)

    Retorna dict com summary: candidates, novos, promovidos, rebaixados,
    inalterados, falhas, e detalhe das mudanças (até 20).
    """
    from src.database.connection import get_session
    from src.database.queries import save_execution

    if not pipeline_obj or data_mode != "database":
        return {"skipped": True, "motivo": motivo}

    async with get_session() as session:
        result = await session.execute(SQL_CANDIDATOS, {"max": max_leads})
        candidatos = [dict(row) for row in result.mappings().all()]

    promovidos: list[dict] = []
    rebaixados: list[dict] = []
    novos = inalterados = falhas = 0

    for c in candidatos:
        email = c["email"]
        before_class = c.get("s2_classificacao")
        before_score = int(c.get("s2_score") or 0)

        if email in running_set:
            continue
        running_set.add(email)
        try:
            results = await pipeline_obj.process_new_lead(
                email, conversion_identifier=f"orquestrador_{motivo}",
            )

            # Constrói resultado igual ao endpoint executar_pipeline
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

    summary = {
        "motivo": motivo,
        "candidates": len(candidatos),
        "novos": novos,
        "promovidos": len(promovidos),
        "rebaixados": len(rebaixados),
        "inalterados": inalterados,
        "falhas": falhas,
        "promovidos_detalhe": promovidos[:20],
        "rebaixados_detalhe": rebaixados[:20],
    }
    logger.info(
        "orquestrador (%s): %d cand · novos=%d promovidos=%d rebaixados=%d inalterados=%d falhas=%d",
        motivo, len(candidatos), novos, len(promovidos), len(rebaixados), inalterados, falhas,
    )
    for p in promovidos[:5]:
        logger.info(
            "  ↑ %s: %s → %s (%d → %d)",
            p["email"], p["de"], p["para"], p["score_de"], p["score_para"],
        )
    return summary
