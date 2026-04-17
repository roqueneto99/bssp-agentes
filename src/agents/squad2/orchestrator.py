"""
Squad 2 — Orquestrador.

Executa os 3 agentes da Squad 2 em sequência:
    Analisador de Engajamento → Scorer → Classificador de Rota

Regras de fluxo:
- Se o Analisador falhar → Scorer usa dados do Squad 1 como fallback
- Se o Scorer falhar → Classificador aplica rota COLD por segurança
- Se o Classificador falhar → resultado registrado mas sem persistência

O orquestrador recebe os dados do Squad 1 e os passa entre agentes,
evitando chamadas repetidas às APIs.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Optional

from ..base import AgentResult, LLMProvider
from .analisador_engajamento import AnalisadorEngajamentoAgent
from .scorer import ScorerAgent
from .classificador_rota import ClassificadorRotaAgent

logger = logging.getLogger(__name__)


class Squad2Orchestrator:
    """
    Orquestra a execução sequencial dos 3 agentes da Squad 2.

    Uso:
        orchestrator = Squad2Orchestrator(
            llm=provider, rdstation=rd_client, hablla=hablla_client,
        )
        results = await orchestrator.execute(
            email="lead@email.com",
            perfil_squad1=squad1_result_data,
        )
    """

    def __init__(
        self,
        llm: LLMProvider,
        rdstation: Any,
        hablla: Any = None,
    ) -> None:
        self.llm = llm
        self.rdstation = rdstation
        self.hablla = hablla

        # Inicializa agentes
        self.analisador = AnalisadorEngajamentoAgent(
            llm=llm, rdstation=rdstation, hablla=hablla,
        )
        self.scorer = ScorerAgent(
            llm=llm, rdstation=rdstation, hablla=hablla,
        )
        self.classificador = ClassificadorRotaAgent(
            llm=llm, rdstation=rdstation, hablla=hablla,
        )

    async def execute(
        self,
        email: str,
        *,
        perfil_squad1: dict | None = None,
    ) -> list[AgentResult]:
        """
        Executa o pipeline completo da Squad 2.

        Args:
            email: Email do lead
            perfil_squad1: Dados agregados do Squad 1 (resumo de todos os agentes).
                          Se não fornecido, o Analisador buscará dados direto das APIs.

        Returns:
            Lista de AgentResult (um por agente + resumo final)
        """
        results: list[AgentResult] = []
        pipeline_start = time.monotonic()

        logger.info("Squad 2 iniciando para %s", email)

        # =====================================================================
        # ETAPA 1: ANALISADOR DE ENGAJAMENTO
        # =====================================================================
        result_engajamento = await self.analisador.run(
            email, perfil_squad1=perfil_squad1,
        )
        results.append(result_engajamento)

        engajamento_data = None
        if result_engajamento.success:
            engajamento_data = result_engajamento.data
            logger.info(
                "Squad 2 — Analisador OK para %s: score_total=%d, canais=%s",
                email,
                engajamento_data.get("score_engajamento_total", 0),
                engajamento_data.get("canais_ativos", []),
            )
        else:
            logger.warning(
                "Squad 2 — Analisador falhou para %s: %s (Scorer usará fallback)",
                email, result_engajamento.error,
            )

        # =====================================================================
        # ETAPA 2: SCORER DE QUALIFICAÇÃO
        # =====================================================================
        result_scorer = await self.scorer.run(
            email,
            perfil_squad1=perfil_squad1,
            engajamento=engajamento_data,
        )
        results.append(result_scorer)

        scoring_data = None
        if result_scorer.success:
            scoring_data = result_scorer.data
            logger.info(
                "Squad 2 — Scorer OK para %s: score=%d, classificacao=%s",
                email,
                scoring_data.get("score_total", 0),
                scoring_data.get("classificacao", "?"),
            )
        else:
            logger.warning(
                "Squad 2 — Scorer falhou para %s: %s (Classificador usará rota COLD)",
                email, result_scorer.error,
            )

        # =====================================================================
        # ETAPA 3: CLASSIFICADOR DE ROTA
        # =====================================================================
        result_classificador = await self.classificador.run(
            email,
            scoring=scoring_data,
            perfil_squad1=perfil_squad1,
        )
        results.append(result_classificador)

        if result_classificador.success:
            rota = result_classificador.data.get("rota", "?")
            logger.info(
                "Squad 2 — Classificador OK para %s: rota=%s",
                email, rota,
            )
        else:
            logger.warning(
                "Squad 2 — Classificador falhou para %s: %s",
                email, result_classificador.error,
            )

        # =====================================================================
        # RESUMO FINAL
        # =====================================================================
        resumo = self._resumo(email, results, pipeline_start, scoring_data, result_classificador)
        results.append(resumo)

        total_ms = resumo.duration_ms
        logger.info(
            "Squad 2 concluído para %s: %d agentes, %.0fms total, rota=%s",
            email, len(results) - 1, total_ms,
            resumo.data.get("rota", "?"),
        )

        return results

    def _resumo(
        self,
        email: str,
        results: list[AgentResult],
        start_time: float,
        scoring_data: dict | None,
        result_classificador: AgentResult,
    ) -> AgentResult:
        """Gera resultado resumo da execução completa da Squad 2."""
        total_ms = (time.monotonic() - start_time) * 1000
        ok = sum(1 for r in results if r.success)
        falhas = sum(1 for r in results if not r.success)

        dados: dict[str, Any] = {
            "squad": "squad2",
            "agentes_executados": len(results),
            "agentes_ok": ok,
            "agentes_falha": falhas,
        }

        # Dados do scoring
        if scoring_data:
            dados["score_total"] = scoring_data.get("score_total", 0)
            dados["classificacao"] = scoring_data.get("classificacao", "COLD")
            dados["dimensoes"] = scoring_data.get("dimensoes", {})
            dados["resumo_scoring"] = scoring_data.get("resumo", "")

        # Dados da rota
        if result_classificador.success:
            cd = result_classificador.data
            dados["rota"] = cd.get("rota", "cold_recycle")
            dados["acoes_recomendadas"] = cd.get("acoes_recomendadas", [])
            dados["briefing_comercial"] = cd.get("briefing_comercial")
            dados["tags_aplicadas"] = cd.get("tags_aplicadas", [])
            dados["persistencia"] = cd.get("persistencia", {})
        else:
            dados["rota"] = "cold_recycle"
            dados["acoes_recomendadas"] = []

        # Decidir se o lead pode seguir para o Squad 3
        rota = dados.get("rota", "cold_recycle")
        pode_seguir = rota in ("sql_handoff", "mql_nurture", "sal_nurture")
        dados["pode_seguir_squad3"] = pode_seguir

        return AgentResult(
            success=ok >= 2,  # pelo menos 2 de 3 agentes com sucesso
            agent_name="squad2_resumo",
            contact_email=email,
            data=dados,
            duration_ms=total_ms,
        )
