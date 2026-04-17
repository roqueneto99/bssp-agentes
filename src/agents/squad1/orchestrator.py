"""
Squad 1 — Orquestrador.

Executa os 4 agentes da Squad 1 em sequência:
    Coletor → Enriquecedor → Deduplicador → Conformidade LGPD

Regras de fluxo:
- Se o Coletor falhar → pipeline para (sem dados, não tem como continuar)
- Se o Enriquecedor falhar → continua com dados brutos do Coletor
- Se o Deduplicador falhar → continua (deduplicação é "nice to have")
- Se o Conformidade detectar "revogado" → pipeline para com flag de bloqueio
- O resultado final é o agregado de todos os agentes

O orquestrador passa o perfil do Coletor para os agentes seguintes,
evitando chamadas repetidas à API do RD Station.
"""

from __future__ import annotations

import logging
import time
from typing import Optional

from ..base import AgentResult, LLMProvider
from .coletor import ColetorAgent
from .enriquecedor import EnriquecedorAgent
from .deduplicador import DeduplicadorAgent
from .conformidade import ConformidadeAgent

logger = logging.getLogger(__name__)


class Squad1Orchestrator:
    """
    Orquestra a execução sequencial dos 4 agentes da Squad 1.

    Uso:
        orchestrator = Squad1Orchestrator(llm=provider, rdstation=client)
        results = await orchestrator.execute(
            email="lead@email.com",
            conversion_identifier="formulario_mba",
        )

    O resultado é uma lista de AgentResult (um por agente executado).
    O último elemento é sempre um "resumo" com o status consolidado.
    """

    def __init__(
        self,
        llm: LLMProvider,
        rdstation,  # RDStationClient
        *,
        skip_dedup: bool = False,
        skip_lgpd: bool = False,
    ) -> None:
        self.llm = llm
        self.rdstation = rdstation
        self.skip_dedup = skip_dedup
        self.skip_lgpd = skip_lgpd

        # Inicializa agentes
        self.coletor = ColetorAgent(llm=llm, rdstation=rdstation)
        self.enriquecedor = EnriquecedorAgent(llm=llm, rdstation=rdstation)
        self.deduplicador = DeduplicadorAgent(llm=llm, rdstation=rdstation)
        self.conformidade = ConformidadeAgent(llm=llm, rdstation=rdstation)

    async def execute(
        self,
        email: str,
        *,
        conversion_identifier: Optional[str] = None,
        webhook_data: Optional[dict] = None,
        segmentation_id: Optional[int] = None,
    ) -> list[AgentResult]:
        """
        Executa o pipeline completo da Squad 1.

        Args:
            email: Email do lead
            conversion_identifier: Identificador da conversão (formulário, etc.)
            webhook_data: Dados brutos do webhook (opcional)
            segmentation_id: ID de segmentação para buscar candidatos de dedup

        Returns:
            Lista de AgentResult (um por agente + resumo final)
        """
        results: list[AgentResult] = []
        pipeline_start = time.monotonic()
        bloqueado = False

        logger.info(
            "Squad 1 iniciando para %s (conversão: %s)",
            email, conversion_identifier or "não informada",
        )

        # =====================================================================
        # ETAPA 1: COLETOR
        # =====================================================================
        result_coletor = await self.coletor.run(
            email,
            conversion_identifier=conversion_identifier,
            webhook_data=webhook_data,
        )
        results.append(result_coletor)

        if not result_coletor.success:
            logger.error(
                "Squad 1 — Coletor falhou para %s: %s. Pipeline interrompido.",
                email, result_coletor.error,
            )
            results.append(self._resumo(email, results, pipeline_start, bloqueado=False))
            return results

        perfil_coletor = result_coletor.data
        completude = perfil_coletor.get("completude_perfil", 0)
        logger.info(
            "Squad 1 — Coletor OK para %s: completude=%.0f%%, "
            "conversões=%d, fonte=%s",
            email,
            completude * 100,
            perfil_coletor.get("metricas_engajamento", {}).get("total_conversoes", 0),
            perfil_coletor.get("fonte_origem", "?"),
        )

        # =====================================================================
        # ETAPA 2: ENRIQUECEDOR
        # =====================================================================
        result_enriquecedor = await self.enriquecedor.run(
            email,
            perfil_coletor=perfil_coletor,
        )
        results.append(result_enriquecedor)

        if result_enriquecedor.success:
            temperatura = result_enriquecedor.data.get("analysis", {}).get("temperatura", "?")
            prioridade = result_enriquecedor.data.get("analysis", {}).get("prioridade_contato", "?")
            logger.info(
                "Squad 1 — Enriquecedor OK para %s: temp=%s, prioridade=%s",
                email, temperatura, prioridade,
            )
        else:
            logger.warning(
                "Squad 1 — Enriquecedor falhou para %s: %s (continuando pipeline)",
                email, result_enriquecedor.error,
            )

        # =====================================================================
        # ETAPA 3: DEDUPLICADOR (opcional)
        # =====================================================================
        if not self.skip_dedup:
            result_dedup = await self.deduplicador.run(
                email,
                perfil_coletor=perfil_coletor,
                segmentation_id=segmentation_id,
            )
            results.append(result_dedup)

            if result_dedup.success:
                n_dupes = len(result_dedup.data.get("duplicados_encontrados", []))
                logger.info(
                    "Squad 1 — Deduplicador OK para %s: %d duplicados",
                    email, n_dupes,
                )
            else:
                logger.warning(
                    "Squad 1 — Deduplicador falhou para %s: %s (continuando)",
                    email, result_dedup.error,
                )
        else:
            logger.info("Squad 1 — Deduplicador pulado (skip_dedup=True)")

        # =====================================================================
        # ETAPA 4: CONFORMIDADE LGPD (opcional)
        # =====================================================================
        if not self.skip_lgpd:
            result_lgpd = await self.conformidade.run(
                email,
                perfil_coletor=perfil_coletor,
            )
            results.append(result_lgpd)

            if result_lgpd.success:
                compliance = result_lgpd.data.get("compliance_status", "?")
                pode_comunicar = result_lgpd.data.get("pode_comunicar", False)
                bloqueado = result_lgpd.data.get("bloqueio_pipeline", False)

                logger.info(
                    "Squad 1 — Conformidade OK para %s: status=%s, "
                    "pode_comunicar=%s, bloqueio=%s",
                    email, compliance, pode_comunicar, bloqueado,
                )

                if bloqueado:
                    logger.warning(
                        "Squad 1 — Lead %s BLOQUEADO pela conformidade LGPD "
                        "(consentimento revogado). Não seguirá para Squad 2.",
                        email,
                    )
            else:
                logger.warning(
                    "Squad 1 — Conformidade falhou para %s: %s (continuando)",
                    email, result_lgpd.error,
                )
        else:
            logger.info("Squad 1 — Conformidade pulada (skip_lgpd=True)")

        # =====================================================================
        # RESUMO FINAL
        # =====================================================================
        resumo = self._resumo(email, results, pipeline_start, bloqueado)
        results.append(resumo)

        total_ms = resumo.duration_ms
        logger.info(
            "Squad 1 concluído para %s: %d agentes, %.0fms total, "
            "bloqueado=%s",
            email, len(results) - 1, total_ms, bloqueado,
        )

        return results

    def _resumo(
        self,
        email: str,
        results: list[AgentResult],
        start_time: float,
        bloqueado: bool,
    ) -> AgentResult:
        """Gera resultado resumo da execução completa da Squad 1."""
        total_ms = (time.monotonic() - start_time) * 1000
        ok = sum(1 for r in results if r.success)
        falhas = sum(1 for r in results if not r.success)

        # Agregar dados relevantes
        dados_agregados = {
            "squad": "squad1",
            "agentes_executados": len(results),
            "agentes_ok": ok,
            "agentes_falha": falhas,
            "bloqueado_lgpd": bloqueado,
        }

        # Extrair dados do Coletor
        for r in results:
            if r.agent_name == "squad1_coletor" and r.success:
                dados_agregados["completude_perfil"] = r.data.get("completude_perfil", 0)
                dados_agregados["fonte_origem"] = r.data.get("fonte_origem", "")
                dados_agregados["metricas_engajamento"] = r.data.get("metricas_engajamento", {})

        # Extrair dados do Enriquecedor
        for r in results:
            if r.agent_name == "squad1_enriquecedor" and r.success:
                analysis = r.data.get("analysis", {})
                dados_agregados["temperatura"] = analysis.get("temperatura", "")
                dados_agregados["area_principal"] = analysis.get("area_principal", "")
                dados_agregados["prioridade_contato"] = analysis.get("prioridade_contato", "")
                dados_agregados["proximo_passo"] = analysis.get("proximo_passo", "")

        # Extrair dados do Deduplicador
        for r in results:
            if r.agent_name == "squad1_deduplicador" and r.success:
                dados_agregados["duplicados_encontrados"] = len(
                    r.data.get("duplicados_encontrados", [])
                )

        # Extrair dados do Conformidade
        for r in results:
            if r.agent_name == "squad1_conformidade" and r.success:
                dados_agregados["compliance_status"] = r.data.get("compliance_status", "")
                dados_agregados["pode_comunicar"] = r.data.get("pode_comunicar", False)

        # Decidir se o lead pode seguir para o Squad 2
        pode_seguir = (
            not bloqueado
            and any(r.agent_name == "squad1_coletor" and r.success for r in results)
        )
        dados_agregados["pode_seguir_squad2"] = pode_seguir

        return AgentResult(
            success=pode_seguir,
            agent_name="squad1_resumo",
            contact_email=email,
            data=dados_agregados,
            duration_ms=total_ms,
        )
