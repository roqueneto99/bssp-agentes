"""
Pipeline de agentes — orquestra a execução sequencial dos squads.

Fluxo de um lead novo:
    Webhook (conversão) → Squad 1 (Captura/Enriquecimento/Dedup/LGPD)
                        → Squad 2 (Scoring) → Squad 3 (Comunicação) → ...

Fluxo de oportunidade:
    Webhook (oportunidade) → Squad 4 (Handoff Comercial)

Cada squad recebe o resultado do anterior e decide se passa adiante.
"""

from __future__ import annotations

import logging
from typing import Optional

from .base import AgentResult, LLMProvider
from .squad1.orchestrator import Squad1Orchestrator
from .squad2.orchestrator import Squad2Orchestrator
from .squad3.orchestrator import Squad3Orchestrator

logger = logging.getLogger(__name__)


class AgentPipeline:
    """
    Orquestra a execução dos squads de agentes para um lead.

    Uso:
        pipeline = AgentPipeline(llm=provider, rdstation=client)
        results = await pipeline.process_new_lead(
            "lead@email.com",
            conversion_identifier="formulario_mba",
        )
    """

    def __init__(
        self,
        llm: LLMProvider,
        rdstation,  # RDStationClient
        hablla=None,  # HabllaClient (opcional)
        sendgrid=None,  # SendGridClient (S2 conecta)
        mensagens_repo=None,  # MensagensRepository | None (S2 conecta)
        squad3_dry_run: bool = True,  # S1: True. S2 em diante: False quando provedor pronto.
    ) -> None:
        self.llm = llm
        self.rdstation = rdstation
        self.hablla = hablla
        self.sendgrid = sendgrid
        self.mensagens_repo = mensagens_repo

        # Inicializa squads
        self.squad1 = Squad1Orchestrator(llm=llm, rdstation=rdstation)
        self.squad2 = Squad2Orchestrator(llm=llm, rdstation=rdstation, hablla=hablla)
        self.squad3 = Squad3Orchestrator(
            llm=llm, rdstation=rdstation, hablla=hablla,
            sendgrid=sendgrid, mensagens_repo=mensagens_repo,
            dry_run=squad3_dry_run,
        )
        # self.squad4 = HandoffOrchestrator(llm=llm, rdstation=rdstation)  # Fase 3

    async def process_new_lead(
        self,
        email: str,
        *,
        conversion_identifier: Optional[str] = None,
        webhook_data: Optional[dict] = None,
        segmentation_id: Optional[int] = None,
        # Squad 3 — opcionais (ainda não populados pelo painel em S1)
        estado_lead: Optional[dict] = None,
        telemetria_lead: Optional[dict] = None,
        resposta: Optional[dict] = None,
    ) -> list[AgentResult]:
        """
        Processa um lead novo que acabou de converter.
        Executa os squads em sequência.
        """
        all_results: list[AgentResult] = []

        logger.info(
            "Pipeline iniciado para %s (conversão: %s)",
            email, conversion_identifier or "não informada",
        )

        # --- Squad 1: Captura, Enriquecimento, Dedup, LGPD ---
        squad1_results = await self.squad1.execute(
            email,
            conversion_identifier=conversion_identifier,
            webhook_data=webhook_data,
            segmentation_id=segmentation_id,
        )
        all_results.extend(squad1_results)

        resumo_squad1 = next(
            (r for r in squad1_results if r.agent_name == "squad1_resumo"),
            None,
        )

        if resumo_squad1 and not resumo_squad1.data.get("pode_seguir_squad2", False):
            logger.warning(
                "Pipeline interrompido após Squad 1 para %s — "
                "bloqueado=%s, agentes_ok=%d, agentes_falha=%d",
                email,
                resumo_squad1.data.get("bloqueado_lgpd", False),
                resumo_squad1.data.get("agentes_ok", 0),
                resumo_squad1.data.get("agentes_falha", 0),
            )
            return all_results

        temperatura = resumo_squad1.data.get("temperatura", "") if resumo_squad1 else ""
        prioridade = resumo_squad1.data.get("prioridade_contato", "") if resumo_squad1 else ""
        logger.info(
            "Squad 1 concluído para %s: temp=%s, prioridade=%s — "
            "pronto para Squad 2",
            email, temperatura, prioridade,
        )

        # --- Squad 2: Qualificação & Scoring ---
        perfil_para_squad2 = dict(resumo_squad1.data) if resumo_squad1 else {}
        for r in squad1_results:
            if r.agent_name == "squad1_coletor" and r.success:
                perfil_para_squad2["dados_basicos"] = r.data.get("dados_basicos", {})
                perfil_para_squad2["interacoes_conteudo"] = r.data.get("interacoes_conteudo", {})
                perfil_para_squad2["funil"] = r.data.get("funil", {})
            if r.agent_name == "squad1_enriquecedor" and r.success:
                perfil_para_squad2["analysis"] = r.data.get("analysis", {})

        squad2_results = await self.squad2.execute(
            email,
            perfil_squad1=perfil_para_squad2,
        )
        all_results.extend(squad2_results)

        resumo_squad2 = next(
            (r for r in squad2_results if r.agent_name == "squad2_resumo"),
            None,
        )

        if resumo_squad2:
            logger.info(
                "Squad 2 concluído para %s: score=%d, rota=%s, pode_seguir_squad3=%s",
                email,
                resumo_squad2.data.get("score_total", 0),
                resumo_squad2.data.get("rota", "?"),
                resumo_squad2.data.get("pode_seguir_squad3", False),
            )

        # --- Squad 3: Comunicação Inteligente (S1: dry-run) ---
        if resumo_squad2 and resumo_squad2.data.get("pode_seguir_squad3", False):
            squad3_results = await self.squad3.execute(
                email,
                perfil_squad1=perfil_para_squad2,
                perfil_squad2=resumo_squad2.data,
                estado_lead=estado_lead or {},
                telemetria_lead=telemetria_lead or {},
                resposta=resposta,
            )
            all_results.extend(squad3_results)

            resumo_squad3 = next(
                (r for r in squad3_results if r.agent_name == "squad3_resumo"),
                None,
            )
            if resumo_squad3:
                logger.info(
                    "Squad 3 concluído para %s: decisao=%s, dry_run=%s",
                    email,
                    resumo_squad3.data.get("decisao_orchestrator"),
                    resumo_squad3.data.get("dry_run"),
                )

        total_ms = sum(r.duration_ms for r in all_results)
        logger.info(
            "Pipeline concluído para %s: %d agentes, %.0fms total",
            email, len(all_results), total_ms,
        )
        return all_results

    async def process_opportunity(self, email: str) -> list[AgentResult]:
        """
        Processa um lead marcado como oportunidade.
        Acionado pelo webhook WEBHOOK.MARKED_OPPORTUNITY.
        Vai direto ao Squad 4 (Handoff Comercial).
        """
        results: list[AgentResult] = []
        logger.info("Pipeline de oportunidade iniciado para %s", email)
        # --- Squad 4: Handoff Comercial (a implementar) ---
        logger.info(
            "Pipeline de oportunidade: Squad 4 ainda não implementado. "
            "Lead %s registrado para processamento futuro.", email,
        )
        return results
