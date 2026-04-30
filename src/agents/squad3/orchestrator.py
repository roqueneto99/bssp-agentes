"""
Squad 3 — Orquestrador.

Executa os 5 agentes da Squad 3 em sequência:
    1. Engajamento Progressivo  — decide o próximo passo da cadência
    2. Recuperação              — decide se entra em modo retomada
    3. Personalização           — escolhe nudge e renderiza mensagem
    4. Multicanal               — despacha (S1: dry-run)
    5. Qualificação Conversacional — processa resposta do lead, se houver

Ordem racional:
    - Recuperação tem prioridade sobre Engajamento Progressivo
      (se lead abandonou, troca a cadência atual pela de retomada).
    - Personalização só roda se houver passo a executar.
    - Multicanal não roda se Personalização falhou ou se mensagem foi 'skipped'.
    - Qualificação Conversacional roda se houver `resposta` no input
      (ex.: webhook chegou junto da rodada do pipeline).

Regras de fluxo:
    - Se rota for sql_handoff / blocked / cliente_existente → não há ação
      (pode_seguir_squad4 ou nenhuma).
    - Se Squad 2 reportou pode_seguir_squad3=False → orchestrator devolve
      'nada_a_fazer' sem chamar agentes.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from ..base import AgentResult, LLMProvider
from .engajamento_progressivo import EngajamentoProgressivoAgent
from .multicanal import MulticanalAgent
from .personalizacao import PersonalizacaoComportamentalAgent
from .qualificador_conversacional import QualificadorConversacionalAgent
from .recuperacao import RecuperacaoAgent

logger = logging.getLogger(__name__)


class Squad3Orchestrator:
    """
    Orquestra a execução dos 5 agentes da Squad 3.

    Uso:
        orchestrator = Squad3Orchestrator(
            llm=provider,
            rdstation=rd_client,
            hablla=hablla_client,
            sendgrid=None,            # S2 conecta
            dry_run=True,             # S1: sem disparar provedor
        )
        results = await orchestrator.execute(
            email="lead@email.com",
            perfil_squad1=...,
            perfil_squad2=...,
            estado_lead=...,          # vindo do banco (S1: dict vazio)
            telemetria_lead=...,      # eventos open/click/reply (S1: dict vazio)
            resposta=...,             # se houver msg do lead a processar
        )
    """

    def __init__(
        self,
        llm: LLMProvider,
        rdstation: Any,
        hablla: Any = None,
        sendgrid: Any = None,
        mensagens_repo: Any = None,
        dry_run: bool = True,
    ) -> None:
        self.llm = llm
        self.rdstation = rdstation
        self.hablla = hablla
        self.sendgrid = sendgrid
        self.mensagens_repo = mensagens_repo
        self.dry_run = dry_run

        self.engajamento = EngajamentoProgressivoAgent(
            llm=llm, rdstation=rdstation, hablla=hablla,
        )
        self.recuperacao = RecuperacaoAgent(
            llm=llm, rdstation=rdstation, hablla=hablla,
        )
        self.personalizacao = PersonalizacaoComportamentalAgent(
            llm=llm, rdstation=rdstation, hablla=hablla,
        )
        self.multicanal = MulticanalAgent(
            llm=llm, rdstation=rdstation, hablla=hablla,
            sendgrid=sendgrid, mensagens_repo=mensagens_repo,
            dry_run=dry_run,
        )
        self.qualificador = QualificadorConversacionalAgent(
            llm=llm, rdstation=rdstation, hablla=hablla,
        )

    async def execute(
        self,
        email: str,
        *,
        perfil_squad1: dict | None = None,
        perfil_squad2: dict | None = None,
        estado_lead: dict | None = None,
        telemetria_lead: dict | None = None,
        resposta: dict | None = None,
    ) -> list[AgentResult]:
        results: list[AgentResult] = []
        pipeline_start = time.monotonic()

        logger.info("Squad 3 iniciando para %s (dry_run=%s)", email, self.dry_run)

        # =====================================================================
        # GATE: pode rodar?
        # =====================================================================
        if not self._pode_rodar(perfil_squad2 or {}):
            resumo = self._resumo(
                email, results, pipeline_start,
                decisao="nada_a_fazer",
                motivo=self._motivo_pular(perfil_squad2 or {}),
            )
            results.append(resumo)
            logger.info("Squad 3 pulado para %s: %s", email, resumo.data.get("motivo"))
            return results

        # =====================================================================
        # ETAPA 1: RECUPERAÇÃO (decide se sobrescreve a cadência atual)
        # =====================================================================
        result_rec = await self.recuperacao.run(
            email,
            telemetria_lead=telemetria_lead,
            estado_lead=estado_lead,
        )
        results.append(result_rec)

        recuperacao_data = result_rec.data if result_rec.success else {}
        em_recuperacao = recuperacao_data.get("decisao") in (
            "iniciar_recuperacao", "continuar_recuperacao",
        )
        desistir = recuperacao_data.get("decisao") == "desistir_devolver_cold"

        # =====================================================================
        # ETAPA 2: ENGAJAMENTO PROGRESSIVO (cadência regular)
        # =====================================================================
        result_eng = await self.engajamento.run(
            email,
            perfil_squad2=perfil_squad2,
            estado_lead=estado_lead,
        )
        results.append(result_eng)

        # Decide qual passo usar (recuperação tem prioridade)
        passo_a_executar: dict | None = None
        if em_recuperacao:
            passo_a_executar = recuperacao_data.get("proximo_passo")
        elif result_eng.success:
            decisao = result_eng.data.get("decisao")
            if decisao in ("primeira_msg", "proxima_msg"):
                passo_a_executar = result_eng.data.get("proximo_passo")

        # =====================================================================
        # ETAPA 3 + 4: PERSONALIZAÇÃO + MULTICANAL (só se há passo)
        # =====================================================================
        if passo_a_executar and not desistir:
            result_pers = await self.personalizacao.run(
                email,
                passo_cadencia=passo_a_executar,
                perfil_squad1=perfil_squad1,
                perfil_squad2=perfil_squad2,
            )
            results.append(result_pers)

            if result_pers.success:
                result_mc = await self.multicanal.run(
                    email,
                    mensagem=result_pers.data,
                    perfil_squad2=perfil_squad2,
                )
                results.append(result_mc)
            else:
                logger.warning(
                    "Squad 3 — Personalização falhou para %s, multicanal pulado",
                    email,
                )

        # =====================================================================
        # ETAPA 5: QUALIFICAÇÃO CONVERSACIONAL (se houver resposta do lead)
        # =====================================================================
        if resposta:
            result_qc = await self.qualificador.run(
                email,
                resposta=resposta,
                perfil_squad2=perfil_squad2,
            )
            results.append(result_qc)

        # =====================================================================
        # RESUMO FINAL
        # =====================================================================
        resumo = self._resumo(
            email, results, pipeline_start,
            decisao=("desistir" if desistir else
                     "recuperacao" if em_recuperacao else
                     "cadencia_regular" if passo_a_executar else "sem_acao"),
            motivo=None,
        )
        results.append(resumo)

        logger.info(
            "Squad 3 concluído para %s: %d agentes, %.0fms total, decisao=%s",
            email, len(results) - 1, resumo.duration_ms,
            resumo.data.get("decisao_orchestrator"),
        )
        return results

    # -----------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------

    def _pode_rodar(self, perfil_squad2: dict) -> bool:
        if not perfil_squad2.get("pode_seguir_squad3", False):
            return False
        rota = perfil_squad2.get("rota")
        # Squad 3 só atua em mql/sal/cold + recuperação. SQL handoff vai para Squad 4.
        if rota in ("sql_handoff", "blocked", "cliente_existente"):
            return False
        return True

    def _motivo_pular(self, perfil_squad2: dict) -> str:
        rota = perfil_squad2.get("rota") or "?"
        if not perfil_squad2.get("pode_seguir_squad3", False):
            if perfil_squad2.get("classificacao") == "BLOCKED":
                return "lgpd_bloqueado"
            return f"squad2_nao_liberou:rota={rota}"
        return f"rota_para_outro_squad:{rota}"

    def _resumo(
        self,
        email: str,
        results: list[AgentResult],
        start_time: float,
        *,
        decisao: str,
        motivo: str | None,
    ) -> AgentResult:
        total_ms = (time.monotonic() - start_time) * 1000
        ok = sum(1 for r in results if r.success)
        falhas = sum(1 for r in results if not r.success)

        # Coletar a mensagem disparada (se existir)
        mensagem_data = next(
            (r.data for r in results
             if r.agent_name == "squad3_personalizacao" and r.success),
            None,
        )
        multicanal_data = next(
            (r.data for r in results
             if r.agent_name == "squad3_multicanal" and r.success),
            None,
        )
        qc_data = next(
            (r.data for r in results
             if r.agent_name == "squad3_qualificador_conversacional" and r.success),
            None,
        )

        dados: dict[str, Any] = {
            "squad": "squad3",
            "agentes_executados": len(results),
            "agentes_ok": ok,
            "agentes_falha": falhas,
            "decisao_orchestrator": decisao,
            "motivo": motivo,
            "dry_run": self.dry_run,
        }

        if mensagem_data:
            dados["mensagem"] = {
                "canal": mensagem_data.get("canal"),
                "nudge": mensagem_data.get("nudge"),
                "passo": mensagem_data.get("passo"),
                "cadencia_nome": mensagem_data.get("cadencia_nome"),
                "template_id": mensagem_data.get("template_id"),
                "prompt_hash": mensagem_data.get("prompt_hash"),
                "assunto": mensagem_data.get("assunto"),
                "razao": mensagem_data.get("razao"),
            }

        if multicanal_data:
            dados["envio"] = {
                "status": multicanal_data.get("status"),
                "external_id": multicanal_data.get("external_id"),
                "razao_skip": multicanal_data.get("razao_skip"),
            }

        if qc_data:
            dados["resposta_lead"] = {
                "intencao": qc_data.get("intencao"),
                "maturidade_bant": qc_data.get("maturidade_bant"),
                "requer_rescoring": qc_data.get("requer_rescoring", False),
                "acao_sugerida": qc_data.get("acao_sugerida"),
            }

        return AgentResult(
            success=falhas == 0 or decisao == "nada_a_fazer",
            agent_name="squad3_resumo",
            contact_email=email,
            data=dados,
            duration_ms=total_ms,
        )
