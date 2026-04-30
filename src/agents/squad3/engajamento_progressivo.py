"""
Squad 3 — Agente 4: Engajamento Progressivo.

Implementa as cadências como máquinas de estado.
Recebe a rota produzida pelo Squad 2, identifica a cadência
correspondente (mql_nurture, sal_nurture, cold_recycle) e decide
o próximo passo a executar.

Em S1 a fonte de verdade das cadências é a tabela `cadencias`
(populada pela migration 002). Como esta sprint roda em dry-run
sem dependência de DB, o agente carrega cadências em memória a
partir de seeds embutidos — espelho do INSERT da migration.

Lógica:
    - Se lead nunca recebeu mensagem desta cadência → retorna passo 0.
    - Se já recebeu N passos → retorna passo N (se ainda houver).
    - Se cadência terminou → marca como concluído e devolve None.
    - Se rota for 'sql_handoff' ou 'cliente_existente' → não há cadência (Squad 4 / nada).
"""

from __future__ import annotations

import logging
import time
from typing import Any

from ..base import AgentResult, LLMProvider

logger = logging.getLogger(__name__)


# Espelho dos seeds da migration 002.
CADENCIAS_SEED: dict[str, dict] = {
    "mql_nurture": {
        "rota": "mql_nurture",
        "janela_total_d": 10,
        "passos": [
            {"ordem": 0, "dia": 0,  "canal": "email",    "nudge": "boas_vindas",   "template_id": "mql_v1_step0"},
            {"ordem": 1, "dia": 2,  "canal": "email",    "nudge": "prova_social",  "template_id": "mql_v1_step1"},
            {"ordem": 2, "dia": 4,  "canal": "whatsapp", "nudge": "depoimento",    "template_id": "mql_v1_step2"},
            {"ordem": 3, "dia": 7,  "canal": "email",    "nudge": "escassez",      "template_id": "mql_v1_step3"},
            {"ordem": 4, "dia": 10, "canal": "whatsapp", "nudge": "fechamento",    "template_id": "mql_v1_step4"},
        ],
    },
    "sal_nurture": {
        "rota": "sal_nurture",
        "janela_total_d": 21,
        "passos": [
            {"ordem": 0, "dia": 0,  "canal": "email", "nudge": "educativa",    "template_id": "sal_v1_step0"},
            {"ordem": 1, "dia": 7,  "canal": "email", "nudge": "prova_social", "template_id": "sal_v1_step1"},
            {"ordem": 2, "dia": 14, "canal": "email", "nudge": "ancoragem",    "template_id": "sal_v1_step2"},
            {"ordem": 3, "dia": 21, "canal": "email", "nudge": "escassez",     "template_id": "sal_v1_step3"},
        ],
    },
    "cold_recycle": {
        "rota": "cold_recycle",
        "janela_total_d": 60,
        "passos": [
            {"ordem": 0, "dia": 30, "canal": "email", "nudge": "loss_aversion", "template_id": "cold_v1_step0"},
            {"ordem": 1, "dia": 60, "canal": "email", "nudge": "ancoragem",     "template_id": "cold_v1_step1"},
        ],
    },
}


CADENCIA_POR_ROTA = {
    "mql_nurture": "mql_nurture",
    "sal_nurture": "sal_nurture",
    "cold_recycle": "cold_recycle",
}


class EngajamentoProgressivoAgent:
    """
    Decide o próximo passo da cadência. Em S1 sem DB:
    estado é passado pelo orchestrator (msgs_enviadas).
    """

    agent_name = "squad3_engajamento_progressivo"

    def __init__(
        self,
        llm: LLMProvider,
        rdstation: Any,
        hablla: Any = None,
    ) -> None:
        self.llm = llm
        self.rdstation = rdstation
        self.hablla = hablla

    async def run(
        self,
        email: str,
        *,
        perfil_squad2: dict | None = None,
        estado_lead: dict | None = None,
    ) -> AgentResult:
        """
        Args:
            email: lead.
            perfil_squad2: resumo do Squad 2 (rota, score, sinais).
            estado_lead: dict {cadencia_atual, msgs_enviadas, ultimo_passo}
                         (S1: vem do orchestrator; S2+: virá do banco).

        Returns:
            data com {cadencia_nome, proximo_passo, terminou, motivo, decisao}.
            decisao ∈ {sem_cadencia, primeira_msg, proxima_msg, concluido}.
        """
        start = time.monotonic()
        try:
            rota = (perfil_squad2 or {}).get("rota") or "cold_recycle"
            cadencia_nome = CADENCIA_POR_ROTA.get(rota)

            if not cadencia_nome:
                # Rotas sem cadência (sql_handoff, cliente_existente, blocked)
                return AgentResult(
                    success=True,
                    agent_name=self.agent_name,
                    contact_email=email,
                    data={
                        "decisao": "sem_cadencia",
                        "motivo": f"rota={rota}",
                    },
                    duration_ms=(time.monotonic() - start) * 1000,
                )

            cadencia = CADENCIAS_SEED[cadencia_nome]
            estado = estado_lead or {}
            msgs_enviadas = int(estado.get("msgs_enviadas", 0))
            cadencia_em_uso = estado.get("cadencia_atual")

            if cadencia_em_uso and cadencia_em_uso != cadencia_nome:
                # Trocou de rota (squad 2 reclassificou) — começa do zero na nova cadência
                msgs_enviadas = 0

            passos = cadencia["passos"]
            if msgs_enviadas >= len(passos):
                return AgentResult(
                    success=True,
                    agent_name=self.agent_name,
                    contact_email=email,
                    data={
                        "decisao": "concluido",
                        "cadencia_nome": cadencia_nome,
                        "msgs_enviadas": msgs_enviadas,
                    },
                    duration_ms=(time.monotonic() - start) * 1000,
                )

            proximo = dict(passos[msgs_enviadas])
            proximo["cadencia_nome"] = cadencia_nome
            decisao = "primeira_msg" if msgs_enviadas == 0 else "proxima_msg"

            logger.info(
                "Engajamento Progressivo para %s: cadencia=%s, passo=%d, decisao=%s",
                email, cadencia_nome, proximo["ordem"], decisao,
            )

            return AgentResult(
                success=True,
                agent_name=self.agent_name,
                contact_email=email,
                data={
                    "decisao": decisao,
                    "cadencia_nome": cadencia_nome,
                    "proximo_passo": proximo,
                    "msgs_enviadas": msgs_enviadas,
                    "total_passos": len(passos),
                },
                duration_ms=(time.monotonic() - start) * 1000,
            )

        except Exception as e:
            logger.error("Engajamento Progressivo falhou para %s: %s", email, e)
            return AgentResult(
                success=False,
                agent_name=self.agent_name,
                contact_email=email,
                error=str(e),
                duration_ms=(time.monotonic() - start) * 1000,
            )
