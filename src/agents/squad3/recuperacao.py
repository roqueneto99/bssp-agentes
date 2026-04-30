"""
Squad 3 — Agente 5: Recuperação.

Detecta abandono de fluxo e dispara sequência de retomada
cross-canal. Critério de desistência após 3 tentativas sem
resposta — neste caso o lead é devolvido a 'cold_recycle'
para reavaliação em 30-60 dias.

Em S1 implementa apenas a lógica de decisão: dada a telemetria
do lead, decide se entra em modo recuperação, qual sub-cadência
usa e qual canal alternativo tentar. A integração real com a
telemetria do SendGrid/Hablla entra em S7.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any

from ..base import AgentResult, LLMProvider

logger = logging.getLogger(__name__)


# Sub-cadência de recuperação (espelho do seed 'recuperacao_default').
RECUPERACAO_PASSOS = [
    {"ordem": 0, "dia": 1, "canal": "whatsapp", "nudge": "fricao",        "template_id": "rec_v1_step0"},
    {"ordem": 1, "dia": 3, "canal": "email",    "nudge": "loss_aversion", "template_id": "rec_v1_step1"},
    {"ordem": 2, "dia": 7, "canal": "whatsapp", "nudge": "ancoragem",     "template_id": "rec_v1_step2"},
]

# Após N tentativas de recuperação sem resposta, lead → cold_recycle.
LIMITE_TENTATIVAS = 3


class RecuperacaoAgent:
    """
    Detecta abandono e dispara retomada cross-canal.
    """

    agent_name = "squad3_recuperacao"

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
        telemetria_lead: dict | None = None,
        estado_lead: dict | None = None,
    ) -> AgentResult:
        """
        Args:
            email: lead.
            telemetria_lead: {abriu, clicou, respondeu, abandonou_pagina_matricula,
                              dias_sem_resposta, msgs_recuperacao_enviadas}.
            estado_lead: estado atual do lead (s3_status etc.).

        Returns:
            data com {decisao, motivo, proximo_passo?, devolver_para}.
            decisao ∈ {nao_recuperar, iniciar_recuperacao, continuar_recuperacao,
                       desistir_devolver_cold}.
        """
        start = time.monotonic()
        try:
            tel = telemetria_lead or {}
            est = estado_lead or {}

            # Já estamos em modo recuperação — decidir continuar ou desistir
            if (est.get("s3_status") or "").lower() == "recuperacao":
                tentativas = int(est.get("msgs_recuperacao_enviadas", 0))
                if tentativas >= LIMITE_TENTATIVAS:
                    return AgentResult(
                        success=True,
                        agent_name=self.agent_name,
                        contact_email=email,
                        data={
                            "decisao": "desistir_devolver_cold",
                            "motivo": f"sem_resposta_apos_{tentativas}_tentativas",
                            "devolver_para": "cold_recycle",
                            "reavaliar_em_dias": 30,
                        },
                        duration_ms=(time.monotonic() - start) * 1000,
                    )
                proximo = dict(RECUPERACAO_PASSOS[tentativas])
                proximo["cadencia_nome"] = "recuperacao_default"
                return AgentResult(
                    success=True,
                    agent_name=self.agent_name,
                    contact_email=email,
                    data={
                        "decisao": "continuar_recuperacao",
                        "tentativas": tentativas,
                        "proximo_passo": proximo,
                    },
                    duration_ms=(time.monotonic() - start) * 1000,
                )

            # Ainda não estamos em recuperação — checar se deve iniciar
            motivo = self._motivo_para_iniciar(tel)
            if not motivo:
                return AgentResult(
                    success=True,
                    agent_name=self.agent_name,
                    contact_email=email,
                    data={"decisao": "nao_recuperar", "motivo": "sem_sinais_abandono"},
                    duration_ms=(time.monotonic() - start) * 1000,
                )

            proximo = dict(RECUPERACAO_PASSOS[0])
            proximo["cadencia_nome"] = "recuperacao_default"
            logger.info(
                "Recuperação — iniciando para %s: motivo=%s",
                email, motivo,
            )
            return AgentResult(
                success=True,
                agent_name=self.agent_name,
                contact_email=email,
                data={
                    "decisao": "iniciar_recuperacao",
                    "motivo": motivo,
                    "proximo_passo": proximo,
                    "iniciado_em": datetime.now(tz=timezone.utc).isoformat(),
                },
                duration_ms=(time.monotonic() - start) * 1000,
            )

        except Exception as e:
            logger.error("Recuperação falhou para %s: %s", email, e)
            return AgentResult(
                success=False,
                agent_name=self.agent_name,
                contact_email=email,
                error=str(e),
                duration_ms=(time.monotonic() - start) * 1000,
            )

    # -----------------------------------------------------------------
    # Heurísticas de detecção de abandono
    # -----------------------------------------------------------------

    def _motivo_para_iniciar(self, tel: dict) -> str | None:
        if tel.get("abandonou_pagina_matricula"):
            return "abandonou_pagina_matricula"
        # Abriu mas não clicou em > 72h
        if tel.get("abriu") and not tel.get("clicou"):
            if (tel.get("dias_sem_resposta") or 0) >= 3:
                return "abriu_sem_clicar_3d"
        # Clicou mas não respondeu em > 72h
        if tel.get("clicou") and not tel.get("respondeu"):
            if (tel.get("dias_sem_resposta") or 0) >= 3:
                return "clicou_sem_responder_3d"
        # Inativo total >= 7 dias
        if (tel.get("dias_sem_resposta") or 0) >= 7:
            return "inativo_7d"
        return None
