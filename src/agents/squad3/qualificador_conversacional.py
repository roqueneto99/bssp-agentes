"""
Squad 3 — Agente 2: Qualificação Conversacional.

Migrado do Squad 2 (originalmente planejado lá no plano v3.0).
Lê respostas do lead vindas de e-mail/WhatsApp e:
    - extrai intenção via NLU (LLM Claude Sonnet)
    - aplica BANT adaptado (Budget, Authority, Need, Timeline)
    - gera lista de sinais comportamentais discretos
    - decide se o lead deve ser reciclado para o Squad 2
      (rescoring) ou promovido a SQL.

Em S1 opera apenas como contrato — recebe um payload de resposta
do lead, devolve uma análise mock determinística e marca
requer_rescoring corretamente. A integração via webhooks do
Hablla / SendGrid Inbound entra na sprint S6.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from ..base import AgentResult, LLMProvider

logger = logging.getLogger(__name__)


# Sinais que disparam reciclagem para o Squad 2
SINAIS_DISPARO_RESCORING = {
    "interesse_alto_explicitado",
    "pediu_atendimento_humano",
    "pediu_inscricao",
    "objecao_preco",
    "objecao_tempo",
}


class QualificadorConversacionalAgent:
    """
    NLU + BANT adaptado nas respostas do lead.

    Em S1: implementação mock. As próximas sprints (S6) ligam
    o LLM real e os webhooks de resposta.
    """

    agent_name = "squad3_qualificador_conversacional"

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
        resposta: dict | None = None,
        perfil_squad2: dict | None = None,
    ) -> AgentResult:
        """
        Args:
            email: Email do lead.
            resposta: dict com {canal, conteudo, mensagem_id, recebida_em}.
            perfil_squad2: resumo do Squad 2 (rota atual, score).

        Returns:
            AgentResult.data com:
                - intencao: str
                - maturidade_bant: int (1..5)
                - sinais_extraidos: list[str]
                - requer_rescoring: bool
                - acao_sugerida: str
        """
        start = time.monotonic()
        try:
            if not resposta or not resposta.get("conteudo"):
                # Sem resposta para analisar — passa adiante.
                return AgentResult(
                    success=True,
                    agent_name=self.agent_name,
                    contact_email=email,
                    data={"sem_resposta": True, "requer_rescoring": False},
                    duration_ms=(time.monotonic() - start) * 1000,
                )

            analise = await self._analisar(resposta)
            requer_rescoring = bool(
                set(analise.get("sinais_extraidos", []))
                & SINAIS_DISPARO_RESCORING
            )
            analise["requer_rescoring"] = requer_rescoring

            logger.info(
                "Qualificador Conversacional para %s: intencao=%s, BANT=%s, rescoring=%s",
                email,
                analise.get("intencao"),
                analise.get("maturidade_bant"),
                requer_rescoring,
            )

            return AgentResult(
                success=True,
                agent_name=self.agent_name,
                contact_email=email,
                data=analise,
                duration_ms=(time.monotonic() - start) * 1000,
            )

        except Exception as e:
            logger.error("Qualificador Conversacional falhou para %s: %s", email, e)
            return AgentResult(
                success=False,
                agent_name=self.agent_name,
                contact_email=email,
                error=str(e),
                duration_ms=(time.monotonic() - start) * 1000,
            )

    # -----------------------------------------------------------------
    # NLU — implementação mock em S1
    # -----------------------------------------------------------------

    async def _analisar(self, resposta: dict) -> dict:
        """
        S1: análise por keywords. S6 substitui por chamada ao Claude
        com prompt BANT adaptado e extração estruturada.
        """
        texto = (resposta.get("conteudo") or "").lower()

        intencao = "nao_classificado"
        sinais: list[str] = []
        maturidade = 1

        if any(k in texto for k in ("preço", "preco", "investimento", "valor", "mensalidade")):
            intencao = "objecao_preco"
            sinais.append("objecao_preco")
            maturidade = 3
        elif any(k in texto for k in ("turma", "começa", "comeca", "início", "inicio", "data")):
            intencao = "duvida_turma"
            sinais.append("interesse_alto_explicitado")
            maturidade = 4
        elif any(k in texto for k in ("falar", "ligar", "consultor", "atendente")):
            intencao = "pediu_humano"
            sinais.append("pediu_atendimento_humano")
            maturidade = 5
        elif any(k in texto for k in ("inscrição", "inscricao", "matrícula", "matricula")):
            intencao = "pediu_inscricao"
            sinais.append("pediu_inscricao")
            maturidade = 5
        elif any(k in texto for k in ("não", "nao tenho tempo", "depois", "futuro")):
            intencao = "objecao_tempo"
            sinais.append("objecao_tempo")
            maturidade = 2

        return {
            "intencao": intencao,
            "maturidade_bant": maturidade,
            "sinais_extraidos": sinais,
            "acao_sugerida": (
                "promover_a_SQL" if maturidade >= 4 else
                "manter_cadencia" if maturidade >= 2 else
                "reduzir_intensidade"
            ),
            "metodo": "keyword_v1",  # S6: trocará para "llm_bant_v1"
        }
