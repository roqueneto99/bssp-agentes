"""
Squad 3 — Comunicação Inteligente.

Conduz o lead pelo funil com comunicação personalizada e
comportamental, a partir do score, da rota e dos sinais
produzidos pelo Squad 2.

Agentes:
    1. EngajamentoProgressivo       — escolhe próximo passo da cadência
    2. Recuperacao                  — detecta abandono e dispara retomada
    3. PersonalizacaoComportamental — escolhe nudge e renderiza mensagem
    4. Multicanal                   — despacha em e-mail / WhatsApp / SMS
    5. QualificadorConversacional   — NLU + BANT nas respostas do lead

Orquestrador:
    Squad3Orchestrator — Executa os 5 agentes em sequência (S1: dry-run).

Em S1, todos os agentes operam sem dependência externa:
    - Sem SendGrid (S2)
    - Sem WhatsApp Business / Hablla send (S5)
    - Sem LLM no Personalização (S3 conecta)
    - Sem webhooks de resposta (S6 conecta)
"""

from .engajamento_progressivo import EngajamentoProgressivoAgent
from .multicanal import MulticanalAgent
from .orchestrator import Squad3Orchestrator
from .personalizacao import PersonalizacaoComportamentalAgent
from .qualificador_conversacional import QualificadorConversacionalAgent
from .recuperacao import RecuperacaoAgent

__all__ = [
    "EngajamentoProgressivoAgent",
    "MulticanalAgent",
    "PersonalizacaoComportamentalAgent",
    "QualificadorConversacionalAgent",
    "RecuperacaoAgent",
    "Squad3Orchestrator",
]
