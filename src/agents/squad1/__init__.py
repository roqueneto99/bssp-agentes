"""
Squad 1 — Captura e Enriquecimento de Leads.

Missão: Garantir que todo lead que entra no sistema esteja completo
e pronto para qualificação.

Agentes:
    1. Coletor     — Captura dados de múltiplas fontes
    2. Enriquecedor — Completa dados faltantes com IA
    3. Deduplicador — Identifica e unifica leads duplicados
    4. Conformidade — Valida consentimentos LGPD

Orquestrador:
    Squad1Orchestrator — Executa os 4 agentes em sequência
"""

from .coletor import ColetorAgent
from .enriquecedor import EnriquecedorAgent
from .deduplicador import DeduplicadorAgent
from .conformidade import ConformidadeAgent
from .orchestrator import Squad1Orchestrator

__all__ = [
    "ColetorAgent",
    "EnriquecedorAgent",
    "DeduplicadorAgent",
    "ConformidadeAgent",
    "Squad1Orchestrator",
]
