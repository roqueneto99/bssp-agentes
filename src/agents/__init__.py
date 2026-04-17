"""
Squads de agentes de IA para automação comercial da BSSP.

Squad 1: Captura / Enriquecimento / Deduplicação / Conformidade LGPD
Squad 2: Qualificação / Scoring (a implementar)
Squad 3: Comunicação Inteligente (a implementar)
Squad 4: Handoff Comercial (a implementar)
"""

from .base import BaseAgent, LLMProvider, AgentResult
from .squad1_enrichment import EnrichmentAgent  # legado (mantido para retrocompatibilidade)
from .squad1 import (
    ColetorAgent,
    EnriquecedorAgent,
    DeduplicadorAgent,
    ConformidadeAgent,
    Squad1Orchestrator,
)
from .pipeline import AgentPipeline

__all__ = [
    "BaseAgent",
    "LLMProvider",
    "AgentResult",
    # Squad 1 — agentes individuais
    "ColetorAgent",
    "EnriquecedorAgent",
    "DeduplicadorAgent",
    "ConformidadeAgent",
    "Squad1Orchestrator",
    # Pipeline geral
    "AgentPipeline",
    # Legado
    "EnrichmentAgent",
]
