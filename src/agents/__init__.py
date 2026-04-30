"""
Squads de agentes de IA para automação comercial da BSSP.

Squad 1: Captura / Enriquecimento / Deduplicação / Conformidade LGPD  (em produção)
Squad 2: Qualificação / Scoring                                       (em produção)
Squad 3: Comunicação Inteligente                                      (S1 — esqueleto)
Squad 4: Handoff Comercial                                            (a implementar)
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
from .squad2 import (
    AnalisadorEngajamentoAgent,
    ScorerAgent,
    ClassificadorRotaAgent,
    Squad2Orchestrator,
)
from .squad3 import (
    EngajamentoProgressivoAgent,
    MulticanalAgent,
    PersonalizacaoComportamentalAgent,
    QualificadorConversacionalAgent,
    RecuperacaoAgent,
    Squad3Orchestrator,
)
from .pipeline import AgentPipeline

__all__ = [
    "BaseAgent",
    "LLMProvider",
    "AgentResult",
    # Squad 1
    "ColetorAgent",
    "EnriquecedorAgent",
    "DeduplicadorAgent",
    "ConformidadeAgent",
    "Squad1Orchestrator",
    # Squad 2
    "AnalisadorEngajamentoAgent",
    "ScorerAgent",
    "ClassificadorRotaAgent",
    "Squad2Orchestrator",
    # Squad 3
    "EngajamentoProgressivoAgent",
    "MulticanalAgent",
    "PersonalizacaoComportamentalAgent",
    "QualificadorConversacionalAgent",
    "RecuperacaoAgent",
    "Squad3Orchestrator",
    # Pipeline geral
    "AgentPipeline",
    # Legado
    "EnrichmentAgent",
]
