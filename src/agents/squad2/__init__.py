"""
Squad 2 — Qualificação & Scoring.

Recebe leads enriquecidos do Squad 1 e aplica modelo de scoring
multicanal para determinar a propensão à matrícula.

Agentes:
    1. Analisador de Engajamento: agrega sinais de todos os canais
       (RD Station + Hablla) em um perfil comportamental unificado
    2. Scorer de Qualificação: aplica modelo de pontuação ponderado
       (fit + interesse + engajamento + timing) e classifica o lead
    3. Classificador de Rota: decide a próxima ação do funil
       (nurture, MQL→SQL, handoff, descarte)
"""

from .analisador_engajamento import AnalisadorEngajamentoAgent
from .scorer import ScorerAgent
from .classificador_rota import ClassificadorRotaAgent
from .orchestrator import Squad2Orchestrator

__all__ = [
    "AnalisadorEngajamentoAgent",
    "ScorerAgent",
    "ClassificadorRotaAgent",
    "Squad2Orchestrator",
]
