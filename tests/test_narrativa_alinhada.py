"""
Testes para a narrativa alinhada do Scorer (Squad 2).

Verifica que o briefing nao mais sai com vocabulario "quente" para leads
classificados como MQL/SAL/COLD — o bug que motivou esta correcao.

Roda standalone:
    cd bssp-agentes
    python -m pytest tests/test_narrativa_alinhada.py -v
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any

import pytest

# Permitir import do projeto sem instalar
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.agents.base import LLMMessage, LLMResponse  # noqa: E402
from src.agents.squad2.scorer import (  # noqa: E402
    ScorerAgent,
    TOM_POR_CLASSE,
)


# -------------------------------------------------------------------
# LLM Mock
# -------------------------------------------------------------------
class MockLLM:
    """LLM que devolve respostas pré-programadas em ordem."""

    def __init__(self, responses: list[dict]) -> None:
        self.responses = list(responses)
        self.calls: list[dict] = []

    async def complete_json(
        self,
        messages: list[LLMMessage],
        *,
        system: str | None = None,
        temperature: float | None = None,
    ) -> dict:
        self.calls.append({
            "system": system or "",
            "user": messages[0].content if messages else "",
            "temperature": temperature,
        })
        if not self.responses:
            raise RuntimeError("MockLLM: sem mais respostas pre-programadas")
        return self.responses.pop(0)


class MockRDStation:
    pass


def _perfil_basico(score_engagement: int = 30, dias_ult_conv: int = 45) -> dict:
    """Perfil de Squad 1 minimamente preenchido para os testes."""
    return {
        "dados_basicos": {
            "name": "Fulano de Tal",
            "job_title": "Analista Contábil",
            "company_name": "Empresa X",
            "city": "Sao Paulo",
            "state": "SP",
        },
        "analysis": {
            "area_principal": "contabilidade",
            "cursos_sugeridos": ["MBA Contabilidade"],
            "temperatura": "morno",
            "resumo_perfil": "Analista contábil com interesse em pós.",
        },
        "metricas_engajamento": {
            "score_engajamento": score_engagement,
            "dias_desde_ultima_conversao": dias_ult_conv,
            "total_conversoes": 2,
            "conversoes_ultimos_30d": 0,
        },
        "interacoes_conteudo": {
            "newsletters": 1, "eventos": 0, "webinars": 0, "materiais": 0,
        },
    }


def _engajamento(score: int = 30) -> dict:
    return {
        "score_engajamento_total": score,
        "engajamento_dimensoes_detalhe": [],
        "scores": {"recencia": 10, "responsividade": 0, "volume_interacao": score},
        "canais_ativos": [],
        "hablla": {"tem_dados": False},
        "sinais_comportamentais": [],
    }


# -------------------------------------------------------------------
# 1) MQL: narrativa nao deve ter linguagem quente
# -------------------------------------------------------------------
@pytest.mark.asyncio
async def test_mql_recebe_linguagem_de_nutricao():
    # 1a chamada (fit/interest): retorna scores que produzem score_total ~60 (MQL)
    llm = MockLLM([
        {  # _inferir_fit_e_interesse
            "fit_score": 70,
            "interest_score": 60,
            "fit_razao": "Cargo alinhado",
            "interest_razao": "Engajamento moderado",
            "resumo": "OTIMISTA — não deve ser usado",  # esse resumo nao deve sair no briefing
        },
        {  # _gerar_narrativa_alinhada
            "resumo": "Lead com perfil aderente em contabilidade. Engajamento moderado, sem interações recentes — convém nutrir antes de acionar consultor.",
            "proximo_passo": "Inserir em fluxo de nutrição avançada com conteúdos da área e re-avaliar em 7 dias.",
        },
    ])

    agent = ScorerAgent(llm=llm, rdstation=MockRDStation())
    result = await agent.run(
        "x@y.com",
        perfil_squad1=_perfil_basico(score_engagement=30, dias_ult_conv=45),
        engajamento=_engajamento(score=30),
    )

    assert result.success
    data = result.data
    assert data["classificacao"] == "MQL", f"esperava MQL, veio {data['classificacao']} ({data['score_total']})"
    assert data["narrativa_alinhada"] is True
    assert "imediato" not in data["resumo"].lower()
    assert "imediato" not in data["proximo_passo"].lower()
    assert "priorizar" not in data["proximo_passo"].lower()
    # E o resumo "otimista" da 1a chamada NAO deve aparecer
    assert "OTIMISTA" not in data["resumo"]


# -------------------------------------------------------------------
# 2) Validador: se LLM da narrativa retornar termo proibido, fallback
# -------------------------------------------------------------------
@pytest.mark.asyncio
async def test_narrativa_com_vocab_proibido_cai_no_fallback():
    llm = MockLLM([
        {  # fit/interest -> score baixo, classe SAL
            "fit_score": 50, "interest_score": 35,
            "fit_razao": "ok", "interest_razao": "ok",
            "resumo": "Resumo neutro original",
        },
        {  # narrativa LLM viola vocabulario (usa "imediato" sendo SAL)
            "resumo": "Lead com forte interesse — abordar imediatamente.",
            "proximo_passo": "Contato comercial imediato via WhatsApp.",
        },
    ])

    agent = ScorerAgent(llm=llm, rdstation=MockRDStation())
    result = await agent.run(
        "x@y.com",
        perfil_squad1=_perfil_basico(score_engagement=20, dias_ult_conv=60),
        engajamento=_engajamento(score=20),
    )
    data = result.data
    # Confirmar classe SAL
    assert data["classificacao"] == "SAL", f"esperava SAL, veio {data['classificacao']} ({data['score_total']})"
    # Como o LLM violou vocabulario proibido, deve ter caido no fallback:
    assert data["narrativa_alinhada"] is False
    # E o proximo_passo deve ser o deterministico (acao recomendada da classe SAL)
    assert data["proximo_passo"] == TOM_POR_CLASSE["SAL"]["acao_recomendada"]


# -------------------------------------------------------------------
# 3) SQL: linguagem quente E permitida
# -------------------------------------------------------------------
@pytest.mark.asyncio
async def test_sql_pode_usar_contato_imediato():
    llm = MockLLM([
        {  # fit/interest altos -> SQL
            "fit_score": 90, "interest_score": 90,
            "fit_razao": "Cargo perfeito", "interest_razao": "Multiplas interacoes",
            "resumo": "Lead muito quente",
        },
        {  # narrativa alinhada SQL com contato imediato — permitido
            "resumo": "Lead com forte intenção e alto engajamento — pronto para abordagem comercial.",
            "proximo_passo": "Contato imediato pelo WhatsApp em até 2h focando em MBA da área.",
        },
    ])
    agent = ScorerAgent(llm=llm, rdstation=MockRDStation())
    result = await agent.run(
        "x@y.com",
        perfil_squad1=_perfil_basico(score_engagement=80, dias_ult_conv=2),
        engajamento=_engajamento(score=80),
    )
    data = result.data
    assert data["classificacao"] == "SQL", f"esperava SQL, veio {data['classificacao']} ({data['score_total']})"
    assert data["narrativa_alinhada"] is True
    assert "imediato" in data["proximo_passo"].lower()  # OK pra SQL


# -------------------------------------------------------------------
# 4) Falha do LLM no 2o call -> fallback gracioso
# -------------------------------------------------------------------
@pytest.mark.asyncio
async def test_falha_llm_narrativa_usa_fallback_deterministico():
    class FailingLLM(MockLLM):
        async def complete_json(self, messages, *, system=None, temperature=None):
            self.calls.append({"system": system, "user": messages[0].content})
            if len(self.calls) == 1:
                # 1a chamada (fit/interest) ok
                return {
                    "fit_score": 60, "interest_score": 50,
                    "fit_razao": "ok", "interest_razao": "ok",
                    "resumo": "Resumo do scorer",
                }
            # 2a chamada (narrativa) explode
            raise RuntimeError("LLM down")

    llm = FailingLLM([])
    agent = ScorerAgent(llm=llm, rdstation=MockRDStation())
    result = await agent.run(
        "x@y.com",
        perfil_squad1=_perfil_basico(score_engagement=40, dias_ult_conv=30),
        engajamento=_engajamento(score=40),
    )
    data = result.data
    # Deve continuar com sucesso
    assert result.success
    # narrativa_alinhada=False indica fallback
    assert data["narrativa_alinhada"] is False
    # proximo_passo veio do TOM_POR_CLASSE
    classe = data["classificacao"]
    assert data["proximo_passo"] == TOM_POR_CLASSE[classe]["acao_recomendada"]


# -------------------------------------------------------------------
# 5) Vocabulário: TOM_POR_CLASSE cobre todas as classes esperadas
# -------------------------------------------------------------------
def test_tom_por_classe_cobre_todas_classes():
    classes = {"SQL", "MQL", "SAL", "COLD"}
    assert classes.issubset(TOM_POR_CLASSE.keys())
    for cls, tom in TOM_POR_CLASSE.items():
        assert "descricao" in tom
        assert "acao_recomendada" in tom
        assert "vocab_proibido" in tom
        assert "vocab_permitido" in tom
