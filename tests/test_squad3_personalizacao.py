"""
Testes da Personalização Comportamental — Sprint 3.

Cobertura:
    - TemplateLoader carrega 9 templates aprovados
    - TemplateLoader rejeita arquivo com variável não declarada
    - TemplateLoader pula rascunhos quando modo=apenas_aprovados
    - PersonalizacaoComportamentalAgent escolhe nudge correto por contexto
    - Inferência de tom: cargo analítico → analitico
    - Polimento via LLM mantém variáveis e fatos
    - Sanity check rejeita placeholder inventado
    - Sanity check rejeita corpo muito curto
    - Fallback determinístico quando LLM indisponível
    - Fallback quando LLM gera saída fora do schema
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.agents.base import LLMMessage, LLMProvider  # noqa: E402
from src.agents.squad3.personalizacao import (  # noqa: E402
    DEFAULT_TEMPLATES_DIR,
    PersonalizacaoComportamentalAgent,
)
from src.agents.squad3.template_loader import (  # noqa: E402
    Template,
    TemplateLoader,
    TemplateValidationError,
    renderizar,
)


# -------------------------------------------------------------------
# LLM mocks
# -------------------------------------------------------------------

class MockLLM:
    """LLM com respostas pré-programadas. Compatível com complete_json."""

    def __init__(self, responses: list[dict] | None = None, fail: bool = False) -> None:
        self.responses = list(responses or [])
        self.fail = fail
        self.calls: list[dict] = []
        self.api_key = "fake-key"
        self.model = "claude-mock-test"

    async def complete_json(self, messages, *, system=None, temperature=None) -> dict:
        self.calls.append({
            "system": system or "",
            "user": messages[0].content if messages else "",
            "temperature": temperature,
        })
        if self.fail:
            raise RuntimeError("LLM mock configurado para falhar")
        if not self.responses:
            raise RuntimeError("MockLLM sem respostas pré-programadas")
        return self.responses.pop(0)


class NoApiKeyLLM:
    """LLM sem api_key — agente deve cair em fallback automaticamente."""
    api_key = ""
    model = "dummy"

    async def complete_json(self, *args, **kwargs):
        raise AssertionError("não deveria ser chamado quando api_key vazia")


# -------------------------------------------------------------------
# TemplateLoader
# -------------------------------------------------------------------

def test_loader_carrega_9_templates_aprovados():
    loader = TemplateLoader(DEFAULT_TEMPLATES_DIR)
    templates = loader.carregar()
    nudges = {t.nudge for t in templates.values()}
    esperados = {
        "prova_social", "escassez", "ancoragem", "loss_aversion", "fricao",
        "boas_vindas", "educativa", "depoimento", "fechamento",
    }
    assert esperados.issubset(nudges), (
        f"faltando: {esperados - nudges}, presentes: {nudges}"
    )
    # Todos têm aprovado_por preenchido
    for t in templates.values():
        assert t.aprovado_por, f"template sem aprovação: {t.id}"


def test_loader_rejeita_variavel_nao_declarada(tmp_path: Path):
    bad = {
        "id": "bad", "version": "v1", "nudge": "x", "canal": "email",
        "assunto": "Olá {nome_curto}",
        "corpo": "Mensagem com {variavel_inventada} aqui.",
        "variaveis_obrigatorias": ["nome_curto"],
        "variaveis_opcionais": [],
        "tons_suportados": ["analitico"],
        "aprovado_por": "x@y.com",
        "aprovado_em": "2026-04-29",
    }
    (tmp_path / "bad.json").write_text(json.dumps(bad), encoding="utf-8")
    loader = TemplateLoader(tmp_path, modo="rascunho_ok")
    # Falhas são logadas mas não levantam: o arquivo simplesmente não entra
    out = loader.carregar()
    assert out == {}


def test_loader_pula_rascunho_em_apenas_aprovados(tmp_path: Path):
    rascunho = {
        "id": "rascunho", "version": "v1", "nudge": "x", "canal": "email",
        "assunto": "{nome_curto}", "corpo": "{nome_curto} oi",
        "variaveis_obrigatorias": ["nome_curto"],
        "variaveis_opcionais": [],
        "tons_suportados": ["analitico"],
        "aprovado_por": None, "aprovado_em": None,
    }
    (tmp_path / "rascunho.json").write_text(json.dumps(rascunho), encoding="utf-8")
    loader_prod = TemplateLoader(tmp_path, modo="apenas_aprovados")
    assert loader_prod.carregar() == {}
    loader_dev = TemplateLoader(tmp_path, modo="rascunho_ok")
    assert len(loader_dev.carregar()) == 1


def test_renderizar_substitui_variaveis():
    tpl = Template(
        id="x", version="v1", nudge="prova_social", canal="email",
        assunto="Oi {nome_curto}",
        corpo="Olá {nome_curto}, sobre {curso}",
        variaveis_obrigatorias=("nome_curto",),
        variaveis_opcionais=("curso",),
        tons_suportados=("analitico",),
        aprovado_por="x", aprovado_em="2026",
        observacoes=None,
    )
    assunto, corpo, faltas = renderizar(tpl, {"nome_curto": "Roque", "curso": "MBA"})
    assert assunto == "Oi Roque"
    assert "Roque" in corpo
    assert "MBA" in corpo
    assert faltas == set()


# -------------------------------------------------------------------
# PersonalizacaoComportamentalAgent — escolha de nudge
# -------------------------------------------------------------------

def test_escolha_nudge_indeciso_quando_score_intermediario():
    agent = PersonalizacaoComportamentalAgent(
        llm=NoApiKeyLLM(), rdstation=None, usar_llm=False,
    )
    # Score 50 → "indeciso" → prova_social
    nudge = agent._escolher_nudge({"score_total": 50, "sinais": []})
    assert nudge == "prova_social"


def test_escolha_nudge_altamente_engajado_quando_score_alto():
    agent = PersonalizacaoComportamentalAgent(
        llm=NoApiKeyLLM(), rdstation=None, usar_llm=False,
    )
    nudge = agent._escolher_nudge({"score_total": 85, "sinais": []})
    assert nudge == "escassez"


def test_inferencia_tom_cargo_analitico():
    agent = PersonalizacaoComportamentalAgent(
        llm=NoApiKeyLLM(), rdstation=None, usar_llm=False,
    )
    tom = agent._inferir_tom(
        ps2={},
        ps1={"dados_basicos": {"job_title": "Engenheiro de Dados"}},
    )
    assert tom == "analitico"


def test_inferencia_tom_perfil_explicito_vence():
    agent = PersonalizacaoComportamentalAgent(
        llm=NoApiKeyLLM(), rdstation=None, usar_llm=False,
    )
    tom = agent._inferir_tom(
        ps2={"perfil_psicologico": "impulsivo"},
        ps1={"dados_basicos": {"job_title": "Engenheiro"}},
    )
    assert tom == "impulsivo"


# -------------------------------------------------------------------
# Pipeline completo (run)
# -------------------------------------------------------------------

def test_run_fallback_sem_llm_devolve_template_puro():
    agent = PersonalizacaoComportamentalAgent(
        llm=NoApiKeyLLM(), rdstation=None, usar_llm=False,
    )
    result = asyncio.run(agent.run(
        "lead@bssp.com.br",
        passo_cadencia={"ordem": 0, "canal": "email", "nudge": "prova_social"},
        perfil_squad1={"dados_basicos": {"first_name": "Roque"}},
        perfil_squad2={"classificacao": "MQL", "score_total": 60},
    ))
    assert result.success
    assert "Roque" in result.data["assunto"] or "Roque" in result.data["corpo"]
    assert result.data["polimento"]["status"] == "fallback_sem_llm"
    assert result.data["template_id"] == "tpl_v1_prova_social"
    assert result.data["template_versao"] == "v1"


def test_run_polimento_llm_sucesso():
    # Polish recebe texto já renderizado; saída não deve conter placeholders.
    polished = {
        "assunto": "Roque, dados sobre o impacto da pós-graduação",
        "corpo": (
            "Olá Roque,\n\nDados internos da BSSP mostram que 89% dos "
            "ex-alunos do MBA Executivo reportam avanço em até 12 meses. "
            "Quer ver as métricas detalhadas em uma chamada de 15 minutos?"
            "\n\nBSSP"
        ),
    }
    llm = MockLLM(responses=[polished])
    agent = PersonalizacaoComportamentalAgent(
        llm=llm, rdstation=None, usar_llm=True,
    )
    result = asyncio.run(agent.run(
        "lead@bssp.com.br",
        passo_cadencia={"ordem": 0, "canal": "email", "nudge": "prova_social"},
        perfil_squad1={
            "dados_basicos": {"first_name": "Roque", "job_title": "Engenheiro"},
            "analysis": {"area_principal": "MBA Executivo"},
        },
        perfil_squad2={"classificacao": "MQL", "score_total": 60},
    ))
    assert result.success, result.error
    assert result.data["polimento"]["status"] == "polido"
    assert result.data["modelo_llm"] == "claude-mock-test"
    assert result.data["tom"] == "analitico"
    assert "Dados" in result.data["corpo"]
    # Output do polish não pode ter placeholders (já renderizado)
    assert "{" not in result.data["corpo"]
    assert "{" not in result.data["assunto"]


def test_sanity_check_rejeita_placeholder_inventado():
    polished = {
        "assunto": "Roque, sobre o {curso_inventado}",
        "corpo": "Olá Roque, sobre o {curso_inventado} com {variavel_alucinada}",
    }
    llm = MockLLM(responses=[polished])
    agent = PersonalizacaoComportamentalAgent(
        llm=llm, rdstation=None, usar_llm=True,
    )
    result = asyncio.run(agent.run(
        "lead@bssp.com.br",
        passo_cadencia={"ordem": 0, "canal": "email", "nudge": "prova_social"},
        perfil_squad1={"dados_basicos": {"first_name": "Roque"}},
        perfil_squad2={"classificacao": "MQL", "score_total": 60},
    ))
    assert result.success
    # Caiu no fallback (texto base do template, sem o polish corrompido)
    assert result.data["polimento"]["status"] == "fallback_sanity_check"
    assert "placeholders_inventados" in result.data["polimento"]["motivo_rejeicao"]
    assert "{curso_inventado}" not in result.data["assunto"]
    assert "{curso_inventado}" not in result.data["corpo"]


def test_fallback_quando_llm_falha():
    llm = MockLLM(fail=True)
    agent = PersonalizacaoComportamentalAgent(
        llm=llm, rdstation=None, usar_llm=True,
    )
    result = asyncio.run(agent.run(
        "lead@bssp.com.br",
        passo_cadencia={"ordem": 0, "canal": "email", "nudge": "prova_social"},
        perfil_squad1={"dados_basicos": {"first_name": "Roque"}},
        perfil_squad2={"classificacao": "MQL", "score_total": 60},
    ))
    assert result.success
    assert result.data["polimento"]["status"] == "fallback_llm_erro"
    # Texto base preservado
    assert "Roque" in result.data["assunto"] or "Roque" in result.data["corpo"]


def test_fallback_quando_corpo_polido_muito_curto():
    polished = {"assunto": "Oi", "corpo": "Curto."}
    llm = MockLLM(responses=[polished])
    agent = PersonalizacaoComportamentalAgent(
        llm=llm, rdstation=None, usar_llm=True,
    )
    result = asyncio.run(agent.run(
        "lead@bssp.com.br",
        passo_cadencia={"ordem": 0, "canal": "email", "nudge": "prova_social"},
        perfil_squad1={"dados_basicos": {"first_name": "Roque"}},
        perfil_squad2={"classificacao": "MQL", "score_total": 60},
    ))
    assert result.success
    assert result.data["polimento"]["status"] == "fallback_sanity_check"
    assert result.data["polimento"]["motivo_rejeicao"] == "corpo_muito_curto"


def test_template_nao_encontrado_devolve_falha():
    agent = PersonalizacaoComportamentalAgent(
        llm=NoApiKeyLLM(), rdstation=None, usar_llm=False,
    )
    result = asyncio.run(agent.run(
        "lead@bssp.com.br",
        passo_cadencia={"ordem": 0, "canal": "email", "nudge": "nudge_inexistente"},
        perfil_squad1={"dados_basicos": {"first_name": "Roque"}},
        perfil_squad2={"classificacao": "MQL", "score_total": 60},
    ))
    assert result.success is False
    assert "template_nao_encontrado" in result.error
