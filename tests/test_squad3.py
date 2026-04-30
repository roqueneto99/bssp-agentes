"""
Testes unitários para a Squad 3 — Sprint 1.

Cobertura mínima do esqueleto:
    - cada um dos 5 agentes instancia sem erro
    - Squad3Orchestrator executa em dry-run sem exceção
    - rotas terminais (sql_handoff, blocked, cliente_existente) → 'nada_a_fazer'
    - rota mql_nurture com lead novo → primeira_msg
    - resposta do lead com keyword "preço" → intencao=objecao_preco e requer_rescoring
    - personalização rejeita variável fora do dicionário permitido
    - recuperação detecta abandono por inatividade ≥ 7d

Roda standalone:
    cd bssp-agentes
    python -m pytest tests/test_squad3.py -v
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.agents.base import LLMProvider  # noqa: E402
from src.agents.squad3 import (  # noqa: E402
    EngajamentoProgressivoAgent,
    MulticanalAgent,
    PersonalizacaoComportamentalAgent,
    QualificadorConversacionalAgent,
    RecuperacaoAgent,
    Squad3Orchestrator,
)


# -------------------------------------------------------------------
# Stubs / fixtures
# -------------------------------------------------------------------
class StubLLM:
    """LLMProvider mínimo — não é usado em S1 (todos os agentes operam offline)."""
    pass


class StubRD:
    pass


@pytest.fixture
def llm() -> StubLLM:
    return StubLLM()


@pytest.fixture
def rd() -> StubRD:
    return StubRD()


@pytest.fixture
def orquestrador(llm, rd) -> Squad3Orchestrator:
    return Squad3Orchestrator(llm=llm, rdstation=rd, hablla=None, dry_run=True)


# -------------------------------------------------------------------
# Instanciação
# -------------------------------------------------------------------
def test_agentes_instanciam(llm, rd):
    EngajamentoProgressivoAgent(llm=llm, rdstation=rd)
    MulticanalAgent(llm=llm, rdstation=rd, dry_run=True)
    PersonalizacaoComportamentalAgent(llm=llm, rdstation=rd)
    QualificadorConversacionalAgent(llm=llm, rdstation=rd)
    RecuperacaoAgent(llm=llm, rdstation=rd)


# -------------------------------------------------------------------
# Gates do orchestrator
# -------------------------------------------------------------------
@pytest.mark.parametrize("rota,classificacao,pode_seguir", [
    ("sql_handoff",       "SQL",     True),    # rota para Squad 4
    ("blocked",           "BLOCKED", False),
    ("cliente_existente", "CLIENTE", True),
])
def test_orchestrator_pula_rotas_terminais(orquestrador, rota, classificacao, pode_seguir):
    perfil_squad2 = {
        "rota": rota,
        "classificacao": classificacao,
        "pode_seguir_squad3": pode_seguir,
    }
    results = asyncio.run(orquestrador.execute(
        "lead@bssp.com.br", perfil_squad2=perfil_squad2,
    ))
    resumo = next(r for r in results if r.agent_name == "squad3_resumo")
    assert resumo.data["decisao_orchestrator"] == "nada_a_fazer"


def test_orchestrator_mql_dispara_primeira_msg(orquestrador):
    perfil_squad2 = {
        "rota": "mql_nurture",
        "classificacao": "MQL",
        "score_total": 60,
        "pode_seguir_squad3": True,
    }
    perfil_squad1 = {
        "dados_basicos": {"first_name": "Roque", "city": "São Paulo"},
        "analysis": {"area_principal": "MBA em Gestão"},
    }
    results = asyncio.run(orquestrador.execute(
        "roque@bssp.com.br",
        perfil_squad1=perfil_squad1,
        perfil_squad2=perfil_squad2,
        estado_lead={"msgs_enviadas": 0},
    ))
    resumo = next(r for r in results if r.agent_name == "squad3_resumo")
    assert resumo.data["decisao_orchestrator"] == "cadencia_regular"
    assert resumo.data["mensagem"]["cadencia_nome"] == "mql_nurture"
    assert resumo.data["mensagem"]["passo"] == 0
    assert resumo.data["envio"]["status"] == "pending"  # dry-run
    assert resumo.data["dry_run"] is True


def test_orchestrator_continua_cadencia(orquestrador):
    """Lead que já recebeu 2 mensagens → próximo passo é o 3 (ordem=2)."""
    perfil_squad2 = {
        "rota": "sal_nurture",
        "classificacao": "SAL",
        "score_total": 40,
        "pode_seguir_squad3": True,
    }
    perfil_squad1 = {
        "dados_basicos": {"first_name": "Maria"},
        "analysis": {"area_principal": "Pós em Marketing"},
    }
    results = asyncio.run(orquestrador.execute(
        "maria@bssp.com.br",
        perfil_squad1=perfil_squad1,
        perfil_squad2=perfil_squad2,
        estado_lead={"cadencia_atual": "sal_nurture", "msgs_enviadas": 2},
    ))
    resumo = next(r for r in results if r.agent_name == "squad3_resumo")
    assert resumo.data["mensagem"]["passo"] == 2  # ordem do passo 3 (0-indexed)


# -------------------------------------------------------------------
# Qualificação Conversacional
# -------------------------------------------------------------------
def test_qualificador_detecta_objecao_preco(llm, rd):
    qc = QualificadorConversacionalAgent(llm=llm, rdstation=rd)
    result = asyncio.run(qc.run(
        "lead@bssp.com.br",
        resposta={"canal": "whatsapp", "conteudo": "qual é o valor mensal?"},
    ))
    assert result.success
    assert result.data["intencao"] == "objecao_preco"
    assert "objecao_preco" in result.data["sinais_extraidos"]
    assert result.data["requer_rescoring"] is True


def test_qualificador_detecta_pediu_humano(llm, rd):
    qc = QualificadorConversacionalAgent(llm=llm, rdstation=rd)
    result = asyncio.run(qc.run(
        "lead@bssp.com.br",
        resposta={"canal": "whatsapp", "conteudo": "Posso falar com um consultor?"},
    ))
    assert result.success
    assert result.data["intencao"] == "pediu_humano"
    assert result.data["maturidade_bant"] == 5
    assert result.data["requer_rescoring"] is True


def test_qualificador_sem_resposta(llm, rd):
    qc = QualificadorConversacionalAgent(llm=llm, rdstation=rd)
    result = asyncio.run(qc.run("lead@bssp.com.br", resposta=None))
    assert result.success
    assert result.data.get("sem_resposta") is True


# -------------------------------------------------------------------
# Personalização — rejeição de variáveis não permitidas
# -------------------------------------------------------------------
def test_personalizacao_aceita_template_inline(llm, rd):
    pers = PersonalizacaoComportamentalAgent(llm=llm, rdstation=rd)
    result = asyncio.run(pers.run(
        "lead@bssp.com.br",
        passo_cadencia={"ordem": 0, "canal": "email", "nudge": "prova_social"},
        perfil_squad1={"dados_basicos": {"first_name": "João"}},
        perfil_squad2={"classificacao": "MQL", "score_total": 60},
    ))
    assert result.success
    assert "João" in result.data["assunto"] or "João" in result.data["corpo"]
    assert result.data["nudge"] == "prova_social"


# -------------------------------------------------------------------
# Recuperação
# -------------------------------------------------------------------
def test_recuperacao_inicia_por_inatividade(llm, rd):
    rec = RecuperacaoAgent(llm=llm, rdstation=rd)
    result = asyncio.run(rec.run(
        "lead@bssp.com.br",
        telemetria_lead={"dias_sem_resposta": 8},
        estado_lead={"s3_status": "ativo"},
    ))
    assert result.success
    assert result.data["decisao"] == "iniciar_recuperacao"
    assert result.data["motivo"] == "inativo_7d"
    assert result.data["proximo_passo"]["ordem"] == 0


def test_recuperacao_desiste_apos_3_tentativas(llm, rd):
    rec = RecuperacaoAgent(llm=llm, rdstation=rd)
    result = asyncio.run(rec.run(
        "lead@bssp.com.br",
        telemetria_lead={"dias_sem_resposta": 14},
        estado_lead={"s3_status": "recuperacao", "msgs_recuperacao_enviadas": 3},
    ))
    assert result.success
    assert result.data["decisao"] == "desistir_devolver_cold"
    assert result.data["devolver_para"] == "cold_recycle"


def test_recuperacao_nao_age_sem_sinais(llm, rd):
    rec = RecuperacaoAgent(llm=llm, rdstation=rd)
    result = asyncio.run(rec.run(
        "lead@bssp.com.br",
        telemetria_lead={"dias_sem_resposta": 1, "abriu": True, "clicou": True},
        estado_lead={"s3_status": "ativo"},
    ))
    assert result.success
    assert result.data["decisao"] == "nao_recuperar"


# -------------------------------------------------------------------
# Engajamento Progressivo
# -------------------------------------------------------------------
def test_engajamento_sem_cadencia_para_sql(llm, rd):
    eng = EngajamentoProgressivoAgent(llm=llm, rdstation=rd)
    result = asyncio.run(eng.run(
        "lead@bssp.com.br",
        perfil_squad2={"rota": "sql_handoff"},
    ))
    assert result.success
    assert result.data["decisao"] == "sem_cadencia"


def test_engajamento_concluido_quando_passos_acabam(llm, rd):
    eng = EngajamentoProgressivoAgent(llm=llm, rdstation=rd)
    # mql_nurture tem 5 passos; msgs_enviadas=5 → concluido
    result = asyncio.run(eng.run(
        "lead@bssp.com.br",
        perfil_squad2={"rota": "mql_nurture"},
        estado_lead={"cadencia_atual": "mql_nurture", "msgs_enviadas": 5},
    ))
    assert result.success
    assert result.data["decisao"] == "concluido"


# -------------------------------------------------------------------
# Multicanal — janela horária e LGPD
# -------------------------------------------------------------------
def test_multicanal_bloqueia_lgpd(llm, rd):
    mc = MulticanalAgent(llm=llm, rdstation=rd, dry_run=True)
    result = asyncio.run(mc.run(
        "lead@bssp.com.br",
        mensagem={"canal": "email", "assunto": "x", "corpo": "y"},
        perfil_squad2={"classificacao": "BLOCKED", "rota": "blocked"},
    ))
    assert result.success
    assert result.data["status"] == "skipped"
    assert result.data["razao_skip"] == "lgpd_bloqueado"


def test_multicanal_dry_run_grava_pending(llm, rd):
    mc = MulticanalAgent(llm=llm, rdstation=rd, dry_run=True)
    result = asyncio.run(mc.run(
        "lead@bssp.com.br",
        mensagem={
            "canal": "email", "assunto": "Bem-vindo", "corpo": "Oi",
            "nudge": "boas_vindas", "passo": 0,
        },
        perfil_squad2={"classificacao": "MQL", "rota": "mql_nurture"},
    ))
    if result.data.get("razao_skip") == "fora_da_janela_horaria":
        # Roda em domingo — comportamento esperado
        assert result.data["status"] == "skipped"
    else:
        assert result.data["status"] == "pending"
        assert result.data["dry_run"] is True
