"""
Squad 2 — Agente 2: Scorer de Qualificação.

Aplica modelo de scoring em 4 dimensões para classificar o lead
em uma escala de propensão à matrícula.

Dimensões do modelo:
    1. FIT SCORE (0-100): O lead tem perfil compatível com pós-graduação?
       - Cargo, empresa, formação, região geográfica
       - Alinhamento com as áreas de curso da BSSP

    2. INTEREST SCORE (0-100): O lead demonstra interesse real?
       - Tipos de conversão (landing pages de curso vs. genéricas)
       - Perguntas feitas em conversas (preço, turma, certificação)
       - Conteúdos acessados (newsletters, webinars, materiais)

    3. ENGAGEMENT SCORE (0-100): Quão engajado está o lead?
       - Vem pronto do Analisador de Engajamento (Agente 1)
       - Volume, recência, multicanalidade, responsividade

    4. TIMING SCORE (0-100): É o momento certo?
       - Proximidade do início de turmas
       - Sazonalidade (jan-mar e jul-ago = picos de matrícula)
       - Velocidade de progressão no funil

Classificação final:
    SQL  (Sales Qualified Lead): score >= 75 — pronto para consultor
    MQL  (Marketing Qualified Lead): score 50-74 — nurture avançado
    SAL  (Sales Accepted Lead): score 35-49 — nurture básico
    COLD: score < 35 — lead frio, manter na base

Este agente USA LLM para inferir interesse e fit a partir de dados
não-estruturados (conversas, anotações, histórico).
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Optional

from ..base import AgentResult, LLMProvider, LLMMessage

logger = logging.getLogger(__name__)

# Meses de pico de matrícula (início de semestre)
MESES_PICO = {1, 2, 3, 7, 8}  # Jan-Mar, Jul-Ago

# Áreas de curso BSSP (para calcular fit)
BSSP_AREAS = {
    "gestao", "contabilidade", "direito", "engenharia",
    "saude", "psicologia", "trabalhista", "pericia",
    "educacao", "tecnologia",
}

# Classificação por faixa de score
CLASSIFICACAO = [
    (75, "SQL"),   # Sales Qualified Lead
    (50, "MQL"),   # Marketing Qualified Lead
    (35, "SAL"),   # Sales Accepted Lead
    (0, "COLD"),   # Lead frio
]

# -------------------------------------------------------------------
# Vocabulario permitido/proibido por classificacao — usado na geracao
# de narrativa alinhada. Evita o viés otimista no briefing comercial
# em que leads MQL/SAL/COLD recebiam recomendações de "contato imediato"
# apesar do score real não justificar.
# -------------------------------------------------------------------
TOM_POR_CLASSE: dict[str, dict] = {
    "SQL": {
        "descricao": "Lead pronto para handoff ao consultor comercial (score ≥ 75).",
        "acao_recomendada": "Contato comercial prioritário em até 2h pelo canal preferido do lead.",
        "vocab_permitido": [
            "contato imediato", "priorizar", "pronto para abordagem",
            "forte intenção", "alto engajamento",
        ],
        "vocab_proibido": [],
    },
    "MQL": {
        "descricao": "Lead morno-quente em nutrição avançada (score 50-74).",
        "acao_recomendada": (
            "Inserir em fluxo de nutrição avançada (conteúdo aprofundado da área), "
            "NÃO acionar consultor ainda. Re-avaliar em 7 dias."
        ),
        "vocab_permitido": [
            "nutrir", "avançar na jornada", "aprofundar interesse",
            "engajamento moderado a alto", "potencial",
        ],
        "vocab_proibido": [
            "imediato", "imediatamente", "priorizar", "priorizado",
            "urgente", "excepcional", "pronto para contato",
            "contato comercial imediato", "deve ser priorizado",
        ],
    },
    "SAL": {
        "descricao": "Lead com interesse parcial — nutrição básica (score 35-49).",
        "acao_recomendada": (
            "Inserir em fluxo de nutrição básica (emails educativos sobre a área). "
            "NÃO acionar consultor. Re-avaliar em 14 dias."
        ),
        "vocab_permitido": [
            "nutrir", "educar", "acompanhar", "potencial moderado",
            "interesse parcial", "engajamento limitado",
        ],
        "vocab_proibido": [
            "imediato", "imediatamente", "priorizar", "priorizado",
            "urgente", "excepcional", "forte intenção", "altíssimo",
            "pronto para", "contato comercial", "deve ser priorizado",
            "alto engajamento", "alta prioridade",
        ],
    },
    "COLD": {
        "descricao": "Lead frio — manter na base, re-engajar via campanha geral (score < 35).",
        "acao_recomendada": (
            "Manter na base — incluir em campanha de re-engajamento em 30-60 dias. "
            "Sem ação comercial direta."
        ),
        "vocab_permitido": [
            "re-engajar", "reciclar", "dados limitados", "baixo engajamento",
            "interesse não confirmado",
        ],
        "vocab_proibido": [
            "imediato", "priorizar", "urgente", "excepcional", "forte",
            "alto", "pronto para", "contato", "priorizado", "interessado",
        ],
    },
}

# Valores de lifecycle_stage que significam "já é aluno/cliente da BSSP".
# Em portugues no RD Station, o valor padrao e "Cliente". Cobrimos tambem
# variantes em ingles.
LIFECYCLE_CLIENTE = {"cliente", "customer", "client"}


def _is_existing_customer(perfil_squad1: dict | None) -> tuple[bool, str]:
    """
    Detecta se o lead ja e aluno/cliente da BSSP.

    Retorna (flag, origem) onde origem e uma string curta usada em logs
    e na razao para explicar como a detecao foi feita.
    Fonte unica: lifecycle_stage do RD Station.
    """
    if not perfil_squad1:
        return False, ""
    stage = (
        (perfil_squad1.get("dados_basicos", {}) or {}).get("lifecycle_stage")
        or (perfil_squad1.get("funil", {}) or {}).get("lifecycle_stage")
        or ""
    )
    if str(stage).strip().lower() in LIFECYCLE_CLIENTE:
        return True, f"lifecycle_stage={stage}"
    return False, ""


class ScorerAgent:
    """
    Agente de scoring que combina dados determinísticos com
    inferência via LLM para classificar o lead.
    """

    agent_name = "squad2_scorer"

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
        perfil_squad1: dict | None = None,
        engajamento: dict | None = None,
    ) -> AgentResult:
        """
        Executa o scoring completo do lead.

        Args:
            email: Email do lead
            perfil_squad1: Dados agregados do Squad 1
            engajamento: Dados do Analisador de Engajamento (Agente 1)
        """
        start = time.monotonic()
        try:
            scoring = await self._scorear(email, perfil_squad1, engajamento)
            duration = (time.monotonic() - start) * 1000

            logger.info(
                "%s concluído para %s: total=%d, classe=%s, %.0fms",
                self.agent_name, email,
                scoring.get("score_total", 0),
                scoring.get("classificacao", "?"),
                duration,
            )

            return AgentResult(
                success=True,
                agent_name=self.agent_name,
                contact_email=email,
                data=scoring,
                duration_ms=duration,
                llm_calls=1,
            )
        except Exception as e:
            duration = (time.monotonic() - start) * 1000
            logger.error("%s falhou para %s: %s", self.agent_name, email, e)
            return AgentResult(
                success=False,
                agent_name=self.agent_name,
                contact_email=email,
                error=str(e),
                duration_ms=duration,
            )

    async def _scorear(
        self,
        email: str,
        perfil_squad1: dict | None,
        engajamento: dict | None,
    ) -> dict:
        """Calcula os 4 scores dimensionais e a classificação final."""

        now = datetime.now(timezone.utc)

        # =============================================================
        # CURTO-CIRCUITO: lead ja e aluno/cliente da BSSP
        # =============================================================
        # Se o lifecycle_stage do RD Station indica que este contato ja
        # matriculou (Cliente), NAO roda scoring de lead. Motivo: um
        # aluno atual nao deve ser abordado como lead novo pelo consultor
        # comercial — o fluxo correto seria retencao/upsell, que esta
        # fora do escopo da Fase 1.
        eh_cliente, fonte = _is_existing_customer(perfil_squad1)
        if eh_cliente:
            logger.info("Scorer: %s ja e cliente (%s) — pulando scoring", email, fonte)
            return {
                "email": email,
                "timestamp": now.isoformat(),
                "score_total": None,  # sem pontuacao — nao e lead
                "classificacao": "CLIENTE",
                "is_existing_customer": True,
                "cliente_fonte": fonte,
                "dimensoes": {},
                "resumo": (
                    "Contato ja e aluno/cliente ativo da BSSP "
                    f"({fonte}). Nao deve ser tratado como lead novo — "
                    "encaminhar para fluxo de retencao/upsell (fora do "
                    "escopo da Fase 1)."
                ),
                "sinais_engajamento": (engajamento or {}).get("sinais_comportamentais", []),
            }

        # =============================================================
        # SCORE 1: ENGAGEMENT (determinístico — vem do Analisador)
        # =============================================================
        engagement_score = 0
        engagement_detalhe: list[dict] = []
        if engajamento:
            engagement_score = engajamento.get("score_engajamento_total", 0)
            engagement_detalhe = engajamento.get("engajamento_dimensoes_detalhe", []) or []
        elif perfil_squad1:
            # Fallback: usar score do Squad 1
            metricas = perfil_squad1.get("metricas_engajamento", {})
            engagement_score = metricas.get("score_engajamento", 0)
        engagement_razao = self._explicar_engagement(engagement_score, engajamento, perfil_squad1)

        # =============================================================
        # SCORE 2: TIMING (determinístico)
        # =============================================================
        timing_score, timing_razao = self._calcular_timing(now, perfil_squad1, engajamento)

        # =============================================================
        # SCORE 3 & 4: FIT + INTEREST (via LLM)
        # =============================================================
        # O LLM analisa dados não-estruturados para inferir:
        # - Fit: perfil compatível com pós-grad BSSP?
        # - Interest: demonstra intenção real de matrícula?
        llm_scores = await self._inferir_fit_e_interesse(
            email, perfil_squad1, engajamento,
        )

        fit_score = llm_scores.get("fit_score", 30)
        interest_score = llm_scores.get("interest_score", 20)
        fit_razao = llm_scores.get("fit_razao", "")
        interest_razao = llm_scores.get("interest_razao", "")
        resumo_llm = llm_scores.get("resumo", "")

        # =============================================================
        # SCORE TOTAL (média ponderada)
        # =============================================================
        pesos = {
            "fit": 0.25,
            "interest": 0.30,
            "engagement": 0.25,
            "timing": 0.20,
        }

        score_total = round(
            fit_score * pesos["fit"]
            + interest_score * pesos["interest"]
            + engagement_score * pesos["engagement"]
            + timing_score * pesos["timing"]
        )

        # Classificação
        classificacao = "COLD"
        for threshold, label in CLASSIFICACAO:
            if score_total >= threshold:
                classificacao = label
                break

        dimensoes = {
            "fit": {"score": fit_score, "peso": pesos["fit"], "razao": fit_razao},
            "interest": {"score": interest_score, "peso": pesos["interest"], "razao": interest_razao},
            "engagement": {
                "score": engagement_score,
                "peso": pesos["engagement"],
                "razao": engagement_razao,
                # Breakdown detalhado das 7 sub-dimensoes do engajamento
                # (atividade_recente, recencia, profundidade, volume,
                # responsividade, multicanalidade, completude). A UI
                # pode expandir e mostrar cada uma com seu score/peso.
                "sub_dimensoes": engagement_detalhe,
            },
            "timing": {"score": timing_score, "peso": pesos["timing"], "razao": timing_razao},
        }

        # =============================================================
        # NARRATIVA ALINHADA AO SCORE (gerada APOS calcular score_total)
        # =============================================================
        # Corrige viés otimista: o resumo_llm inicial e gerado SEM saber
        # do score final, o que fazia briefings de MQL/SAL saírem com
        # linguagem de SQL ("contato imediato", "priorizar", "excepcional").
        # Aqui rodamos uma segunda chamada ja com o score calculado e
        # com regras explicitas de vocabulario por classe.
        narrativa = await self._gerar_narrativa_alinhada(
            email=email,
            score_total=score_total,
            classificacao=classificacao,
            dimensoes=dimensoes,
            perfil_squad1=perfil_squad1,
            fallback_resumo=resumo_llm,
        )

        return {
            "email": email,
            "timestamp": now.isoformat(),
            "score_total": score_total,
            "classificacao": classificacao,
            "dimensoes": dimensoes,
            "resumo": narrativa["resumo"],
            "proximo_passo": narrativa["proximo_passo"],
            "narrativa_alinhada": narrativa.get("origem") == "llm_alinhado",
            "sinais_engajamento": (engajamento or {}).get("sinais_comportamentais", []),
        }

    # ------------------------------------------------------------------
    # Timing Score
    # ------------------------------------------------------------------

    def _calcular_timing(
        self,
        now: datetime,
        perfil_squad1: dict | None,
        engajamento: dict | None,
    ) -> tuple[int, str]:
        """
        Calcula score de timing (0-100) — APENAS "e o momento certo?".

        Timing responde a uma pergunta simples: a janela comercial esta
        aberta? Nao se preocupa com o QUANTO o lead interage (isso e
        engajamento). Composicao:

          Sazonalidade     ........ 0..50  (mes atual no calendario)
          Recencia         ........ 0..50  (dias desde ultima conversao)

        A quantidade/tipo de interacoes nos ultimos 30 dias entra no
        score de ENGAJAMENTO, nao aqui.
        """
        score = 0
        partes: list[str] = []

        # --- Sazonalidade (0..50) ---
        mes_atual = now.month
        if mes_atual in MESES_PICO:
            score += 50
            partes.append("mes em temporada de matricula (jan-mar/jul-ago)")
        elif mes_atual in {4, 5, 6, 9, 10}:
            score += 25
            partes.append("periodo intermediario de matricula")
        else:
            score += 10
            partes.append("fora de temporada (nov/dez)")

        # --- Recencia pela ultima conversao (0..50) ---
        dias_ult = None
        if perfil_squad1:
            dias_ult = (perfil_squad1.get("metricas_engajamento", {}) or {}).get(
                "dias_desde_ultima_conversao"
            )
        if dias_ult is None:
            partes.append("sem historico de conversoes")
        elif dias_ult <= 7:
            score += 50
            partes.append(f"ultima conversao ha {dias_ult}d (muito recente)")
        elif dias_ult <= 14:
            score += 35
            partes.append(f"ultima conversao ha {dias_ult}d")
        elif dias_ult <= 30:
            score += 20
            partes.append(f"ultima conversao ha {dias_ult}d")
        elif dias_ult <= 60:
            score += 8
            partes.append(f"ultima conversao ha {dias_ult}d (esfriando)")
        else:
            partes.append(f"ultima conversao ha {dias_ult}d (fora da janela)")

        razao = "; ".join(partes).capitalize() + "."
        return min(score, 100), razao

    # ------------------------------------------------------------------
    # Engagement razao (explicacao para a dimensao deterministica)
    # ------------------------------------------------------------------

    def _explicar_engagement(
        self,
        score: int,
        engajamento: dict | None,
        perfil_squad1: dict | None,
    ) -> str:
        """
        Gera uma razao textual para o score de engajamento, destacando
        interacoes ATIVAS (eventos, webinars, materiais, mensagens) por
        tipo — que e o que o comercial quer ver.
        """
        if not engajamento and not perfil_squad1:
            return "Sem dados de engajamento disponiveis."

        partes: list[str] = []

        sub = (engajamento or {}).get("scores", {}) or {}
        canais = (engajamento or {}).get("canais_ativos", []) or []
        hablla = (engajamento or {}).get("hablla", {}) or {}
        interacoes = (perfil_squad1 or {}).get("interacoes_conteudo", {}) or {}
        metricas = (perfil_squad1 or {}).get("metricas_engajamento", {}) or {}

        # Visao geral
        if score >= 70:
            partes.append("lead altamente engajado")
        elif score >= 40:
            partes.append("engajamento moderado")
        elif score >= 20:
            partes.append("engajamento baixo")
        else:
            partes.append("engajamento minimo")

        # Interacoes ATIVAS em 30 dias — o coracao do engajamento
        eventos_30 = (interacoes.get("eventos_30d", 0) or 0) + (interacoes.get("webinars_30d", 0) or 0)
        materiais_30 = interacoes.get("materiais_30d", 0) or 0
        formularios_30 = interacoes.get("formularios_30d", 0) or 0
        newsletters_30 = interacoes.get("newsletters_30d", 0) or 0
        conv_30d = metricas.get("conversoes_ultimos_30d", 0) or 0

        ativ: list[str] = []
        if eventos_30:
            ativ.append(f"{eventos_30} evento(s)/webinar(s)")
        if materiais_30:
            ativ.append(f"{materiais_30} material(is)")
        if formularios_30:
            ativ.append(f"{formularios_30} formulario(s)")
        if newsletters_30 >= 3:
            ativ.append(f"{newsletters_30} newsletters")
        if ativ:
            partes.append("em 30d: " + ", ".join(ativ))
        elif conv_30d:
            partes.append(f"{conv_30d} conversao(s) em 30d")

        # Contexto historico (separado)
        total_conv = metricas.get("total_conversoes", 0) or 0
        dias_ult = metricas.get("dias_desde_ultima_conversao")
        if total_conv and not ativ and not conv_30d:
            if dias_ult is not None:
                partes.append(f"{total_conv} conversao(s) historicas (ultima ha {dias_ult}d)")
            else:
                partes.append(f"{total_conv} conversao(s) no historico")

        # Hablla
        total_msgs = hablla.get("total_msgs_recebidas_do_lead", 0) or 0
        cards_abertos = hablla.get("cards_abertos", 0) or 0
        if total_msgs:
            partes.append(f"{total_msgs} msg(s) Hablla")
        if cards_abertos:
            partes.append(f"{cards_abertos} card(s) ativo(s)")

        # Canais
        if canais:
            partes.append(f"canais: {', '.join(canais)}")

        # Sinais de alerta
        if sub.get("recencia", 0) <= 15 and not ativ and conv_30d == 0:
            partes.append("sem interacoes recentes")
        if sub.get("responsividade", 0) == 0 and total_msgs:
            partes.append("lead nao respondeu mensagens")

        return "; ".join(partes).capitalize() + "."

    # ------------------------------------------------------------------
    # Inferência via LLM (Fit + Interest)
    # ------------------------------------------------------------------

    async def _inferir_fit_e_interesse(
        self,
        email: str,
        perfil_squad1: dict | None,
        engajamento: dict | None,
    ) -> dict:
        """
        Usa LLM para analisar dados não-estruturados e inferir
        fit e interesse do lead.
        """
        # Montar contexto para o LLM
        contexto_parts: list[str] = []

        if perfil_squad1:
            dados = perfil_squad1.get("dados_basicos", {})
            contexto_parts.append(f"DADOS DO LEAD:")
            contexto_parts.append(f"- Nome: {dados.get('name', 'N/A')}")
            contexto_parts.append(f"- Email: {email}")
            contexto_parts.append(f"- Cargo: {dados.get('job_title', 'N/A')}")
            contexto_parts.append(f"- Empresa: {dados.get('company_name', 'N/A')}")
            contexto_parts.append(f"- Cidade/Estado: {dados.get('city', 'N/A')}/{dados.get('state', 'N/A')}")

            # Análise do Squad 1
            analysis = perfil_squad1.get("analysis", {})
            if analysis:
                contexto_parts.append(f"\nANÁLISE SQUAD 1:")
                contexto_parts.append(f"- Temperatura: {analysis.get('temperatura', 'N/A')}")
                contexto_parts.append(f"- Área principal: {analysis.get('area_principal', 'N/A')}")
                contexto_parts.append(f"- Cursos sugeridos: {', '.join(analysis.get('cursos_sugeridos', []))}")
                contexto_parts.append(f"- Resumo: {analysis.get('resumo_perfil', 'N/A')}")

            # Interações de conteúdo
            interacoes = perfil_squad1.get("interacoes_conteudo", {})
            if interacoes:
                contexto_parts.append(f"\nINTERAÇÕES COM CONTEÚDO:")
                contexto_parts.append(f"- Newsletters: {interacoes.get('newsletters', 0)}")
                contexto_parts.append(f"- Eventos: {interacoes.get('eventos', 0)}")
                contexto_parts.append(f"- Webinars: {interacoes.get('webinars', 0)}")
                contexto_parts.append(f"- Materiais: {interacoes.get('materiais', 0)}")

        if engajamento:
            hablla = engajamento.get("hablla", {})
            if hablla.get("tem_dados"):
                contexto_parts.append(f"\nDADOS HABLLA (CRM/WHATSAPP):")
                contexto_parts.append(f"- Total de conversas: {hablla.get('total_conversas', 0)}")
                contexto_parts.append(f"- Canais: {', '.join(hablla.get('canais_com_interacao', []))}")
                contexto_parts.append(f"- Msgs do lead: {hablla.get('total_msgs_recebidas_do_lead', 0)}")
                contexto_parts.append(f"- Cards/Deals: {hablla.get('total_cards', 0)} (abertos: {hablla.get('cards_abertos', 0)})")

                # Anotações da equipe (insights qualitativos valiosos)
                anotacoes = hablla.get("anotacoes_recentes", [])
                if anotacoes:
                    contexto_parts.append(f"\nANOTAÇÕES DA EQUIPE COMERCIAL:")
                    for a in anotacoes:
                        autor = a.get("author", "equipe")
                        texto = a.get("content", "")
                        if texto:
                            contexto_parts.append(f"  [{autor}]: {texto}")

            # Sinais comportamentais
            sinais = engajamento.get("sinais_comportamentais", [])
            if sinais:
                contexto_parts.append(f"\nSINAIS COMPORTAMENTAIS:")
                for s in sinais:
                    contexto_parts.append(f"- {s.get('tipo')}: {s.get('descricao')} ({s.get('peso')})")

        if not contexto_parts:
            # Sem dados — retorna scores baixos
            return {
                "fit_score": 20,
                "interest_score": 15,
                "fit_razao": "Dados insuficientes para avaliar fit",
                "interest_razao": "Dados insuficientes para avaliar interesse",
                "resumo": "Lead com dados muito limitados para scoring",
            }

        contexto = "\n".join(contexto_parts)

        system_prompt = """Você é um analista de qualificação de leads especializado em pós-graduação.

A BSSP é uma instituição de ensino que oferece cursos de pós-graduação nas áreas:
gestão, contabilidade, direito, engenharia, saúde, psicologia, trabalhista, perícia, educação, tecnologia.

Analise os dados do lead e retorne um JSON com:
1. fit_score (0-100): O perfil do lead é compatível com pós-graduação na BSSP?
   - Considere: cargo, empresa, formação, região, área de atuação
   - Score alto: profissional empregado, área alinhada com cursos BSSP, região atendida
   - Score baixo: estudante, sem dados de perfil, área muito distante dos cursos

2. interest_score (0-100): O lead demonstra interesse real em pós-graduação?
   - Considere: tipos de conteúdo acessado, perguntas sobre cursos, frequência de interações
   - Score alto: interagiu com conteúdo de cursos específicos, perguntou sobre preço/turma, múltiplas conversas
   - Score baixo: apenas uma visita genérica, sem interação de conteúdo relevante

3. fit_razao: Justificativa curta (1-2 frases) para o fit_score
4. interest_razao: Justificativa curta (1-2 frases) para o interest_score
5. resumo: Resumo de 2-3 frases sobre o potencial deste lead

RESPONDA APENAS COM JSON VÁLIDO, sem markdown."""

        try:
            result = await self.llm.complete_json(
                messages=[LLMMessage(role="user", content=contexto)],
                system=system_prompt,
                # temperature=0: maxima reprodutibilidade. Valores > 0
                # introduzem variancia entre execucoes e fazem o mesmo
                # lead oscilar de classificacao (SAL<->MQL<->SQL).
                temperature=0.0,
            )
            # Validar e limitar scores
            result["fit_score"] = max(0, min(100, int(result.get("fit_score", 30))))
            result["interest_score"] = max(0, min(100, int(result.get("interest_score", 20))))
            return result

        except Exception as e:
            logger.warning("LLM scoring falhou para %s: %s — usando heurística", email, e)
            return self._fallback_heuristico(perfil_squad1, engajamento)

    # ------------------------------------------------------------------
    # Narrativa alinhada ao score (resumo + proximo_passo coerentes)
    # ------------------------------------------------------------------

    async def _gerar_narrativa_alinhada(
        self,
        *,
        email: str,
        score_total: int,
        classificacao: str,
        dimensoes: dict,
        perfil_squad1: dict | None,
        fallback_resumo: str,
    ) -> dict:
        """
        Gera (resumo, proximo_passo) coerentes com a faixa de score.

        Roda DEPOIS do calculo de score_total. Injeta a classificacao e
        as regras de tom no prompt do LLM — evita que briefings de
        MQL/SAL/COLD saiam com vocabulario de SQL.

        Em caso de falha do LLM, retorna o resumo original + um
        proximo_passo deterministico pela classe (TOM_POR_CLASSE).
        """
        tom = TOM_POR_CLASSE.get(classificacao, TOM_POR_CLASSE["COLD"])

        # Montar contexto compacto com os 4 scores e razoes
        dados_basicos = (perfil_squad1 or {}).get("dados_basicos", {}) or {}
        analysis = (perfil_squad1 or {}).get("analysis", {}) or {}

        contexto = [
            f"LEAD: {email}",
            f"  Nome:  {dados_basicos.get('name', 'N/A')}",
            f"  Cargo: {dados_basicos.get('job_title', 'N/A')}",
            f"  Área:  {analysis.get('area_principal', 'N/A')}",
            "",
            f"CLASSIFICAÇÃO FINAL: {classificacao} (score {score_total}/100)",
            f"  Fit:        {dimensoes['fit']['score']}/100 — {dimensoes['fit']['razao']}",
            f"  Interest:   {dimensoes['interest']['score']}/100 — {dimensoes['interest']['razao']}",
            f"  Engagement: {dimensoes['engagement']['score']}/100 — {dimensoes['engagement']['razao']}",
            f"  Timing:     {dimensoes['timing']['score']}/100 — {dimensoes['timing']['razao']}",
        ]
        contexto_str = "\n".join(contexto)

        vocab_proibido = ", ".join(f'"{v}"' for v in tom["vocab_proibido"]) or "(nenhum)"
        vocab_permitido = ", ".join(f'"{v}"' for v in tom["vocab_permitido"])

        system_prompt = f"""Você é redator técnico de briefings comerciais em uma instituição de pós-graduação.

Sua ÚNICA tarefa é escrever o resumo e o próximo passo de um lead JÁ classificado, com tom EXATAMENTE alinhado à classificação. Não reavalie o score. Não discorde da classificação.

CLASSIFICAÇÃO DESTE LEAD: {classificacao} — {tom['descricao']}

AÇÃO RECOMENDADA PARA ESTA CLASSE: {tom['acao_recomendada']}

REGRAS DE VOCABULÁRIO (obrigatórias):
- NÃO use os termos: {vocab_proibido}
- Prefira termos como: {vocab_permitido}

REGRAS DE CONTEÚDO:
- Seja factual. Se dados são incompletos, diga "dados incompletos"; não invente interesse.
- O próximo_passo DEVE refletir a ação recomendada acima — não recomende "contato comercial imediato" se a classe é MQL/SAL/COLD.
- Se o score foi rebaixado por engajamento baixo ou timing ruim, o resumo deve mencionar essa limitação.

Retorne JSON com exatamente estas chaves:
{{
  "resumo": "2-3 frases descrevendo o lead, coerentes com a classificação {classificacao}.",
  "proximo_passo": "1 frase objetiva com a ação concreta, alinhada à classe {classificacao}."
}}

RESPONDA APENAS COM JSON VÁLIDO, sem markdown."""

        try:
            result = await self.llm.complete_json(
                messages=[LLMMessage(role="user", content=contexto_str)],
                system=system_prompt,
                temperature=0.0,
            )
            resumo = str(result.get("resumo", "")).strip()
            proximo = str(result.get("proximo_passo", "")).strip()
            if not resumo or not proximo:
                raise ValueError("LLM retornou campos vazios")

            # Validador de coerencia: se algum termo proibido aparecer,
            # reforça o fallback deterministico ao inves de deixar passar.
            texto_completo = f"{resumo} {proximo}".lower()
            violou = [v for v in tom["vocab_proibido"] if v.lower() in texto_completo]
            if violou:
                logger.warning(
                    "Narrativa de %s violou vocabulário proibido (%s) — usando fallback",
                    email, violou,
                )
                return {
                    "resumo": fallback_resumo or f"Lead classificado como {classificacao}.",
                    "proximo_passo": tom["acao_recomendada"],
                    "origem": "fallback_vocab_violado",
                }

            return {
                "resumo": resumo,
                "proximo_passo": proximo,
                "origem": "llm_alinhado",
            }

        except Exception as e:
            logger.warning(
                "Narrativa alinhada falhou para %s: %s — usando fallback deterministico",
                email, e,
            )
            return {
                "resumo": fallback_resumo or f"Lead classificado como {classificacao}.",
                "proximo_passo": tom["acao_recomendada"],
                "origem": "fallback_erro_llm",
            }

    def _fallback_heuristico(
        self,
        perfil_squad1: dict | None,
        engajamento: dict | None,
    ) -> dict:
        """Fallback determinístico caso o LLM falhe."""
        fit = 30
        interest = 20

        if perfil_squad1:
            dados = perfil_squad1.get("dados_basicos", {})
            analysis = perfil_squad1.get("analysis", {})

            # Fit: cargo e área
            if dados.get("job_title"):
                fit += 15
            if analysis.get("area_principal") and analysis["area_principal"] in BSSP_AREAS:
                fit += 20
            if dados.get("company_name"):
                fit += 10
            if dados.get("city"):
                fit += 5

            # Interest: temperatura do Squad 1
            temp = analysis.get("temperatura", "")
            if temp == "quente":
                interest += 40
            elif temp == "morno":
                interest += 20
            elif temp == "frio":
                interest += 5

        if engajamento:
            scores = engajamento.get("scores", {})
            vol = scores.get("volume_interacao", 0)
            if vol >= 50:
                interest += 15
            elif vol >= 25:
                interest += 8

        return {
            "fit_score": min(fit, 100),
            "interest_score": min(interest, 100),
            "fit_razao": "Avaliação heurística (LLM indisponível)",
            "interest_razao": "Avaliação heurística (LLM indisponível)",
            # Este resumo serve apenas como FALLBACK, se a narrativa
            # alinhada (_gerar_narrativa_alinhada) tambem falhar.
            "resumo": "Scoring baseado em heurísticas — LLM temporariamente indisponível",
        }
