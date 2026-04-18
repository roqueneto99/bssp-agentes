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
        # SCORE 1: ENGAGEMENT (determinístico — vem do Analisador)
        # =============================================================
        engagement_score = 0
        if engajamento:
            engagement_score = engajamento.get("score_engajamento_total", 0)
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

        return {
            "email": email,
            "timestamp": now.isoformat(),
            "score_total": score_total,
            "classificacao": classificacao,
            "dimensoes": {
                "fit": {"score": fit_score, "peso": pesos["fit"], "razao": fit_razao},
                "interest": {"score": interest_score, "peso": pesos["interest"], "razao": interest_razao},
                "engagement": {"score": engagement_score, "peso": pesos["engagement"], "razao": engagement_razao},
                "timing": {"score": timing_score, "peso": pesos["timing"], "razao": timing_razao},
            },
            "resumo": resumo_llm,
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
        Calcula score de timing (0-100) e devolve tambem uma razao
        textual explicando a composicao.

        Fatores:
        - Proximidade de início de turmas (picos jan-mar, jul-ago)
        - Velocidade de progressão (conversões recentes = urgência)
        - Recência da última interação
        """
        score = 0
        partes: list[str] = []

        # Sazonalidade: meses de pico de matrícula
        mes_atual = now.month
        if mes_atual in MESES_PICO:
            score += 40
            partes.append("mes em temporada de matricula (jan-mar/jul-ago)")
        elif mes_atual in {4, 5, 6, 9, 10}:
            score += 20
            partes.append("periodo intermediario de matricula")
        else:
            score += 10
            partes.append("fora de temporada (nov/dez)")

        # Recência da última interação
        recencia = 0
        if engajamento:
            recencia = engajamento.get("scores", {}).get("recencia", 0)
        if recencia >= 80:
            score += 35
            partes.append("interacao muito recente")
        elif recencia >= 50:
            score += 20
            partes.append("interacao recente")
        elif recencia >= 20:
            score += 10
            partes.append("ultima interacao com algum atraso")
        else:
            partes.append("sem interacoes recentes")

        # Velocidade de progressão (múltiplas conversões recentes)
        total_conv = 0
        if perfil_squad1:
            total_conv = perfil_squad1.get("metricas_engajamento", {}).get("total_conversoes", 0)
            if total_conv >= 5:
                score += 25
                partes.append(f"alta velocidade no funil ({total_conv} conversoes)")
            elif total_conv >= 3:
                score += 15
                partes.append(f"{total_conv} conversoes (velocidade moderada)")
            elif total_conv >= 1:
                score += 5
                partes.append(f"{total_conv} conversao(s) registradas")
            else:
                partes.append("sem conversoes registradas")

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
        Gera uma razao textual para o score de engajamento.
        O score em si vem do Analisador de Engajamento (ja determinístico);
        aqui so explicamos o que contribuiu para ele.
        """
        if not engajamento and not perfil_squad1:
            return "Sem dados de engajamento disponiveis."

        partes: list[str] = []

        sub = (engajamento or {}).get("scores", {}) or {}
        canais = (engajamento or {}).get("canais_ativos", []) or []
        rd = (engajamento or {}).get("rd", {}) or {}
        hablla = (engajamento or {}).get("hablla", {}) or {}

        # Visao geral pelo score
        if score >= 70:
            partes.append("lead altamente engajado")
        elif score >= 40:
            partes.append("engajamento moderado")
        elif score >= 20:
            partes.append("engajamento baixo")
        else:
            partes.append("engajamento minimo")

        # Volume e multicanalidade
        if canais:
            partes.append(f"{len(canais)} canal(is): {', '.join(canais)}")

        # Conversoes / interacoes RD
        total_conv = 0
        if perfil_squad1:
            total_conv = perfil_squad1.get("metricas_engajamento", {}).get("total_conversoes", 0) or 0
        if total_conv:
            partes.append(f"{total_conv} conversao(s) no RD Station")

        # Dados Hablla
        total_msgs = hablla.get("total_msgs_recebidas_do_lead", 0) or 0
        cards_abertos = hablla.get("cards_abertos", 0) or 0
        if total_msgs:
            partes.append(f"{total_msgs} mensagem(ns) no Hablla")
        if cards_abertos:
            partes.append(f"{cards_abertos} card(s) ativo(s)")

        # Sinais de alerta
        if sub.get("recencia", 0) <= 15:
            partes.append("sem interacoes nas ultimas semanas")
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
            "resumo": "Scoring baseado em heurísticas — LLM temporariamente indisponível",
        }
