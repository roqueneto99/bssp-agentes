"""
Squad 2 — Agente 3: Classificador de Rota.

Com base no scoring do Agente 2, decide a PRÓXIMA AÇÃO do funil
para cada lead. Persiste a decisão no RD Station (tags + campos)
e no Hablla (tags + anotação).

Rotas possíveis:
    SQL_HANDOFF:     Score >= 75 → Transferir para consultor comercial
                     - Cria tag "sql-pronto" no RD Station
                     - Atualiza cf_rota_squad2 = "sql_handoff"
                     - Prepara briefing para o vendedor

    MQL_NURTURE:     Score 50-74 → Entrar em fluxo de nutrição avançado
                     - Cria tag "mql-nutricao" no RD Station
                     - Define fluxo de automação recomendado
                     - Gera sugestão de conteúdo personalizado

    SAL_NURTURE:     Score 35-49 → Nutrição básica com conteúdo educativo
                     - Cria tag "sal-acompanhamento"
                     - Sugere conteúdos genéricos

    COLD_RECYCLE:    Score < 35 → Manter na base, re-engajar em 30-60 dias
                     - Cria tag "cold-reciclagem"
                     - Define data de re-avaliação

    BLOCKED:         Lead bloqueado por LGPD → Nenhuma ação de comunicação
                     - Preserva bloqueio do Squad 1
                     - Registra motivo

Ações de persistência:
    RD Station:
        - Tags: adiciona tag da rota
        - Custom fields: cf_rota_squad2, cf_score_squad2, cf_classificacao_squad2,
          cf_data_scoring
    Hablla (se disponível):
        - Tags na pessoa
        - Anotação com resumo do scoring

100% determinístico — sem LLM. Lógica de regras baseada no scoring.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from ..base import AgentResult, LLMProvider

logger = logging.getLogger(__name__)

# Mapeamento de classificação → rota
ROTA_MAP = {
    "SQL": "sql_handoff",
    "MQL": "mql_nurture",
    "SAL": "sal_nurture",
    "COLD": "cold_recycle",
}

# Tags a aplicar no RD Station por rota
TAGS_ROTA = {
    "sql_handoff": ["squad2-scored", "sql-pronto", "prioridade-handoff"],
    "mql_nurture": ["squad2-scored", "mql-nutricao"],
    "sal_nurture": ["squad2-scored", "sal-acompanhamento"],
    "cold_recycle": ["squad2-scored", "cold-reciclagem"],
    "blocked": ["squad2-scored", "lgpd-bloqueado"],
}


class ClassificadorRotaAgent:
    """
    Agente determinístico que aplica a rota correta ao lead
    e persiste a decisão nos sistemas (RD Station + Hablla).
    """

    agent_name = "squad2_classificador_rota"

    def __init__(
        self,
        llm: LLMProvider,  # Não usado, mas mantido por compatibilidade
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
        scoring: dict | None = None,
        perfil_squad1: dict | None = None,
    ) -> AgentResult:
        """
        Classifica a rota e persiste a decisão.

        Args:
            email: Email do lead
            scoring: Resultado do Scorer (Agente 2)
            perfil_squad1: Dados do Squad 1 (para checar bloqueio LGPD)
        """
        start = time.monotonic()
        try:
            resultado = await self._classificar(email, scoring, perfil_squad1)
            duration = (time.monotonic() - start) * 1000

            logger.info(
                "%s concluído para %s: rota=%s, score=%d, %.0fms",
                self.agent_name, email,
                resultado.get("rota", "?"),
                resultado.get("score_total", 0),
                duration,
            )

            return AgentResult(
                success=True,
                agent_name=self.agent_name,
                contact_email=email,
                data=resultado,
                duration_ms=duration,
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

    async def _classificar(
        self,
        email: str,
        scoring: dict | None,
        perfil_squad1: dict | None,
    ) -> dict:
        """Decide a rota e persiste."""
        now = datetime.now(timezone.utc)

        # Verificar bloqueio LGPD
        bloqueado = False
        if perfil_squad1:
            bloqueado = perfil_squad1.get("bloqueado_lgpd", False)
            # Também checar no resumo do Squad 1
            if not bloqueado:
                compliance = perfil_squad1.get("compliance_status", "")
                if compliance == "revogado":
                    bloqueado = True

        if bloqueado:
            rota = "blocked"
            motivo = "Lead bloqueado pela conformidade LGPD — consentimento revogado"
        elif not scoring:
            rota = "cold_recycle"
            motivo = "Sem dados de scoring disponíveis"
        else:
            classificacao = scoring.get("classificacao", "COLD")
            rota = ROTA_MAP.get(classificacao, "cold_recycle")
            motivo = scoring.get("resumo", "")

        score_total = (scoring or {}).get("score_total", 0)
        classificacao = (scoring or {}).get("classificacao", "COLD")

        # Gerar ações recomendadas
        acoes = self._gerar_acoes(rota, scoring, perfil_squad1)

        # Gerar briefing — obrigatoriamente para SQL (handoff) e tambem
        # para MQL ou qualquer score >= 50 (a pedido do comercial, para
        # ter contexto rico quando o lead esta 'morno-quente' e pode
        # ser abordado sob demanda).
        briefing = None
        if rota == "sql_handoff" or score_total >= 50:
            briefing = self._gerar_briefing(email, scoring, perfil_squad1)

        resultado = {
            "email": email,
            "timestamp": now.isoformat(),
            "rota": rota,
            "classificacao": classificacao,
            "score_total": score_total,
            "motivo": motivo,
            "bloqueado_lgpd": bloqueado,
            "acoes_recomendadas": acoes,
            "briefing_comercial": briefing,
            "tags_aplicadas": [],
            "persistencia": {"rdstation": False, "hablla": False},
        }

        # =============================================================
        # PERSISTIR NO RD STATION
        # =============================================================
        rd_ok = await self._persistir_rdstation(email, rota, score_total, classificacao, now)
        resultado["persistencia"]["rdstation"] = rd_ok
        resultado["tags_aplicadas"] = TAGS_ROTA.get(rota, [])

        # =============================================================
        # PERSISTIR NO HABLLA (se disponível)
        # =============================================================
        hablla_ok = await self._persistir_hablla(email, rota, score_total, classificacao, motivo)
        resultado["persistencia"]["hablla"] = hablla_ok

        return resultado

    # ------------------------------------------------------------------
    # Ações Recomendadas por Rota
    # ------------------------------------------------------------------

    def _gerar_acoes(
        self,
        rota: str,
        scoring: dict | None,
        perfil_squad1: dict | None,
    ) -> list[dict]:
        """Gera lista de ações recomendadas baseadas na rota."""
        acoes: list[dict] = []

        if rota == "sql_handoff":
            acoes.append({
                "acao": "transferir_para_consultor",
                "prioridade": "alta",
                "descricao": "Lead pronto para contato comercial — agendar ligação em até 2h",
            })
            acoes.append({
                "acao": "enviar_email_pre_contato",
                "prioridade": "alta",
                "descricao": "Enviar email personalizado de boas-vindas antes da ligação",
            })
            area = ""
            if perfil_squad1:
                area = (perfil_squad1.get("analysis", {}).get("area_principal", "")
                        or perfil_squad1.get("area_principal", ""))
            if area:
                acoes.append({
                    "acao": "preparar_proposta",
                    "prioridade": "media",
                    "descricao": f"Preparar proposta focada na área: {area}",
                })

        elif rota == "mql_nurture":
            acoes.append({
                "acao": "inserir_fluxo_nurture_avancado",
                "prioridade": "alta",
                "descricao": "Inserir lead no fluxo de nutrição avançada (emails + WhatsApp)",
            })
            acoes.append({
                "acao": "enviar_conteudo_area",
                "prioridade": "media",
                "descricao": "Enviar material rico sobre a área de interesse identificada",
            })
            acoes.append({
                "acao": "agendar_reavaliacao",
                "prioridade": "media",
                "descricao": "Re-avaliar scoring em 7 dias",
            })

        elif rota == "sal_nurture":
            acoes.append({
                "acao": "inserir_fluxo_nurture_basico",
                "prioridade": "media",
                "descricao": "Inserir lead no fluxo de nutrição básica (emails educativos)",
            })
            acoes.append({
                "acao": "agendar_reavaliacao",
                "prioridade": "baixa",
                "descricao": "Re-avaliar scoring em 14 dias",
            })

        elif rota == "cold_recycle":
            acoes.append({
                "acao": "manter_na_base",
                "prioridade": "baixa",
                "descricao": "Manter na base — re-engajar com campanha geral em 30-60 dias",
            })

        elif rota == "blocked":
            acoes.append({
                "acao": "nenhuma_comunicacao",
                "prioridade": "critica",
                "descricao": "NÃO enviar comunicação — consentimento LGPD revogado",
            })

        return acoes

    # ------------------------------------------------------------------
    # Briefing Comercial (para SQL)
    # ------------------------------------------------------------------

    def _gerar_briefing(
        self,
        email: str,
        scoring: dict | None,
        perfil_squad1: dict | None,
    ) -> dict:
        """Gera briefing para o consultor comercial (rota SQL)."""
        briefing: dict[str, Any] = {"email": email}

        if perfil_squad1:
            dados = perfil_squad1.get("dados_basicos", {})
            analysis = perfil_squad1.get("analysis", {})

            briefing["nome"] = dados.get("name", "")
            briefing["cargo"] = dados.get("job_title", "")
            briefing["empresa"] = dados.get("company_name", "")
            briefing["telefone"] = (
                dados.get("personal_phone") or dados.get("mobile_phone") or ""
            )
            briefing["cidade_estado"] = (
                f"{dados.get('city', '')}/{dados.get('state', '')}"
            )
            briefing["area_interesse"] = analysis.get("area_principal", "")
            briefing["cursos_sugeridos"] = analysis.get("cursos_sugeridos", [])
            briefing["resumo_perfil"] = analysis.get("resumo_perfil", "")
            briefing["proximo_passo_sugerido"] = analysis.get("proximo_passo", "")

        if scoring:
            briefing["score"] = scoring.get("score_total", 0)
            briefing["classificacao"] = scoring.get("classificacao", "")
            briefing["resumo_scoring"] = scoring.get("resumo", "")

            # Pontos fortes (dimensões com score alto)
            dimensoes = scoring.get("dimensoes", {})
            pontos_fortes = []
            for dim, info in dimensoes.items():
                if isinstance(info, dict) and info.get("score", 0) >= 70:
                    razao = info.get("razao", "")
                    pontos_fortes.append(f"{dim}: {info['score']} — {razao}" if razao else f"{dim}: {info['score']}")
            briefing["pontos_fortes"] = pontos_fortes

        return briefing

    # ------------------------------------------------------------------
    # Persistência RD Station
    # ------------------------------------------------------------------

    async def _persistir_rdstation(
        self,
        email: str,
        rota: str,
        score: int,
        classificacao: str,
        now: datetime,
    ) -> bool:
        """Persiste scoring e rota no RD Station via tags + custom fields."""
        try:
            # 1. Adicionar tags da rota
            tags = TAGS_ROTA.get(rota, ["squad2-scored"])
            # Remover tags de rotas anteriores
            tags_score = [f"score-{classificacao.lower()}"]
            all_tags = tags + tags_score

            await self.rdstation.add_tags(email, all_tags)

            # 2. Atualizar custom fields
            custom_fields = {
                "cf_rota_squad2": rota,
                "cf_score_squad2": str(score),
                "cf_classificacao_squad2": classificacao,
                "cf_data_scoring": now.strftime("%Y-%m-%d %H:%M"),
            }
            await self.rdstation.upsert_contact(email, custom_fields)

            logger.info(
                "RD Station atualizado para %s: tags=%s, rota=%s, score=%d",
                email, all_tags, rota, score,
            )
            return True

        except Exception as e:
            logger.error("Erro ao persistir no RD Station para %s: %s", email, e)
            return False

    # ------------------------------------------------------------------
    # Persistência Hablla
    # ------------------------------------------------------------------

    async def _persistir_hablla(
        self,
        email: str,
        rota: str,
        score: int,
        classificacao: str,
        motivo: str,
    ) -> bool:
        """Persiste scoring no Hablla via tags + anotação."""
        if not self.hablla:
            return False

        try:
            # Buscar pessoa no Hablla
            pessoa = await self.hablla.search_person_by_email(email)
            if not pessoa:
                logger.info("Lead %s não encontrado no Hablla — skip", email)
                return False

            person_id = pessoa.get("id") or pessoa.get("_id") or ""
            if not person_id:
                return False

            # 1. Adicionar tags (Hablla precisa de IDs, não nomes)
            tag_names = [f"squad2-{classificacao.lower()}", f"rota-{rota}"]
            try:
                tag_ids = await self.hablla.resolve_tag_ids(tag_names)
                if tag_ids:
                    await self.hablla.add_person_tags(person_id, tag_ids)
            except Exception as e:
                logger.warning("Erro ao adicionar tags Hablla para %s: %s", email, e)

            # 2. Criar anotação com resumo do scoring
            nota = (
                f"[Squad 2 — Scoring Automático]\n"
                f"Score: {score}/100 | Classificação: {classificacao}\n"
                f"Rota: {rota}\n"
                f"{motivo}"
            )
            try:
                await self.hablla.create_annotation(
                    content=nota, person_id=person_id,
                )
            except Exception as e:
                logger.warning("Erro ao criar anotação Hablla para %s: %s", email, e)

            logger.info(
                "Hablla atualizado para %s: tags=%s, anotação criada",
                email, tag_names,
            )
            return True

        except Exception as e:
            logger.error("Erro geral ao persistir no Hablla para %s: %s", email, e)
            return False
