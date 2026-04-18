"""
Squad 2 — Agente 1: Analisador de Engajamento Multicanal.

Agrega sinais de engajamento de todas as fontes (RD Station + Hablla)
em um perfil comportamental unificado. Esse perfil alimenta o Scorer.

Fontes de dados:
    RD Station:
        - Conversões (formulários, landing pages, materiais)
        - Eventos de email (aberturas, cliques)
        - Estágio no funil
        - Tags e campos customizados

    Hablla:
        - Conversas WhatsApp/Telegram/Instagram/Email
        - Mensagens enviadas/recebidas (volume e frequência)
        - Tempo de resposta médio
        - Cartões/deals abertos
        - Anotações da equipe comercial
        - Tickets de atendimento

Saída: Perfil de engajamento com métricas normalizadas (0-100)
       e sinais comportamentais categorizados.

100% determinístico — sem LLM. Coleta e normaliza dados.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from ..base import AgentResult, LLMProvider

logger = logging.getLogger(__name__)


class AnalisadorEngajamentoAgent:
    """
    Agente que coleta e unifica dados de engajamento multicanal.

    Não usa LLM — é puramente determinístico. Busca dados nas APIs
    do RD Station e Hablla, normaliza e calcula métricas.
    """

    agent_name = "squad2_analisador_engajamento"

    def __init__(
        self,
        llm: LLMProvider,  # Mantido por compatibilidade, mas não usado
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
    ) -> AgentResult:
        """
        Executa a análise de engajamento multicanal.

        Args:
            email: Email do lead
            perfil_squad1: Dados do Squad 1 (evita re-fetch do RD Station)
        """
        start = time.monotonic()
        try:
            engajamento = await self._analisar(email, perfil_squad1)
            duration = (time.monotonic() - start) * 1000

            logger.info(
                "%s concluído para %s: score_total=%d, canais=%d, %.0fms",
                self.agent_name, email,
                engajamento.get("score_engajamento_total", 0),
                len(engajamento.get("canais_ativos", [])),
                duration,
            )

            return AgentResult(
                success=True,
                agent_name=self.agent_name,
                contact_email=email,
                data=engajamento,
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

    async def _analisar(
        self,
        email: str,
        perfil_squad1: dict | None,
    ) -> dict:
        """Coleta e unifica dados de engajamento de todas as fontes."""

        now = datetime.now(timezone.utc)
        resultado: dict[str, Any] = {
            "email": email,
            "timestamp": now.isoformat(),
            "canais_ativos": [],
        }

        # =============================================================
        # 1. DADOS DO RD STATION (via perfil_squad1 ou fetch direto)
        # =============================================================
        rd_metrics = await self._coletar_rdstation(email, perfil_squad1)
        resultado["rdstation"] = rd_metrics

        if rd_metrics.get("tem_dados"):
            resultado["canais_ativos"].append("rdstation")

        # =============================================================
        # 2. DADOS DO HABLLA (se client disponível)
        # =============================================================
        hablla_metrics = await self._coletar_hablla(email)
        resultado["hablla"] = hablla_metrics

        for canal in hablla_metrics.get("canais_com_interacao", []):
            if canal not in resultado["canais_ativos"]:
                resultado["canais_ativos"].append(canal)

        # =============================================================
        # 3. CALCULAR SCORES NORMALIZADOS (0-100)
        # =============================================================
        scores = self._calcular_scores(rd_metrics, hablla_metrics, now)
        resultado["scores"] = scores

        # Score total ponderado
        score_total = self._score_total_ponderado(scores)
        resultado["score_engajamento_total"] = score_total

        # =============================================================
        # 4. SINAIS COMPORTAMENTAIS
        # =============================================================
        sinais = self._extrair_sinais(rd_metrics, hablla_metrics, scores, now)
        resultado["sinais_comportamentais"] = sinais

        return resultado

    # ------------------------------------------------------------------
    # Coleta RD Station
    # ------------------------------------------------------------------

    async def _coletar_rdstation(
        self, email: str, perfil_squad1: dict | None,
    ) -> dict:
        """Extrai métricas de engajamento do RD Station."""
        metrics: dict[str, Any] = {"tem_dados": False}

        try:
            if perfil_squad1:
                # Reusar dados do Squad 1
                metricas_s1 = perfil_squad1.get("metricas_engajamento", {})
                metrics["total_conversoes"] = metricas_s1.get("total_conversoes", 0)
                metrics["score_engajamento_s1"] = metricas_s1.get("score_engajamento", 0)
                metrics["total_oportunidades"] = metricas_s1.get("total_oportunidades", 0)
                metrics["completude_perfil"] = perfil_squad1.get("completude_perfil", 0)
                metrics["temperatura_s1"] = (
                    perfil_squad1.get("analysis", {}).get("temperatura", "")
                    or perfil_squad1.get("temperatura", "")
                )
                metrics["prioridade_s1"] = (
                    perfil_squad1.get("analysis", {}).get("prioridade_contato", "")
                    or perfil_squad1.get("prioridade_contato", "")
                )

                # Datas
                metrics["created_at"] = perfil_squad1.get("dados_basicos", {}).get("created_at", "")
                metrics["last_conversion"] = perfil_squad1.get("dados_basicos", {}).get("last_conversion_date", "")

                # Interações com conteúdo — totais + janelas recentes
                interacoes = perfil_squad1.get("interacoes_conteudo", {}) or {}
                metrics["newsletters"] = interacoes.get("newsletters", 0)
                metrics["eventos"] = interacoes.get("eventos", 0)
                metrics["webinars"] = interacoes.get("webinars", 0)
                metrics["materiais"] = interacoes.get("materiais", 0)
                metrics["formularios"] = interacoes.get("formularios", 0)
                # Ultimos 30 dias (usados para 'atividade recente' no score)
                metrics["eventos_30d"] = interacoes.get("eventos_30d", 0)
                metrics["webinars_30d"] = interacoes.get("webinars_30d", 0)
                metrics["materiais_30d"] = interacoes.get("materiais_30d", 0)
                metrics["newsletters_30d"] = interacoes.get("newsletters_30d", 0)
                metrics["formularios_30d"] = interacoes.get("formularios_30d", 0)
                metrics["total_30d"] = interacoes.get("total_30d", 0)
                # Ultimos 90 dias
                metrics["total_90d"] = interacoes.get("total_90d", 0)

                # Conversoes do coletor (ja filtradas por janela)
                metrics["conversoes_ultimos_30d"] = metricas_s1.get("conversoes_ultimos_30d", 0)
                metrics["conversoes_ultimos_90d"] = metricas_s1.get("conversoes_ultimos_90d", 0)
                metrics["dias_desde_ultima_conversao"] = metricas_s1.get("dias_desde_ultima_conversao")

                metrics["tem_dados"] = True
            else:
                # Fetch direto — contato + eventos
                contact = await self.rdstation.get_contact(email=email)
                cd = contact.to_api_payload()

                metrics["created_at"] = cd.get("created_at", "")
                metrics["last_conversion"] = cd.get("last_conversion_date", "")
                metrics["completude_perfil"] = self._calc_completude(cd)

                # Eventos
                try:
                    events = await self.rdstation.get_contact_events(
                        email, event_types=["CONVERSION", "OPPORTUNITY"],
                    )
                    metrics["total_conversoes"] = sum(
                        1 for e in events if e.get("event_type") == "CONVERSION"
                    )
                    metrics["total_oportunidades"] = sum(
                        1 for e in events if e.get("event_type") == "OPPORTUNITY"
                    )
                except Exception:
                    metrics["total_conversoes"] = 0
                    metrics["total_oportunidades"] = 0

                # Funil
                try:
                    funnel = await self.rdstation.get_funnel_stage(email)
                    metrics["estagio_funil"] = funnel.get("lifecycle_stage", "")
                    metrics["score_perfil"] = funnel.get("contact_owner_email", "")
                except Exception:
                    pass

                metrics["tem_dados"] = True

        except Exception as e:
            logger.warning("Erro ao coletar RD Station para %s: %s", email, e)

        return metrics

    def _calc_completude(self, contact_data: dict) -> float:
        """Calcula completude do perfil (0-1)."""
        campos = [
            "name", "email", "job_title", "company_name",
            "personal_phone", "city", "state", "linkedin",
        ]
        preenchidos = sum(1 for c in campos if contact_data.get(c))
        return preenchidos / len(campos) if campos else 0

    # ------------------------------------------------------------------
    # Coleta Hablla
    # ------------------------------------------------------------------

    async def _coletar_hablla(self, email: str) -> dict:
        """Extrai métricas de engajamento do Hablla."""
        metrics: dict[str, Any] = {
            "tem_dados": False,
            "canais_com_interacao": [],
        }

        if not self.hablla:
            return metrics

        try:
            # 1. Buscar pessoa no Hablla
            pessoa = await self.hablla.search_person_by_email(email)
            if not pessoa:
                return metrics

            person_id = pessoa.get("id") or pessoa.get("_id") or ""
            if not person_id:
                return metrics

            metrics["tem_dados"] = True
            metrics["person_id"] = person_id
            metrics["nome_hablla"] = pessoa.get("name", "")
            metrics["customer_status"] = pessoa.get("customer_status", "")

            # Tags — pode ser lista de dicts ou strings
            raw_tags = pessoa.get("tags", [])
            metrics["tags_hablla"] = [
                t.get("name", str(t)) if isinstance(t, dict) else str(t)
                for t in raw_tags
            ]

            # 2. Serviços / Atendimentos (= conversas multicanal)
            try:
                svcs_data = await self.hablla.list_services(
                    person_id=person_id, limit=50,
                )
                services = svcs_data.get("results", [])
                total_services = svcs_data.get("totalItems", len(services))
                metrics["total_conversas"] = total_services

                canais_set: set[str] = set()
                status_counts: dict[str, int] = {}

                for svc in services:
                    canal = (svc.get("type") or "").lower()
                    if canal:
                        canais_set.add(canal)
                    st = (svc.get("status") or "").lower()
                    status_counts[st] = status_counts.get(st, 0) + 1

                metrics["canais_com_interacao"] = sorted(canais_set)
                metrics["services_status"] = status_counts

                # Última interação
                if services:
                    ultima = max(
                        services,
                        key=lambda s: s.get("updated_at") or s.get("created_at") or "",
                    )
                    metrics["ultima_conversa_data"] = (
                        ultima.get("updated_at") or ultima.get("created_at") or ""
                    )
                    metrics["ultima_conversa_canal"] = (
                        ultima.get("type") or ""
                    )
                    metrics["ultima_conversa_status"] = (
                        ultima.get("status") or ""
                    )

                # Atividade nos últimos 7 e 30 dias
                now = datetime.now(timezone.utc)
                recent_7d = 0
                recent_30d = 0
                for svc in services:
                    dt_str = svc.get("created_at") or ""
                    if dt_str:
                        try:
                            dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
                            if dt.tzinfo is None:
                                dt = dt.replace(tzinfo=timezone.utc)
                            delta = (now - dt).days
                            if delta <= 7:
                                recent_7d += 1
                            if delta <= 30:
                                recent_30d += 1
                        except (ValueError, AttributeError):
                            pass
                metrics["services_ultimos_7d"] = recent_7d
                metrics["services_ultimos_30d"] = recent_30d

                # Mensagens (não disponíveis via token de integração)
                # mas podemos estimar com base nos services
                metrics["total_msgs_enviadas_pela_bssp"] = 0
                metrics["total_msgs_recebidas_do_lead"] = 0
                metrics["tempo_resposta_medio_s"] = None

            except Exception as e:
                logger.warning("Erro ao buscar services Hablla para %s: %s", email, e)

            # 3. Cartões/deals
            try:
                cards_data = await self.hablla.list_cards(person_id=person_id)
                cards = cards_data.get("results", [])
                metrics["total_cards"] = cards_data.get("totalItems", len(cards))
                metrics["cards_abertos"] = sum(
                    1 for c in cards
                    if (c.get("status") or "").lower() in ("open", "aberto", "em_andamento")
                )
                metrics["cards_ganhos"] = sum(
                    1 for c in cards
                    if (c.get("status") or "").lower() in ("won", "ganho")
                )
                metrics["cards_perdidos"] = sum(
                    1 for c in cards
                    if (c.get("status") or "").lower() in ("lost", "perdido")
                )
                # Valor total dos deals
                metrics["valor_total_deals"] = sum(
                    c.get("value", 0) or 0 for c in cards
                )
            except Exception as e:
                logger.warning("Erro ao buscar cards Hablla para %s: %s", email, e)
                metrics["total_cards"] = 0
                metrics["cards_abertos"] = 0

            # 4. Anotações (notas da equipe comercial)
            try:
                annots_data = await self.hablla.list_annotations(
                    person_id=person_id, limit=20,
                )
                annotations = annots_data.get("results", [])
                metrics["total_anotacoes"] = annots_data.get("totalItems", len(annotations))
                # Extrair conteúdo para contexto do scorer
                metrics["anotacoes_recentes"] = [
                    {
                        "content": (a.get("content") or "")[:200],
                        "created_at": a.get("created_at") or "",
                        "author": a.get("user") or "",
                    }
                    for a in annotations[:5]  # últimas 5
                ]
            except Exception as e:
                logger.warning("Erro ao buscar anotações Hablla para %s: %s", email, e)
                metrics["total_anotacoes"] = 0
                metrics["anotacoes_recentes"] = []

            # 5. Tasks (tarefas vinculadas)
            try:
                tasks_data = await self.hablla.list_tasks(
                    person_id=person_id, limit=20,
                )
                tasks = tasks_data.get("results", [])
                metrics["total_tasks"] = tasks_data.get("totalItems", len(tasks))
            except Exception as e:
                logger.warning("Erro ao buscar tasks Hablla para %s: %s", email, e)
                metrics["total_tasks"] = 0

        except Exception as e:
            logger.warning("Erro geral ao coletar Hablla para %s: %s", email, e)

        return metrics

    # ------------------------------------------------------------------
    # Cálculo de Scores
    # ------------------------------------------------------------------

    def _calcular_scores(
        self,
        rd: dict,
        hablla: dict,
        now: datetime,
    ) -> dict:
        """Calcula scores normalizados (0-100) para cada dimensão."""

        scores: dict[str, int] = {}

        # --- Score de Volume de Interação (0-100) ---
        # Baseado no total de touchpoints em todos os canais
        conversoes = rd.get("total_conversoes", 0)
        conversas = hablla.get("total_conversas", 0)
        msgs_recebidas = hablla.get("total_msgs_recebidas_do_lead", 0)
        cards = hablla.get("total_cards", 0)
        anotacoes = hablla.get("total_anotacoes", 0)

        touchpoints = conversoes + conversas + msgs_recebidas + cards + anotacoes
        # Escala: 1=10, 3=30, 5=50, 10=80, 15+=100
        if touchpoints >= 15:
            scores["volume_interacao"] = 100
        elif touchpoints >= 10:
            scores["volume_interacao"] = 80
        elif touchpoints >= 5:
            scores["volume_interacao"] = 50 + (touchpoints - 5) * 6
        elif touchpoints >= 1:
            scores["volume_interacao"] = touchpoints * 10
        else:
            scores["volume_interacao"] = 0

        # --- Score de Recência (0-100) ---
        # Quão recente foi a última interação
        last_dates = []
        for dt_field in [rd.get("last_conversion"), hablla.get("ultima_conversa_data")]:
            if dt_field:
                try:
                    dt = datetime.fromisoformat(dt_field.replace("Z", "+00:00"))
                    last_dates.append(dt)
                except (ValueError, AttributeError):
                    pass

        if last_dates:
            most_recent = max(last_dates)
            if most_recent.tzinfo is None:
                most_recent = most_recent.replace(tzinfo=timezone.utc)
            days_ago = (now - most_recent).days
            # Escala: 0-1d=100, 2-3d=90, 4-7d=70, 8-14d=50, 15-30d=30, 30-60d=15, 60+=5
            if days_ago <= 1:
                scores["recencia"] = 100
            elif days_ago <= 3:
                scores["recencia"] = 90
            elif days_ago <= 7:
                scores["recencia"] = 70
            elif days_ago <= 14:
                scores["recencia"] = 50
            elif days_ago <= 30:
                scores["recencia"] = 30
            elif days_ago <= 60:
                scores["recencia"] = 15
            else:
                scores["recencia"] = 5
        else:
            scores["recencia"] = 0

        # --- Score de Multicanal (0-100) ---
        # Quantos canais diferentes o lead usou
        canais = len(hablla.get("canais_com_interacao", []))
        if rd.get("tem_dados"):
            canais += 1  # RD Station = formulários/email
        # Escala: 1=25, 2=50, 3=75, 4+=100
        scores["multicanalidade"] = min(canais * 25, 100)

        # --- Score de Responsividade (0-100) ---
        # Quão rápido o lead responde (via Hablla)
        tempo_resp = hablla.get("tempo_resposta_medio_s")
        msgs_recebidas = hablla.get("total_msgs_recebidas_do_lead", 0)

        if tempo_resp is not None and msgs_recebidas > 0:
            # Escala: <60s=100, <300s=80, <900s=60, <3600s=40, <86400s=20, >86400=5
            if tempo_resp < 60:
                scores["responsividade"] = 100
            elif tempo_resp < 300:
                scores["responsividade"] = 80
            elif tempo_resp < 900:
                scores["responsividade"] = 60
            elif tempo_resp < 3600:
                scores["responsividade"] = 40
            elif tempo_resp < 86400:
                scores["responsividade"] = 20
            else:
                scores["responsividade"] = 5
        elif msgs_recebidas > 0:
            scores["responsividade"] = 50  # respondeu mas sem tempo exato
        else:
            scores["responsividade"] = 0  # sem dados de resposta

        # --- Score de Completude (0-100) ---
        completude = rd.get("completude_perfil", 0)
        if isinstance(completude, float) and completude <= 1:
            completude = completude * 100
        scores["completude_perfil"] = min(int(completude), 100)

        # --- Score de Profundidade (0-100) ---
        # Indica o quanto o lead avançou em interações significativas
        oportunidades = rd.get("total_oportunidades", 0)
        cards_abertos = hablla.get("cards_abertos", 0)
        anotacoes_equipe = hablla.get("total_anotacoes", 0)

        depth = 0
        if oportunidades > 0:
            depth += 40  # Marcado como oportunidade no RD Station
        if cards_abertos > 0:
            depth += 30  # Deal ativo no Hablla
        if anotacoes_equipe > 0:
            depth += 15  # Equipe já interagiu
        if conversoes >= 3:
            depth += 15  # Múltiplas conversões
        scores["profundidade"] = min(depth, 100)

        # --- Score de ATIVIDADE RECENTE (0-100) ---
        # Mede se o lead esta ATIVO hoje (distinto de volume, que e cumulativo).
        # Participar de eventos/webinars recentes e o sinal mais forte;
        # downloads e formularios sao sinais medios; newsletters contam pouco.
        # Tambem considera mensagens Hablla e conversoes gerais em 30 dias.
        eventos_30 = (rd.get("eventos_30d", 0) or 0) + (rd.get("webinars_30d", 0) or 0)
        materiais_30 = rd.get("materiais_30d", 0) or 0
        newsletters_30 = rd.get("newsletters_30d", 0) or 0
        formularios_30 = rd.get("formularios_30d", 0) or 0
        conv_30d = rd.get("conversoes_ultimos_30d", 0) or 0

        # Mensagens Hablla recentes (se disponivel)
        msgs_30_hablla = 0
        try:
            from datetime import timedelta  # noqa: PLC0415
            cutoff = now - timedelta(days=30)
            for m in hablla.get("mensagens_recentes", []) or []:
                dt_str = m.get("created_at") or m.get("timestamp") or ""
                if dt_str:
                    try:
                        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        if dt >= cutoff and m.get("from_lead"):
                            msgs_30_hablla += 1
                    except (ValueError, TypeError):
                        pass
        except Exception:
            pass

        atividade = 0
        if eventos_30 >= 2:
            atividade += 45
        elif eventos_30 == 1:
            atividade += 30
        if materiais_30 >= 2:
            atividade += 20
        elif materiais_30 == 1:
            atividade += 12
        if formularios_30 >= 1:
            atividade += min(formularios_30 * 8, 15)
        if msgs_30_hablla >= 3:
            atividade += 15
        elif msgs_30_hablla >= 1:
            atividade += 8
        if conv_30d >= 3:
            atividade += 10
        elif conv_30d == 2:
            atividade += 5
        if newsletters_30 >= 3:
            atividade += 3
        scores["atividade_recente"] = min(atividade, 100)

        return scores

    def _score_total_ponderado(self, scores: dict) -> int:
        """Calcula score total como média ponderada.

        Pesos rebalanceados para priorizar ATIVIDADE RECENTE (interacao
        ativa em 30 dias) como principal sinal de engajamento — e a
        intencao do scoring comercial. Volume cumulativo e completude
        ficam como pesos menores (um 'bom' lead inativo nao vale muito).
        """
        pesos = {
            "atividade_recente": 0.30,   # interacoes em eventos/materiais/mensagens nos ultimos 30d
            "recencia": 0.20,            # quao recente foi a ultima interacao
            "profundidade": 0.15,        # oportunidades/deals/anotacoes
            "volume_interacao": 0.10,    # touchpoints totais (historico)
            "responsividade": 0.10,      # quao rapido responde no Hablla
            "multicanalidade": 0.08,     # quantos canais usa
            "completude_perfil": 0.07,   # quanto do perfil esta preenchido
        }

        total_peso = 0.0
        total_score = 0.0
        for dim, peso in pesos.items():
            if dim in scores:
                total_score += scores[dim] * peso
                total_peso += peso

        if total_peso == 0:
            return 0
        return round(total_score / total_peso)

    # ------------------------------------------------------------------
    # Sinais Comportamentais
    # ------------------------------------------------------------------

    def _extrair_sinais(
        self,
        rd: dict,
        hablla: dict,
        scores: dict,
        now: datetime,
    ) -> list[dict]:
        """Extrai sinais comportamentais relevantes para o Scorer."""
        sinais: list[dict] = []

        # Sinal: Lead muito engajado recentemente
        if scores.get("recencia", 0) >= 80 and scores.get("volume_interacao", 0) >= 50:
            sinais.append({
                "tipo": "alto_engajamento_recente",
                "descricao": "Lead com múltiplas interações nos últimos dias",
                "peso": "positivo_forte",
            })

        # Sinal: Multicanal (usa mais de um canal)
        canais = hablla.get("canais_com_interacao", [])
        if len(canais) >= 2:
            sinais.append({
                "tipo": "engajamento_multicanal",
                "descricao": f"Lead interage em {len(canais)} canais: {', '.join(canais)}",
                "peso": "positivo",
            })

        # Sinal: WhatsApp ativo (forte indicador de interesse)
        if "whatsapp" in [c.lower() for c in canais]:
            sinais.append({
                "tipo": "whatsapp_ativo",
                "descricao": "Lead tem conversas ativas via WhatsApp",
                "peso": "positivo_forte",
            })

        # Sinal: Responde rápido
        tempo_resp = hablla.get("tempo_resposta_medio_s")
        if tempo_resp and tempo_resp < 300:
            sinais.append({
                "tipo": "resposta_rapida",
                "descricao": f"Tempo médio de resposta: {int(tempo_resp)}s",
                "peso": "positivo",
            })

        # Sinal: Deal/cartão aberto
        if hablla.get("cards_abertos", 0) > 0:
            sinais.append({
                "tipo": "deal_aberto",
                "descricao": "Existe oportunidade/cartão ativo no pipeline",
                "peso": "positivo_forte",
            })

        # Sinal: Anotações da equipe (alguém já interagiu)
        if hablla.get("total_anotacoes", 0) > 0:
            sinais.append({
                "tipo": "equipe_engajada",
                "descricao": "Equipe comercial já possui anotações sobre este lead",
                "peso": "positivo",
            })

        # Sinal: Oportunidade marcada no RD Station
        if rd.get("total_oportunidades", 0) > 0:
            sinais.append({
                "tipo": "oportunidade_rdstation",
                "descricao": "Lead marcado como oportunidade no RD Station",
                "peso": "positivo_forte",
            })

        # Sinal: Lead frio (sem interação recente)
        if scores.get("recencia", 0) <= 15 and scores.get("volume_interacao", 0) <= 20:
            sinais.append({
                "tipo": "lead_inativo",
                "descricao": "Sem interação significativa há mais de 30 dias",
                "peso": "negativo",
            })

        # Sinal: Perfil incompleto
        if scores.get("completude_perfil", 0) < 30:
            sinais.append({
                "tipo": "perfil_incompleto",
                "descricao": "Dados do lead muito incompletos (<30%)",
                "peso": "negativo",
            })

        return sinais
