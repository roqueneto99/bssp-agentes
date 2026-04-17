"""
Squad 1 — Agente Enriquecedor.

Responsabilidades:
- Receber perfil consolidado do Coletor
- Usar IA para inferir dados faltantes (área de interesse, curso, segmento)
- Classificar temperatura do lead (frio/morno/quente)
- Gerar resumo do perfil para o consultor comercial
- Persistir enriquecimentos no RD Station (campos cf_* + tags)

Este agente substitui o antigo squad1_enrichment.py, agora como parte
do pipeline completo da Squad 1.

APIs utilizadas:
- PATCH /platform/contacts/email:{email} — atualizar campos cf_*
- POST /platform/contacts/email:{email}/tag — adicionar tags
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Optional

from ..base import BaseAgent, AgentResult, LLMMessage, LLMProvider

logger = logging.getLogger(__name__)

# Mapeamento de cursos/áreas da BSSP
BSSP_AREAS = {
    "gestão": [
        "MBA Gestão Empresarial",
        "MBA Gestão de Projetos",
        "MBA Gestão de Pessoas",
    ],
    "contabilidade": [
        "Pós em Contabilidade",
        "Perícia Contábil",
        "Auditoria",
    ],
    "direito": [
        "Pós em Direito",
        "Direito Tributário",
        "Direito Trabalhista",
    ],
    "engenharia": [
        "Pós em Engenharia",
        "Engenharia de Segurança do Trabalho",
    ],
    "saúde": [
        "Pós em Saúde",
        "Gestão em Saúde",
        "Enfermagem do Trabalho",
    ],
    "psicologia": [
        "Pós em Psicologia",
        "Psicologia Organizacional",
    ],
    "trabalhista": [
        "Direito Trabalhista",
        "Perícia Trabalhista",
    ],
    "perícia": [
        "Perícia Contábil",
        "Perícia Judicial",
    ],
    "educação": [
        "Pós em Educação",
        "Docência do Ensino Superior",
    ],
    "tecnologia": [
        "MBA em Tecnologia",
        "Gestão de TI",
        "Ciência de Dados",
    ],
}


class EnriquecedorAgent(BaseAgent):
    """
    Agente de enriquecimento inteligente de leads.

    Recebe o perfil consolidado do Coletor e usa IA para:
    1. Identificar área de interesse principal e secundárias
    2. Sugerir cursos relevantes da BSSP
    3. Classificar temperatura (frio/morno/quente)
    4. Gerar resumo do perfil para o time comercial
    5. Persistir resultados no RD Station
    """

    agent_name = "squad1_enriquecedor"

    def get_system_prompt(self) -> str:
        areas_desc = "\n".join(
            f"- {area}: {', '.join(cursos)}" for area, cursos in BSSP_AREAS.items()
        )
        return f"""Você é um analista de leads especializado em pós-graduação no Brasil.
Sua tarefa é analisar o dossiê completo de um lead e gerar um perfil enriquecido.

CONTEXTO:
- A BSSP é um centro educacional que oferece cursos de pós-graduação
- Áreas e cursos disponíveis:
{areas_desc}
- Os leads chegam por formulários, webinars, downloads de material, eventos

VOCÊ DEVE RETORNAR um JSON com esta estrutura exata:
{{
    "area_principal": "a área de maior interesse",
    "areas_secundarias": ["outras áreas de interesse"],
    "cursos_sugeridos": ["até 3 cursos mais relevantes para este lead"],
    "temperatura": "frio | morno | quente",
    "motivo_temperatura": "explicação curta de por que classificou assim",
    "resumo_perfil": "2-3 frases resumindo quem é este lead e o que busca",
    "segmento_ia": "micro-segmento comportamental (ex: 'contador_senior_perito')",
    "tags_sugeridas": ["novas tags para adicionar ao lead"],
    "prioridade_contato": "alta | media | baixa",
    "proximo_passo": "ação sugerida para o time comercial"
}}

IMPORTANTE - ANALISE DE CONTEUDOS:
O dossie inclui uma secao "Analise de Conteudos Interagidos" com os temas de
interesse do lead extraidos dos nomes de eventos, newsletters e materiais que
o lead interagiu. USE ESSES DADOS para inferir a area de interesse e cursos
relevantes. Por exemplo:
- Lead que interagiu com "IFRS - CPC 51" ou "Pericia Contabil" = contabilidade
- Lead que interagiu com "Fundo de Funil" = marketing/gestao
- Lead que interagiu com muitos eventos/webinars = alto engajamento

CRITERIOS DE TEMPERATURA:
- QUENTE: cargo definido + multiplas interacoes (3+) + area clara + recencia < 7 dias
  OU lead que abriu/interagiu com 5+ conteudos recentes
- MORNO: algumas interacoes ou dados parciais, interesse identificavel
- FRIO: poucos dados, interesse vago, recencia > 30 dias ou primeira interacao

CRITÉRIOS DE PRIORIDADE:
- ALTA: quente + completude > 60% + score engajamento > 50
- MEDIA: morno OU (quente + dados muito incompletos)
- BAIXA: frio OU score engajamento < 20

Responda APENAS com o JSON, sem texto adicional."""

    async def run(
        self,
        contact_email: str,
        *,
        perfil_coletor: Optional[dict] = None,
    ) -> AgentResult:
        """
        Override do run() para aceitar perfil pré-coletado.

        Se perfil_coletor é fornecido (vindo do Coletor), usa direto.
        Se não, busca do RD Station (modo standalone).
        """
        import time

        start = time.monotonic()
        try:
            if perfil_coletor:
                contact_data = perfil_coletor
            else:
                # Modo standalone — busca da API
                contact = await self.rdstation.get_contact(email=contact_email)
                contact_data = contact.to_api_payload()
                contact_data["uuid"] = contact.uuid
                contact_data["email"] = contact.email
                try:
                    funil = await self.rdstation.get_funnel_stage(contact_email)
                    contact_data["funil"] = funil
                except Exception:
                    contact_data["funil"] = {}

            result_data = await self.analyze(contact_data)

            duration = (time.monotonic() - start) * 1000
            temperatura = result_data.get("analysis", {}).get("temperatura", "?")
            logger.info(
                "Enriquecedor concluído para %s em %.0fms — temp=%s",
                contact_email, duration, temperatura,
            )

            return AgentResult(
                success=True,
                agent_name=self.agent_name,
                contact_email=contact_email,
                data=result_data,
                duration_ms=duration,
            )

        except Exception as e:
            duration = (time.monotonic() - start) * 1000
            logger.error("Enriquecedor falhou para %s: %s", contact_email, e)
            return AgentResult(
                success=False,
                agent_name=self.agent_name,
                contact_email=contact_email,
                error=str(e),
                duration_ms=duration,
            )

    async def analyze(self, contact_data: dict) -> dict:
        """
        Analisa o lead e gera perfil enriquecido.

        Fluxo:
        1. Prepara contexto completo para o LLM
        2. Chama LLM para análise inteligente
        3. Se LLM falhar, usa análise por regras (fallback)
        4. Persiste resultados no RD Station
        """
        email = contact_data.get("email", "")
        logger.info("Enriquecedor analisando lead: %s", email)

        # --- 1. Preparar contexto ---
        lead_context = self._build_lead_context(contact_data)

        # --- 2. Chamar LLM ---
        try:
            analysis = await self.llm.complete_json(
                messages=[
                    LLMMessage(
                        role="user",
                        content=f"Analise este dossiê completo do lead:\n\n{lead_context}",
                    )
                ],
                system=self.get_system_prompt(),
                temperature=0.2,
            )
        except Exception as e:
            logger.warning("LLM falhou para %s: %s — usando regras", email, e)
            analysis = self._rule_based_analysis(contact_data)

        # --- 3. Aplicar mudanças no RD Station ---
        updates = await self._apply_enrichment(email, contact_data, analysis)

        return {
            "analysis": analysis,
            "updates_applied": updates,
            "lead_email": email,
        }

    def _build_lead_context(self, contact_data: dict) -> str:
        """Monta contexto completo do lead para o LLM."""
        parts = []

        # Dados básicos
        parts.append(f"Nome: {contact_data.get('nome', contact_data.get('name', '(sem nome)'))}")
        parts.append(f"Email: {contact_data.get('email', '?')}")

        cargo = contact_data.get("cargo", contact_data.get("job_title", ""))
        if cargo:
            parts.append(f"Cargo: {cargo}")

        empresa = contact_data.get("empresa", "")
        if empresa:
            parts.append(f"Empresa: {empresa}")

        cidade = contact_data.get("cidade", contact_data.get("city", ""))
        estado = contact_data.get("estado", contact_data.get("state", ""))
        if cidade or estado:
            loc = f"{cidade} - {estado}".strip(" -")
            parts.append(f"Localização: {loc}")

        linkedin = contact_data.get("linkedin", "")
        if linkedin:
            parts.append(f"LinkedIn: {linkedin}")

        # Tags
        tags = contact_data.get("tags", [])
        if tags:
            parts.append(f"Tags: {', '.join(tags)}")

        # Funil
        funil = contact_data.get("funil", contact_data.get("funnel", {}))
        if funil:
            parts.append(f"Estágio do funil: {funil.get('lifecycle_stage', '?')}")
            if funil.get("fit_score"):
                parts.append(f"Score de Perfil: {funil['fit_score']}")
            if funil.get("interest_score"):
                parts.append(f"Score de Interesse: {funil['interest_score']}")

        # Métricas do Coletor
        metricas = contact_data.get("metricas_engajamento", {})
        if metricas:
            parts.append(f"\n--- Métricas de Engajamento ---")
            parts.append(f"Total conversões: {metricas.get('total_conversoes', 0)}")
            parts.append(f"Score engajamento: {metricas.get('score_engajamento', 0)}/100")
            parts.append(f"Frequência mensal: {metricas.get('frequencia_mensal', 0)}")
            if metricas.get("dias_desde_ultima_conversao") is not None:
                parts.append(
                    f"Última conversão: {metricas['dias_desde_ultima_conversao']} dias atrás"
                )

        # Fonte de origem
        fonte = contact_data.get("fonte_origem", "")
        if fonte:
            parts.append(f"Fonte de origem: {fonte}")
        tipo = contact_data.get("tipo_conteudo", "")
        if tipo:
            parts.append(f"Tipo de conteúdo: {tipo}")

        # Analise de conteudos (gerada pelo Coletor)
        analise = contact_data.get("analise_conteudos", {})
        if analise:
            parts.append("\n--- Analise de Conteudos Interagidos ---")
            tipos = analise.get("tipos_conteudo_interagido", {})
            if tipos:
                tipos_str = ", ".join(
                    "%s: %d" % (k, v) for k, v in tipos.items()
                )
                parts.append("Tipos de conteudo: %s" % tipos_str)
            parts.append(
                "Total interacoes com conteudo: %d" % analise.get("total_interacoes_conteudo", 0)
            )
            temas = analise.get("temas_interesse", [])
            if temas:
                parts.append("Temas de interesse detectados: %s" % ", ".join(temas[:6]))
            tema_freq = analise.get("tema_mais_frequente", "")
            if tema_freq:
                parts.append("Tema mais frequente: %s" % tema_freq)
            nl = analise.get("engajamento_newsletter", 0)
            ev = analise.get("engajamento_eventos", 0)
            mt = analise.get("engajamento_materiais", 0)
            if nl or ev or mt:
                parts.append(
                    "Engajamento: %d newsletters, %d eventos/webinars, %d materiais" % (nl, ev, mt)
                )

        # Conversoes recentes (ate 10)
        conversoes = contact_data.get("historico_conversoes", [])
        if conversoes:
            parts.append(
                "\n--- Historico de Conversoes (%d total) ---" % len(conversoes)
            )
            for c in conversoes[:10]:
                ci = c.get("conversion_identifier", c.get("event_identifier", "?"))
                ts = c.get("event_timestamp", c.get("created_at", "?"))
                parts.append("  - %s (%s)" % (ci, ts))

        # Completude
        completude = contact_data.get("completude_perfil")
        if completude is not None:
            parts.append(f"\nCompletude do perfil: {completude * 100:.0f}%")

        # Dados faltantes
        faltantes = contact_data.get("dados_faltantes", [])
        if faltantes:
            parts.append(f"Dados faltantes: {', '.join(faltantes)}")

        # Campos personalizados existentes
        for cf in ("cf_score_ia", "cf_temperatura", "cf_motivo_interesse", "cf_segmento_ia"):
            val = contact_data.get(cf)
            if val:
                parts.append(f"{cf}: {val}")

        return "\n".join(parts)

    def _rule_based_analysis(self, contact_data: dict) -> dict:
        """Análise por regras (fallback se LLM falhar)."""
        tags = [t.lower() for t in contact_data.get("tags", [])]
        cargo = contact_data.get("cargo", contact_data.get("job_title", ""))

        # Identificar áreas pelas tags
        areas = []
        for tag in tags:
            for area in BSSP_AREAS:
                if area in tag and area not in areas:
                    areas.append(area)

        area_principal = areas[0] if areas else "não identificada"
        areas_sec = areas[1:3] if len(areas) > 1 else []

        # Cursos sugeridos
        cursos = []
        for area in areas[:2]:
            cursos.extend(BSSP_AREAS.get(area, [])[:1])

        # Temperatura baseada em métricas do Coletor
        metricas = contact_data.get("metricas_engajamento", {})
        score_eng = metricas.get("score_engajamento", 0)
        total_conv = metricas.get("total_conversoes", 0)
        dias_ultima = metricas.get("dias_desde_ultima_conversao", 999)

        score = 0
        nome = contact_data.get("nome", contact_data.get("name", ""))
        if nome and nome != "(sem nome)":
            score += 1
        if cargo:
            score += 2
        if total_conv >= 3:
            score += 2
        elif total_conv >= 1:
            score += 1
        if areas:
            score += 1
        if dias_ultima <= 7:
            score += 1

        if score >= 5:
            temperatura = "quente"
        elif score >= 3:
            temperatura = "morno"
        else:
            temperatura = "frio"

        # Prioridade
        completude = contact_data.get("completude_perfil", 0)
        if temperatura == "quente" and completude > 0.6 and score_eng > 50:
            prioridade = "alta"
        elif temperatura in ("quente", "morno"):
            prioridade = "media"
        else:
            prioridade = "baixa"

        # Dados faltantes
        faltantes = contact_data.get("dados_faltantes", [])
        if not faltantes:
            faltantes = []
            if not contact_data.get("telefone") and not contact_data.get("personal_phone"):
                faltantes.append("telefone")
            if not contact_data.get("cidade") and not contact_data.get("city"):
                faltantes.append("cidade")
            if not cargo:
                faltantes.append("cargo")

        return {
            "area_principal": area_principal,
            "areas_secundarias": areas_sec,
            "cursos_sugeridos": cursos[:3],
            "temperatura": temperatura,
            "motivo_temperatura": (
                f"Score heurístico: {score}/7 — "
                f"{total_conv} conversões, "
                f"engajamento {score_eng}/100 "
                f"(fallback por regras)"
            ),
            "resumo_perfil": (
                f"Lead com {total_conv} conversões, "
                f"interesse em {', '.join(areas[:2]) or 'área não identificada'}."
            ),
            "segmento_ia": f"{area_principal}_{temperatura}",
            "tags_sugeridas": [f"enriquecido-{datetime.now().strftime('%Y%m')}"],
            "prioridade_contato": prioridade,
            "proximo_passo": self._sugerir_proximo_passo(temperatura, faltantes),
        }

    def _sugerir_proximo_passo(
        self, temperatura: str, faltantes: list[str]
    ) -> str:
        """Sugere próximo passo para o time comercial."""
        if temperatura == "quente":
            return "Contato telefônico imediato pelo consultor comercial"
        elif temperatura == "morno":
            if faltantes:
                return f"Enviar email de nutrição solicitando: {', '.join(faltantes[:2])}"
            return "Incluir em fluxo de nutrição com conteúdo da área de interesse"
        else:
            return "Manter em fluxo de nutrição automático (conteúdo educacional)"

    async def _apply_enrichment(
        self, email: str, contact_data: dict, analysis: dict
    ) -> dict:
        """Persiste enriquecimentos no RD Station."""
        updates_applied = {"fields": {}, "tags": []}

        # --- Campos personalizados ---
        fields_to_update = {}

        temperatura = analysis.get("temperatura", "")
        if temperatura:
            fields_to_update["cf_temperatura"] = temperatura

        area = analysis.get("area_principal", "")
        cursos = analysis.get("cursos_sugeridos", [])
        motivo = area
        if cursos:
            motivo = f"{area} — {', '.join(cursos[:2])}"
        if motivo:
            fields_to_update["cf_motivo_interesse"] = motivo[:255]

        fields_to_update["cf_ultima_interacao_agente"] = datetime.now().strftime(
            "%Y-%m-%d %H:%M"
        )

        segmento = analysis.get("segmento_ia", "")
        if not segmento:
            segmento = analysis.get("area_principal", "")
            if analysis.get("areas_secundarias"):
                segmento += f" + {analysis['areas_secundarias'][0]}"
        if segmento:
            fields_to_update["cf_segmento_ia"] = segmento[:255]

        if fields_to_update:
            try:
                await self.rdstation.upsert_contact(email, fields_to_update)
                updates_applied["fields"] = fields_to_update
                logger.info(
                    "Enriquecedor atualizou campos de %s: %s",
                    email, list(fields_to_update.keys()),
                )
            except Exception as e:
                logger.error("Falha ao atualizar campos de %s: %s", email, e)

        # --- Tags ---
        new_tags = []
        suggested_tags = analysis.get("tags_sugeridas", [])
        existing_tags = [t.lower() for t in contact_data.get("tags", [])]

        for tag in suggested_tags:
            if tag.lower() not in existing_tags:
                new_tags.append(tag)

        # Tags de status
        status_tags = [
            ("enriquecido-ia", True),
            (f"temp-{temperatura}", bool(temperatura)),
            (f"prioridade-{analysis.get('prioridade_contato', '')}",
             bool(analysis.get("prioridade_contato"))),
        ]
        for tag, condition in status_tags:
            if condition and tag and tag.lower() not in existing_tags:
                new_tags.append(tag)

        if new_tags:
            try:
                await self.rdstation.add_tags(email, new_tags)
                updates_applied["tags"] = new_tags
                logger.info("Enriquecedor adicionou tags a %s: %s", email, new_tags)
            except Exception as e:
                logger.error("Falha ao adicionar tags a %s: %s", email, e)

        return updates_applied
