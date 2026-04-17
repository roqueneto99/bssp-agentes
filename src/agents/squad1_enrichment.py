"""
Squad 1 — Agente de Captura e Enriquecimento.

Responsabilidades:
- Receber leads novos (via webhook de conversão)
- Analisar tags, cargo, histórico de conversões
- Inferir área de interesse e curso mais provável
- Classificar temperatura inicial (frio/morno/quente)
- Enriquecer dados no RD Station (campos personalizados + tags)
- Passar lead para o Squad 2 (Qualificação/Scoring)

O agente usa IA para interpretar os dados brutos do lead e gerar
insights acionáveis que o resto do pipeline vai consumir.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from .base import BaseAgent, AgentResult, LLMMessage, LLMProvider

logger = logging.getLogger(__name__)

# Mapeamento de cursos/áreas da BSSP (baseado nas tags observadas)
BSSP_AREAS = {
    "gestão": ["MBA Gestão Empresarial", "MBA Gestão de Projetos", "MBA Gestão de Pessoas"],
    "contabilidade": ["Pós em Contabilidade", "Perícia Contábil", "Auditoria"],
    "direito": ["Pós em Direito", "Direito Tributário", "Direito Trabalhista"],
    "engenharia": ["Pós em Engenharia", "Engenharia de Segurança do Trabalho"],
    "saúde": ["Pós em Saúde", "Gestão em Saúde", "Enfermagem do Trabalho"],
    "psicologia": ["Pós em Psicologia", "Psicologia Organizacional"],
    "trabalhista": ["Direito Trabalhista", "Perícia Trabalhista"],
    "perícia": ["Perícia Contábil", "Perícia Judicial"],
    "educação": ["Pós em Educação", "Docência do Ensino Superior"],
    "tecnologia": ["MBA em Tecnologia", "Gestão de TI", "Ciência de Dados"],
}


class EnrichmentAgent(BaseAgent):
    """
    Agente de enriquecimento de leads.

    Analisa dados disponíveis no RD Station e usa IA para:
    1. Identificar a área de interesse principal
    2. Sugerir cursos relevantes
    3. Classificar temperatura (frio/morno/quente)
    4. Gerar resumo do perfil para o consultor comercial
    """

    agent_name = "squad1_enrichment"

    def get_system_prompt(self) -> str:
        return """Você é um analista de leads especializado em pós-graduação no Brasil.
Sua tarefa é analisar os dados de um lead (contato) e gerar um perfil enriquecido.

CONTEXTO:
- A BSSP é um centro educacional que oferece cursos de pós-graduação
- Áreas: gestão, contabilidade, direito, engenharia, saúde, psicologia, educação, tecnologia
- Os leads chegam por formulários, webinars, downloads de material, eventos

VOCÊ DEVE RETORNAR um JSON com esta estrutura exata:
{
    "area_principal": "a área de maior interesse (gestão, contabilidade, direito, etc.)",
    "areas_secundarias": ["outras áreas de interesse"],
    "cursos_sugeridos": ["até 3 cursos mais relevantes para este lead"],
    "temperatura": "frio | morno | quente",
    "motivo_temperatura": "explicação curta de por que classificou assim",
    "resumo_perfil": "2-3 frases resumindo quem é este lead e o que busca",
    "tags_sugeridas": ["novas tags para adicionar ao lead"],
    "dados_faltantes": ["dados importantes que estão faltando no perfil"]
}

CRITÉRIOS DE TEMPERATURA:
- QUENTE: tem cargo definido + múltiplas interações + área clara de interesse
- MORNO: tem algumas interações ou dados parciais, interesse identificável
- FRIO: poucos dados, interesse vago, pode ser curiosidade

Responda APENAS com o JSON, sem texto adicional."""

    async def analyze(self, contact_data: dict) -> dict:
        """
        Analisa o lead e gera perfil enriquecido.

        Fluxo:
        1. Prepara contexto do lead para o LLM
        2. Chama o LLM para análise
        3. Aplica as mudanças no RD Station
        4. Retorna resultado
        """
        email = contact_data.get("email", "")
        logger.info("Squad 1 analisando lead: %s", email)

        # --- 1. Preparar contexto ---
        lead_context = self._build_lead_context(contact_data)

        # --- 2. Chamar LLM ---
        try:
            analysis = await self.llm.complete_json(
                messages=[
                    LLMMessage(role="user", content=f"Analise este lead:\n\n{lead_context}")
                ],
                system=self.get_system_prompt(),
                temperature=0.2,
            )
        except Exception as e:
            logger.error("Falha na chamada LLM para %s: %s", email, e)
            # Fallback: análise baseada em regras simples
            analysis = self._rule_based_analysis(contact_data)

        # --- 3. Aplicar mudanças no RD Station ---
        updates = await self._apply_enrichment(email, contact_data, analysis)

        # --- 4. Resultado ---
        return {
            "analysis": analysis,
            "updates_applied": updates,
            "lead_email": email,
        }

    def _build_lead_context(self, contact_data: dict) -> str:
        """Monta o contexto do lead para o LLM analisar."""
        parts = []

        name = contact_data.get("name", "(sem nome)")
        parts.append(f"Nome: {name}")
        parts.append(f"Email: {contact_data.get('email', '?')}")

        if contact_data.get("job_title"):
            parts.append(f"Cargo: {contact_data['job_title']}")
        if contact_data.get("city") or contact_data.get("state"):
            loc = f"{contact_data.get('city', '')} - {contact_data.get('state', '')}".strip(" -")
            parts.append(f"Localização: {loc}")

        tags = contact_data.get("tags", [])
        if tags:
            parts.append(f"Tags: {', '.join(tags)}")

        # Dados do funil
        funnel = contact_data.get("funnel", {})
        if funnel:
            parts.append(f"Estágio do funil: {funnel.get('lifecycle_stage', '?')}")
            if funnel.get("fit"):
                parts.append(f"Score de Perfil: {funnel['fit']}")
            if funnel.get("interest"):
                parts.append(f"Score de Interesse: {funnel['interest']}")

        # Campos personalizados existentes
        for cf in ("cf_score_ia", "cf_temperatura", "cf_motivo_interesse", "cf_segmento_ia"):
            val = contact_data.get(cf)
            if val:
                parts.append(f"{cf}: {val}")

        # Áreas identificáveis pelas tags
        areas_from_tags = []
        for tag in tags:
            tag_lower = tag.lower()
            for area in BSSP_AREAS:
                if area in tag_lower:
                    areas_from_tags.append(area)
        if areas_from_tags:
            parts.append(f"Áreas inferidas das tags: {', '.join(set(areas_from_tags))}")

        return "\n".join(parts)

    def _rule_based_analysis(self, contact_data: dict) -> dict:
        """
        Análise baseada em regras (fallback se LLM falhar).
        Funciona sem IA, usando heurísticas simples.
        """
        tags = [t.lower() for t in contact_data.get("tags", [])]
        name = contact_data.get("name", "")
        job_title = contact_data.get("job_title", "")

        # Identifica áreas pelas tags
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

        # Temperatura
        score = 0
        if name and name != "(sem nome)":
            score += 1
        if job_title:
            score += 2
        if len(tags) > 5:
            score += 2
        elif len(tags) > 2:
            score += 1
        if areas:
            score += 1

        if score >= 5:
            temperatura = "quente"
        elif score >= 3:
            temperatura = "morno"
        else:
            temperatura = "frio"

        # Dados faltantes
        faltantes = []
        if not contact_data.get("personal_phone") and not contact_data.get("mobile_phone"):
            faltantes.append("telefone")
        if not contact_data.get("city"):
            faltantes.append("cidade")
        if not job_title:
            faltantes.append("cargo")

        return {
            "area_principal": area_principal,
            "areas_secundarias": areas_sec,
            "cursos_sugeridos": cursos[:3],
            "temperatura": temperatura,
            "motivo_temperatura": f"Score heurístico: {score}/7 (análise por regras, LLM indisponível)",
            "resumo_perfil": f"Lead com {len(tags)} tags, interesse em {', '.join(areas[:2]) or 'área não identificada'}.",
            "tags_sugeridas": [f"enriquecido-{datetime.now().strftime('%Y%m')}"],
            "dados_faltantes": faltantes,
        }

    async def _apply_enrichment(
        self, email: str, contact_data: dict, analysis: dict
    ) -> dict:
        """
        Aplica as mudanças no RD Station baseado na análise.
        Atualiza campos personalizados e adiciona tags.
        """
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

        segmento = analysis.get("area_principal", "")
        if analysis.get("areas_secundarias"):
            segmento += f" + {analysis['areas_secundarias'][0]}"
        if segmento:
            fields_to_update["cf_segmento_ia"] = segmento[:255]

        if fields_to_update:
            try:
                await self.rdstation.upsert_contact(email, fields_to_update)
                updates_applied["fields"] = fields_to_update
                logger.info("Campos atualizados para %s: %s", email, list(fields_to_update.keys()))
            except Exception as e:
                logger.error("Falha ao atualizar campos de %s: %s", email, e)

        # --- Tags ---
        new_tags = []
        suggested_tags = analysis.get("tags_sugeridas", [])
        existing_tags = [t.lower() for t in contact_data.get("tags", [])]

        for tag in suggested_tags:
            if tag.lower() not in existing_tags:
                new_tags.append(tag)

        # Tag de status do enriquecimento
        enrichment_tag = "enriquecido-ia"
        if enrichment_tag not in existing_tags:
            new_tags.append(enrichment_tag)

        # Tag de temperatura
        temp_tag = f"temp-{temperatura}" if temperatura else ""
        if temp_tag and temp_tag not in existing_tags:
            new_tags.append(temp_tag)

        if new_tags:
            try:
                await self.rdstation.add_tags(email, new_tags)
                updates_applied["tags"] = new_tags
                logger.info("Tags adicionadas a %s: %s", email, new_tags)
            except Exception as e:
                logger.error("Falha ao adicionar tags a %s: %s", email, e)

        return updates_applied
