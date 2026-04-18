"""
Squad 1 — Agente Coletor.

Responsabilidades:
- Capturar dados completos de um lead a partir de múltiplas fontes do RD Station
- Consolidar dados de contato, eventos/conversões, segmentações e funil
- Gerar um perfil unificado ("dossiê") para os agentes seguintes consumirem
- Identificar a fonte de origem do lead (formulário, ad, webinar, etc.)
- Calcular métricas de engajamento (total de conversões, frequência, recência)

Fontes de dados (APIs funcionando):
- GET /platform/contacts/email:{email} — dados cadastrais completos
- GET /platform/contacts/{uuid}/events — histórico de conversões e oportunidades
- GET /platform/contacts/email:{email}/funnels — estágio do funil + scores
- GET /platform/segmentations — segmentações disponíveis (contexto)
- GET /platform/contacts/fields — campos disponíveis (contexto)

Saída: dict com perfil consolidado pronto para o Enriquecedor.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Optional

from ..base import BaseAgent, AgentResult, LLMMessage, LLMProvider

logger = logging.getLogger(__name__)


class ColetorAgent(BaseAgent):
    """
    Agente responsável por coletar e consolidar todos os dados
    disponíveis de um lead no RD Station.

    Diferente dos outros agentes, o Coletor NÃO usa LLM para análise.
    Ele é puramente determinístico: busca dados, normaliza e consolida.
    O LLM só é usado opcionalmente para inferir a fonte de origem
    quando o conversion_identifier é ambíguo.
    """

    agent_name = "squad1_coletor"

    # Mapeamento de conversion_identifier para fonte
    SOURCE_PATTERNS = {
        "formulario": "formulário",
        "form": "formulário",
        "landing": "landing page",
        "lp_": "landing page",
        "webinar": "webinar",
        "ebook": "ebook/material",
        "download": "ebook/material",
        "material": "ebook/material",
        "evento": "evento presencial",
        "workshop": "evento presencial",
        "ads": "anúncio pago",
        "google": "anúncio pago",
        "facebook": "rede social",
        "instagram": "rede social",
        "linkedin": "rede social",
        "indicacao": "indicação",
        "referral": "indicação",
        "import": "importação manual",
    }

    def get_system_prompt(self) -> str:
        return """Você é um classificador de fontes de leads educacionais.
Dado o nome de um formulário/evento de conversão, identifique a fonte de origem.

Responda APENAS com JSON:
{
    "fonte_origem": "formulário | landing page | webinar | ebook/material | evento presencial | anúncio pago | rede social | indicação | importação manual | outro",
    "tipo_conteudo": "descrição curta do tipo de conteúdo (ex: 'Webinar sobre MBA Gestão')",
    "intencao_estimada": "informacional | exploratório | decisão"
}"""

    async def run(
        self,
        contact_email: str,
        *,
        conversion_identifier: Optional[str] = None,
        webhook_data: Optional[dict] = None,
    ) -> AgentResult:
        """
        Override do run() para aceitar dados extras do webhook.

        Args:
            contact_email: Email do lead
            conversion_identifier: Nome do formulário/evento que gerou a conversão
            webhook_data: Dados crus do webhook (opcional, enriquece o perfil)
        """
        import time

        start = time.monotonic()
        try:
            # 1. Buscar dados do contato na API
            contact = await self.rdstation.get_contact(email=contact_email)
            contact_data = contact.to_api_payload()
            contact_data["uuid"] = contact.uuid
            contact_data["email"] = contact.email

            # 2. Executar coleta completa
            result_data = await self.analyze(
                contact_data,
                conversion_identifier=conversion_identifier,
                webhook_data=webhook_data,
            )

            duration = (time.monotonic() - start) * 1000
            logger.info(
                "Coletor concluído para %s em %.0fms — %d conversões, fonte=%s",
                contact_email, duration,
                result_data.get("metricas_engajamento", {}).get("total_conversoes", 0),
                result_data.get("fonte_origem", "?"),
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
            logger.error("Coletor falhou para %s: %s", contact_email, e)
            return AgentResult(
                success=False,
                agent_name=self.agent_name,
                contact_email=contact_email,
                error=str(e),
                duration_ms=duration,
            )

    async def analyze(
        self,
        contact_data: dict,
        *,
        conversion_identifier: Optional[str] = None,
        webhook_data: Optional[dict] = None,
    ) -> dict:
        """
        Coleta e consolida todos os dados disponíveis do lead.

        Fluxo:
        1. Normaliza dados do contato (campos padrão + custom)
        2. Busca histórico de eventos/conversões
        3. Busca estágio do funil e scores
        4. Identifica fonte de origem
        5. Calcula métricas de engajamento
        6. Gera perfil consolidado
        """
        email = contact_data.get("email", "")
        uuid = contact_data.get("uuid", "")

        logger.info("Coletor iniciando coleta para %s (uuid=%s)", email, uuid)

        # --- 1. Normalizar dados do contato ---
        perfil = self._normalizar_contato(contact_data, webhook_data)

        # --- 1.1 dados_basicos (formato canônico consumido pelo Squad 2) ---
        # Squad 2 (Analisador de Engajamento, Scorer, Classificador) le
        # dados em INGLES a partir de perfil_squad1.dados_basicos.
        # Este dict reexpõe os campos crus do RD Station que nao sao
        # duplicados em _normalizar_contato (que usa nomes em PT).
        # Em particular, last_conversion_date e created_at sao usados
        # para calcular recencia/timing — sem isso a recencia fica 0 e
        # o lead parece "sem interacoes recentes" mesmo tendo conversoes.
        perfil["dados_basicos"] = {
            "uuid": contact_data.get("uuid", ""),
            "email": contact_data.get("email", ""),
            "name": contact_data.get("name", ""),
            "job_title": contact_data.get("job_title", ""),
            "company_name": contact_data.get("company_name", "") or (webhook_data or {}).get("company", ""),
            "personal_phone": contact_data.get("personal_phone", ""),
            "mobile_phone": contact_data.get("mobile_phone", ""),
            "city": contact_data.get("city", ""),
            "state": contact_data.get("state", ""),
            "country": contact_data.get("country", "Brasil"),
            "linkedin": contact_data.get("linkedin", ""),
            "website": contact_data.get("website", ""),
            "created_at": contact_data.get("created_at", ""),
            "last_conversion_date": contact_data.get("last_conversion_date", ""),
            "first_conversion_date": contact_data.get("first_conversion_date", ""),
            "tags": contact_data.get("tags", []),
        }

        # --- 2. Buscar histórico de eventos ---
        eventos = await self._buscar_eventos(uuid)
        perfil["historico_conversoes"] = eventos.get("conversions", [])
        perfil["historico_oportunidades"] = eventos.get("opportunities", [])

        # --- 3. Buscar funil ---
        funil = contact_data.get("funnel", {})
        if not funil:
            try:
                funil = await self.rdstation.get_funnel_stage(email)
            except Exception as e:
                logger.warning("Falha ao buscar funil de %s: %s", email, e)
                funil = {}
        perfil["funil"] = self._normalizar_funil(funil)

        # --- 4. Identificar fonte de origem ---
        fonte = self._identificar_fonte(
            conversion_identifier,
            perfil["historico_conversoes"],
        )
        perfil["fonte_origem"] = fonte["fonte_origem"]
        perfil["tipo_conteudo"] = fonte.get("tipo_conteudo", "")
        perfil["conversion_identifier"] = conversion_identifier or ""

        # Se não conseguiu identificar por regras, tenta LLM
        if (
            fonte["fonte_origem"] == "outro"
            and conversion_identifier
            and self.llm
        ):
            try:
                fonte_llm = await self._classificar_fonte_llm(conversion_identifier)
                if fonte_llm.get("fonte_origem") != "outro":
                    perfil["fonte_origem"] = fonte_llm["fonte_origem"]
                    perfil["tipo_conteudo"] = fonte_llm.get("tipo_conteudo", "")
            except Exception as e:
                logger.warning("LLM falhou ao classificar fonte: %s", e)

        # --- 5. Calcular métricas de engajamento ---
        perfil["metricas_engajamento"] = self._calcular_engajamento(
            perfil["historico_conversoes"],
            perfil["historico_oportunidades"],
            perfil["funil"],
        )

        # --- 6. Analisar historico de conteudos interagidos ---
        # NOTA: A API do RD Station nao expoe aberturas/cliques de email
        # por contato (dados visiveis apenas na interface web).
        # Usamos as conversoes como proxy para entender interesses.
        perfil["analise_conteudos"] = self._analisar_historico_conteudos(
            perfil["historico_conversoes"]
        )

        # --- 7. Identificar dados faltantes ---
        perfil["dados_faltantes"] = self._identificar_dados_faltantes(perfil)

        # --- 8. Completude do perfil (%) ---
        perfil["completude_perfil"] = self._calcular_completude(perfil)

        return perfil

    def _normalizar_contato(
        self, contact_data: dict, webhook_data: Optional[dict] = None
    ) -> dict:
        """Normaliza e consolida dados do contato de múltiplas fontes."""
        perfil = {
            "uuid": contact_data.get("uuid", ""),
            "email": contact_data.get("email", ""),
            "nome": contact_data.get("name", ""),
            "cargo": contact_data.get("job_title", ""),
            "empresa": "",  # RD Marketing não tem empresa — vem do webhook
            "telefone": (
                contact_data.get("personal_phone")
                or contact_data.get("mobile_phone")
                or ""
            ),
            "celular": contact_data.get("mobile_phone", ""),
            "cidade": contact_data.get("city", ""),
            "estado": contact_data.get("state", ""),
            "pais": contact_data.get("country", "Brasil"),
            "linkedin": contact_data.get("linkedin", ""),
            "website": contact_data.get("website", ""),
            "tags": contact_data.get("tags", []),
            "extra_emails": contact_data.get("extra_emails", []),
            # Campos personalizados
            "cf_score_ia": contact_data.get("cf_score_ia"),
            "cf_temperatura": contact_data.get("cf_temperatura"),
            "cf_motivo_interesse": contact_data.get("cf_motivo_interesse"),
            "cf_segmento_ia": contact_data.get("cf_segmento_ia"),
            "cf_ultima_interacao_agente": contact_data.get(
                "cf_ultima_interacao_agente"
            ),
        }

        # Enriquecer com dados do webhook (ex: empresa, que não está na API)
        if webhook_data:
            if webhook_data.get("company") and not perfil["empresa"]:
                perfil["empresa"] = webhook_data["company"]
            if webhook_data.get("job_title") and not perfil["cargo"]:
                perfil["cargo"] = webhook_data["job_title"]
            if webhook_data.get("personal_phone") and not perfil["telefone"]:
                perfil["telefone"] = webhook_data["personal_phone"]

        return perfil

    async def _buscar_eventos(self, uuid: str) -> dict:
        """Busca todos os eventos (conversões + oportunidades) do contato."""
        if not uuid:
            return {"conversions": [], "opportunities": [],
                    "total_conversions": 0, "total_opportunities": 0}

        try:
            return await self.rdstation.get_contact_all_events(uuid, max_pages=5)
        except Exception as e:
            logger.warning("Falha ao buscar eventos do UUID %s: %s", uuid, e)
            return {"conversions": [], "opportunities": [],
                    "total_conversions": 0, "total_opportunities": 0}

    def _normalizar_funil(self, funil_data: dict | list) -> dict:
        """Normaliza dados do funil em formato padronizado."""
        if isinstance(funil_data, list):
            funil_data = funil_data[0] if funil_data else {}

        return {
            "lifecycle_stage": funil_data.get("lifecycle_stage", "Lead"),
            "opportunity": funil_data.get("opportunity", False),
            "fit_score": funil_data.get("fit", funil_data.get("fit_score")),
            "interest_score": funil_data.get(
                "interest", funil_data.get("interest_score")
            ),
            "contact_owner_email": funil_data.get("contact_owner_email"),
        }

    def _identificar_fonte(
        self,
        conversion_identifier: Optional[str],
        conversoes: list[dict],
    ) -> dict:
        """
        Identifica a fonte de origem do lead por regras determinísticas.
        Analisa o conversion_identifier e o histórico de conversões.
        """
        result = {"fonte_origem": "outro", "tipo_conteudo": ""}

        # Primeiro, tenta pelo conversion_identifier atual
        if conversion_identifier:
            ci_lower = conversion_identifier.lower()
            for pattern, fonte in self.SOURCE_PATTERNS.items():
                if pattern in ci_lower:
                    result["fonte_origem"] = fonte
                    result["tipo_conteudo"] = conversion_identifier
                    return result

        # Se não achou, tenta pelo histórico (primeira conversão = origem)
        if conversoes:
            # Ordenar por data (mais antigo primeiro)
            sorted_conv = sorted(
                conversoes,
                key=lambda c: c.get("event_timestamp", c.get("created_at", "")),
            )
            first = sorted_conv[0] if sorted_conv else {}
            first_ci = (
                first.get("conversion_identifier")
                or first.get("event_identifier")
                or ""
            )
            if first_ci:
                ci_lower = first_ci.lower()
                for pattern, fonte in self.SOURCE_PATTERNS.items():
                    if pattern in ci_lower:
                        result["fonte_origem"] = fonte
                        result["tipo_conteudo"] = first_ci
                        return result

        return result

    async def _classificar_fonte_llm(self, conversion_identifier: str) -> dict:
        """Usa LLM para classificar a fonte quando regras não bastam."""
        response = await self.llm.complete_json(
            messages=[
                LLMMessage(
                    role="user",
                    content=(
                        f"Classifique a fonte de origem deste lead baseado "
                        f"no identificador de conversão:\n\n"
                        f"conversion_identifier: \"{conversion_identifier}\""
                    ),
                )
            ],
            system=self.get_system_prompt(),
            temperature=0.1,
        )
        return response

    def _calcular_engajamento(
        self,
        conversoes: list[dict],
        oportunidades: list[dict],
        funil: dict,
    ) -> dict:
        """
        Calcula métricas de engajamento do lead.

        Métricas:
        - total_conversoes: quantidade total de conversões
        - total_oportunidades: vezes marcado como oportunidade
        - dias_desde_primeira_conversao: "idade" do lead
        - dias_desde_ultima_conversao: recência
        - frequencia_media: conversões por mês (se > 30 dias)
        - score_engajamento: 0-100 baseado nas métricas acima
        """
        now = datetime.utcnow()
        total_conv = len(conversoes)
        total_opp = len(oportunidades)

        # Extrair timestamps das conversões
        timestamps = []
        for c in conversoes:
            ts_str = c.get("event_timestamp") or c.get("created_at") or ""
            if ts_str:
                try:
                    ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                    timestamps.append(ts.replace(tzinfo=None))
                except (ValueError, TypeError):
                    continue

        timestamps.sort()
        # IMPORTANTE: quando nao ha conversoes, dias_primeira/ultima
        # devem ser None (e nao 0). Antes o codigo usava now como fallback,
        # o que fazia o scoring achar que a ultima conversao foi "hoje"
        # e dar +20 pontos de engajamento a um lead sem conversao alguma.
        if timestamps:
            primeira = timestamps[0]
            ultima = timestamps[-1]
            dias_primeira = (now - primeira).days
            dias_ultima = (now - ultima).days
        else:
            primeira = None
            ultima = None
            dias_primeira = None
            dias_ultima = None

        # Conversoes recentes (janelas de 30 e 90 dias) — usadas pelo
        # Scorer para medir VELOCIDADE real, diferente de total cumulativo.
        conv_30d = sum(1 for t in timestamps if (now - t).days <= 30)
        conv_90d = sum(1 for t in timestamps if (now - t).days <= 90)

        # Frequência média (conversões/mês)
        if dias_primeira is not None and dias_primeira > 30 and total_conv > 1:
            meses = max(dias_primeira / 30, 1)
            frequencia = round(total_conv / meses, 2)
        else:
            frequencia = float(total_conv)

        # Score de engajamento (0-100)
        score = 0
        score += min(total_conv * 10, 30)          # até 30 pts por conversões
        score += min(total_opp * 20, 20)            # até 20 pts por oportunidades
        score += min(frequencia * 10, 20)           # até 20 pts por frequência
        if dias_ultima is not None and dias_ultima <= 7:
            score += 20                              # 20 pts se ativo na última semana
        elif dias_ultima is not None and dias_ultima <= 30:
            score += 10                              # 10 pts se ativo no último mês
        # Bonus por funil avançado
        stage = funil.get("lifecycle_stage", "").lower()
        if "qualificado" in stage:
            score += 10
        score = min(score, 100)

        return {
            "total_conversoes": total_conv,
            "total_oportunidades": total_opp,
            "conversoes_ultimos_30d": conv_30d,
            "conversoes_ultimos_90d": conv_90d,
            "dias_desde_primeira_conversao": dias_primeira,
            "dias_desde_ultima_conversao": dias_ultima,
            "frequencia_mensal": frequencia,
            "score_engajamento": score,
            "primeira_conversao": primeira.isoformat() if primeira else None,
            "ultima_conversao": ultima.isoformat() if ultima else None,
        }

    def _analisar_historico_conteudos(self, conversoes: list[dict]) -> dict:
        """
        Analisa os nomes das conversões para extrair temas de interesse.

        Os conversion_identifiers do RD Station contêm nomes de:
        - Newsletters (ex: "NEWSLETTER 13-2026")
        - Eventos/webinars (ex: "[EVENTO] IFRS - CPC 51: Apresentacao...")
        - Materiais/ebooks (ex: "[DES] Fundo de Funil - Abril")
        - Formulários de inscrição

        NOTA: A API do RD Station NÃO expõe dados de abertura/clique
        de email por contato. Essas informações só estão disponíveis
        na interface web. Os conversion_identifiers são nossa melhor
        proxy para entender com que conteúdos o lead interagiu.
        """
        temas = []
        tipos_conteudo = {"newsletter": 0, "evento": 0, "material": 0,
                         "formulario": 0, "webinar": 0, "outro": 0}
        conteudos_detalhados = []

        for c in conversoes:
            ci = (c.get("conversion_identifier")
                  or c.get("event_identifier") or "").strip()
            ts = c.get("event_timestamp", c.get("created_at", ""))
            if not ci:
                continue

            ci_lower = ci.lower()

            # Classificar tipo de conteúdo
            if "newsletter" in ci_lower or "news" in ci_lower:
                tipos_conteudo["newsletter"] += 1
            elif "evento" in ci_lower or "[evento]" in ci_lower or "workshop" in ci_lower:
                tipos_conteudo["evento"] += 1
            elif "webinar" in ci_lower or "live" in ci_lower:
                tipos_conteudo["webinar"] += 1
            elif "ebook" in ci_lower or "material" in ci_lower or "download" in ci_lower or "[des]" in ci_lower:
                tipos_conteudo["material"] += 1
            elif "form" in ci_lower or "inscri" in ci_lower or "cadastr" in ci_lower:
                tipos_conteudo["formulario"] += 1
            else:
                tipos_conteudo["outro"] += 1

            # Extrair temas/palavras-chave dos nomes
            # Remove prefixos comuns como [EVENTO], [DES], [EMAIL X]
            tema_limpo = ci
            for prefix in ("[EVENTO]", "[DES]", "[EMAIL 1]", "[EMAIL 2]",
                           "[EMAIL 3]", "[EMAIL 4]", "[EMAIL 5]",
                           "NEWSLETTER"):
                tema_limpo = tema_limpo.replace(prefix, "").strip()
            # Remove datas do final
            tema_limpo = tema_limpo.split(" - 1")[0].split(" - 2")[0].strip(" -")

            if tema_limpo and len(tema_limpo) > 3:
                temas.append(tema_limpo)

            conteudos_detalhados.append({
                "nome": ci,
                "data": ts[:10] if ts else "",
                "tema_extraido": tema_limpo,
            })

        # Contar temas mais frequentes (agrupar por similaridade simples)
        temas_unicos = []
        for tema in temas:
            found = False
            for tu in temas_unicos:
                # Se um tema contém o outro, agrupar
                if tema.lower() in tu["tema"].lower() or tu["tema"].lower() in tema.lower():
                    tu["count"] += 1
                    found = True
                    break
            if not found:
                temas_unicos.append({"tema": tema, "count": 1})

        temas_unicos.sort(key=lambda x: x["count"], reverse=True)

        return {
            "tipos_conteudo_interagido": {k: v for k, v in tipos_conteudo.items() if v > 0},
            "total_interacoes_conteudo": len(conversoes),
            "temas_interesse": [t["tema"] for t in temas_unicos[:8]],
            "tema_mais_frequente": temas_unicos[0]["tema"] if temas_unicos else "",
            "conteudos_detalhados": conteudos_detalhados[:15],  # até 15 mais recentes
            "engajamento_newsletter": tipos_conteudo["newsletter"],
            "engajamento_eventos": tipos_conteudo["evento"] + tipos_conteudo["webinar"],
            "engajamento_materiais": tipos_conteudo["material"],
        }

    def _identificar_dados_faltantes(self, perfil: dict) -> list[str]:
        """Lista campos importantes que estão faltando."""
        faltantes = []
        campos_importantes = {
            "nome": "Nome completo",
            "cargo": "Cargo profissional",
            "empresa": "Empresa",
            "telefone": "Telefone",
            "cidade": "Cidade",
            "estado": "Estado",
            "linkedin": "LinkedIn",
        }
        for campo, label in campos_importantes.items():
            if not perfil.get(campo):
                faltantes.append(label)
        return faltantes

    def _calcular_completude(self, perfil: dict) -> float:
        """Calcula % de completude do perfil (0.0 a 1.0)."""
        campos_checagem = [
            "nome", "email", "cargo", "empresa", "telefone",
            "cidade", "estado", "linkedin",
        ]
        preenchidos = sum(
            1 for c in campos_checagem if perfil.get(c)
        )
        return round(preenchidos / len(campos_checagem), 2)
