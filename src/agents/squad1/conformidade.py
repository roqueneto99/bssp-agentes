"""
Squad 1 — Agente de Conformidade LGPD.

Responsabilidades:
- Validar se o lead tem bases legais (legal_bases) registradas no RD Station
- Verificar se há consentimento válido para comunicação por email
- Identificar leads sem base legal e sinalizar para regularização
- Adicionar tags de compliance para controle do time jurídico/marketing
- Bloquear leads sem consentimento de seguirem no pipeline

Lei Geral de Proteção de Dados (LGPD - Lei 13.709/2018):
- Art. 7: Tratamento de dados só é lícito com base legal válida
- Art. 8: Consentimento deve ser livre, informado e inequívoco
- Art. 9: Titular tem direito à informação sobre tratamento

Bases legais aceitas pelo RD Station:
- consent (consentimento): mais comum para marketing
- legitimate_interest (interesse legítimo): para relacionamento existente
- pre_existent_contract (contrato pré-existente): para clientes ativos
- judicial_process, vital_interest, public_interest: mais raros

Categorias:
- communications: envio de emails/SMS/WhatsApp
- data_processing: armazenamento e processamento

APIs utilizadas:
- GET /platform/contacts/email:{email} — verificar legal_bases do contato
- PATCH /platform/contacts/email:{email} — atualizar campos de compliance
- POST /platform/contacts/email:{email}/tag — adicionar tags LGPD
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Optional

from ..base import BaseAgent, AgentResult, LLMProvider

logger = logging.getLogger(__name__)


# Status de conformidade
class ComplianceStatus:
    CONFORME = "conforme"                  # Tem base legal válida
    PARCIAL = "parcial"                    # Tem base legal mas incompleta
    NAO_CONFORME = "nao_conforme"          # Sem base legal registrada
    REVOGADO = "revogado"                  # Consentimento foi revogado (declined)


class ConformidadeAgent(BaseAgent):
    """
    Agente de conformidade LGPD.

    Verifica se cada lead tem bases legais adequadas para
    tratamento de dados e comunicação, conforme a LGPD.

    Não usa LLM — toda a lógica é determinística baseada nas
    regras da LGPD e nas bases legais registradas no RD Station.
    """

    agent_name = "squad1_conformidade"

    # Bases legais que autorizam comunicação de marketing
    BASES_COMUNICACAO_VALIDAS = {
        "consent",
        "legitimate_interest",
        "pre_existent_contract",
    }

    # Bases legais que autorizam processamento de dados
    BASES_PROCESSAMENTO_VALIDAS = {
        "consent",
        "legitimate_interest",
        "pre_existent_contract",
        "judicial_process",
        "vital_interest",
        "public_interest",
    }

    def get_system_prompt(self) -> str:
        # Não usa LLM, mas mantém o método por compatibilidade com BaseAgent
        return ""

    async def run(
        self,
        contact_email: str,
        *,
        perfil_coletor: Optional[dict] = None,
    ) -> AgentResult:
        """
        Verifica conformidade LGPD de um lead.

        Args:
            contact_email: Email do lead
            perfil_coletor: Perfil do Coletor (evita re-buscar da API)
        """
        import time

        start = time.monotonic()
        try:
            # Obter dados do contato (precisa dos legal_bases)
            contact = await self.rdstation.get_contact(email=contact_email)
            contact_data = contact.to_api_payload()
            contact_data["uuid"] = contact.uuid
            contact_data["email"] = contact.email
            contact_data["legal_bases"] = [
                lb.to_dict() for lb in contact.legal_bases
            ]

            # Merge com dados do Coletor se disponível
            if perfil_coletor:
                contact_data["perfil_coletor"] = perfil_coletor

            result_data = await self.analyze(contact_data)

            duration = (time.monotonic() - start) * 1000
            status = result_data.get("compliance_status", "?")
            logger.info(
                "Conformidade concluída para %s em %.0fms — status=%s",
                contact_email, duration, status,
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
            logger.error("Conformidade falhou para %s: %s", contact_email, e)
            return AgentResult(
                success=False,
                agent_name=self.agent_name,
                contact_email=contact_email,
                error=str(e),
                duration_ms=duration,
            )

    async def analyze(self, contact_data: dict) -> dict:
        """
        Analisa conformidade LGPD do lead.

        Fluxo:
        1. Extrair e categorizar bases legais existentes
        2. Verificar autorização para comunicação
        3. Verificar autorização para processamento de dados
        4. Determinar status geral de compliance
        5. Gerar recomendações
        6. Aplicar tags e campos no RD Station
        """
        email = contact_data.get("email", "")
        logger.info("Conformidade analisando: %s", email)

        legal_bases = contact_data.get("legal_bases", [])

        # --- 1. Categorizar bases legais ---
        analise_bases = self._categorizar_bases_legais(legal_bases)

        # --- 2. Verificar comunicação ---
        pode_comunicar = self._verificar_comunicacao(analise_bases)

        # --- 3. Verificar processamento ---
        pode_processar = self._verificar_processamento(analise_bases)

        # --- 4. Status geral ---
        compliance_status = self._determinar_status(
            analise_bases, pode_comunicar, pode_processar
        )

        # --- 5. Recomendações ---
        recomendacoes = self._gerar_recomendacoes(
            compliance_status, analise_bases, pode_comunicar, pode_processar
        )

        # --- 6. Ações no RD Station ---
        acoes = await self._aplicar_compliance(
            email, compliance_status, pode_comunicar, pode_processar, recomendacoes
        )

        return {
            "lead_email": email,
            "compliance_status": compliance_status,
            "pode_comunicar": pode_comunicar,
            "pode_processar": pode_processar,
            "bases_legais": analise_bases,
            "total_bases": len(legal_bases),
            "recomendacoes": recomendacoes,
            "acoes_realizadas": acoes,
            "bloqueio_pipeline": compliance_status == ComplianceStatus.REVOGADO,
            "verificado_em": datetime.now().isoformat(),
        }

    def _categorizar_bases_legais(self, legal_bases: list[dict]) -> dict:
        """
        Categoriza as bases legais por categoria e tipo.

        Retorna estrutura organizada das bases encontradas.
        """
        resultado = {
            "communications": {
                "bases": [],
                "tem_consentimento": False,
                "consentimento_ativo": False,
                "tipos_encontrados": [],
            },
            "data_processing": {
                "bases": [],
                "tem_consentimento": False,
                "consentimento_ativo": False,
                "tipos_encontrados": [],
            },
            "outras_categorias": [],
            "tem_alguma_base": len(legal_bases) > 0,
            "algum_revogado": False,
        }

        for lb in legal_bases:
            category = lb.get("category", "")
            lb_type = lb.get("type", "")
            status = lb.get("status", "")

            entry = {
                "type": lb_type,
                "status": status,
                "category": category,
            }

            if category == "communications":
                resultado["communications"]["bases"].append(entry)
                resultado["communications"]["tipos_encontrados"].append(lb_type)
                if lb_type == "consent":
                    resultado["communications"]["tem_consentimento"] = True
                    if status == "granted":
                        resultado["communications"]["consentimento_ativo"] = True
            elif category == "data_processing":
                resultado["data_processing"]["bases"].append(entry)
                resultado["data_processing"]["tipos_encontrados"].append(lb_type)
                if lb_type == "consent":
                    resultado["data_processing"]["tem_consentimento"] = True
                    if status == "granted":
                        resultado["data_processing"]["consentimento_ativo"] = True
            else:
                resultado["outras_categorias"].append(entry)

            if status == "declined":
                resultado["algum_revogado"] = True

        return resultado

    def _verificar_comunicacao(self, analise: dict) -> bool:
        """Verifica se o lead pode receber comunicações de marketing."""
        comm = analise.get("communications", {})
        bases = comm.get("bases", [])

        for base in bases:
            tipo = base.get("type", "")
            status = base.get("status", "")

            # Se tem consentimento revogado, NÃO pode comunicar
            if tipo == "consent" and status == "declined":
                return False

            # Se tem base válida e status granted, PODE comunicar
            if tipo in self.BASES_COMUNICACAO_VALIDAS and status == "granted":
                return True

        # Se não tem nenhuma base de comunicação, não pode
        return False

    def _verificar_processamento(self, analise: dict) -> bool:
        """Verifica se os dados do lead podem ser processados."""
        proc = analise.get("data_processing", {})
        bases = proc.get("bases", [])

        # Se tem base de processamento válida e ativa
        for base in bases:
            tipo = base.get("type", "")
            status = base.get("status", "")

            if tipo == "consent" and status == "declined":
                return False

            if tipo in self.BASES_PROCESSAMENTO_VALIDAS and status == "granted":
                return True

        # Se não tem base de processamento mas tem de comunicação, assume ok
        # (o processamento é implícito na comunicação, por interesse legítimo)
        comm = analise.get("communications", {})
        comm_bases = comm.get("bases", [])
        for base in comm_bases:
            if base.get("type") in self.BASES_PROCESSAMENTO_VALIDAS and base.get("status") == "granted":
                return True

        # Se não tem nenhuma base, verifica se ao menos existe alguma
        return analise.get("tem_alguma_base", False)

    def _determinar_status(
        self,
        analise: dict,
        pode_comunicar: bool,
        pode_processar: bool,
    ) -> str:
        """Determina o status geral de compliance."""

        # Se algum consentimento foi revogado (declined)
        if analise.get("algum_revogado"):
            comm = analise.get("communications", {})
            # Se especificamente a comunicação foi revogada
            for base in comm.get("bases", []):
                if base.get("type") == "consent" and base.get("status") == "declined":
                    return ComplianceStatus.REVOGADO

        # Se pode comunicar e processar = conforme
        if pode_comunicar and pode_processar:
            return ComplianceStatus.CONFORME

        # Se pode processar mas não comunicar = parcial
        if pode_processar and not pode_comunicar:
            return ComplianceStatus.PARCIAL

        # Se não tem nenhuma base = não conforme
        if not analise.get("tem_alguma_base"):
            return ComplianceStatus.NAO_CONFORME

        # Tem alguma base mas sem permissão clara = parcial
        return ComplianceStatus.PARCIAL

    def _gerar_recomendacoes(
        self,
        status: str,
        analise: dict,
        pode_comunicar: bool,
        pode_processar: bool,
    ) -> list[str]:
        """Gera recomendações de ação baseado no status de compliance."""
        recs = []

        if status == ComplianceStatus.REVOGADO:
            recs.append(
                "URGENTE: Lead revogou consentimento. Remover de todos os "
                "fluxos de automação e listas de email imediatamente."
            )
            recs.append(
                "Não enviar nenhuma comunicação até que novo consentimento "
                "seja obtido voluntariamente."
            )
            return recs

        if status == ComplianceStatus.NAO_CONFORME:
            recs.append(
                "Lead sem base legal registrada. Necessário obter consentimento "
                "antes de qualquer comunicação de marketing."
            )
            recs.append(
                "Incluir em fluxo de reconfirmação de consentimento (double opt-in)."
            )

        if not pode_comunicar and status != ComplianceStatus.REVOGADO:
            recs.append(
                "Sem autorização para comunicação de marketing. "
                "Solicitar consentimento via formulário ou landing page."
            )

        if not analise["communications"].get("tem_consentimento"):
            recs.append(
                "Registrar base legal de comunicação (consent ou legitimate_interest) "
                "no RD Station."
            )

        if not analise["data_processing"].get("bases"):
            recs.append(
                "Considerar registrar base legal de processamento de dados "
                "(legitimate_interest recomendado para leads de pós-graduação)."
            )

        if status == ComplianceStatus.CONFORME:
            recs.append("Lead em conformidade com a LGPD. Pode seguir no pipeline.")

        return recs

    async def _aplicar_compliance(
        self,
        email: str,
        status: str,
        pode_comunicar: bool,
        pode_processar: bool,
        recomendacoes: list[str],
    ) -> dict:
        """Aplica tags e campos de compliance no RD Station."""
        acoes = {"tags_adicionadas": [], "campos_atualizados": {}}

        # --- Tags de compliance ---
        tags = []
        tag_map = {
            ComplianceStatus.CONFORME: "lgpd-conforme",
            ComplianceStatus.PARCIAL: "lgpd-parcial",
            ComplianceStatus.NAO_CONFORME: "lgpd-pendente",
            ComplianceStatus.REVOGADO: "lgpd-revogado",
        }
        tag_status = tag_map.get(status)
        if tag_status:
            tags.append(tag_status)

        if not pode_comunicar:
            tags.append("lgpd-sem-comunicacao")

        tags.append(f"lgpd-verificado-{datetime.now().strftime('%Y%m')}")

        if tags:
            try:
                await self.rdstation.add_tags(email, tags)
                acoes["tags_adicionadas"] = tags
                logger.info("Conformidade adicionou tags a %s: %s", email, tags)
            except Exception as e:
                logger.error("Falha ao adicionar tags LGPD a %s: %s", email, e)

        # --- Campo de última verificação ---
        campos = {
            "cf_ultima_interacao_agente": (
                f"LGPD verificado: {status} — {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            ),
        }
        try:
            await self.rdstation.upsert_contact(email, campos)
            acoes["campos_atualizados"] = campos
        except Exception as e:
            logger.error("Falha ao atualizar campos LGPD de %s: %s", email, e)

        return acoes
