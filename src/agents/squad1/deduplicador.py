"""
Squad 1 — Agente Deduplicador.

Responsabilidades:
- Identificar leads duplicados no RD Station
- Critérios de duplicidade: email alternativo, nome+telefone, nome+empresa
- Decidir qual registro manter (o mais completo/recente)
- Unificar dados dos duplicados no registro principal
- Marcar duplicados com tags para revisão manual ou merge automático

Estratégia de detecção:
1. Email exato (extra_emails): se um contato tem emails alternativos que
   batem com o email principal de outro contato
2. Nome normalizado + telefone: match fuzzy no nome + telefone exato
3. Nome normalizado + empresa: para empresas pequenas (menos provável)

APIs utilizadas:
- GET /platform/contacts/email:{email} — buscar contato
- GET /platform/segmentations/{id}/contacts — buscar contatos por segmentação
- PATCH /platform/contacts/email:{email} — consolidar dados
- POST /platform/contacts/email:{email}/tag — marcar duplicados

NOTA: O RD Station não tem API de merge nativa. O Deduplicador marca
duplicados com tags e consolida dados manualmente via API.
"""

from __future__ import annotations

import logging
import re
import unicodedata
from datetime import datetime
from typing import Any, Optional

from ..base import BaseAgent, AgentResult, LLMMessage, LLMProvider

logger = logging.getLogger(__name__)


def normalize_name(name: str) -> str:
    """Normaliza nome para comparação (sem acentos, lowercase, sem espaços extras)."""
    if not name:
        return ""
    # Remove acentos
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_text = nfkd.encode("ASCII", "ignore").decode("ASCII")
    # Lowercase + trim
    return re.sub(r"\s+", " ", ascii_text.lower().strip())


def normalize_phone(phone: str) -> str:
    """Normaliza telefone para comparação (só dígitos)."""
    if not phone:
        return ""
    digits = re.sub(r"\D", "", phone)
    # Se tem 13 dígitos (55 + DDD + número), remove o 55
    if len(digits) == 13 and digits.startswith("55"):
        digits = digits[2:]
    # Se tem 10-11 dígitos, tá ok (DDD + número)
    return digits


class DeduplicadorAgent(BaseAgent):
    """
    Agente de deduplicação de leads.

    Busca leads potencialmente duplicados e consolida dados.
    Trabalha em duas modalidades:
    - Focal: verifica se um lead específico tem duplicados
    - Batch: varre uma segmentação inteira (uso periódico)
    """

    agent_name = "squad1_deduplicador"

    # Limite de contatos para comparar em modo focal
    MAX_COMPARE = 200

    def get_system_prompt(self) -> str:
        return """Você é um analista de dados especializado em deduplicação de leads.
Dado dois perfis de leads, determine se são a mesma pessoa.

Responda APENAS com JSON:
{
    "sao_duplicados": true | false,
    "confianca": 0.0 a 1.0,
    "motivo": "explicação curta",
    "registro_principal": "email do registro mais completo/recente",
    "campos_a_consolidar": {"campo": "valor do registro secundário que complementa o principal"}
}

CRITÉRIOS:
- Nomes muito similares + mesmo telefone = alta confiança (0.9+)
- Mesmo nome + mesma empresa = média confiança (0.7)
- Emails do mesmo domínio corporativo + mesmo nome = média confiança (0.6)
- Apenas nomes similares sem outro dado = baixa confiança (0.3)

Só marque como duplicado se confiança >= 0.7."""

    async def run(
        self,
        contact_email: str,
        *,
        perfil_coletor: Optional[dict] = None,
        segmentation_id: Optional[int] = None,
    ) -> AgentResult:
        """
        Verifica duplicados para um lead específico.

        Args:
            contact_email: Email do lead a verificar
            perfil_coletor: Perfil do Coletor (evita re-buscar)
            segmentation_id: ID de segmentação para buscar candidatos
        """
        import time

        start = time.monotonic()
        try:
            # Obter dados do lead alvo
            if perfil_coletor:
                lead_data = perfil_coletor
            else:
                contact = await self.rdstation.get_contact(email=contact_email)
                lead_data = contact.to_api_payload()
                lead_data["uuid"] = contact.uuid
                lead_data["email"] = contact.email

            result_data = await self.analyze(
                lead_data,
                segmentation_id=segmentation_id,
            )

            duration = (time.monotonic() - start) * 1000
            n_dupes = len(result_data.get("duplicados_encontrados", []))
            logger.info(
                "Deduplicador concluído para %s em %.0fms — %d duplicados encontrados",
                contact_email, duration, n_dupes,
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
            logger.error("Deduplicador falhou para %s: %s", contact_email, e)
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
        segmentation_id: Optional[int] = None,
    ) -> dict:
        """
        Busca e identifica duplicados de um lead.

        Fluxo:
        1. Normaliza dados do lead alvo
        2. Busca candidatos a duplicados (por segmentação ou extra_emails)
        3. Compara cada candidato com regras determinísticas
        4. Usa LLM para casos ambíguos (confiança 0.5-0.7)
        5. Consolida dados e aplica tags
        """
        email = contact_data.get("email", "")
        logger.info("Deduplicador analisando: %s", email)

        # --- 1. Normalizar lead alvo ---
        lead_norm = self._normalize_lead(contact_data)

        # --- 2. Buscar candidatos ---
        candidatos = await self._buscar_candidatos(
            email, contact_data, segmentation_id
        )

        if not candidatos:
            return {
                "duplicados_encontrados": [],
                "total_candidatos_analisados": 0,
                "acoes_realizadas": [],
                "lead_email": email,
            }

        # --- 3. Comparar candidatos ---
        duplicados = []
        for candidato in candidatos:
            if candidato.get("email", "").lower() == email.lower():
                continue  # Pular o próprio lead

            cand_norm = self._normalize_lead(candidato)
            match_result = self._comparar_leads(lead_norm, cand_norm)

            if match_result["confianca"] >= 0.7:
                duplicados.append({
                    "email": candidato.get("email", ""),
                    "nome": candidato.get("name", candidato.get("nome", "")),
                    **match_result,
                })
            elif match_result["confianca"] >= 0.5 and self.llm:
                # Caso ambíguo — consultar LLM
                try:
                    llm_result = await self._verificar_duplicado_llm(
                        contact_data, candidato
                    )
                    if llm_result.get("sao_duplicados") and llm_result.get("confianca", 0) >= 0.7:
                        duplicados.append({
                            "email": candidato.get("email", ""),
                            "nome": candidato.get("name", candidato.get("nome", "")),
                            **llm_result,
                            "verificado_por_llm": True,
                        })
                except Exception as e:
                    logger.warning("LLM falhou ao verificar duplicado: %s", e)

        # --- 4. Aplicar ações ---
        acoes = []
        for dupe in duplicados:
            acao = await self._processar_duplicado(email, contact_data, dupe)
            if acao:
                acoes.append(acao)

        return {
            "duplicados_encontrados": duplicados,
            "total_candidatos_analisados": len(candidatos),
            "acoes_realizadas": acoes,
            "lead_email": email,
        }

    def _normalize_lead(self, lead: dict) -> dict:
        """Normaliza campos do lead para comparação."""
        nome = lead.get("nome", lead.get("name", ""))
        return {
            "email": (lead.get("email") or "").lower().strip(),
            "nome_norm": normalize_name(nome),
            "nome_original": nome,
            "telefone_norm": normalize_phone(
                lead.get("telefone")
                or lead.get("personal_phone")
                or lead.get("mobile_phone")
                or ""
            ),
            "celular_norm": normalize_phone(
                lead.get("celular") or lead.get("mobile_phone") or ""
            ),
            "empresa": (
                lead.get("empresa", lead.get("company", ""))
            ).lower().strip(),
            "extra_emails": [
                e.lower().strip() for e in lead.get("extra_emails", [])
            ],
            "tags": lead.get("tags", []),
            "cidade": (
                lead.get("cidade", lead.get("city", ""))
            ).lower().strip(),
        }

    async def _buscar_candidatos(
        self,
        email: str,
        contact_data: dict,
        segmentation_id: Optional[int] = None,
    ) -> list[dict]:
        """
        Busca leads candidatos a duplicados.

        Estratégias:
        1. Verificar extra_emails do lead contra contatos existentes
        2. Se tem segmentation_id, buscar contatos da segmentação
        3. Usar a lista de segmentações para encontrar contatos da mesma segmentação
        """
        candidatos = []
        emails_vistos = {email.lower()}

        # Estratégia 1: Checar extra_emails
        extra_emails = contact_data.get("extra_emails", [])
        for extra_email in extra_emails:
            if extra_email.lower() in emails_vistos:
                continue
            try:
                contact = await self.rdstation.get_contact(email=extra_email)
                cand = contact.to_api_payload()
                cand["email"] = contact.email
                cand["uuid"] = contact.uuid
                candidatos.append(cand)
                emails_vistos.add(extra_email.lower())
            except Exception:
                continue  # Email não existe como contato separado

        # Estratégia 2: Buscar contatos da segmentação
        if segmentation_id:
            try:
                seg_data = await self.rdstation.get_segmentation_contacts(
                    segmentation_id, page_size=125
                )
                seg_contacts = seg_data.get("contacts", []) if isinstance(seg_data, dict) else []
                for sc in seg_contacts[:self.MAX_COMPARE]:
                    sc_email = sc.get("email", "").lower()
                    if sc_email and sc_email not in emails_vistos:
                        candidatos.append(sc)
                        emails_vistos.add(sc_email)
            except Exception as e:
                logger.warning("Falha ao buscar segmentação %s: %s", segmentation_id, e)

        # Estratégia 3: Se nenhum candidato e temos nome, buscar em segmentações
        if not candidatos and contact_data.get("nome", contact_data.get("name")):
            try:
                segmentations = await self.rdstation.list_segmentations()
                # Pega primeira segmentação ativa para ter um pool de comparação
                for seg in segmentations[:3]:
                    seg_id = seg.get("id")
                    if not seg_id:
                        continue
                    seg_data = await self.rdstation.get_segmentation_contacts(
                        seg_id, page_size=125
                    )
                    seg_contacts = (
                        seg_data.get("contacts", [])
                        if isinstance(seg_data, dict) else []
                    )
                    for sc in seg_contacts:
                        sc_email = sc.get("email", "").lower()
                        if sc_email and sc_email not in emails_vistos:
                            candidatos.append(sc)
                            emails_vistos.add(sc_email)
                    if len(candidatos) >= self.MAX_COMPARE:
                        break
            except Exception as e:
                logger.warning("Falha ao buscar segmentações: %s", e)

        return candidatos[:self.MAX_COMPARE]

    def _comparar_leads(self, lead_a: dict, lead_b: dict) -> dict:
        """
        Compara dois leads normalizados e retorna score de duplicidade.

        Retorna dict com: confianca (0-1), motivo, campos_a_consolidar
        """
        confianca = 0.0
        motivos = []
        consolidar = {}

        nome_a = lead_a["nome_norm"]
        nome_b = lead_b["nome_norm"]
        tel_a = lead_a["telefone_norm"] or lead_a["celular_norm"]
        tel_b = lead_b["telefone_norm"] or lead_b["celular_norm"]
        empresa_a = lead_a["empresa"]
        empresa_b = lead_b["empresa"]

        # Check 1: Email aparece nos extra_emails do outro
        if lead_b["email"] in lead_a["extra_emails"]:
            confianca = 0.95
            motivos.append(f"Email {lead_b['email']} está nos extra_emails")
            return {
                "confianca": confianca,
                "motivo": "; ".join(motivos),
                "campos_a_consolidar": consolidar,
            }

        if lead_a["email"] in lead_b["extra_emails"]:
            confianca = 0.95
            motivos.append(f"Email {lead_a['email']} está nos extra_emails do candidato")
            return {
                "confianca": confianca,
                "motivo": "; ".join(motivos),
                "campos_a_consolidar": consolidar,
            }

        # Check 2: Nome idêntico + telefone idêntico
        if nome_a and nome_b and nome_a == nome_b:
            confianca += 0.4
            motivos.append("Nomes idênticos")

            if tel_a and tel_b and tel_a == tel_b:
                confianca += 0.5
                motivos.append("Telefones idênticos")
            elif empresa_a and empresa_b and empresa_a == empresa_b:
                confianca += 0.3
                motivos.append("Mesma empresa")
            elif lead_a.get("cidade") and lead_a["cidade"] == lead_b.get("cidade"):
                confianca += 0.1
                motivos.append("Mesma cidade")

        # Check 3: Nomes similares (80%+ de match em tokens)
        elif nome_a and nome_b:
            tokens_a = set(nome_a.split())
            tokens_b = set(nome_b.split())
            if tokens_a and tokens_b:
                overlap = len(tokens_a & tokens_b) / max(len(tokens_a), len(tokens_b))
                if overlap >= 0.8:
                    confianca += 0.3
                    motivos.append(f"Nomes similares ({overlap:.0%} overlap)")
                    if tel_a and tel_b and tel_a == tel_b:
                        confianca += 0.5
                        motivos.append("Telefones idênticos")

        # Check 4: Mesmo domínio corporativo no email + nome similar
        domain_a = lead_a["email"].split("@")[-1] if "@" in lead_a["email"] else ""
        domain_b = lead_b["email"].split("@")[-1] if "@" in lead_b["email"] else ""
        free_domains = {
            "gmail.com", "hotmail.com", "outlook.com", "yahoo.com",
            "yahoo.com.br", "bol.com.br", "uol.com.br", "terra.com.br",
        }
        if (
            domain_a and domain_b
            and domain_a == domain_b
            and domain_a not in free_domains
            and nome_a and nome_b
        ):
            tokens_a = set(nome_a.split())
            tokens_b = set(nome_b.split())
            overlap = len(tokens_a & tokens_b) / max(len(tokens_a), len(tokens_b)) if tokens_a and tokens_b else 0
            if overlap >= 0.5:
                confianca += 0.2
                motivos.append(f"Mesmo domínio corporativo ({domain_a})")

        # Identificar campos para consolidar (do candidato que complementa o principal)
        if confianca >= 0.5:
            campos_b = {
                "empresa": lead_b.get("empresa"),
                "telefone": lead_b.get("telefone_norm"),
                "cidade": lead_b.get("cidade"),
            }
            for campo, valor in campos_b.items():
                if valor and not getattr(lead_a, campo, lead_a.get(campo)):
                    consolidar[campo] = valor

        return {
            "confianca": min(confianca, 1.0),
            "motivo": "; ".join(motivos) if motivos else "Sem correspondência significativa",
            "campos_a_consolidar": consolidar,
        }

    async def _verificar_duplicado_llm(
        self, lead_a: dict, lead_b: dict
    ) -> dict:
        """Usa LLM para casos ambíguos."""
        context = (
            f"Lead A:\n"
            f"  Email: {lead_a.get('email', '?')}\n"
            f"  Nome: {lead_a.get('nome', lead_a.get('name', '?'))}\n"
            f"  Cargo: {lead_a.get('cargo', lead_a.get('job_title', '?'))}\n"
            f"  Empresa: {lead_a.get('empresa', '?')}\n"
            f"  Telefone: {lead_a.get('telefone', lead_a.get('personal_phone', '?'))}\n"
            f"  Cidade: {lead_a.get('cidade', lead_a.get('city', '?'))}\n"
            f"\nLead B:\n"
            f"  Email: {lead_b.get('email', '?')}\n"
            f"  Nome: {lead_b.get('nome', lead_b.get('name', '?'))}\n"
            f"  Cargo: {lead_b.get('cargo', lead_b.get('job_title', '?'))}\n"
            f"  Empresa: {lead_b.get('empresa', '?')}\n"
            f"  Telefone: {lead_b.get('telefone', lead_b.get('personal_phone', '?'))}\n"
            f"  Cidade: {lead_b.get('cidade', lead_b.get('city', '?'))}\n"
        )

        return await self.llm.complete_json(
            messages=[
                LLMMessage(
                    role="user",
                    content=f"Esses dois leads são a mesma pessoa?\n\n{context}",
                )
            ],
            system=self.get_system_prompt(),
            temperature=0.1,
        )

    async def _processar_duplicado(
        self, email_principal: str, lead_principal: dict, duplicado: dict
    ) -> Optional[dict]:
        """
        Processa um duplicado encontrado:
        1. Consolida campos faltantes no registro principal
        2. Marca o duplicado com tag
        """
        acao = {
            "tipo": "duplicado_detectado",
            "email_principal": email_principal,
            "email_duplicado": duplicado.get("email", ""),
            "confianca": duplicado.get("confianca", 0),
            "motivo": duplicado.get("motivo", ""),
            "consolidacoes": {},
        }

        # Consolidar campos faltantes
        campos = duplicado.get("campos_a_consolidar", {})
        if campos:
            try:
                await self.rdstation.upsert_contact(email_principal, campos)
                acao["consolidacoes"] = campos
                logger.info(
                    "Deduplicador consolidou campos de %s → %s: %s",
                    duplicado["email"], email_principal, list(campos.keys()),
                )
            except Exception as e:
                logger.error("Falha ao consolidar campos: %s", e)

        # Marcar duplicado com tag
        dupe_email = duplicado.get("email", "")
        if dupe_email:
            try:
                tag = f"duplicado-de-{email_principal[:50]}"
                await self.rdstation.add_tags(dupe_email, [
                    "duplicado-detectado",
                    tag,
                    f"dedup-{datetime.now().strftime('%Y%m%d')}",
                ])
                acao["tags_adicionadas"] = True
                logger.info(
                    "Deduplicador marcou %s como duplicado de %s",
                    dupe_email, email_principal,
                )
            except Exception as e:
                logger.error("Falha ao marcar duplicado %s: %s", dupe_email, e)

        return acao
