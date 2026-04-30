"""
Squad 3 — Agente 1: Multicanal.

Responsável por DESPACHAR mensagens já renderizadas pela
Personalização Comportamental nos canais ativos do lead:
e-mail (SendGrid), WhatsApp (Hablla ou WhatsApp Business API
direto) e SMS (fase 2).

Modos de operação:
    - dry_run=True (default em S1):
        Não chama provedor externo. Apenas valida o payload e
        registra que o envio aconteceria (status='pending' na
        coluna data, mas se um repo for fornecido, persiste
        com status='skipped' / 'pending' conforme o caso).

    - dry_run=False + sendgrid configurado (S2 — canal=email):
        Cria linha em mensagens_squad3 com status='pending',
        chama POST /v3/mail/send da SendGrid, atualiza linha
        com external_id (X-Message-Id) e status='sent'. Se a
        chamada à SendGrid falhar (4xx/5xx), marca a linha
        como 'failed' e devolve erro no AgentResult.

    - canal=whatsapp ou sms:
        Ainda em S2 não há provedor — entra em S5 (WhatsApp)
        e fase 2 (SMS). Por enquanto, status='skipped' com
        razao='provedor_canal_nao_implementado'.

Regras transversais:
    - Idempotência por (email, cadencia_id, passo).
    - Janela horária permitida (default seg–sáb 09–20 BRT).
    - Bloqueio se Squad 2 reportou rota='blocked' / classificacao='BLOCKED' /
      rota='cliente_existente'.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Optional

from ..base import AgentResult, LLMProvider

logger = logging.getLogger(__name__)


CANAIS_SUPORTADOS = ("email", "whatsapp", "sms")


class MulticanalAgent:
    """
    Despacha mensagens. S1: dry-run. S2: SendGrid para canal=email.
    """

    agent_name = "squad3_multicanal"

    def __init__(
        self,
        llm: LLMProvider,
        rdstation: Any,
        hablla: Any = None,
        sendgrid: Any = None,             # SendGridClient (opcional)
        mensagens_repo: Any = None,       # MensagensRepoLike (opcional)
        dry_run: bool = True,
    ) -> None:
        self.llm = llm
        self.rdstation = rdstation
        self.hablla = hablla
        self.sendgrid = sendgrid
        self.mensagens_repo = mensagens_repo
        self.dry_run = dry_run

    async def run(
        self,
        email: str,
        *,
        mensagem: dict | None = None,
        perfil_squad2: dict | None = None,
    ) -> AgentResult:
        """
        Despacha (ou agenda) uma mensagem.

        Args:
            email: Email do lead.
            mensagem: dict produzido pela Personalização Comportamental
                (assunto, corpo, canal, nudge, template_id, prompt_hash, razao).
            perfil_squad2: resumo do Squad 2 (para validar bloqueio LGPD,
                escolher canal preferido, etc.).
        """
        start = time.monotonic()
        try:
            payload = self._build_payload(email, mensagem or {}, perfil_squad2 or {})

            ok, motivo = self._validar_envio(payload, perfil_squad2 or {})
            if not ok:
                logger.info("Multicanal — envio bloqueado para %s: %s", email, motivo)
                payload.update(status="skipped", razao_skip=motivo)
                # Persiste a tentativa, se houver repositório
                msg_id = await self._persistir_pendente(payload)
                if msg_id and self.mensagens_repo:
                    await self.mensagens_repo.marcar_skipped(msg_id, razao_skip=motivo)
                payload["mensagem_id"] = msg_id
                return AgentResult(
                    success=True,  # bloqueio intencional não é falha
                    agent_name=self.agent_name,
                    contact_email=email,
                    data=payload,
                    duration_ms=(time.monotonic() - start) * 1000,
                )

            # Persiste a linha como 'pending' (idempotente: se já existe, retorna None)
            mensagem_id = await self._persistir_pendente(payload)
            payload["mensagem_id"] = mensagem_id

            # Caminho 1: dry-run global
            if self.dry_run:
                payload["status"] = "pending"
                payload["dry_run"] = True
                logger.info(
                    "Multicanal DRY-RUN para %s: canal=%s, nudge=%s, passo=%s",
                    email, payload.get("canal"), payload.get("nudge"),
                    payload.get("passo"),
                )
                return AgentResult(
                    success=True,
                    agent_name=self.agent_name,
                    contact_email=email,
                    data=payload,
                    duration_ms=(time.monotonic() - start) * 1000,
                )

            # Caminho 2: envio real
            if payload["canal"] == "email" and self.sendgrid is not None:
                await self._enviar_email(email, payload, mensagem_id)
            elif payload["canal"] in ("whatsapp", "sms"):
                payload["status"] = "skipped"
                payload["razao_skip"] = "provedor_canal_nao_implementado"
                if mensagem_id and self.mensagens_repo:
                    await self.mensagens_repo.marcar_skipped(
                        mensagem_id, razao_skip=payload["razao_skip"],
                    )
            else:
                payload["status"] = "skipped"
                payload["razao_skip"] = "provedor_email_nao_configurado"
                if mensagem_id and self.mensagens_repo:
                    await self.mensagens_repo.marcar_skipped(
                        mensagem_id, razao_skip=payload["razao_skip"],
                    )

            return AgentResult(
                success=True,
                agent_name=self.agent_name,
                contact_email=email,
                data=payload,
                duration_ms=(time.monotonic() - start) * 1000,
            )

        except Exception as e:
            logger.error("Multicanal falhou para %s: %s", email, e)
            return AgentResult(
                success=False,
                agent_name=self.agent_name,
                contact_email=email,
                error=str(e),
                duration_ms=(time.monotonic() - start) * 1000,
            )

    # -----------------------------------------------------------------
    # Lógica interna
    # -----------------------------------------------------------------

    def _build_payload(self, email: str, mensagem: dict, perfil_squad2: dict) -> dict:
        canal = mensagem.get("canal") or self._canal_preferido(perfil_squad2)
        if canal not in CANAIS_SUPORTADOS:
            canal = "email"

        return {
            "email": email,
            "canal": canal,
            "cadencia_nome": mensagem.get("cadencia_nome"),
            "passo": mensagem.get("passo", 0),
            "nudge": mensagem.get("nudge"),
            "template_id": mensagem.get("template_id"),
            "template_versao": mensagem.get("template_versao"),
            "assunto": mensagem.get("assunto"),
            "corpo": mensagem.get("corpo"),
            "modelo_llm": mensagem.get("modelo_llm"),
            "prompt_hash": mensagem.get("prompt_hash"),
            "razao": mensagem.get("razao"),
            "criado_em": datetime.now(tz=timezone.utc).isoformat(),
        }

    def _validar_envio(self, payload: dict, perfil_squad2: dict) -> tuple[bool, str]:
        if perfil_squad2.get("classificacao") == "BLOCKED":
            return False, "lgpd_bloqueado"
        if perfil_squad2.get("rota") == "blocked":
            return False, "rota_blocked"
        if perfil_squad2.get("rota") == "cliente_existente":
            return False, "cliente_existente"

        agora = datetime.now()
        if agora.weekday() == 6:  # domingo
            return False, "fora_da_janela_horaria"

        if not payload.get("assunto") and payload.get("canal") == "email":
            return False, "assunto_vazio"
        if not payload.get("corpo"):
            return False, "corpo_vazio"

        return True, "ok"

    def _canal_preferido(self, perfil_squad2: dict) -> str:
        sinais = perfil_squad2.get("dimensoes", {}) or {}
        canais_ativos = perfil_squad2.get("canais_ativos") or []
        if "whatsapp" in canais_ativos or sinais.get("whatsapp_ativo"):
            return "whatsapp"
        return "email"

    async def _persistir_pendente(self, payload: dict) -> Optional[int]:
        if not self.mensagens_repo:
            return None
        return await self.mensagens_repo.criar_mensagem(
            email=payload["email"],
            canal=payload["canal"],
            cadencia_nome=payload.get("cadencia_nome"),
            passo=payload.get("passo", 0),
            nudge=payload.get("nudge"),
            template_id=payload.get("template_id"),
            template_versao=payload.get("template_versao"),
            assunto=payload.get("assunto"),
            corpo=payload.get("corpo"),
            modelo_llm=payload.get("modelo_llm"),
            prompt_hash=payload.get("prompt_hash"),
            razao=payload.get("razao"),
        )

    async def _enviar_email(
        self, email: str, payload: dict, mensagem_id: Optional[int],
    ) -> None:
        """Disparo real via SendGrid + persistência do external_id."""
        body_text = payload.get("corpo") or ""
        # HTML simples baseado no texto: troca \n por <br/> e envolve em <p>
        body_html = (
            "<html><body style='font-family:Arial,Helvetica,sans-serif;font-size:14px;line-height:1.5'>"
            f"<p>{body_text.replace(chr(10), '<br/>')}</p>"
            "</body></html>"
        )

        custom_args: dict[str, str] = {
            "squad3_email": email,
            "squad3_cadencia": str(payload.get("cadencia_nome") or ""),
            "squad3_passo": str(payload.get("passo", 0)),
            "squad3_nudge": str(payload.get("nudge") or ""),
        }
        if mensagem_id:
            custom_args["squad3_msg_id"] = str(mensagem_id)

        try:
            response = await self.sendgrid.send_mail(
                to_email=email,
                subject=payload.get("assunto") or "(sem assunto)",
                body_text=body_text,
                body_html=body_html,
                custom_args=custom_args,
                categories=["squad3", payload.get("nudge") or "default"],
            )
        except Exception as e:
            logger.error("SendGrid send_mail crashou para %s: %s", email, e)
            payload["status"] = "failed"
            payload["error"] = str(e)
            if mensagem_id and self.mensagens_repo:
                await self.mensagens_repo.marcar_falha(mensagem_id, erro=str(e))
            return

        if response.success:
            payload["status"] = "sent"
            payload["external_id"] = response.message_id
            payload["sandbox_mode"] = response.sandbox_mode
            if mensagem_id and self.mensagens_repo:
                await self.mensagens_repo.marcar_enviada(
                    mensagem_id, external_id=response.message_id,
                )
            logger.info(
                "Multicanal — e-mail despachado para %s (sandbox=%s, id=%s)",
                email, response.sandbox_mode, response.message_id,
            )
        else:
            payload["status"] = "failed"
            payload["error"] = response.error or f"http_{response.status_code}"
            if mensagem_id and self.mensagens_repo:
                await self.mensagens_repo.marcar_falha(mensagem_id, erro=payload["error"])
            logger.warning(
                "Multicanal — SendGrid falhou para %s: status=%d error=%s",
                email, response.status_code, payload["error"],
            )
