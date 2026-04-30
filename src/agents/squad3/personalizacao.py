"""
Squad 3 — Agente 3: Personalização Comportamental.

Pipeline (S3):
    1. Escolher nudge a partir da matriz de decisão (situação → nudge).
    2. Carregar template versionado e aprovado pelo TemplateLoader.
    3. Renderizar com variáveis do perfil do lead (Squad 1 + 2).
    4. Polir via LLM (Claude Sonnet) adaptando o tom psicológico:
        - analitico  → ênfase em dados, comparações, certificações
        - impulsivo  → CTA direto, frases curtas, urgência
        - cauteloso  → garantias institucionais, tempo para decidir
    5. Sanity check: rejeita output do LLM que introduza variáveis fora
       do dicionário declarado pelo template (mitigação anti-alucinação).
    6. Fallback: se o LLM falhar, timeout, ou sanity check rejeitar,
       devolve o template renderizado puro (sem polish).

Cada mensagem produzida grava prompt_hash, modelo, tom, template_id,
template_version e razao para auditoria — campos que vão para
mensagens_squad3 via Multicanal.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from pathlib import Path
from typing import Any, Optional

from ..base import AgentResult, LLMMessage, LLMProvider
from .template_loader import (
    Template,
    TemplateLoader,
    TONS_SUPORTADOS,
    VARIAVEIS_GLOBAIS,
    renderizar,
)

logger = logging.getLogger(__name__)


# Matriz de decisão simplificada (situação → nudge primário).
# Espelha §5.2 do diagnóstico Squad 3.
MATRIZ_NUDGE = {
    "abandonou_matricula":   ("fricao", "loss_aversion"),
    "altamente_engajado":    ("escassez", "fechamento"),
    "indeciso":              ("prova_social", "educativa"),
    "inativo_7d":            ("loss_aversion", "ancoragem"),
    "campanha_carreira":     ("prova_social", "depoimento"),
    "campanha_preco":        ("ancoragem", "escassez"),
    "default":               ("prova_social",),
}

# Pasta padrão dos templates (relativa a este arquivo)
DEFAULT_TEMPLATES_DIR = Path(__file__).parent / "templates"

# Detecção barata de "alucinação" no output do LLM:
# qualquer {placeholder} que não esteja no dicionário declarado é descartado.
PLACEHOLDER_RE = re.compile(r"\{(\w+)\}")


class PersonalizacaoComportamentalAgent:
    """
    Escolhe nudge + carrega template + LLM polish + sanity check.

    O LLM é OPCIONAL: se llm.api_key for vazio (provider dummy) ou se
    a chamada falhar, devolvemos o template puro como fallback. Isso
    permite rodar o pipeline em ambientes sem LLM_API_KEY (testes locais,
    sandbox SendGrid sem chamadas externas).
    """

    agent_name = "squad3_personalizacao"

    def __init__(
        self,
        llm: LLMProvider,
        rdstation: Any,
        hablla: Any = None,
        templates_dir: Optional[Path] = None,
        loader_modo: str = "apenas_aprovados",
        usar_llm: bool = True,
    ) -> None:
        self.llm = llm
        self.rdstation = rdstation
        self.hablla = hablla
        self.usar_llm = usar_llm

        self.loader = TemplateLoader(
            templates_dir or DEFAULT_TEMPLATES_DIR,
            modo=loader_modo,
        )
        # Carrega na inicialização — falha barulhenta agora é melhor que
        # silenciosa em runtime.
        self.loader.carregar()

    async def run(
        self,
        email: str,
        *,
        passo_cadencia: dict | None = None,
        perfil_squad2: dict | None = None,
        perfil_squad1: dict | None = None,
    ) -> AgentResult:
        start = time.monotonic()
        try:
            passo = passo_cadencia or {}
            ps2 = perfil_squad2 or {}
            ps1 = perfil_squad1 or {}

            nudge = passo.get("nudge") or self._escolher_nudge(ps2)
            template = self._lookup_template(nudge, passo)
            if template is None:
                return AgentResult(
                    success=False,
                    agent_name=self.agent_name,
                    contact_email=email,
                    error=f"template_nao_encontrado:nudge={nudge}",
                    duration_ms=(time.monotonic() - start) * 1000,
                )

            tom = self._inferir_tom(ps2, ps1)
            variaveis = self._montar_variaveis(email, ps1, ps2)

            assunto_base, corpo_base, faltas = renderizar(template, variaveis)
            faltas_obrigatorias = faltas & set(template.variaveis_obrigatorias)
            if faltas_obrigatorias:
                logger.warning(
                    "Personalização — variáveis obrigatórias faltando para %s: %s",
                    email, sorted(faltas_obrigatorias),
                )
                return AgentResult(
                    success=False,
                    agent_name=self.agent_name,
                    contact_email=email,
                    error=f"variaveis_faltantes:{','.join(sorted(faltas_obrigatorias))}",
                    duration_ms=(time.monotonic() - start) * 1000,
                )

            # Tenta polir com LLM. Se falhar, usa o texto base.
            assunto, corpo, polimento_info = await self._polir_ou_fallback(
                template=template,
                assunto_base=assunto_base,
                corpo_base=corpo_base,
                tom=tom,
                ps2=ps2,
            )

            data = {
                "canal": passo.get("canal") or template.canal,
                "cadencia_nome": passo.get("cadencia_nome"),
                "passo": passo.get("ordem", 0),
                "nudge": nudge,
                "tom": tom,
                "template_id": template.id,
                "template_versao": template.version,
                "assunto": assunto,
                "corpo": corpo,
                "modelo_llm": polimento_info.get("modelo"),
                "prompt_hash": self._hash_prompt(template, variaveis, tom),
                "razao": (
                    f"Nudge {nudge} (tom {tom}) aplicado para perfil "
                    f"{ps2.get('classificacao', '?')}. "
                    f"Polimento LLM: {polimento_info.get('status', 'n/a')}."
                ),
                "variaveis_usadas": list(variaveis.keys()),
                "polimento": polimento_info,
            }

            return AgentResult(
                success=True,
                agent_name=self.agent_name,
                contact_email=email,
                data=data,
                duration_ms=(time.monotonic() - start) * 1000,
            )

        except Exception as e:
            logger.error("Personalização falhou para %s: %s", email, e)
            return AgentResult(
                success=False,
                agent_name=self.agent_name,
                contact_email=email,
                error=str(e),
                duration_ms=(time.monotonic() - start) * 1000,
            )

    # -----------------------------------------------------------------
    # Escolha de nudge / tom
    # -----------------------------------------------------------------

    def _escolher_nudge(self, perfil_squad2: dict) -> str:
        score = perfil_squad2.get("score_total", 0) or 0
        sinais = set(perfil_squad2.get("sinais", []) or [])

        if "abandonou_matricula" in sinais:
            return MATRIZ_NUDGE["abandonou_matricula"][0]
        if score >= 80:
            return MATRIZ_NUDGE["altamente_engajado"][0]
        if 40 <= score <= 60:
            return MATRIZ_NUDGE["indeciso"][0]
        if "lead_inativo" in sinais:
            return MATRIZ_NUDGE["inativo_7d"][0]
        return MATRIZ_NUDGE["default"][0]

    def _inferir_tom(self, ps2: dict, ps1: dict) -> str:
        """
        Inferência simples a partir de sinais. Se Squad 2 já tiver
        gravado um perfil_psicologico, usar isso direto. Senão,
        heurísticas básicas em sinais e dados.
        """
        explicito = (ps2.get("perfil_psicologico") or "").lower()
        if explicito in TONS_SUPORTADOS:
            return explicito

        sinais = set(ps2.get("sinais", []) or [])

        # Cargo analítico (TI, dados, finanças, engenharia) → analítico
        cargo = ((ps1.get("dados_basicos") or {}).get("job_title") or "").lower()
        if any(k in cargo for k in (
            "engenheir", "analista", "auditor", "controller", "data", "dados",
            "financ", "actuari", "actuário", "atuário",
        )):
            return "analitico"

        # Sinais de alta intensidade → impulsivo
        if "alto_engajamento_recente" in sinais and "resposta_rapida" in sinais:
            return "impulsivo"

        # Sinais de prudência (perfil incompleto, baixa interação) → cauteloso
        if "perfil_incompleto" in sinais or "lead_inativo" in sinais:
            return "cauteloso"

        return "cauteloso"  # default conservador

    # -----------------------------------------------------------------
    # Lookup de template
    # -----------------------------------------------------------------

    def _lookup_template(self, nudge: str, passo: dict) -> Optional[Template]:
        # Se o passo trouxe um template_id explícito, prioriza
        tpl_id = passo.get("template_id")
        if tpl_id:
            tpl = self.loader.por_id(tpl_id)
            if tpl is not None:
                return tpl
        # Senão, lookup por (nudge, canal)
        canal = passo.get("canal")
        return self.loader.por_nudge(nudge, canal=canal)

    # -----------------------------------------------------------------
    # Variáveis
    # -----------------------------------------------------------------

    def _montar_variaveis(
        self, email: str, ps1: dict, ps2: dict,
    ) -> dict[str, str]:
        nome_curto = (
            (ps1.get("dados_basicos") or {}).get("first_name")
            or email.split("@")[0].split(".")[0].title()
        )
        curso = (
            (ps1.get("analysis") or {}).get("area_principal")
            or "Pós-Graduação BSSP"
        )
        turma = "agosto/2026"
        valor_mensal = "R$ 490,00"
        cidade = (ps1.get("dados_basicos") or {}).get("city") or "São Paulo"

        return {
            "nome_curto": nome_curto,
            "curso": curso,
            "turma": turma,
            "valor_mensal": valor_mensal,
            "cidade": cidade,
            # 'prazo' fica vazio enquanto Marketing não definir; o template
            # que precisar dele explicitamente vai listar em obrigatórias.
        }

    # -----------------------------------------------------------------
    # Polimento via LLM
    # -----------------------------------------------------------------

    async def _polir_ou_fallback(
        self,
        *,
        template: Template,
        assunto_base: str,
        corpo_base: str,
        tom: str,
        ps2: dict,
    ) -> tuple[str, str, dict]:
        """
        Tenta polir com LLM. Se qualquer etapa falhar (LLM indisponível,
        JSON inválido, sanity check rejeitar), devolve o texto base.
        """
        if not self.usar_llm or not getattr(self.llm, "api_key", ""):
            return assunto_base, corpo_base, {
                "status": "fallback_sem_llm",
                "modelo": None,
            }

        try:
            polished = await self._chamar_llm(
                template=template,
                assunto_base=assunto_base,
                corpo_base=corpo_base,
                tom=tom,
                ps2=ps2,
            )
        except Exception as e:
            logger.warning("Personalização — LLM falhou (%s) — usando fallback", e)
            return assunto_base, corpo_base, {
                "status": "fallback_llm_erro",
                "modelo": getattr(self.llm, "model", None),
                "erro": str(e)[:200],
            }

        # Sanity check
        ok, motivo = self._sanity_check(polished, template)
        if not ok:
            logger.warning(
                "Personalização — sanity check rejeitou polish (%s) — usando fallback",
                motivo,
            )
            return assunto_base, corpo_base, {
                "status": "fallback_sanity_check",
                "modelo": getattr(self.llm, "model", None),
                "motivo_rejeicao": motivo,
            }

        return polished["assunto"], polished["corpo"], {
            "status": "polido",
            "modelo": getattr(self.llm, "model", None),
            "tom": tom,
        }

    async def _chamar_llm(
        self,
        *,
        template: Template,
        assunto_base: str,
        corpo_base: str,
        tom: str,
        ps2: dict,
    ) -> dict:
        """
        Chama o LLM com prompt rígido. Resposta deve ser JSON com
        chaves 'assunto' e 'corpo'. As variáveis declaradas no
        template são listadas no prompt como 'únicos placeholders
        permitidos' — o sanity check faz a verificação dura depois.
        """
        sistema = (
            "Você é um copywriter da BSSP que adapta mensagens de marketing "
            "para captação de alunos de pós-graduação. Sua função é refinar "
            "tom e fluência sem inventar fatos.\n\n"
            "REGRAS RÍGIDAS:\n"
            "1. Mantenha exatamente as mesmas informações factuais do texto base.\n"
            "2. NÃO invente nomes de cursos, turmas, valores, prazos ou estatísticas.\n"
            "3. NÃO adicione placeholders como {algo} além dos já presentes.\n"
            "4. Mantenha tamanho similar (±20% do original).\n"
            "5. Responda APENAS com JSON válido: "
            "{\"assunto\": \"...\", \"corpo\": \"...\"}.\n"
        )

        guidance_tom = {
            "analitico": (
                "Tom analítico: dados concretos, comparações objetivas, frases diretas. "
                "Evite linguagem emocional. Cite a empregabilidade ou os 12 meses pós-conclusão."
            ),
            "impulsivo": (
                "Tom impulsivo: frases curtas, CTA único e claro, senso de urgência. "
                "Evite parágrafos longos. Linhas de 1-2 frases. Termine com pergunta direta."
            ),
            "cauteloso": (
                "Tom cauteloso: ressalte garantias institucionais (autorização MEC, "
                "tradição da BSSP, ex-alunos consagrados), e respeite o ritmo do leitor. "
                "Evite urgência."
            ),
        }.get(tom, "")

        usuario = (
            f"NUDGE: {template.nudge}\n"
            f"TOM ALVO: {tom} — {guidance_tom}\n"
            f"CLASSIFICAÇÃO DO LEAD: {ps2.get('classificacao', '?')}\n\n"
            f"=== TEXTO BASE (já com nome/curso substituídos) ===\n"
            f"ASSUNTO: {assunto_base}\n\n"
            f"CORPO:\n{corpo_base}\n\n"
            f"=== FIM ===\n\n"
            f"Reescreva no tom alvo, mantendo todos os fatos. NÃO insira "
            f"chaves como {{algo}} no texto. Responda apenas com o JSON."
        )

        # Usa complete_json se disponível (Sonnet/OpenAI suportam JSON forçado)
        if hasattr(self.llm, "complete_json"):
            return await self.llm.complete_json(
                [LLMMessage(role="user", content=usuario)],
                system=sistema,
                temperature=0.3,
            )

        # Fallback: complete normal e parse manual
        resp = await self.llm.complete(
            [LLMMessage(role="user", content=usuario)],
            system=sistema,
            temperature=0.3,
            response_format="json",
        )
        text = resp.content.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(lines[1:-1]) if len(lines) > 2 else text
        return json.loads(text)

    def _sanity_check(self, polished: dict, template: Template) -> tuple[bool, str]:
        """
        Como o polish recebe TEXTO JÁ RENDERIZADO (sem placeholders), a
        regra anti-alucinação é simples: o output não pode conter NENHUM
        token de placeholder do tipo {nome}. Se o LLM escreveu {algo},
        ou ele inventou um placeholder novo (proibido) ou re-introduziu
        um placeholder do template original (também proibido — quebra a
        renderização). Em ambos os casos, descarta e usa o texto base.
        """
        if not isinstance(polished, dict):
            return False, "resposta_nao_e_dict"
        assunto = polished.get("assunto")
        corpo = polished.get("corpo")
        if not isinstance(assunto, str) or not isinstance(corpo, str):
            return False, "assunto_ou_corpo_nao_string"
        if not assunto.strip() or not corpo.strip():
            return False, "assunto_ou_corpo_vazio"

        # Anti-alucinação dura: nenhum placeholder pode aparecer no output.
        # Texto base já chegou renderizado; a presença de qualquer {x} é
        # sinal de que o LLM inventou ou reintroduziu placeholder.
        usadas = set(PLACEHOLDER_RE.findall(assunto)) | set(PLACEHOLDER_RE.findall(corpo))
        if usadas:
            return False, f"placeholders_inventados:{sorted(usadas)}"

        # Tamanho do corpo (cinto-segurança contra LLM verboso ou cortante)
        if len(corpo) < max(40, int(0.4 * len(template.corpo))):
            return False, "corpo_muito_curto"
        if len(corpo) > 3 * len(template.corpo) + 200:
            return False, "corpo_muito_longo"

        return True, "ok"

    def _hash_prompt(
        self, template: Template, variaveis: dict, tom: str,
    ) -> str:
        payload = json.dumps(
            {
                "tpl": template.id,
                "ver": template.version,
                "tom": tom,
                "var": sorted(variaveis.items()),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]
