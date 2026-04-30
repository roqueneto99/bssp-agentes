"""
Template loader para o Squad 3 — Personalização Comportamental.

Carrega templates JSON versionados de src/agents/squad3/templates/,
valida (variáveis declaradas vs. usadas) e expõe lookup por nudge,
template_id ou (rota, passo).

Estrutura de um template (arquivo .json):

    {
        "id": "mql_v1_step1_prova_social",
        "version": "v1",
        "nudge": "prova_social",
        "canal": "email",
        "rota": "mql_nurture",          # opcional — ajuda no lookup por cadência
        "passo": 1,                      # opcional — idem
        "assunto": "{nome_curto}, veja o que ...",
        "corpo": "Olá {nome_curto}, ...",
        "variaveis_obrigatorias": ["nome_curto"],
        "variaveis_opcionais": ["curso", "turma"],
        "tons_suportados": ["analitico", "impulsivo", "cauteloso"],
        "aprovado_por": "marketing@bssp.com.br",
        "aprovado_em": "2026-04-29T15:00:00-03:00",
        "observacoes": "Aprovado na revisão de 29/04 — Diretoria de Marketing"
    }

Regras:
    - Templates rascunho (aprovado_por null) NÃO são carregados em produção
      por padrão. Use TemplateLoader(modo="rascunho_ok") para incluir.
    - Variáveis usadas no corpo/assunto precisam estar em
      variaveis_obrigatorias ∪ variaveis_opcionais ∪ VARIAVEIS_GLOBAIS.
    - Versionamento: cada arquivo é uma versão imutável. Novas versões
      criam novo arquivo (ex.: mql_v2_step1_prova_social.json) — assim
      mensagens já enviadas continuam reproduzíveis pelo prompt_hash.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

logger = logging.getLogger(__name__)


# Variáveis que qualquer template pode usar — precisam ser preenchidas
# pela Personalização a partir do perfil dos squads anteriores.
VARIAVEIS_GLOBAIS = {
    "nome_curto", "curso", "turma", "valor_mensal", "prazo", "cidade",
}

# Tons suportados — adaptação por perfil psicológico (do diagnóstico §5.3).
TONS_SUPORTADOS = ("analitico", "impulsivo", "cauteloso")

VAR_RE = re.compile(r"\{(\w+)\}")


@dataclass
class Template:
    """Template carregado e validado."""

    id: str
    version: str
    nudge: str
    canal: str
    assunto: str
    corpo: str
    variaveis_obrigatorias: tuple[str, ...]
    variaveis_opcionais: tuple[str, ...]
    tons_suportados: tuple[str, ...]
    aprovado_por: Optional[str]
    aprovado_em: Optional[str]
    observacoes: Optional[str]
    rota: Optional[str] = None
    passo: Optional[int] = None
    arquivo: Optional[str] = None

    @property
    def aprovado(self) -> bool:
        return bool(self.aprovado_por)

    @property
    def variaveis_permitidas(self) -> set[str]:
        return (
            set(self.variaveis_obrigatorias)
            | set(self.variaveis_opcionais)
            | VARIAVEIS_GLOBAIS
        )

    @property
    def variaveis_usadas(self) -> set[str]:
        return set(VAR_RE.findall(self.assunto)) | set(VAR_RE.findall(self.corpo))


class TemplateValidationError(Exception):
    """Erro de validação ao carregar um template."""


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

class TemplateLoader:
    """
    Carrega e valida templates JSON do diretório indicado.

    Uso típico (em prod):
        loader = TemplateLoader(Path(__file__).parent / "templates")
        tpl = loader.por_nudge("prova_social", canal="email")

    Uso em testes:
        loader = TemplateLoader(tmp_path, modo="rascunho_ok")
    """

    def __init__(
        self,
        diretorio: Path,
        *,
        modo: str = "apenas_aprovados",
    ) -> None:
        if modo not in ("apenas_aprovados", "rascunho_ok"):
            raise ValueError(f"modo inválido: {modo}")
        self.diretorio = Path(diretorio)
        self.modo = modo
        self._templates: dict[str, Template] = {}
        self._carregados = False

    def carregar(self) -> dict[str, Template]:
        """Lê todos os JSON do diretório e valida."""
        self._templates.clear()
        if not self.diretorio.exists():
            logger.warning("Pasta de templates não existe: %s", self.diretorio)
            self._carregados = True
            return {}

        arquivos = sorted(self.diretorio.glob("*.json"))
        carregados = 0
        ignorados_rascunho = 0
        falhas = 0

        for arquivo in arquivos:
            try:
                tpl = self._carregar_arquivo(arquivo)
            except TemplateValidationError as e:
                logger.error("Template inválido em %s: %s", arquivo.name, e)
                falhas += 1
                continue

            if not tpl.aprovado and self.modo == "apenas_aprovados":
                logger.info("Template ignorado (rascunho): %s", arquivo.name)
                ignorados_rascunho += 1
                continue

            if tpl.id in self._templates:
                raise TemplateValidationError(
                    f"id duplicado: {tpl.id} (arquivos: "
                    f"{self._templates[tpl.id].arquivo}, {arquivo.name})"
                )
            self._templates[tpl.id] = tpl
            carregados += 1

        self._carregados = True
        logger.info(
            "TemplateLoader: %d carregados, %d rascunhos ignorados, %d falhas (de %d arquivos)",
            carregados, ignorados_rascunho, falhas, len(arquivos),
        )
        return dict(self._templates)

    def _ensure_loaded(self) -> None:
        if not self._carregados:
            self.carregar()

    def _carregar_arquivo(self, arquivo: Path) -> Template:
        try:
            data = json.loads(arquivo.read_text(encoding="utf-8"))
        except json.JSONDecodeError as e:
            raise TemplateValidationError(f"JSON inválido: {e}") from e

        for campo in ("id", "version", "nudge", "canal", "assunto", "corpo"):
            if not data.get(campo):
                raise TemplateValidationError(f"campo obrigatório ausente: {campo}")

        tpl = Template(
            id=data["id"],
            version=data["version"],
            nudge=data["nudge"],
            canal=data["canal"],
            assunto=data["assunto"],
            corpo=data["corpo"],
            variaveis_obrigatorias=tuple(data.get("variaveis_obrigatorias", [])),
            variaveis_opcionais=tuple(data.get("variaveis_opcionais", [])),
            tons_suportados=tuple(data.get("tons_suportados", TONS_SUPORTADOS)),
            aprovado_por=data.get("aprovado_por"),
            aprovado_em=data.get("aprovado_em"),
            observacoes=data.get("observacoes"),
            rota=data.get("rota"),
            passo=data.get("passo"),
            arquivo=arquivo.name,
        )

        # Validações estruturais
        usadas = tpl.variaveis_usadas
        permitidas = tpl.variaveis_permitidas
        nao_declaradas = usadas - permitidas
        if nao_declaradas:
            raise TemplateValidationError(
                f"variáveis usadas mas não declaradas: {sorted(nao_declaradas)}"
            )

        nao_usadas = set(tpl.variaveis_obrigatorias) - usadas
        if nao_usadas:
            raise TemplateValidationError(
                f"variáveis declaradas como obrigatórias mas não usadas: "
                f"{sorted(nao_usadas)}"
            )

        for tom in tpl.tons_suportados:
            if tom not in TONS_SUPORTADOS:
                raise TemplateValidationError(f"tom inválido: {tom}")

        return tpl

    # ------------------------------------------------------------------
    # Lookup
    # ------------------------------------------------------------------

    def todos(self) -> list[Template]:
        self._ensure_loaded()
        return list(self._templates.values())

    def por_id(self, template_id: str) -> Optional[Template]:
        self._ensure_loaded()
        return self._templates.get(template_id)

    def por_nudge(
        self,
        nudge: str,
        *,
        canal: Optional[str] = None,
        rota: Optional[str] = None,
    ) -> Optional[Template]:
        """
        Retorna o primeiro template que case com (nudge, canal, rota).
        Se houver múltiplos casamentos, retorna o de maior versão.
        """
        self._ensure_loaded()
        candidatos = [t for t in self._templates.values() if t.nudge == nudge]
        if canal:
            candidatos = [t for t in candidatos if t.canal == canal]
        if rota:
            candidatos = [t for t in candidatos if t.rota == rota or t.rota is None]
        if not candidatos:
            return None
        # Ordenar por versão decrescente (lexicográfica é OK para v1, v2, ...)
        candidatos.sort(key=lambda t: t.version, reverse=True)
        return candidatos[0]

    def por_passo(
        self,
        rota: str,
        passo: int,
        *,
        canal: Optional[str] = None,
    ) -> Optional[Template]:
        """Lookup pelo (rota, passo) — útil quando o engajamento progressivo já decidiu."""
        self._ensure_loaded()
        candidatos = [
            t for t in self._templates.values()
            if t.rota == rota and t.passo == passo
        ]
        if canal:
            candidatos = [t for t in candidatos if t.canal == canal]
        if not candidatos:
            return None
        candidatos.sort(key=lambda t: t.version, reverse=True)
        return candidatos[0]


# ---------------------------------------------------------------------------
# Renderização
# ---------------------------------------------------------------------------

def renderizar(
    template: Template,
    variaveis: dict[str, str],
) -> tuple[str, str, set[str]]:
    """
    Substitui {var} no assunto e no corpo. Retorna (assunto, corpo, faltas).
    'faltas' contém variáveis usadas mas ausentes em `variaveis`.

    Apenas substitui variáveis declaradas no template (obrigatórias ou
    opcionais) ou globais. Variáveis fora desse conjunto são uma falha
    de validação — _carregar_arquivo já garantiu que o template não tem
    nenhuma, mas a função volta a checar para o caso de manipulação em
    runtime.
    """
    permitidas = template.variaveis_permitidas
    faltas: set[str] = set()
    obrigatorias = set(template.variaveis_obrigatorias)

    def render(texto: str) -> str:
        def repl(match: re.Match) -> str:
            key = match.group(1)
            if key not in permitidas:
                faltas.add(key + "(nao_permitida)")
                return match.group(0)
            if key not in variaveis:
                if key in obrigatorias:
                    faltas.add(key)
                # Para opcionais ausentes, deixa o placeholder — o LLM
                # vai polir e ele se encarrega de remover frases penduradas.
                # Mas como hoje renderizamos com placeholder, faltamos:
                faltas.add(key)
                return match.group(0)
            return str(variaveis[key])

        return VAR_RE.sub(repl, texto)

    return render(template.assunto), render(template.corpo), faltas
