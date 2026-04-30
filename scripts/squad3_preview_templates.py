#!/usr/bin/env python3
"""
Preview de templates do Squad 3 — gera amostras renderizadas para
revisão pela Diretoria de Marketing antes da aprovação formal.

Uso:
    cd bssp-agentes
    ./.venv/bin/python scripts/squad3_preview_templates.py \\
        --output Squad3_Templates_Amostras.md

    # ou com nome/curso customizados:
    ./.venv/bin/python scripts/squad3_preview_templates.py \\
        --nome "Maria" --curso "MBA em Marketing" --turma "outubro/2026"

    # Para incluir templates rascunho (não aprovados):
    ./.venv/bin/python scripts/squad3_preview_templates.py --rascunho-ok

O script NÃO chama o LLM (não consome créditos). Renderiza apenas o
texto base — o polimento de tom acontece em runtime no agente, com
o LLM real, e gera variações por lead. Isso aqui mostra o "esqueleto"
factual que será adaptado.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.agents.squad3.template_loader import (  # noqa: E402
    TemplateLoader,
    renderizar,
)

DEFAULT_TEMPLATES_DIR = Path(ROOT, "src", "agents", "squad3", "templates")


def make_args():
    parser = argparse.ArgumentParser(description="Gera preview dos templates do Squad 3")
    parser.add_argument("--nome", default="Roque", help="Nome do lead fictício (default: Roque)")
    parser.add_argument("--curso", default="MBA Executivo em Gestão", help="Curso de interesse")
    parser.add_argument("--turma", default="agosto/2026", help="Turma alvo")
    parser.add_argument("--valor-mensal", default="R$ 490,00", help="Valor mensal do investimento")
    parser.add_argument("--cidade", default="São Paulo", help="Cidade do lead")
    parser.add_argument("--output", default="-", help="Arquivo .md de saída (default: stdout)")
    parser.add_argument("--rascunho-ok", action="store_true", help="Inclui templates rascunho")
    return parser.parse_args()


def main():
    args = make_args()
    modo = "rascunho_ok" if args.rascunho_ok else "apenas_aprovados"
    loader = TemplateLoader(DEFAULT_TEMPLATES_DIR, modo=modo)
    templates = loader.carregar()

    if not templates:
        print("Nenhum template encontrado.", file=sys.stderr)
        return 1

    variaveis = {
        "nome_curto": args.nome,
        "curso": args.curso,
        "turma": args.turma,
        "valor_mensal": args.valor_mensal,
        "cidade": args.cidade,
    }

    out_lines: list[str] = []
    out_lines.append("# BSSP — Amostras de Templates do Squad 3")
    out_lines.append("")
    out_lines.append(f"_Gerado em {datetime.now().strftime('%d/%m/%Y %H:%M')}_  ")
    out_lines.append(f"_Lead fictício_: **{args.nome}** · _Curso_: **{args.curso}** · _Turma_: **{args.turma}**")
    out_lines.append("")
    out_lines.append(
        "Este documento mostra cada template renderizado com variáveis fictícias. "
        "Em produção, o agente de Personalização passa este texto base por um LLM "
        "(Claude Sonnet) que adapta o tom (analítico / impulsivo / cauteloso) "
        "antes do envio. As variações por tom não estão aqui — esta é a versão "
        "neutra, factual."
    )
    out_lines.append("")
    out_lines.append("---")
    out_lines.append("")

    # Ordem visual: por nudge alfabético
    for tpl in sorted(templates.values(), key=lambda t: t.nudge):
        assunto, corpo, faltas = renderizar(tpl, variaveis)
        out_lines.append(f"## Nudge: `{tpl.nudge}`")
        out_lines.append("")
        out_lines.append(f"- **id:** `{tpl.id}` · versão **{tpl.version}**")
        out_lines.append(f"- **canal:** {tpl.canal}")
        out_lines.append(f"- **aprovado por:** {tpl.aprovado_por or '_rascunho_'} ({tpl.aprovado_em or '—'})")
        if tpl.observacoes:
            out_lines.append(f"- **observações:** {tpl.observacoes}")
        if faltas:
            out_lines.append(f"- ⚠️ **variáveis faltando:** {sorted(faltas)}")
        out_lines.append("")
        out_lines.append(f"**Assunto:** {assunto}")
        out_lines.append("")
        out_lines.append("**Corpo:**")
        out_lines.append("")
        out_lines.append("```")
        out_lines.append(corpo)
        out_lines.append("```")
        out_lines.append("")
        out_lines.append("---")
        out_lines.append("")

    out_lines.append("## Como aprovar")
    out_lines.append("")
    out_lines.append(
        "1. Marketing revisa cada template acima.  \n"
        "2. Mudanças solicitadas → engenharia cria nova versão (ex.: `tpl_v2_prova_social.json`).  \n"
        "3. Aprovação final → engenharia atualiza `aprovado_por` e `aprovado_em` no JSON correspondente.  \n"
        "4. Apenas templates com `aprovado_por` preenchido entram em produção.  \n"
        "5. Em runtime, o LLM polirá cada um destes textos para o tom apropriado por lead."
    )

    output = "\n".join(out_lines) + "\n"

    if args.output == "-":
        print(output)
    else:
        Path(args.output).write_text(output, encoding="utf-8")
        print(f"Escrito em {args.output}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
