# Templates do Squad 3 — BSSP

Cada arquivo `.json` é um template versionado, carregado pelo
`TemplateLoader` (`src/agents/squad3/template_loader.py`).

## Workflow de aprovação

1. Engenharia escreve o template como rascunho (sem `aprovado_por`).
2. Marketing revisa rendering em `scripts/squad3_preview_templates.py`
   ou no painel.
3. Quando aprovado, preenche `aprovado_por` (e-mail), `aprovado_em`
   (ISO-8601 com timezone) e opcionalmente `observacoes`.
4. Em produção, apenas templates com `aprovado_por` preenchido entram
   no `Loader` (modo `apenas_aprovados`).

## Versionamento

- Cada arquivo é uma **versão imutável**. Para mudar o texto, crie
  novo arquivo `mql_v2_step1_prova_social.json` em vez de editar o v1.
- O `prompt_hash` gravado em `mensagens_squad3` referencia o
  template+variáveis exatos usados — manter o arquivo permite
  reproduzir a mensagem que foi enviada.

## Variáveis

Variáveis declaradas em `variaveis_obrigatorias` e `variaveis_opcionais`
+ as 6 globais (`nome_curto`, `curso`, `turma`, `valor_mensal`,
`prazo`, `cidade`) podem ser usadas no `assunto` e no `corpo`. O
`TemplateLoader` rejeita o template em load se houver variável usada
mas não declarada (mitigação contra LLM inventando nomes em runtime).

## Tons psicológicos

`tons_suportados` é uma lista entre `analitico`, `impulsivo`, `cauteloso`.
O LLM (Claude Sonnet) faz o polimento final adaptando o texto base ao tom
inferido a partir do perfil do lead — mantendo as variáveis declaradas
e os fatos do template original.
