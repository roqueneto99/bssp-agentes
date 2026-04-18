#!/bin/bash
# ============================================================
# BSSP — Squad Leads — git add + commit + push (deploy Railway)
# Duplo-clique neste arquivo no Finder para enviar alteracoes.
# ============================================================

cd "$(dirname "$0")"

echo ""
echo "============================================"
echo "  BSSP — Push para GitHub / Railway"
echo "============================================"
echo ""
echo ">> Pasta: $(pwd)"
echo ""

# 0) Limpar lock antigo (caso exista por aborto anterior)
if [ -f ".git/index.lock" ]; then
    echo ">> Removendo .git/index.lock antigo..."
    rm -f .git/index.lock
fi

# 1) Garantir identidade do git (fallback se nao estiver configurado)
if [ -z "$(git config user.email)" ]; then
    echo ">> Definindo identidade local do git..."
    git config user.email "roquecneto@gmail.com"
    git config user.name "Roque Neto"
fi

# 2) Mostrar arquivos alterados
echo ">> Alteracoes (staged + unstaged):"
git status --short
echo ""

# 3) Montar mensagem de commit (usa data/hora)
STAMP=$(date "+%Y-%m-%d %H:%M")
DEFAULT_MSG="painel: ajustes ($STAMP)"

# Se ha um argumento, usa como mensagem; senao, usa default
MSG="${1:-$DEFAULT_MSG}"

# 4) Staging completo
DIRTY=$(git status --porcelain | wc -l | tr -d ' ')
if [ "$DIRTY" != "0" ]; then
    echo ">> git add -A"
    git add -A

    echo ">> git commit -m \"$MSG\""
    git commit -m "$MSG"
    COMMIT_RESULT=$?
    echo ""
    if [ $COMMIT_RESULT -ne 0 ]; then
        echo ">> FALHA no commit. Verifique o erro acima."
        echo ""
        read -p "Pressione Enter para fechar..."
        exit 1
    fi
else
    echo ">> Nada para commitar."
    echo ""
fi

# 5) Mostrar commits pendentes
echo ">> Commits ainda nao enviados:"
git log --oneline origin/main..HEAD 2>/dev/null || echo "   (nao foi possivel comparar com origin)"
echo ""

# 6) Push
echo ">> git push origin main"
if git push origin main; then
    echo ""
    echo ">> OK! Railway vai rebuildar automaticamente (~1-2 min)."
    echo ">> Acompanhe: https://railway.app"
else
    echo ""
    echo ">> FALHA no push. Verifique autenticacao GitHub."
    echo "   Se for primeira vez, pode precisar configurar token/SSH."
fi

echo ""
read -p "Pressione Enter para fechar..."
