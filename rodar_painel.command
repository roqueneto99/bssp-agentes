#!/bin/bash
# ============================================================
# BSSP — Squad Leads — iniciar painel
# Duplo-clique neste arquivo no Finder para rodar.
# ============================================================

set -e
cd "$(dirname "$0")"

echo ""
echo "============================================"
echo "  BSSP — Squad Leads"
echo "============================================"
echo ""

# 1) Python 3
if ! command -v python3 >/dev/null 2>&1; then
    echo "ERRO: python3 nao encontrado. Instale via: brew install python"
    read -p "Pressione Enter para fechar..."
    exit 1
fi

# 2) Ambiente virtual (cria se nao existir)
if [ ! -d ".venv" ]; then
    echo ">> Criando ambiente virtual (.venv)..."
    python3 -m venv .venv
fi

# 3) Ativar venv
source .venv/bin/activate

# 4) Instalar dependencias (rapido se ja instaladas)
echo ">> Checando dependencias..."
pip install --quiet --upgrade pip
pip install --quiet fastapi uvicorn httpx pydantic python-dotenv

# 5) Subir o painel (ele abre o navegador sozinho em http://localhost:8501)
echo ""
echo ">> Painel: http://localhost:8501"
echo ">> Para parar: Ctrl+C  ou  feche esta janela"
echo ""
python3 painel.py
