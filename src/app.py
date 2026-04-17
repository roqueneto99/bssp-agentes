"""
Aplicação FastAPI principal — BSSP Agentes.

Ponto de entrada do sistema de agentes de automação comercial.
Em produção, roda via: uvicorn src.app:app --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from src.integrations.rdstation import RDStationClient
from src.webhooks.rdstation_receiver import WebhookConfig, router as webhook_router
from src.api_routes import router as api_router, init_agent

logger = logging.getLogger(__name__)

# Carrega .env manualmente (sem depender de python-dotenv)
_env_file = Path(__file__).parent.parent / ".env"
if _env_file.exists():
    for line in _env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip())

# ---------------------------------------------------------------------------
# Client compartilhado
# ---------------------------------------------------------------------------

rdstation_client: RDStationClient | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup e shutdown da aplicação."""
    global rdstation_client

    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    logger.info("Iniciando BSSP Agentes...")

    WebhookConfig.load_from_env()

    api_key = os.getenv("RDSTATION_API_KEY", "")
    plan = os.getenv("RDSTATION_PLAN", "pro")

    if api_key:
        rdstation_client = RDStationClient(api_key=api_key, plan=plan)
        logger.info("RD Station via API Key (plano: %s)", plan)
    else:
        rdstation_client = RDStationClient(
            client_id=os.getenv("RDSTATION_CLIENT_ID", ""),
            client_secret=os.getenv("RDSTATION_CLIENT_SECRET", ""),
            refresh_token=os.getenv("RDSTATION_REFRESH_TOKEN", ""),
            plan=plan,
        )
        logger.info("RD Station via OAuth (plano: %s)", plan)

    # Inicializa agente do Squad 1
    init_agent(rdstation_client)
    logger.info("Squad 1 (Enriquecimento) inicializado")

    yield

    if rdstation_client:
        await rdstation_client.close()
    logger.info("BSSP Agentes encerrado.")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="BSSP Agentes - Automação Comercial",
    description="Sistema de agentes de IA para automação do funil de vendas da BSSP.",
    version="0.1.0",
    lifespan=lifespan,
)

# CORS (para o dashboard local)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Rotas
app.include_router(webhook_router)
app.include_router(api_router)

# Arquivos estáticos (dashboard)
static_dir = Path(__file__).parent.parent / "dashboard"
if static_dir.exists():
    app.mount("/dashboard", StaticFiles(directory=str(static_dir), html=True), name="dashboard")


@app.get("/")
async def root():
    return {
        "service": "bssp-agentes",
        "version": "0.1.0",
        "status": "running",
        "dashboard": "/dashboard",
    }


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "rdstation_connected": rdstation_client is not None,
    }
