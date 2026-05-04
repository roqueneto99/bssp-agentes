#!/usr/bin/env python3
"""
Painel de Controle — BSSP Squad Leads.

Dois modos de operação:
  1. DATABASE (padrão): consulta PostgreSQL local — instantâneo, sem rate limits.
     Requer DATABASE_URL configurado e carga inicial feita (sync_job.py --full).
  2. API (fallback): consulta diretamente a API do RD Station.
     Ativado automaticamente se DATABASE_URL não estiver configurado.

Uso:
    python3 painel.py
    Abre em http://localhost:8501
"""

import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent))
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

from fastapi import BackgroundTasks, FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from src.integrations.rdstation.client import RDStationClient
from src.agents.base import LLMProvider
from src.agents.squad1.orchestrator import Squad1Orchestrator
from src.agents.pipeline import AgentPipeline
from src.integrations.hablla.client import HabllaClient
from src.integrations.sendgrid import SendGridClient, SendGridConfig
from src.database.queries_squad3 import MensagensRepository, NullMensagensRepo
from src.webhooks import sendgrid_receiver as sendgrid_webhook
from src.orquestrador import run as run_orquestrador
from src.api.routers import leads_pipeline

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
logger = logging.getLogger("painel")

# --- Globals ---
app = FastAPI(title="BSSP Painel Squad Leads")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

rdstation: Optional[RDStationClient] = None
llm: Optional[LLMProvider] = None
orchestrator: Optional[Squad1Orchestrator] = None
hablla: Optional[HabllaClient] = None
pipeline: Optional[AgentPipeline] = None
sendgrid: Optional[SendGridClient] = None
mensagens_repo: Optional[object] = None

# Modo de dados: "database" ou "api"
DATA_MODE: str = "api"

execucoes: list = []
running_leads: set = set()

_orquestrador_status: dict = {
    "running": False,
    "last_run_id": None,
    "last_started_at": None,
    "last_finished_at": None,
    "last_summary": None,
}

async def _run_orquestrador_bg(run_id: str, max_leads: int) -> None:
    from datetime import datetime as _dt, timezone as _tz
    _orquestrador_status["running"] = True
    _orquestrador_status["last_run_id"] = run_id
    _orquestrador_status["last_started_at"] = _dt.now(_tz.utc).isoformat()
    _orquestrador_status["last_summary"] = None
    try:
        summary = await run_orquestrador(
            pipeline_obj=pipeline,
            running_set=running_leads,
            data_mode=DATA_MODE,
            max_leads=max_leads,
            motivo=f"manual_{run_id[:8]}",
        )
        _orquestrador_status["last_summary"] = summary
    except Exception as e:
        logger.error("orquestrador bg run %s falhou: %s", run_id, e)
        _orquestrador_status["last_summary"] = {"error": str(e)[:300]}
    finally:
        _orquestrador_status["running"] = False
        _orquestrador_status["last_finished_at"] = _dt.now(_tz.utc).isoformat()




# --- Cache de segmentacoes (para filtro por data — modo API) ---
# Chave: "segid" → {"contacts": [...], "ts": float, "pages": int}
_seg_cache: dict[str, dict] = {}
_cache_building: dict[str, bool] = {}  # flag para evitar builds simultâneos
CACHE_TTL = 600  # 10 minutos

# Cache da LISTA de segmentacoes (nome+id). Usado tanto por /api/segmentacoes
# quanto pelo /api/leads (para resolver o nome quando filtra por seg_id).
_segs_list_cache: dict = {"data": None, "ts": 0.0}
_SEGS_LIST_TTL = 600  # 10 minutos

# --- Auto-sync scheduler ---
# Garante que o Postgres esteja sempre <= SYNC_INTERVAL_HOURS atrasado em relacao
# ao RD Station, sem depender de cron externo. Painel.py roda 24/7 no Railway,
# entao um loop interno eh suficiente.
SYNC_AUTO_ENABLED = os.getenv("SYNC_AUTO_ENABLED", "true").lower() in ("1", "true", "yes")
SYNC_INTERVAL_HOURS = int(os.getenv("SYNC_INTERVAL_HOURS", "4"))
# Janela maior que o intervalo da uma margem de seguranca (caso uma execucao
# atrase ou falhe, a proxima ainda cobre o gap).
SYNC_HOURS_WINDOW = int(os.getenv("SYNC_HOURS_WINDOW", "6"))
# Full_sync de reconciliacao: por padrao, domingo 03:00 BRT (= 06:00 UTC).
SYNC_FULL_WEEKDAY_UTC = int(os.getenv("SYNC_FULL_WEEKDAY_UTC", "6"))  # 0=seg..6=dom
SYNC_FULL_HOUR_UTC = int(os.getenv("SYNC_FULL_HOUR_UTC", "6"))

_sync_lock = asyncio.Lock()
_sync_task: Optional[asyncio.Task] = None
_sync_status: dict = {
    "auto_enabled": SYNC_AUTO_ENABLED,
    "interval_hours": SYNC_INTERVAL_HOURS,
    "hours_window": SYNC_HOURS_WINDOW,
    "full_weekday_utc": SYNC_FULL_WEEKDAY_UTC,
    "full_hour_utc": SYNC_FULL_HOUR_UTC,
    "running": False,
    "last_run_at": None,
    "last_run_mode": None,
    "last_run_status": None,
    "last_run_contacts": None,
    "last_run_error": None,
    "next_run_at": None,
    "total_runs": 0,
    "total_errors": 0,
}


async def _get_segs_list(force: bool = False) -> list[dict]:
    """Retorna a lista de segmentacoes, cacheada em memoria."""
    now = time.time()
    cached = _segs_list_cache.get("data")
    if cached is not None and not force and (now - _segs_list_cache.get("ts", 0)) < _SEGS_LIST_TTL:
        return cached
    if rdstation is None:
        return []
    segs = await rdstation.list_segmentations()
    _segs_list_cache["data"] = segs or []
    _segs_list_cache["ts"] = now
    return _segs_list_cache["data"]


async def _provisionar_custom_fields(rd: RDStationClient) -> None:
    """Cria custom fields necessários para o Squad 2 (ignora se já existem)."""
    campos = [
        ("cf_rota_squad2", "STRING", "Rota Squad 2"),
        ("cf_score_squad2", "STRING", "Score Squad 2"),
        ("cf_classificacao_squad2", "STRING", "Classificação Squad 2"),
        ("cf_data_scoring", "STRING", "Data Scoring Squad 2"),
    ]
    for name, ftype, label in campos:
        try:
            await rd.create_custom_field(name, ftype, label=label)
            logger.info("Custom field criado: %s", name)
        except Exception as e:
            err = str(e)
            if "already" in err.lower() or "409" in err or "existe" in err.lower():
                logger.info("Custom field já existe: %s", name)
            else:
                logger.warning("Erro ao criar custom field %s: %s", name, err)


# Mount webhook routers
app.include_router(sendgrid_webhook.router)
app.include_router(leads_pipeline.router)


@app.on_event("startup")
async def startup():
    global rdstation, llm, orchestrator, hablla, pipeline, DATA_MODE, sendgrid, mensagens_repo

    # --- RD Station (tolerante: se faltar credencial, segue sem o client) ---
    rd_client_id = os.getenv("RDSTATION_CLIENT_ID", "")
    rd_client_secret = os.getenv("RDSTATION_CLIENT_SECRET", "")
    rd_refresh_token = os.getenv("RDSTATION_REFRESH_TOKEN", "")

    if rd_client_id and rd_client_secret:
        try:
            rdstation = RDStationClient(
                client_id=rd_client_id,
                client_secret=rd_client_secret,
                refresh_token=rd_refresh_token,
                plan=os.getenv("RDSTATION_PLAN", "pro"),
            )
            logger.info("RDStationClient inicializado (modo OAuth)")
        except Exception as exc:  # noqa: BLE001
            logger.error("Falha ao inicializar RDStationClient: %s", exc)
            rdstation = None
    else:
        logger.warning(
            "Credenciais RD Station ausentes "
            "(RDSTATION_CLIENT_ID / RDSTATION_CLIENT_SECRET) — "
            "painel subirá em modo degradado, sem sync novo."
        )
        rdstation = None

    # --- LLM ---
    llm_key = os.getenv("LLM_API_KEY", "")
    llm = LLMProvider(
        provider="anthropic",
        api_key=llm_key or "dummy",
        model="claude-sonnet-4-20250514",
        temperature=0.2,
    )
    if not llm_key:
        logger.warning("LLM_API_KEY ausente — agentes rodarão com provider dummy")

    # --- Orquestradores (só se rdstation existir) ---
    if rdstation is not None:
        orchestrator = Squad1Orchestrator(llm=llm, rdstation=rdstation)
    else:
        orchestrator = None

    # Hablla (opcional — só inicializa se token configurado)
    hablla_token = os.getenv("HABLLA_API_TOKEN", "")
    if hablla_token:
        hablla = HabllaClient(
            api_token=hablla_token,
            workspace_id=os.getenv("HABLLA_WORKSPACE_ID", ""),
        )
        logger.info("HabllaClient inicializado (workspace: %s)", os.getenv("HABLLA_WORKSPACE_ID", "N/A"))
    else:
        logger.warning("HABLLA_API_TOKEN não configurado — Squad 2 rodará sem dados Hablla")

    # SendGrid (opcional — só inicializa se a API key estiver presente)
    sg_config = SendGridConfig.from_env()
    if sg_config.is_configured:
        sendgrid = SendGridClient(sg_config)
        logger.info(
            "SendGridClient inicializado (sandbox=%s, from=%s)",
            sg_config.sandbox_mode, sg_config.from_email,
        )
    else:
        sendgrid = None
        logger.warning("SENDGRID_API_KEY ausente — Squad 3 rodará em dry-run para e-mail")

    # Repositório de mensagens (só se DATABASE_URL existe)
    db_url = os.getenv("DATABASE_URL", "")
    if db_url:
        try:
            from src.database.connection import get_session_factory
            mensagens_repo = MensagensRepository(get_session_factory())
            logger.info("MensagensRepository conectado ao Postgres")
        except Exception as e:
            mensagens_repo = NullMensagensRepo()
            logger.warning("MensagensRepository falhou (%s) — usando NullRepo", e)
    else:
        mensagens_repo = NullMensagensRepo()

    # Pipeline completo (Squad 1 → Squad 2 → Squad 3)
    if rdstation is not None:
        pipeline = AgentPipeline(
            llm=llm,
            rdstation=rdstation,
            hablla=hablla,
            sendgrid=sendgrid,
            mensagens_repo=mensagens_repo,
            squad3_dry_run=(sendgrid is None),
        )
        logger.info(
            "AgentPipeline inicializado (squad3_dry_run=%s)",
            sendgrid is None,
        )
    else:
        pipeline = None
        logger.warning("Pipeline não inicializado — RD Station ausente")

    # Webhook SendGrid: configura singletons no módulo do receiver
    sendgrid_webhook.configure(
        sendgrid_client=sendgrid,
        mensagens_repo=mensagens_repo,
    )

    # Provisionar custom fields no RD Station (background, só se client existe)
    if rdstation is not None:
        asyncio.create_task(_provisionar_custom_fields(rdstation))

    # Detectar modo: DATABASE se DATABASE_URL configurado
    db_url = os.getenv("DATABASE_URL", "")
    if db_url:
        try:
            # 1) Aplicar migrations SQL pendentes (idempotente — todos os
            #    CREATEs usam IF NOT EXISTS). Roda antes do init_db pois cria
            #    tabelas/colunas que os modelos SQLAlchemy não cobrem (ex.: Squad 3).
            try:
                from scripts.run_migrations import main as run_migrations
                rc = await run_migrations()
                if rc != 0:
                    logger.warning("run_migrations retornou código %s — seguindo mesmo assim", rc)
                else:
                    logger.info("Migrations SQL aplicadas com sucesso")
            except Exception as mig_exc:  # noqa: BLE001
                logger.warning("Falha ao aplicar migrations SQL: %s — seguindo com create_all", mig_exc)

            # 2) Cria/verifica as tabelas dos modelos SQLAlchemy (idempotente).
            from src.database.connection import init_db
            await init_db()
            DATA_MODE = "database"
            logger.info("Modo DATABASE ativo — consultas via PostgreSQL")
        except Exception as e:
            DATA_MODE = "api"
            logger.warning("DATABASE_URL configurado mas falhou ao conectar: %s", e)
            logger.warning("Usando modo API (fallback)")
    else:
        DATA_MODE = "api"
        logger.info("Modo API ativo — consultas diretas ao RD Station")

    # Scheduler interno: mantem o Postgres atualizado sem depender de cron externo.
    # Exige modo DATABASE ativo + client RD disponivel.
    global _sync_task
    if DATA_MODE == "database" and rdstation is not None and SYNC_AUTO_ENABLED:
        _sync_task = asyncio.create_task(_auto_sync_loop())
        logger.info("Task de auto-sync agendada")
    elif not SYNC_AUTO_ENABLED:
        logger.info("Auto-sync desabilitado via SYNC_AUTO_ENABLED=false")
    elif DATA_MODE != "database":
        logger.info("Auto-sync inativo — modo atual: %s", DATA_MODE)

    logger.info("Painel iniciado (modo: %s)", DATA_MODE)


@app.on_event("shutdown")
async def shutdown():
    global _sync_task
    if _sync_task is not None and not _sync_task.done():
        _sync_task.cancel()
        try:
            await _sync_task
        except (asyncio.CancelledError, Exception):
            pass
    if rdstation:
        await rdstation.close()
    if llm:
        await llm.close()
    if hablla:
        await hablla.close()
    if DATA_MODE == "database":
        from src.database.connection import close_db
        await close_db()


# --- API ---

@app.get("/api/segmentacoes")
async def listar_segmentacoes():
    try:
        segs = await _get_segs_list()
        return [{"id": s.get("id"), "name": s.get("name"),
                 "standard": s.get("standard", False)} for s in segs]
    except Exception as e:
        raise HTTPException(500, str(e))


def _resolve_seg(segs, segmentation_id):
    """Resolve segmentation id and name from the list."""
    seg_name = "Segmentacao %d" % segmentation_id
    if segmentation_id == 0:
        if not segs:
            return None, "Nenhuma"
        return segs[0].get("id"), segs[0].get("name", "?")
    for s in segs:
        if s.get("id") == segmentation_id:
            seg_name = s.get("name", seg_name)
            break
    return segmentation_id, seg_name


def _enrich_contacts(contacts):
    """Add execution status to contacts."""
    for c in contacts:
        em = c.get("email", "")
        c["_squad1_running"] = em in running_leads
        last_exec = None
        for ex in reversed(execucoes):
            if ex.get("email") == em:
                last_exec = ex
                break
        c["_last_exec"] = last_exec


def _contact_matches_date(contact, date_from, date_to, date_field="created_at"):
    """Check if contact's date field is within the date range.

    date_field pode ser:
      - "created_at" (padrao): data de criacao do lead na base
      - "last_conversion_date": data da ultima conversao
    """
    raw = contact.get(date_field) or ""
    # Fallback: se o campo escolhido estiver vazio, tenta o outro
    if not raw:
        fallback = "created_at" if date_field == "last_conversion_date" else "last_conversion_date"
        raw = contact.get(fallback) or ""
    if not raw:
        return False
    # Extract YYYY-MM-DD from ISO string like "2026-04-13T10:30:00.000-03:00"
    d = raw[:10]
    if date_from and d < date_from:
        return False
    if date_to and d > date_to:
        return False
    return True


async def _build_seg_cache(seg_id: int) -> list[dict]:
    """
    Carrega TODOS os contatos de uma segmentacao e cacheia.

    Usa requisicoes paralelas em batches de 15 (o token bucket do
    rate limiter comeca cheio com 120 tokens para segmentations no
    plano Pro, entao as primeiras ~120 paginas vao em burst rapido).

    Retorna a lista completa de contatos.
    """
    cache_key = str(seg_id)

    # Cache valido?
    cached = _seg_cache.get(cache_key)
    if cached and (time.time() - cached["ts"]) < CACHE_TTL:
        logger.info("Cache hit para seg %s (%d contatos)", seg_id, len(cached["contacts"]))
        return cached["contacts"]

    # Evitar builds simultaneos
    if _cache_building.get(cache_key):
        logger.info("Cache ja esta sendo construido para seg %s, aguardando...", seg_id)
        for _ in range(180):  # max 90s espera
            await asyncio.sleep(0.5)
            cached = _seg_cache.get(cache_key)
            if cached and (time.time() - cached["ts"]) < CACHE_TTL:
                return cached["contacts"]
            if not _cache_building.get(cache_key):
                break
        cached = _seg_cache.get(cache_key)
        if cached:
            return cached["contacts"]
        # Se nao conseguiu, tenta construir
        return await _build_seg_cache(seg_id)

    _cache_building[cache_key] = True
    t0 = time.time()

    try:
        # Fase 1: Buscar primeira pagina + total via header
        import math

        # Usar order=last_conversion_date:desc para priorizar leads
        # mais ativos/recentes (unico campo de ordenacao suportado
        # pela API de segmentacao conforme documentacao RD Station)
        ORDER = "last_conversion_date:desc"

        first_data, total_rows = await rdstation.get_segmentation_contacts_with_total(
            seg_id, page=1, page_size=125,
        )
        first_contacts = []
        if isinstance(first_data, dict):
            first_contacts = first_data.get("contacts", [])
        elif isinstance(first_data, list):
            first_contacts = first_data

        if not first_contacts or len(first_contacts) < 125:
            _seg_cache[cache_key] = {
                "contacts": first_contacts,
                "ts": time.time(),
                "pages": 1,
                "total_rows": total_rows or len(first_contacts),
            }
            logger.info(
                "Cache construido para seg %s: %d contatos, 1 pagina, %.1fs",
                seg_id, len(first_contacts), time.time() - t0,
            )
            return first_contacts

        all_contacts = list(first_contacts)

        # Calcular paginas a buscar: minimo entre total real e MAX_CACHE_PAGES
        # 200 paginas = 25.000 leads = cobre a maioria dos leads ativos
        MAX_CACHE_PAGES = 200
        total_api_pages = math.ceil(total_rows / 125) if total_rows > 0 else MAX_CACHE_PAGES
        pages_to_fetch = min(total_api_pages, MAX_CACHE_PAGES)

        logger.info(
            "Cache build seg %s: total_rows=%d (%d paginas), buscando %d paginas (max %d)",
            seg_id, total_rows, total_api_pages, pages_to_fetch, MAX_CACHE_PAGES,
        )

        # Fase 2: Buscar paginas restantes em batches paralelos
        # O rate limiter comeca com 120 tokens (burst), entao as primeiras
        # ~120 paginas vao rapido (~10s). Depois, 2 req/s.
        BATCH_SIZE = 20
        page_num = 2

        while page_num <= pages_to_fetch:
            end_page = min(page_num + BATCH_SIZE, pages_to_fetch + 1)
            tasks = [
                rdstation.get_segmentation_contacts(
                    seg_id, page=p, page_size=125,
                    order=ORDER,
                )
                for p in range(page_num, end_page)
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            batch_done = False
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.warning("Erro ao buscar pagina %d: %s", page_num + i, result)
                    batch_done = True
                    break

                contacts = []
                if isinstance(result, dict):
                    contacts = result.get("contacts", [])
                elif isinstance(result, list):
                    contacts = result

                if not contacts:
                    batch_done = True
                    break

                all_contacts.extend(contacts)

                if len(contacts) < 125:
                    batch_done = True
                    break

            elapsed_so_far = time.time() - t0
            logger.info(
                "  Cache build seg %s: %d contatos, pag %d/%d, %.1fs",
                seg_id, len(all_contacts),
                min(end_page - 1, pages_to_fetch), pages_to_fetch,
                elapsed_so_far,
            )

            if batch_done:
                break

            page_num = end_page

        elapsed = time.time() - t0
        _seg_cache[cache_key] = {
            "contacts": all_contacts,
            "ts": time.time(),
            "pages": pages_to_fetch,
            "total_rows": total_rows,
        }
        logger.info(
            "Cache COMPLETO para seg %s: %d de %d contatos, %d paginas, %.1fs",
            seg_id, len(all_contacts), total_rows, pages_to_fetch, elapsed,
        )
        return all_contacts
    finally:
        _cache_building[cache_key] = False


async def _preload_cache_background(seg_id: int):
    """Pre-carrega cache em background (chamado no startup ou apos invalidacao)."""
    try:
        await _build_seg_cache(seg_id)
    except Exception as e:
        logger.error("Erro ao pre-carregar cache para seg %s: %s", seg_id, e)


@app.get("/api/leads")
async def listar_leads(
    segmentation_id: int = Query(default=0),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=125, ge=25, le=500),
    search: str = Query(default=""),
    date_from: str = Query(default=""),
    date_to: str = Query(default=""),
    date_field: str = Query(default="created_at"),
    temperatura: str = Query(default=""),
    classificacao: str = Query(default=""),
):
    """
    Busca leads. Sempre le do banco local (PostgreSQL).

    Se o banco estiver indisponivel, retorna 503 — nao ha fallback
    automatico para a API do RD Station (por decisao de arquitetura).
    """
    if DATA_MODE != "database":
        raise HTTPException(
            503,
            "PostgreSQL indisponivel. Verifique DATABASE_URL e reinicie o servico."
        )

    # --- Resolucao de segmentacao ---
    # No modo database o Postgres nao armazena a relacao
    # contato<->segmentacao; por isso, quando o usuario seleciona uma
    # segmentacao especifica, precisamos buscar a lista de emails dela
    # via API do RD Station (com cache) e filtrar a query SQL.
    # segmentation_id=0 significa "sem filtro" (toda a base).
    email_filter = None
    seg_name = None
    if segmentation_id and segmentation_id > 0:
        if rdstation is None:
            raise HTTPException(
                503,
                "Filtro por segmentacao requer credenciais do RD Station. "
                "Configure RDSTATION_CLIENT_ID/SECRET ou selecione "
                "\"Todos os contatos da base de Leads\"."
            )
        try:
            # Resolver nome da segmentacao (best-effort, usa cache local)
            try:
                for s in await _get_segs_list():
                    if s.get("id") == segmentation_id:
                        seg_name = s.get("name")
                        break
            except Exception as e:  # noqa: BLE001
                logger.warning("Nao foi possivel resolver nome da seg %d: %s", segmentation_id, e)

            contacts_seg = await _build_seg_cache(segmentation_id)
            email_filter = [c.get("email") for c in contacts_seg if c.get("email")]
            logger.info(
                "Filtro por segmentacao %d (%s): %d emails",
                segmentation_id, seg_name or "?", len(email_filter),
            )
        except HTTPException:
            raise
        except Exception as e:  # noqa: BLE001
            logger.error("Falha ao buscar contatos da seg %d: %s", segmentation_id, e)
            raise HTTPException(
                502,
                "Nao foi possivel buscar a segmentacao no RD Station: %s" % e,
            )

    try:
        from src.database.queries import list_leads as db_list_leads
        # Mapear date_field do frontend para coluna do banco
        db_date_field = "rd_created_at" if date_field == "created_at" else date_field
        result = await db_list_leads(
            page=page,
            page_size=page_size,
            search=search,
            date_from=date_from,
            date_to=date_to,
            date_field=db_date_field,
            temperatura=temperatura,
            classificacao=classificacao,
            email_filter=email_filter,
        )
        # Enriquecer com status de running
        for c in result["contacts"]:
            c["_squad1_running"] = c.get("email", "") in running_leads
            # Merge com execucoes em memoria (se ainda nao persistidas no DB)
            em = c.get("email", "")
            if not c.get("_last_exec"):
                for ex in reversed(execucoes):
                    if ex.get("email") == em:
                        c["_last_exec"] = ex
                        break
        # Exibe nome da segmentacao na UI (campo que o frontend ja consome)
        if seg_name:
            result["segmentation_name"] = seg_name
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Erro ao consultar PostgreSQL: %s", e)
        raise HTTPException(503, "Erro ao consultar banco: %s" % e)


@app.get("/api/lead/{email}")
async def detalhe_lead(email: str):
    """
    Detalhes de um lead. Sempre le do banco local.

    Se o lead nao estiver na base (ex: criado no RD apos o ultimo sync),
    retorna 404 explicito. Use o sync incremental para trazer leads novos.
    """
    if DATA_MODE != "database":
        raise HTTPException(
            503,
            "PostgreSQL indisponivel. Verifique DATABASE_URL e reinicie o servico."
        )

    try:
        from src.database.queries import get_lead_detail
        result = await get_lead_detail(email)
        if not result:
            raise HTTPException(
                404,
                "Lead nao encontrado no banco. Pode ter sido criado apos o ultimo sync."
            )
        # Merge com execucoes em memoria (ainda nao persistidas no DB).
        # DB vem ordenado ASC (mais antiga -> mais recente).
        # Memoria tambem esta em ordem cronologica (append order).
        # Deduplica por timestamp para evitar duplicar a execucao mais
        # recente (que fica em ambos quando save_execution roda com sucesso)
        # e concatena mem no FINAL para preservar a garantia "ultimo
        # elemento = execucao mais recente".
        mem_hist = [ex for ex in execucoes if ex.get("email") == email]
        if mem_hist:
            db_execs = result.get("execucoes", [])
            db_timestamps = {ex.get("timestamp") for ex in db_execs if ex.get("timestamp")}
            new_from_mem = [ex for ex in mem_hist if ex.get("timestamp") not in db_timestamps]
            result["execucoes"] = db_execs + new_from_mem
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Erro ao buscar lead no DB: %s", e)
        raise HTTPException(503, "Erro ao consultar banco: %s" % e)


@app.post("/api/squad1/executar/{email}")
async def executar_squad1(email: str):
    if email in running_leads:
        raise HTTPException(409, "Squad 1 ja rodando para %s" % email)

    running_leads.add(email)
    start = time.monotonic()
    try:
        results = await orchestrator.execute(email, conversion_identifier="painel_manual")
        total_ms = (time.monotonic() - start) * 1000

        resultado = {
            "email": email,
            "timestamp": datetime.now().isoformat(),
            "total_ms": round(total_ms),
            "agentes": {},
        }
        for r in results:
            resultado["agentes"][r.agent_name] = {
                "success": r.success,
                "duration_ms": round(r.duration_ms),
                "error": r.error,
                "data": r.data,
            }
        resumo = resultado["agentes"].get("squad1_resumo", {}).get("data", {})
        resultado["resumo"] = {
            "temperatura": resumo.get("temperatura", "-"),
            "prioridade": resumo.get("prioridade_contato", "-"),
            "area": resumo.get("area_principal", "-"),
            "compliance": resumo.get("compliance_status", "-"),
            "pode_seguir": resumo.get("pode_seguir_squad2", False),
            "duplicados": resumo.get("duplicados_encontrados", 0),
        }
        execucoes.append(resultado)
        if len(execucoes) > 200:
            execucoes.pop(0)

        # Persistir no PostgreSQL (se disponível)
        if DATA_MODE == "database":
            try:
                from src.database.queries import save_execution
                await save_execution(resultado)
            except Exception as db_err:
                logger.warning("Erro ao salvar execução no DB: %s", db_err)

        return resultado
    except Exception as e:
        logger.error("Erro Squad 1 para %s: %s", email, e)
        raise HTTPException(500, str(e))
    finally:
        running_leads.discard(email)


@app.post("/api/pipeline/executar/{email}")
async def executar_pipeline(email: str):
    """Executa o pipeline completo: Squad 1 → Squad 2 (→ Squad 3 futuro)."""
    if email in running_leads:
        raise HTTPException(409, "Pipeline já rodando para %s" % email)

    running_leads.add(email)
    start = time.monotonic()
    try:
        results = await pipeline.process_new_lead(
            email, conversion_identifier="painel_manual",
        )
        total_ms = (time.monotonic() - start) * 1000

        resultado = {
            "email": email,
            "timestamp": datetime.now().isoformat(),
            "total_ms": round(total_ms),
            "tipo": "pipeline_completo",
            "agentes": {},
        }
        for r in results:
            resultado["agentes"][r.agent_name] = {
                "success": r.success,
                "duration_ms": round(r.duration_ms),
                "error": r.error,
                "data": r.data,
            }

        # Resumo Squad 1 (também como "resumo" para compatibilidade com frontend)
        resumo_s1 = resultado["agentes"].get("squad1_resumo", {}).get("data", {})
        resultado["resumo_squad1"] = {
            "temperatura": resumo_s1.get("temperatura", "-"),
            "prioridade": resumo_s1.get("prioridade_contato", "-"),
            "area": resumo_s1.get("area_principal", "-"),
            "compliance": resumo_s1.get("compliance_status", "-"),
            "pode_seguir": resumo_s1.get("pode_seguir_squad2", False),
        }
        resultado["resumo"] = resultado["resumo_squad1"]  # alias

        # Resumo Squad 2
        resumo_s2 = resultado["agentes"].get("squad2_resumo", {}).get("data", {})
        resultado["resumo_squad2"] = {
            "score_total": resumo_s2.get("score_total", 0),
            "classificacao": resumo_s2.get("classificacao", "-"),
            "rota": resumo_s2.get("rota", "-"),
            "acoes_recomendadas": resumo_s2.get("acoes_recomendadas", []),
            "briefing_comercial": resumo_s2.get("briefing_comercial"),
            "tags_aplicadas": resumo_s2.get("tags_aplicadas", []),
            "pode_seguir_squad3": resumo_s2.get("pode_seguir_squad3", False),
            "persistencia": resumo_s2.get("persistencia", {}),
        }

        # Dimensões do scoring
        scorer_data = resultado["agentes"].get("squad2_scorer", {}).get("data", {})
        resultado["resumo_squad2"]["dimensoes"] = scorer_data.get("dimensoes", {})

        execucoes.append(resultado)
        if len(execucoes) > 200:
            execucoes.pop(0)

        # Persistir no PostgreSQL (se disponível)
        if DATA_MODE == "database":
            try:
                from src.database.queries import save_execution
                await save_execution(resultado)
            except Exception as db_err:
                logger.warning("Erro ao salvar execução no DB: %s", db_err)

        return resultado
    except Exception as e:
        logger.error("Erro pipeline para %s: %s", email, e)
        raise HTTPException(500, str(e))
    finally:
        running_leads.discard(email)


@app.get("/api/execucoes")
async def listar_execucoes():
    return list(reversed(execucoes))


@app.post("/api/cache/invalidar")
async def invalidar_cache(
    segmentation_id: int = Query(default=0),
    rebuild: bool = Query(default=False),
):
    """Invalida o cache de uma segmentacao (ou de todas se seg=0).
    Se rebuild=true, inicia reconstrucao em background."""
    if segmentation_id:
        key = str(segmentation_id)
        removed = key in _seg_cache
        _seg_cache.pop(key, None)
        if rebuild:
            asyncio.create_task(_preload_cache_background(segmentation_id))
        return {"invalidated": removed, "key": key, "rebuilding": rebuild}
    else:
        count = len(_seg_cache)
        _seg_cache.clear()
        return {"invalidated": True, "cleared": count}


@app.post("/api/cache/preload/{seg_id}")
async def preload_cache(seg_id: int):
    """
    Pre-carga de cache de segmentacao da API do RD Station.

    No modo DATABASE (padrao em producao), esse cache e desnecessario —
    as consultas vao direto ao PostgreSQL. Retornamos no-op para nao
    gastar rate limit da API.
    """
    if DATA_MODE == "database":
        return {"status": "skipped", "reason": "modo database ativo"}

    cached = _seg_cache.get(str(seg_id))
    if cached and (time.time() - cached["ts"]) < CACHE_TTL:
        return {
            "status": "already_cached",
            "contacts": len(cached["contacts"]),
            "age_s": round(time.time() - cached["ts"]),
        }
    if _cache_building.get(str(seg_id)):
        return {"status": "building"}
    asyncio.create_task(_preload_cache_background(seg_id))
    return {"status": "started"}


@app.get("/api/hablla/diagnose/{email}")
async def diagnose_hablla(email: str):
    """
    Diagnostico do Hablla para um lead especifico.

    Retorna:
      - Se o HabllaClient esta inicializado
      - Se a pessoa foi encontrada (por email)
      - Quantos services/cards/anotacoes/mensagens tem
      - Canais em que interagiu
      - Resumo do que o Analisador de Engajamento ve

    Util para responder "por que canais=apenas rdstation?".
    """
    import os as _os  # para nao ofuscar o 'os' global
    result: dict = {
        "email": email,
        "hablla_token_configurado": bool(_os.getenv("HABLLA_API_TOKEN", "")),
        "hablla_workspace_id": _os.getenv("HABLLA_WORKSPACE_ID", "") or None,
        "client_inicializado": hablla is not None,
    }
    if hablla is None:
        result["motivo"] = (
            "HabllaClient = None no startup. Cheque HABLLA_API_TOKEN "
            "nas variaveis de ambiente do Railway."
        )
        return result

    # 1. Tentar achar a pessoa
    try:
        pessoa = await hablla.search_person_by_email(email)
    except Exception as e:  # noqa: BLE001
        result["erro_busca"] = str(e)
        return result

    if not pessoa:
        result["encontrado"] = False
        result["motivo"] = (
            "Email nao encontrado no Hablla (endpoint /v2/persons com "
            "search=email nao retornou match). O contato pode existir "
            "com outro email principal ou nao estar no workspace."
        )
        return result

    person_id = pessoa.get("id") or pessoa.get("_id") or ""
    result["encontrado"] = True
    result["person_id"] = person_id
    result["nome_hablla"] = pessoa.get("name", "")
    result["customer_status"] = pessoa.get("customer_status", "")
    result["emails_cadastrados"] = [
        e.get("email") if isinstance(e, dict) else e
        for e in (pessoa.get("emails") or [])
    ]
    result["phones_cadastrados"] = [
        p.get("phone") if isinstance(p, dict) else p
        for p in (pessoa.get("phones") or [])
    ]
    tags = pessoa.get("tags", []) or []
    result["tags_hablla"] = [
        t.get("name") if isinstance(t, dict) else str(t) for t in tags
    ]

    # 2. Services (atendimentos = conversas multicanal)
    try:
        svcs = await hablla.list_services(person_id=person_id, limit=50)
        lista = svcs.get("results", []) or []
        result["services_total"] = svcs.get("totalItems", len(lista))
        canais = sorted({(s.get("type") or "").lower() for s in lista if s.get("type")})
        result["canais_com_interacao"] = canais
        if lista:
            ultimo = max(lista, key=lambda s: s.get("updated_at") or s.get("created_at") or "")
            result["ultima_conversa"] = {
                "canal": ultimo.get("type"),
                "status": ultimo.get("status"),
                "data": ultimo.get("updated_at") or ultimo.get("created_at"),
            }
    except Exception as e:  # noqa: BLE001
        result["erro_services"] = str(e)

    # 3. Cards (deals no pipeline Hablla)
    try:
        cards = await hablla.list_cards(person_id=person_id, limit=50)
        lc = cards.get("results", []) or []
        result["cards_total"] = cards.get("totalItems", len(lc))
        result["cards_abertos"] = sum(1 for c in lc if (c.get("status") or "").lower() == "open")
    except Exception as e:  # noqa: BLE001
        result["erro_cards"] = str(e)

    # 4. Anotacoes
    try:
        anot = await hablla.list_annotations(person_id=person_id, limit=50)
        la = anot.get("results", []) or []
        result["anotacoes_total"] = anot.get("totalItems", len(la))
        result["ultimas_anotacoes"] = [
            {"autor": a.get("user_name") or a.get("author") or "?",
             "texto": (a.get("content") or a.get("message") or "")[:120]}
            for a in la[:3]
        ]
    except Exception as e:  # noqa: BLE001
        result["erro_anotacoes"] = str(e)

    # 5. Tags
    try:
        tags_list = await hablla.list_tags()
        result["tags_total_workspace"] = len(tags_list or [])
    except Exception as e:  # noqa: BLE001
        result["erro_tags"] = str(e)

    return result


@app.get("/api/cache/status")
async def cache_status():
    """Retorna status de todos os caches ativos."""
    now = time.time()
    entries = []
    for key, val in _seg_cache.items():
        age = now - val["ts"]
        entries.append({
            "seg_id": key,
            "contacts": len(val["contacts"]),
            "total_rows": val.get("total_rows", 0),
            "pages": val.get("pages", 0),
            "age_s": round(age),
            "fresh": age < CACHE_TTL,
        })
    building = [k for k, v in _cache_building.items() if v]
    return {"caches": entries, "building": building, "ttl": CACHE_TTL}


# --- Database endpoints ---

@app.get("/api/db/stats")
async def db_stats():
    """Estatísticas do banco de dados (só funciona no modo database)."""
    if DATA_MODE != "database":
        return {"mode": "api", "message": "PostgreSQL não configurado"}
    try:
        from src.database.queries import get_stats
        stats = await get_stats()
        stats["mode"] = "database"
        return stats
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/db/mode")
async def db_mode():
    """Retorna o modo de dados atual."""
    return {"mode": DATA_MODE}


async def _execute_sync(mode: str, hours: int) -> dict:
    """Executa um sync (full ou incremental) e atualiza _sync_status.

    Usa _sync_lock para evitar duas execucoes simultaneas (manual + agendada).
    Nao levanta excecao — registra o erro no status e retorna.
    """
    if _sync_lock.locked():
        logger.info("Sync ja em andamento — pulando disparo (%s)", mode)
        return {"status": "skipped", "reason": "already_running", "mode": mode}

    from src.database.sync import full_sync, incremental_sync

    async with _sync_lock:
        _sync_status["running"] = True
        _sync_status["last_run_mode"] = mode
        _sync_status["last_run_error"] = None
        started = datetime.now(timezone.utc)
        try:
            if mode == "full":
                result = await full_sync()
            else:
                result = await incremental_sync(since_hours=hours)
            _sync_status["last_run_status"] = result.get("status", "unknown")
            _sync_status["last_run_contacts"] = result.get("total_contacts", 0)
            _sync_status["total_runs"] += 1
            logger.info(
                "Sync %s concluido: %s (%s leads)",
                mode, _sync_status["last_run_status"],
                _sync_status["last_run_contacts"],
            )
            return result
        except Exception as e:
            _sync_status["last_run_status"] = "failed"
            _sync_status["last_run_error"] = str(e)[:500]
            _sync_status["total_errors"] += 1
            logger.error("Erro no sync %s: %s", mode, e, exc_info=True)
            return {"status": "failed", "error": str(e)}
        finally:
            _sync_status["running"] = False
            _sync_status["last_run_at"] = started.isoformat()


async def _score_recent_leads_batch(max_leads: int = 20) -> dict:
    """Roda Squad 1+2 nos N leads mais recentes sem s2_processado_em.
    Chamado pelo auto-sync apos sync incremental pra popular leads novos.

    Retorna dict com {scored, failed, candidates}.
    """
    from sqlalchemy import text
    from src.database.connection import get_session
    from src.database.queries import save_execution

    if not pipeline or DATA_MODE != "database":
        return {"scored": 0, "failed": 0, "candidates": 0, "skipped": True}

    async with get_session() as session:
        result = await session.execute(text("""
            SELECT email FROM leads
            WHERE s2_processado_em IS NULL
              AND email IS NOT NULL AND email != \'\'
            ORDER BY rd_created_at DESC NULLS LAST
            LIMIT :max
        """), {"max": max_leads})
        emails = [row[0] for row in result.fetchall()]

    scored, failed = 0, 0
    for email in emails:
        if email in running_leads:
            continue
        running_leads.add(email)
        try:
            results = await pipeline.process_new_lead(
                email, conversion_identifier="auto_sync_scoring",
            )
            resultado = {"email": email, "tipo": "pipeline_completo", "agentes": {}}
            for r in results:
                resultado["agentes"][r.agent_name] = {
                    "success": r.success,
                    "duration_ms": round(r.duration_ms),
                    "error": r.error,
                    "data": r.data,
                }
            resumo_s1 = resultado["agentes"].get("squad1_resumo", {}).get("data", {})
            resultado["resumo_squad1"] = {
                "temperatura": resumo_s1.get("temperatura", "-"),
                "prioridade": resumo_s1.get("prioridade_contato", "-"),
                "area": resumo_s1.get("area_principal", "-"),
                "compliance": resumo_s1.get("compliance_status", "-"),
                "pode_seguir": resumo_s1.get("pode_seguir_squad2", False),
            }
            resumo_s2 = resultado["agentes"].get("squad2_resumo", {}).get("data", {})
            resultado["resumo_squad2"] = {
                "score_total": resumo_s2.get("score_total", 0),
                "classificacao": resumo_s2.get("classificacao", "-"),
                "rota": resumo_s2.get("rota", "-"),
                "acoes_recomendadas": resumo_s2.get("acoes_recomendadas", []),
                "briefing_comercial": resumo_s2.get("briefing_comercial"),
                "tags_aplicadas": resumo_s2.get("tags_aplicadas", []),
                "pode_seguir_squad3": resumo_s2.get("pode_seguir_squad3", False),
            }
            scorer_data = resultado["agentes"].get("squad2_scorer", {}).get("data", {})
            resultado["resumo_squad2"]["dimensoes"] = scorer_data.get("dimensoes", {})

            await save_execution(resultado)
            scored += 1
        except Exception as e:
            logger.warning("auto_score: falha em %s: %s", email, e)
            failed += 1
        finally:
            running_leads.discard(email)

    logger.info(
        "auto_score: %d scorados, %d falhas, %d candidatos",
        scored, failed, len(emails),
    )
    return {"scored": scored, "failed": failed, "candidates": len(emails)}


async def _auto_sync_loop():
    """Loop eterno do scheduler: dispara incremental a cada SYNC_INTERVAL_HOURS
    e full_sync no dia/hora UTC configurados (reconciliacao semanal).

    Tolerante a falhas: se um sync levantar, o loop continua.
    """
    if not SYNC_AUTO_ENABLED:
        logger.info("Auto-sync desabilitado (SYNC_AUTO_ENABLED=false)")
        return

    logger.info(
        "Auto-sync habilitado — incremental a cada %dh (janela %dh), "
        "full semanal aos weekday=%d %02d:00 UTC",
        SYNC_INTERVAL_HOURS, SYNC_HOURS_WINDOW,
        SYNC_FULL_WEEKDAY_UTC, SYNC_FULL_HOUR_UTC,
    )

    # Pequeno atraso inicial para o startup terminar de estabilizar (migrations,
    # conexoes, etc.) antes de bater na API externa.
    await asyncio.sleep(60)

    # Controla o ultimo dia em que o full rodou para nao repetir dentro do mesmo dia.
    last_full_day: Optional[str] = None

    while True:
        now = datetime.now(timezone.utc)

        # Decide se eh hora de um full_sync (uma vez por semana no slot configurado)
        is_full_slot = (
            now.weekday() == SYNC_FULL_WEEKDAY_UTC
            and now.hour == SYNC_FULL_HOUR_UTC
            and last_full_day != now.date().isoformat()
        )

        mode = "full" if is_full_slot else "incremental"
        try:
            await _execute_sync(mode=mode, hours=SYNC_HOURS_WINDOW)
            if mode == "full":
                last_full_day = now.date().isoformat()
        except Exception as e:
            logger.error("Exception no loop de auto-sync: %s", e, exc_info=True)

        # Proximo disparo
        sleep_seconds = SYNC_INTERVAL_HOURS * 3600
        next_run = datetime.now(timezone.utc) + timedelta(seconds=sleep_seconds)
        _sync_status["next_run_at"] = next_run.isoformat()

        try:
            await asyncio.sleep(sleep_seconds)
        except asyncio.CancelledError:
            logger.info("Auto-sync loop cancelado")
            raise


import uuid as _uuid

@app.post("/api/orquestrador/run")
async def trigger_orquestrador(
    background_tasks: BackgroundTasks,
    max_leads: int = Query(default=30, ge=1, le=200),
):
    """Dispara em background. Retorna 202 imediato."""
    if _orquestrador_status["running"]:
        return {"started": False, "reason": "already_running", "current_run": _orquestrador_status["last_run_id"]}
    run_id = str(_uuid.uuid4())
    background_tasks.add_task(_run_orquestrador_bg, run_id, max_leads)
    return {"started": True, "run_id": run_id, "max_leads": max_leads, "status_url": "/api/orquestrador/status", "estimated_seconds": max_leads * 30}


@app.get("/api/orquestrador/status")
async def status_orquestrador():
    """Status da última execução."""
    return _orquestrador_status


@app.post("/api/db/sync")
async def trigger_sync(
    mode: str = Query(default="incremental"),
    hours: int = Query(default=24),
):
    """Dispara sincronização em background (só funciona no modo database)."""
    if DATA_MODE != "database":
        raise HTTPException(400, "PostgreSQL não configurado")

    asyncio.create_task(_execute_sync(mode=mode, hours=hours))
    return {"status": "started", "mode": mode}


@app.get("/api/db/sync/status")
async def sync_status():
    """Retorna status do auto-sync: ultima execucao, proxima, totais, etc."""
    return _sync_status


# --- Frontend ---
@app.get("/", response_class=HTMLResponse)
async def index():
    html_path = Path(__file__).parent / "static" / "index.html"
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    import webbrowser, threading
    port = int(os.getenv("PAINEL_PORT", "8501"))
    def open_browser():
        time.sleep(1.5)
        webbrowser.open("http://localhost:%d" % port)
    threading.Thread(target=open_browser, daemon=True).start()
    print("\n" + "=" * 50)
    print("  BSSP - Squad Leads")
    print("  http://localhost:%d" % port)
    print("  Modo: %s" % ("DATABASE" if os.getenv("DATABASE_URL") else "API"))
    print("=" * 50 + "\n")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
