"""
Cliente principal da API do RD Station Marketing.

Encapsula todos os endpoints com:
- Rate limiting automático (respeita limites por conta e por lead)
- Retry com backoff exponencial no erro 429
- Autenticação OAuth com refresh transparente
- Logging estruturado

Uso com API Key (recomendado):
    from src.integrations.rdstation import RDStationClient

    client = RDStationClient(api_key="seu_token_privado", plan="pro")

    # Buscar contato
    contact = await client.get_contact("lead@email.com")

    # Atualizar contato (upsert)
    await client.upsert_contact("lead@email.com", {"name": "João", "cf_score_ia": 85.5})

    # Adicionar tags
    await client.add_tags("lead@email.com", ["quente", "webinar_abril"])

    # Atualizar estágio do funil
    await client.update_funnel_stage("lead@email.com", lifecycle_stage="Lead Qualificado")

Uso com OAuth2 (alternativo):
    client = RDStationClient(
        client_id="...", client_secret="...", refresh_token="...", plan="pro",
    )
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Optional

import httpx

from .auth import RDStationAuth
from .models import (
    Contact,
    ContactIdentifier,
    FunnelStage,
    FunnelStageEnum,
    WebhookEvent,
)
from .rate_limiter import RateLimiter

logger = logging.getLogger(__name__)

BASE_URL = "https://api.rd.services"

# Retry config
MAX_RETRIES = 3
INITIAL_BACKOFF = 1.0  # segundos


class RDStationError(Exception):
    """Erro genérico da API do RD Station."""

    def __init__(self, status_code: int, message: str, raw: dict | None = None):
        self.status_code = status_code
        self.message = message
        self.raw = raw or {}
        super().__init__(f"HTTP {status_code}: {message}")


class RateLimitExceeded(RDStationError):
    """Rate limit excedido mesmo após retries."""

    def __init__(self, message: str, remaining_ms: int = 0):
        self.remaining_ms = remaining_ms
        super().__init__(429, message)


class RDStationClient:
    """
    Cliente async para a API do RD Station Marketing.

    Todos os métodos respeitam rate limits automaticamente.
    Se o rate limit for atingido, aguarda antes de prosseguir.
    """

    def __init__(
        self,
        *,
        # Modo API Key (simples — recomendado)
        api_key: Optional[str] = None,
        # Modo OAuth2 (alternativo)
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        refresh_token: Optional[str] = None,
        access_token: Optional[str] = None,
        # Config geral
        plan: str = "pro",
        max_retries: int = MAX_RETRIES,
    ) -> None:
        self.auth = RDStationAuth(
            api_key=api_key,
            client_id=client_id,
            client_secret=client_secret,
            refresh_token=refresh_token,
            access_token=access_token,
        )
        self.rate_limiter = RateLimiter(plan=plan)
        self.max_retries = max_retries
        self._http: Optional[httpx.AsyncClient] = None

        # Cache de funil: {email: (timestamp, data)}  —  TTL 10 min
        self._funnel_cache: dict[str, tuple[float, dict]] = {}
        self._funnel_cache_ttl = 600  # 10 minutos

    @property
    def http(self) -> httpx.AsyncClient:
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(
                base_url=BASE_URL,
                timeout=30.0,
            )
        return self._http

    # ------------------------------------------------------------------
    # Request base com retry e rate limiting
    # ------------------------------------------------------------------

    async def _request(
        self,
        method: str,
        path: str,
        *,
        rate_limit_resource: str,
        rate_limit_entity: Optional[str] = None,
        json: Optional[dict] = None,
        params: Optional[dict] = None,
    ) -> dict | list | None:
        """
        Faz uma requisição à API com rate limiting e retry automático.

        Fluxo:
        1. Adquire token do rate limiter (espera se necessário)
        2. Obtém headers de autenticação
        3. Faz a requisição
        4. Se 429: aguarda remaining_time e retenta
        5. Se outro erro: lança RDStationError
        """
        # Rate limit preventivo (client-side)
        await self.rate_limiter.acquire(
            rate_limit_resource, entity_id=rate_limit_entity
        )

        headers = await self.auth.get_headers()
        backoff = INITIAL_BACKOFF

        for attempt in range(1, self.max_retries + 1):
            try:
                response = await self.http.request(
                    method, path, headers=headers, json=json, params=params
                )

                if response.status_code == 429:
                    error_data = response.json()
                    remaining_ms = error_data.get("remaining_time", 0)
                    wait_seconds = (remaining_ms / 1000) + 0.5  # margem

                    if attempt < self.max_retries:
                        logger.warning(
                            "429 rate limit no endpoint %s (tentativa %d/%d). "
                            "Aguardando %.1fs (remaining_time=%dms)",
                            path, attempt, self.max_retries,
                            wait_seconds, remaining_ms,
                        )
                        await asyncio.sleep(max(wait_seconds, backoff))
                        backoff *= 2
                        # Renova headers caso o token tenha expirado durante a espera
                        headers = await self.auth.get_headers()
                        continue
                    else:
                        raise RateLimitExceeded(
                            f"Rate limit excedido após {self.max_retries} tentativas: "
                            f"{error_data.get('error', 'unknown')}",
                            remaining_ms=remaining_ms,
                        )

                if response.status_code == 204:
                    return None

                # Retry automático em 5xx (500, 502, 503, 504)
                if response.status_code in (500, 502, 503, 504):
                    if attempt < self.max_retries:
                        logger.warning(
                            "HTTP %d no endpoint %s (tentativa %d/%d). "
                            "Retentando em %.1fs...",
                            response.status_code, path,
                            attempt, self.max_retries, backoff,
                        )
                        await asyncio.sleep(backoff)
                        backoff *= 2
                        headers = await self.auth.get_headers()
                        continue
                    else:
                        raise RDStationError(
                            status_code=response.status_code,
                            message=(
                                f"Servidor retornou {response.status_code} após "
                                f"{self.max_retries} tentativas"
                            ),
                        )

                if response.status_code >= 400:
                    error_data = {}
                    try:
                        error_data = response.json()
                    except Exception:
                        pass
                    raise RDStationError(
                        status_code=response.status_code,
                        message=error_data.get("error_message", response.text[:200]),
                        raw=error_data,
                    )

                if response.status_code == 200:
                    return response.json()
                return response.json()

            except (httpx.ConnectError, httpx.ReadTimeout) as e:
                if attempt < self.max_retries:
                    logger.warning(
                        "Erro de conexão no endpoint %s (tentativa %d/%d): %s. "
                        "Retentando em %.1fs...",
                        path, attempt, self.max_retries, e, backoff,
                    )
                    await asyncio.sleep(backoff)
                    backoff *= 2
                    continue
                raise RDStationError(
                    status_code=0,
                    message=f"Falha de conexão após {self.max_retries} tentativas: {e}",
                )

        # Nunca deveria chegar aqui
        raise RDStationError(0, "Esgotou retries sem resultado")

    # ------------------------------------------------------------------
    # CONTATOS
    # ------------------------------------------------------------------

    async def get_contact(
        self,
        email: Optional[str] = None,
        uuid: Optional[str] = None,
    ) -> Contact:
        """
        Busca um contato pelo email ou UUID.

        Squad 1 (Captura/Enriquecimento) usa para puxar perfil completo.
        Squad 2 (Qualificação) usa para ler dados antes de scoring.
        """
        if email:
            identifier, value = "email", email
        elif uuid:
            identifier, value = "uuid", uuid
        else:
            raise ValueError("Informe email ou uuid")

        data = await self._request(
            "GET",
            f"/platform/contacts/{identifier}:{value}",
            rate_limit_resource="contacts_account",
        )
        return Contact.from_api_response(data)

    async def create_contact(self, contact: Contact) -> Contact:
        """
        Cria um contato novo. Falha se email já existe.
        Para upsert, use upsert_contact().
        """
        data = await self._request(
            "POST",
            "/platform/contacts",
            rate_limit_resource="contacts_account",
            json=contact.to_api_payload(),
        )
        return Contact.from_api_response(data)

    async def upsert_contact(
        self,
        email: str,
        fields: dict[str, Any],
    ) -> Contact:
        """
        Cria ou atualiza contato (PATCH com upsert).

        ATENÇÃO: Limitado a 24 chamadas por lead a cada 24h.
        Os agentes devem acumular mudanças e fazer um único PATCH consolidado.

        Args:
            email: Email do contato
            fields: Campos a atualizar (NÃO incluir "email" — a API não aceita)

        Squad 1: enriquecimento de dados (telefone, cidade, cargo)
        Squad 2: grava cf_score_ia, cf_temperatura
        Squad 4: atualiza dono do lead
        """
        # API não aceita "email" no body quando identifica por email
        fields.pop("email", None)

        data = await self._request(
            "PATCH",
            f"/platform/contacts/email:{email}",
            rate_limit_resource="contacts_account",
            rate_limit_entity=email,
            json=fields,
        )

        # Também consome cota per-lead
        await self.rate_limiter.acquire(
            "contacts_per_lead", entity_id=email
        )

        return Contact.from_api_response(data)

    async def batch_upsert_contacts(
        self,
        updates: list[tuple[str, dict[str, Any]]],
        *,
        concurrency: int = 5,
    ) -> list[Contact | RDStationError]:
        """
        Atualiza múltiplos contatos com controle de concorrência.
        Ideal para o Squad 2 aplicar scores em lote.

        Args:
            updates: Lista de (email, fields) para atualizar
            concurrency: Máximo de requisições simultâneas

        Returns:
            Lista de Contact (sucesso) ou RDStationError (falha) na mesma ordem
        """
        semaphore = asyncio.Semaphore(concurrency)
        results: list[Contact | RDStationError] = [None] * len(updates)  # type: ignore

        async def _update(idx: int, email: str, fields: dict) -> None:
            async with semaphore:
                try:
                    results[idx] = await self.upsert_contact(email, fields)
                except RDStationError as e:
                    results[idx] = e
                    logger.error("Falha ao atualizar %s: %s", email, e)

        tasks = [
            _update(i, email, fields)
            for i, (email, fields) in enumerate(updates)
        ]
        await asyncio.gather(*tasks)
        return results

    # ------------------------------------------------------------------
    # TAGS
    # ------------------------------------------------------------------

    async def add_tags(self, email: str, tags: list[str]) -> Contact:
        """
        Adiciona tags a um contato existente (acumulativas).
        Não substitui tags anteriores.

        Squad 1: marca origem (fonte_webinar, fonte_ebook)
        Squad 2: marca resultado do scoring (quente, morno, frio)

        Defesa: o RD Station rejeita tags com maiúsculas
        (VALUES_MUST_BE_LOWERCASE). Normalizamos aqui para isolar
        os callers — qualquer tag com letra grande (ex.: vinda do
        LLM no Enriquecedor com 'Alta' ou 'Quente') cai em lowercase
        + strip antes do POST. Tags vazias são descartadas.
        """
        normalized = [t.strip().lower() for t in (tags or []) if t and t.strip()]
        if not normalized:
            logger.debug("add_tags: lista vazia para %s, ignorando", email)
            return Contact.from_api_response({"email": email})

        # Log se houve transformação de caixa
        original_clean = [t.strip() for t in (tags or []) if t and t.strip()]
        if normalized != original_clean:
            logger.info(
                "add_tags: normalizou tags para %s: %s -> %s",
                email, original_clean, normalized,
            )

        data = await self._request(
            "POST",
            f"/platform/contacts/email:{email}/tag",
            rate_limit_resource="tags_account",
            rate_limit_entity=email,
            json={"tags": normalized},
        )

        # Consome cota per-lead de tags
        await self.rate_limiter.acquire("tags_per_lead", entity_id=email)

        return Contact.from_api_response(data)

    # ------------------------------------------------------------------
    # FUNIS DE CONTATOS
    # ------------------------------------------------------------------

    async def get_funnel_stage(self, email: str) -> dict:
        """
        Consulta o estágio do funil e scores de Lead Scoring.

        Retorna: lifecycle_stage, opportunity, fit_score, interest_score
        Squad 2 (Qualificação/Scoring) usa para ler scores nativos do RD.

        Inclui:
        - Cache local (TTL 10min) para evitar chamadas repetidas
        - Fallback ao cache quando endpoint retorna 5xx
        """
        key = email.lower().strip()

        # 1. Verifica cache válido
        if key in self._funnel_cache:
            ts, cached = self._funnel_cache[key]
            if (time.time() - ts) < self._funnel_cache_ttl:
                logger.debug("Funil cache hit para %s", email)
                return cached

        # 2. Tenta buscar da API (com retry em 5xx via _request)
        try:
            data = await self._request(
                "GET",
                f"/platform/contacts/email:{email}/funnels/default",
                rate_limit_resource="funnels_account",
                rate_limit_entity=email,
            )
            # Salva no cache
            self._funnel_cache[key] = (time.time(), data)
            return data

        except RDStationError as e:
            # Se 5xx mesmo após retries, tenta fallback ao cache expirado
            if e.status_code >= 500 and key in self._funnel_cache:
                _, stale = self._funnel_cache[key]
                logger.warning(
                    "Funil API %d para %s — usando cache stale como fallback",
                    e.status_code, email,
                )
                return stale
            raise

    async def update_funnel_stage(
        self,
        email: str,
        *,
        lifecycle_stage: Optional[str] = None,
        opportunity: Optional[bool] = None,
        contact_owner_email: Optional[str] = None,
    ) -> dict:
        """
        Atualiza o estágio do funil de um contato.

        Squad 2: move para "Lead Qualificado" após scoring
        Squad 4: marca opportunity=True para handoff ao consultor
        """
        payload: dict[str, Any] = {}
        if lifecycle_stage:
            payload["lifecycle_stage"] = lifecycle_stage
        if opportunity is not None:
            payload["opportunity"] = opportunity
        if contact_owner_email:
            payload["contact_owner_email"] = contact_owner_email

        data = await self._request(
            "PUT",
            f"/platform/contacts/email:{email}/funnels/default",
            rate_limit_resource="funnels_account",
            rate_limit_entity=email,
            json=payload,
        )
        # Invalida cache — dados mudaram
        key = email.lower().strip()
        self._funnel_cache.pop(key, None)
        return data

    # ------------------------------------------------------------------
    # SEGMENTAÇÕES
    # ------------------------------------------------------------------

    async def list_segmentations(self) -> list[dict]:
        """
        Lista todas as segmentações disponíveis.
        Squad 3 (Comunicação) usa para identificar grupos-alvo.
        """
        data = await self._request(
            "GET",
            "/platform/segmentations",
            rate_limit_resource="segmentations",
        )
        return data if isinstance(data, list) else data.get("segmentations", [])

    async def get_segmentation_contacts(
        self,
        segmentation_id: int,
        *,
        page: int = 1,
        page_size: int = 125,
        order: str | None = None,
        search: str | None = None,
    ) -> dict:
        """
        Lista contatos de uma segmentação específica.
        Retorna paginado (max 125 por página).

        Args:
            segmentation_id: ID da segmentação
            page: Página (começa em 1)
            page_size: Registros por página (max 125)
            order: Ordenação (ex: "last_conversion_date:desc", "name:asc")
            search: Busca por nome do contato (parcial)
        """
        params: dict[str, Any] = {"page": page, "page_size": page_size}
        if order:
            params["order"] = order
        if search:
            params["search"] = search

        data = await self._request(
            "GET",
            f"/platform/segmentations/{segmentation_id}/contacts",
            rate_limit_resource="segmentations",
            params=params,
        )
        return data

    async def get_segmentation_contacts_with_total(
        self,
        segmentation_id: int,
        *,
        page: int = 1,
        page_size: int = 125,
    ) -> tuple[dict, int]:
        """
        Igual a get_segmentation_contacts, mas retorna tambem o total
        de contatos via header pagination-total-rows.

        Returns:
            Tuple (data_dict, total_rows)
        """
        params: dict[str, Any] = {"page": page, "page_size": page_size}

        await self.rate_limiter.acquire("segmentations")
        headers = await self.auth.get_headers()

        response = await self.http.request(
            "GET",
            f"/platform/segmentations/{segmentation_id}/contacts",
            headers=headers,
            params=params,
        )

        if response.status_code >= 400:
            error_data = {}
            try:
                error_data = response.json()
            except Exception:
                pass
            raise RDStationError(
                status_code=response.status_code,
                message=error_data.get("error_message", response.text[:200]),
                raw=error_data,
            )

        total_rows = int(response.headers.get("pagination-total-rows", 0))
        data = response.json()
        return data, total_rows

    # ------------------------------------------------------------------
    # FLUXOS DE AUTOMAÇÃO
    # ------------------------------------------------------------------

    async def list_automation_flows(self) -> list[dict]:
        """
        Lista fluxos de automação disponíveis.
        Limite: 40 req/hora em todos os planos.
        """
        data = await self._request(
            "GET",
            "/platform/automation-flows",
            rate_limit_resource="flows_list",
        )
        return data if isinstance(data, list) else data.get("flows", [])

    async def insert_lead_in_flow(
        self, flow_id: int, email: str
    ) -> dict:
        """
        Insere lead em um fluxo de automação.

        ATENÇÃO: Limite muito restritivo!
        - Pro: 10 inserções/hora
        - Advanced: 100 inserções/hora

        Squad 3 (Comunicação) usa para acionar nurturing.
        """
        data = await self._request(
            "POST",
            f"/platform/automation-flows/{flow_id}/leads",
            rate_limit_resource="flows_insert",
            json={"email": email},
        )
        return data

    # ------------------------------------------------------------------
    # WEBHOOKS (configuração)
    # ------------------------------------------------------------------

    async def list_webhooks(self) -> list[dict]:
        """Lista webhooks configurados na conta."""
        data = await self._request(
            "GET",
            "/integrations/webhooks",
            rate_limit_resource="funnels_account",  # usa cota genérica
        )
        return data if isinstance(data, list) else data.get("webhooks", [])

    async def create_webhook(
        self,
        url: str,
        entity_type: str = "WEBHOOK.CONVERTED",
        *,
        auth_header: Optional[str] = None,
        auth_key: Optional[str] = None,
    ) -> dict:
        """
        Cria um webhook para receber eventos do RD Station.

        Args:
            url: URL HTTPS do endpoint que vai receber os eventos
            entity_type: "WEBHOOK.CONVERTED" ou "WEBHOOK.MARKED_OPPORTUNITY"
            auth_header: Nome do header de autenticação (opcional)
            auth_key: Valor do header de autenticação (opcional)
        """
        payload: dict[str, Any] = {
            "entity_type": entity_type,
            "url": url,
        }
        if auth_header and auth_key:
            payload["http_method"] = "POST"
            payload["auth_header"] = auth_header
            payload["auth_key"] = auth_key

        data = await self._request(
            "POST",
            "/integrations/webhooks",
            rate_limit_resource="funnels_account",
            json=payload,
        )
        return data

    # ------------------------------------------------------------------
    # CAMPOS PERSONALIZADOS
    # ------------------------------------------------------------------

    async def list_custom_fields(self) -> list[dict]:
        """Lista todos os campos (padrão e personalizados) da conta."""
        data = await self._request(
            "GET",
            "/platform/contacts/fields",
            rate_limit_resource="contacts_account",
        )
        return data if isinstance(data, list) else data.get("fields", [])

    async def create_custom_field(
        self,
        name: str,
        field_type: str = "STRING",
        *,
        label: Optional[str] = None,
        presentation_type: str = "TEXT_INPUT",
    ) -> dict:
        """
        Cria um campo personalizado.

        Args:
            name: api_identifier (ex: "cf_score_ia"). Sem palavras reservadas!
            field_type: STRING, NUMBER, etc.
            label: Label visível no RD Station
            presentation_type: TEXT_INPUT, TEXT_AREA, URL, etc.
        """
        display = label or name
        payload: dict[str, Any] = {
            "api_identifier": name,
            "data_type": field_type,
            "presentation_type": presentation_type,
            "name": {"pt-BR": display},
            "label": {"pt-BR": display},
        }

        data = await self._request(
            "POST",
            "/platform/contacts/fields",
            rate_limit_resource="contacts_account",
            json=payload,
        )
        return data

    # ------------------------------------------------------------------
    # EVENTOS / HISTÓRICO DO CONTATO
    # ------------------------------------------------------------------

    async def get_contact_events(
        self,
        uuid: str,
        *,
        event_type: str = "CONVERSION",
        page: int = 1,
        direction: str = "desc",
    ) -> list[dict]:
        """
        Lista eventos (conversões/oportunidades) de um contato.

        O endpoint requer o UUID do contato (não aceita email).
        Use get_contact() antes para obter o UUID.

        Args:
            uuid: UUID do contato no RD Station
            event_type: "CONVERSION" ou "OPPORTUNITY"
            page: Página (10 resultados por página)
            direction: "asc" ou "desc" (por data de criação)

        Returns:
            Lista de eventos com event_type, event_identifier,
            event_timestamp e payload.
        """
        data = await self._request(
            "GET",
            f"/platform/contacts/{uuid}/events",
            rate_limit_resource="events_account",
            params={
                "event_type": event_type,
                "order": "created_at",
                "direction": direction,
                "page": page,
            },
        )
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return data.get("events", data.get("items", []))
        return []

    async def get_contact_all_events(
        self,
        uuid: str,
        *,
        max_pages: int = 20,
    ) -> dict:
        """
        Busca conversões e oportunidades de um contato, paginando.

        Retorna dict com:
            conversions: lista de conversões
            opportunities: lista de oportunidades
            total_conversions: contagem
            total_opportunities: contagem
        """
        conversions: list[dict] = []
        opportunities: list[dict] = []

        # Busca conversões (até max_pages)
        for page in range(1, max_pages + 1):
            events = await self.get_contact_events(
                uuid, event_type="CONVERSION", page=page
            )
            if not events:
                break
            conversions.extend(events)
            if len(events) < 10:  # última página
                break

        # Busca oportunidades (geralmente poucas)
        for page in range(1, max_pages + 1):
            events = await self.get_contact_events(
                uuid, event_type="OPPORTUNITY", page=page
            )
            if not events:
                break
            opportunities.extend(events)
            if len(events) < 10:
                break

        return {
            "conversions": conversions,
            "opportunities": opportunities,
            "total_conversions": len(conversions),
            "total_opportunities": len(opportunities),
        }

    # ------------------------------------------------------------------
    # ANALYTICS
    # ------------------------------------------------------------------

    async def get_email_stats(self, **params) -> dict:
        """
        Estatísticas de email marketing.
        Limite: 60 req/hora (Pro e Advanced).
        """
        data = await self._request(
            "GET",
            "/platform/analytics/emails",
            rate_limit_resource="analytics",
            params=params,
        )
        return data

    async def get_funnel_stats(self, **params) -> dict:
        """Estatísticas do funil. Apenas plano Advanced."""
        data = await self._request(
            "GET",
            "/platform/analytics/funnel",
            rate_limit_resource="analytics",
            params=params,
        )
        return data

    async def get_conversion_stats(self, **params) -> dict:
        """Estatísticas de conversão. Apenas plano Advanced."""
        data = await self._request(
            "GET",
            "/platform/analytics/conversions",
            rate_limit_resource="analytics",
            params=params,
        )
        return data

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:
        """Encerra conexões HTTP."""
        if self._http and not self._http.is_closed:
            await self._http.aclose()
        await self.auth.close()

    async def __aenter__(self) -> "RDStationClient":
        return self

    async def __aexit__(self, *args) -> None:
        await self.close()
