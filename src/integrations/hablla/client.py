"""
Client para a API do Hablla (CRM conversacional).

O Hablla é usado pela BSSP como plataforma central de comunicação
multicanal (WhatsApp, Telegram, Instagram, Facebook, Email) e como
CRM complementar ao RD Station Marketing.

Endpoints cobertos (validados contra a API real):
    - Persons (contatos): busca, filtro por email, tags
    - Services (atendimentos/conversas): listagem, filtro por pessoa
    - Cards (deals/oportunidades): listagem, filtro por pessoa
    - Annotations (notas internas): listagem por pessoa/card/service
    - Tasks (tarefas): listagem
    - Tags: listagem
    - Organizations: listagem, detalhes

Autenticação: Token direto no header Authorization (sem prefixo Bearer)
Endpoints: /v1/workspaces/{workspace_id}/... e /v2/workspaces/{workspace_id}/...

Uso:
    client = HabllaClient(api_token="token", workspace_id="ws_id")
    pessoa = await client.search_person_by_email("lead@email.com")
    services = await client.list_services(person_id=pessoa["id"])
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)

BASE_URL = "https://api.hablla.com"

# Rate limit conservador (sem documentação oficial de limites)
MAX_REQUESTS_PER_SECOND = 5
REQUEST_INTERVAL = 1.0 / MAX_REQUESTS_PER_SECOND


class HabllaError(Exception):
    """Erro genérico da API do Hablla."""

    def __init__(self, status_code: int, message: str, raw: dict | None = None):
        self.status_code = status_code
        self.message = message
        self.raw = raw or {}
        super().__init__(f"Hablla API {status_code}: {message}")


class HabllaClient:
    """
    Client assíncrono para a API REST do Hablla.

    A API utiliza workspace_id no path de todos os endpoints.
    Autenticação é feita via token direto no header Authorization.
    """

    def __init__(
        self,
        api_token: str,
        *,
        workspace_id: str = "",
        base_url: str = BASE_URL,
        max_retries: int = 3,
    ) -> None:
        self.api_token = api_token
        self.workspace_id = workspace_id
        self.base_url = base_url.rstrip("/")
        self.max_retries = max_retries
        self._http: Optional[httpx.AsyncClient] = None
        self._last_request_time: float = 0.0
        self._lock = asyncio.Lock()

    def _ws_path(self, version: str, resource: str) -> str:
        """Monta path: /{version}/workspaces/{workspace_id}/{resource}"""
        return f"/{version}/workspaces/{self.workspace_id}/{resource}"

    @property
    def http(self) -> httpx.AsyncClient:
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=30.0,
                headers={
                    "Authorization": self.api_token,
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
            )
        return self._http

    async def _throttle(self) -> None:
        """Rate limiting simples — max N req/s."""
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_request_time
            if elapsed < REQUEST_INTERVAL:
                await asyncio.sleep(REQUEST_INTERVAL - elapsed)
            self._last_request_time = time.monotonic()

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: dict | None = None,
        params: dict | None = None,
    ) -> dict | list | None:
        """Requisição HTTP com retry e rate limiting."""
        await self._throttle()

        backoff = 1.0
        for attempt in range(1, self.max_retries + 1):
            try:
                response = await self.http.request(
                    method, path, json=json, params=params,
                )

                if response.status_code == 429:
                    wait = float(response.headers.get("Retry-After", backoff))
                    logger.warning(
                        "Hablla 429 rate limit em %s (tentativa %d/%d). "
                        "Aguardando %.1fs",
                        path, attempt, self.max_retries, wait,
                    )
                    if attempt < self.max_retries:
                        await asyncio.sleep(wait)
                        backoff *= 2
                        continue
                    raise HabllaError(429, "Rate limit excedido")

                if response.status_code == 204:
                    return None

                if response.status_code >= 400:
                    error_data = {}
                    try:
                        error_data = response.json()
                    except Exception:
                        pass
                    msg = error_data.get("message", response.text[:300])
                    if isinstance(msg, list):
                        msg = "; ".join(str(m) for m in msg)
                    raise HabllaError(
                        status_code=response.status_code,
                        message=str(msg),
                        raw=error_data,
                    )

                return response.json()

            except (httpx.ConnectError, httpx.ReadTimeout) as e:
                if attempt < self.max_retries:
                    logger.warning(
                        "Hablla erro de rede em %s (tentativa %d/%d): %s",
                        path, attempt, self.max_retries, e,
                    )
                    await asyncio.sleep(backoff)
                    backoff *= 2
                    continue
                raise HabllaError(0, f"Erro de rede: {e}")

        raise HabllaError(0, "Máximo de retries atingido")

    # ------------------------------------------------------------------
    # PESSOAS (Persons)
    # ------------------------------------------------------------------

    async def get_person(self, person_id: str) -> dict:
        """Busca uma pessoa pelo ID interno do Hablla."""
        path = self._ws_path("v1", f"persons/{person_id}")
        data = await self._request("GET", path)
        return data if isinstance(data, dict) else {}

    async def search_person_by_email(self, email: str) -> dict | None:
        """
        Busca uma pessoa pelo email.

        Usa o endpoint v2/persons com search para encontrar.
        Retorna None se não encontrada.
        """
        path = self._ws_path("v2", "persons")
        data = await self._request(
            "GET", path, params={"search": email, "page": 1, "limit": 5},
        )
        if not isinstance(data, dict):
            return None
        for person in data.get("results", []):
            emails = person.get("emails", [])
            for e in emails:
                if isinstance(e, dict) and e.get("email", "").lower() == email.lower():
                    return person
                elif isinstance(e, str) and e.lower() == email.lower():
                    return person
        return None

    async def search_person_by_phone(self, phone: str) -> dict | None:
        """Busca uma pessoa pelo telefone."""
        digits = "".join(c for c in phone if c.isdigit())
        path = self._ws_path("v2", "persons")
        data = await self._request(
            "GET", path, params={"search": digits, "page": 1, "limit": 5},
        )
        if not isinstance(data, dict):
            return None
        for person in data.get("results", []):
            phones = person.get("phones", [])
            for p in phones:
                p_digits = "".join(
                    c for c in (p.get("phone", "") if isinstance(p, dict) else str(p))
                    if c.isdigit()
                )
                if p_digits and (
                    p_digits == digits or p_digits.endswith(digits[-9:])
                ):
                    return person
        return None

    async def list_persons(
        self,
        *,
        search: str = "",
        page: int = 1,
        limit: int = 25,
    ) -> dict:
        """
        Lista pessoas com paginação.

        Retorna dict com keys: results, count, totalItems, page, limit, totalPages
        """
        path = self._ws_path("v2", "persons")
        params: dict[str, Any] = {"page": page, "limit": limit}
        if search:
            params["search"] = search
        data = await self._request("GET", path, params=params)
        return data if isinstance(data, dict) else {"results": [], "totalItems": 0}

    async def update_person(self, person_id: str, fields: dict) -> dict:
        """Atualiza campos de uma pessoa existente."""
        path = self._ws_path("v1", f"persons/{person_id}")
        data = await self._request("PUT", path, json=fields)
        return data if isinstance(data, dict) else {}

    async def add_person_tags(self, person_id: str, tags: list[str]) -> dict:
        """Adiciona tags a uma pessoa."""
        path = self._ws_path("v1", f"persons/{person_id}/add-tags")
        data = await self._request("PUT", path, json={"tags": tags})
        return data if isinstance(data, dict) else {}

    async def remove_person_tags(self, person_id: str, tags: list[str]) -> dict:
        """Remove tags de uma pessoa."""
        path = self._ws_path("v1", f"persons/{person_id}/remove-tags")
        data = await self._request("PUT", path, json={"tags": tags})
        return data if isinstance(data, dict) else {}

    # ------------------------------------------------------------------
    # SERVIÇOS / ATENDIMENTOS (Services = conversas multicanal)
    # ------------------------------------------------------------------

    async def list_services(
        self,
        *,
        person_id: str | None = None,
        status: str | None = None,
        page: int = 1,
        limit: int = 50,
    ) -> dict:
        """
        Lista services (atendimentos/conversas multicanal).

        Usa v2 que funciona com o token de integração.
        Status possíveis: in_attendance, in_bot, pending, finished, etc.

        Retorna dict com keys: results, count, totalItems, page, limit, totalPages
        """
        path = self._ws_path("v2", "services")
        params: dict[str, Any] = {"page": page, "limit": limit}
        if person_id:
            params["person_id"] = person_id
        if status:
            params["status"] = status
        data = await self._request("GET", path, params=params)
        return data if isinstance(data, dict) else {"results": [], "totalItems": 0}

    async def get_service(self, service_id: str) -> dict:
        """Busca detalhes de um service (atendimento) via v2."""
        path = self._ws_path("v2", f"services/{service_id}")
        data = await self._request("GET", path)
        return data if isinstance(data, dict) else {}

    # ------------------------------------------------------------------
    # CARTÕES / DEALS (Cards)
    # ------------------------------------------------------------------

    async def list_cards(
        self,
        *,
        person_id: str | None = None,
        board_id: str | None = None,
        list_id: str | None = None,
        status: str | None = None,
        page: int = 1,
        limit: int = 50,
    ) -> dict:
        """
        Lista cards/deals (requer person, board, list, service ou organization).

        Status possíveis: open, won, lost, etc.

        Retorna dict com keys: results, count, totalItems, page, limit, totalPages
        """
        path = self._ws_path("v2", "cards")
        params: dict[str, Any] = {"page": page, "limit": limit}
        if person_id:
            params["person"] = person_id
        if board_id:
            params["board"] = board_id
        if list_id:
            params["list"] = list_id
        if status:
            params["status"] = status
        data = await self._request("GET", path, params=params)
        return data if isinstance(data, dict) else {"results": [], "totalItems": 0}

    async def get_card(self, card_id: str) -> dict:
        """Busca detalhes de um card específico."""
        path = self._ws_path("v1", f"cards/{card_id}")
        data = await self._request("GET", path)
        return data if isinstance(data, dict) else {}

    async def create_card(self, card_data: dict) -> dict:
        """Cria um novo card/deal."""
        path = self._ws_path("v1", "cards")
        data = await self._request("POST", path, json=card_data)
        return data if isinstance(data, dict) else {}

    async def update_card(self, card_id: str, fields: dict) -> dict:
        """Atualiza um card existente."""
        path = self._ws_path("v1", f"cards/{card_id}")
        data = await self._request("PUT", path, json=fields)
        return data if isinstance(data, dict) else {}

    async def add_card_tags(self, card_id: str, tags: list[str]) -> dict:
        """Adiciona tags a um card."""
        path = self._ws_path("v1", f"cards/{card_id}/add-tags")
        data = await self._request("PUT", path, json={"tags": tags})
        return data if isinstance(data, dict) else {}

    # ------------------------------------------------------------------
    # ANOTAÇÕES (Annotations)
    # ------------------------------------------------------------------

    async def list_annotations(
        self,
        *,
        person_id: str | None = None,
        card_id: str | None = None,
        service_id: str | None = None,
        page: int = 1,
        limit: int = 50,
    ) -> dict:
        """
        Lista anotações (requer person, card, service ou organization).

        Retorna dict com keys: results, count, totalItems, page, limit, totalPages
        """
        path = self._ws_path("v1", "annotations")
        params: dict[str, Any] = {"page": page, "limit": limit}
        if person_id:
            params["person"] = person_id
        if card_id:
            params["card"] = card_id
        if service_id:
            params["service"] = service_id
        data = await self._request("GET", path, params=params)
        return data if isinstance(data, dict) else {"results": [], "totalItems": 0}

    async def create_annotation(
        self,
        *,
        content: str,
        person_id: str | None = None,
        card_id: str | None = None,
        service_id: str | None = None,
        annotation_type: str = "text",
    ) -> dict:
        """
        Cria uma anotação vinculada a uma pessoa, card ou service.

        annotation_type: text, image, video, document, json
        """
        path = self._ws_path("v1", "annotations")
        body: dict[str, Any] = {"content": content, "type": annotation_type}
        if person_id:
            body["person"] = person_id
        if card_id:
            body["card"] = card_id
        if service_id:
            body["service"] = service_id
        data = await self._request("POST", path, json=body)
        return data if isinstance(data, dict) else {}

    # ------------------------------------------------------------------
    # TASKS (Tarefas)
    # ------------------------------------------------------------------

    async def list_tasks(
        self,
        *,
        person_id: str | None = None,
        page: int = 1,
        limit: int = 50,
    ) -> dict:
        """Lista tarefas com paginação."""
        path = self._ws_path("v2", "tasks")
        params: dict[str, Any] = {"page": page, "limit": limit}
        if person_id:
            params["person"] = person_id
        data = await self._request("GET", path, params=params)
        return data if isinstance(data, dict) else {"results": [], "totalItems": 0}

    # ------------------------------------------------------------------
    # TAGS
    # ------------------------------------------------------------------

    async def list_tags(
        self, *, page: int = 1, limit: int = 100,
    ) -> list[dict]:
        """Lista todas as tags do workspace."""
        path = self._ws_path("v1", "tags")
        data = await self._request(
            "GET", path, params={"page": page, "limit": limit},
        )
        if isinstance(data, dict):
            return data.get("results", [])
        return data if isinstance(data, list) else []

    async def create_tag(self, name: str, color: str = "#6366f1") -> dict:
        """Cria uma nova tag no workspace."""
        path = self._ws_path("v1", "tags")
        data = await self._request("POST", path, json={"name": name, "color": color})
        return data if isinstance(data, dict) else {}

    async def resolve_tag_ids(
        self, tag_names: list[str], *, create_missing: bool = True,
    ) -> list[str]:
        """
        Resolve nomes de tags para IDs do Hablla.

        Se a tag não existe e create_missing=True, tenta criar.
        Retorna lista de IDs prontos para uso em add-tags.
        """
        # Buscar todas as tags existentes
        all_tags = await self.list_tags(limit=200)
        name_to_id: dict[str, str] = {}
        for t in all_tags:
            tname = (t.get("name") or "").lower().strip()
            tid = t.get("id") or t.get("_id") or ""
            if tname and tid:
                name_to_id[tname] = tid

        result_ids: list[str] = []
        for name in tag_names:
            key = name.lower().strip()
            if key in name_to_id:
                result_ids.append(name_to_id[key])
            elif create_missing:
                try:
                    new_tag = await self.create_tag(name)
                    tid = new_tag.get("id") or new_tag.get("_id") or ""
                    if tid:
                        result_ids.append(tid)
                        name_to_id[key] = tid
                except HabllaError as e:
                    if e.status_code == 401:
                        logger.info(
                            "Sem permissão para criar tag '%s' — ignorando", name,
                        )
                    else:
                        logger.warning("Erro ao criar tag '%s': %s", name, e)
                except Exception as e:
                    logger.warning("Erro ao criar tag '%s': %s", name, e)

        return result_ids

    # ------------------------------------------------------------------
    # ORGANIZATIONS
    # ------------------------------------------------------------------

    async def list_organizations(
        self, *, page: int = 1, limit: int = 25,
    ) -> dict:
        """Lista organizações."""
        path = self._ws_path("v1", "organizations")
        data = await self._request(
            "GET", path, params={"page": page, "limit": limit},
        )
        return data if isinstance(data, dict) else {"results": [], "totalItems": 0}

    async def get_organization(self, org_id: str) -> dict:
        """Busca uma organização pelo ID."""
        path = self._ws_path("v1", f"organizations/{org_id}")
        data = await self._request("GET", path)
        return data if isinstance(data, dict) else {}

    # ------------------------------------------------------------------
    # BOARDS / LISTS  (necessário pra resolver curso e etapa de um card)
    # ------------------------------------------------------------------

    async def list_boards(self, *, page: int = 1, limit: int = 100) -> list[dict]:
        """Lista todos os boards (cada curso costuma ser um board).
        Tenta v2 primeiro (token de integração só tem permissão lá), fallback v1."""
        for version in ("v2", "v1"):
            path = self._ws_path(version, "boards")
            try:
                data = await self._request(
                    "GET", path, params={"page": page, "limit": limit},
                )
                if isinstance(data, dict):
                    return data.get("results", [])
                return data if isinstance(data, list) else []
            except HabllaError as e:
                if e.status_code in (401, 403, 404):
                    logger.info("list_boards %s falhou: %s — tentando próximo", version, e.status_code)
                    continue
                raise
        return []

    async def list_lists(
        self,
        *,
        board_id: Optional[str] = None,
        page: int = 1,
        limit: int = 200,
    ) -> list[dict]:
        """Lista as lists (etapas). Hablla não tem /lists global; ficam
        aninhadas em /boards/{id}/lists. Se board_id for None, agrega
        de todos os boards conhecidos."""
        if board_id:
            for version in ("v2", "v1"):
                path = self._ws_path(version, f"boards/{board_id}/lists")
                try:
                    data = await self._request(
                        "GET", path, params={"page": page, "limit": limit},
                    )
                    if isinstance(data, dict):
                        return data.get("results", [])
                    return data if isinstance(data, list) else []
                except HabllaError as e:
                    if e.status_code in (401, 403, 404):
                        continue
                    raise
            return []

        # Sem board_id: percorre todos os boards e concatena as lists
        boards = await self.list_boards(limit=200)
        all_lists: list[dict] = []
        for b in boards:
            bid = str(b.get("id") or b.get("_id") or "")
            if not bid:
                continue
            try:
                lists = await self.list_lists(board_id=bid, limit=limit)
                # injeta board_id nas lists pra correlacionar depois
                for l in lists:
                    if isinstance(l, dict) and "board_id" not in l:
                        l["board_id"] = bid
                all_lists.extend(lists)
            except Exception as e:
                logger.warning("list_lists(board=%s) falhou: %s", bid, e)
        return all_lists

    # ------------------------------------------------------------------
    # USERS
    # ------------------------------------------------------------------

    async def list_users(self, *, limit_per_page: int = 100, max_pages: int = 20) -> list[dict]:
        """Lista usuários do workspace, paginado. Por padrão pega até 2000 users."""
        out: list[dict] = []
        for page in range(1, max_pages + 1):
            path = self._ws_path("v1", "users")
            data = await self._request(
                "GET", path, params={"page": page, "limit": limit_per_page},
            )
            page_items: list = []
            if isinstance(data, list):
                page_items = data
            elif isinstance(data, dict):
                page_items = data.get("results", []) or []
            if not page_items:
                break
            out.extend(page_items)
            if len(page_items) < limit_per_page:
                break
        return out

    # ------------------------------------------------------------------
    # UTILITÁRIOS
    # ------------------------------------------------------------------

    async def health_check(self) -> bool:
        """Verifica se a API está acessível e o token é válido."""
        try:
            path = self._ws_path("v2", "persons")
            await self._request("GET", path, params={"page": 1, "limit": 1})
            return True
        except Exception:
            return False

    async def close(self) -> None:
        """Fecha o client HTTP."""
        if self._http and not self._http.is_closed:
            await self._http.aclose()
