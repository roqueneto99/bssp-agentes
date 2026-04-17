"""
Autenticação para a API do RD Station Marketing.

Suporta dois modos:
1. API Key privada (simples — recomendado para integração interna)
2. OAuth2 (para apps publicados na App Store do RD Station)

Uso com API Key (modo padrão):
    auth = RDStationAuth(api_key="seu_token_privado")
    headers = await auth.get_headers()
    # → {"Authorization": "Bearer <token>", "Content-Type": "application/json"}

Uso com OAuth2:
    auth = RDStationAuth(
        client_id="...",
        client_secret="...",
        refresh_token="...",
    )
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

TOKEN_URL = "https://api.rd.services/auth/token"
REFRESH_MARGIN_SECONDS = 300


@dataclass
class TokenInfo:
    access_token: str
    refresh_token: str
    expires_at: float


class RDStationAuth:
    """
    Gerencia autenticação com o RD Station.

    Modo API Key:
        Usa o token privado direto — sem expiração, sem refresh.
        Ideal para integração interna como o nosso sistema de agentes.

    Modo OAuth2:
        Gerencia access_token com refresh automático a cada 24h.
        Necessário apenas para apps publicados na App Store.
    """

    def __init__(
        self,
        *,
        # Modo API Key (simples)
        api_key: Optional[str] = None,
        # Modo OAuth2
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        refresh_token: Optional[str] = None,
        access_token: Optional[str] = None,
        token_expires_in: int = 86400,
    ) -> None:
        self._api_key = api_key
        self._use_oauth = not api_key and bool(client_id and client_secret)

        # OAuth fields
        self.client_id = client_id or ""
        self.client_secret = client_secret or ""
        self._token: Optional[TokenInfo] = None
        self._lock = asyncio.Lock()
        self._http_client: Optional[httpx.AsyncClient] = None

        if self._use_oauth:
            if access_token and refresh_token:
                self._token = TokenInfo(
                    access_token=access_token,
                    refresh_token=refresh_token,
                    expires_at=time.monotonic() + token_expires_in,
                )
            elif refresh_token:
                self._token = TokenInfo(
                    access_token="",
                    refresh_token=refresh_token,
                    expires_at=0,
                )

        if not api_key and not self._use_oauth:
            raise ValueError(
                "Informe api_key (Token Privado) ou client_id + client_secret (OAuth)."
            )

    @property
    def mode(self) -> str:
        return "oauth" if self._use_oauth else "api_key"

    @property
    def _client(self) -> httpx.AsyncClient:
        if self._http_client is None or self._http_client.is_closed:
            self._http_client = httpx.AsyncClient(timeout=30.0)
        return self._http_client

    # ------------------------------------------------------------------
    # API Key mode
    # ------------------------------------------------------------------

    async def get_headers(self) -> dict[str, str]:
        """Retorna headers prontos para usar nas requisições."""
        if self._api_key:
            return {
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        # OAuth mode
        token = await self._get_oauth_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    # ------------------------------------------------------------------
    # OAuth mode
    # ------------------------------------------------------------------

    def _is_expired(self) -> bool:
        if self._token is None:
            return True
        return time.monotonic() >= (self._token.expires_at - REFRESH_MARGIN_SECONDS)

    async def _refresh_access_token(self) -> TokenInfo:
        if self._token is None:
            raise RuntimeError("Nenhum token OAuth disponível.")

        logger.info("Renovando access_token do RD Station...")
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self._token.refresh_token,
        }

        try:
            response = await self._client.post(TOKEN_URL, json=payload)
            response.raise_for_status()
            data = response.json()
            new_token = TokenInfo(
                access_token=data["access_token"],
                refresh_token=data.get("refresh_token", self._token.refresh_token),
                expires_at=time.monotonic() + data.get("expires_in", 86400),
            )
            logger.info("Token renovado com sucesso.")
            return new_token
        except httpx.HTTPStatusError as e:
            logger.error("Falha ao renovar token: HTTP %d", e.response.status_code)
            raise
        except Exception as e:
            logger.error("Erro inesperado ao renovar token: %s", e)
            raise

    async def _get_oauth_token(self) -> str:
        if not self._is_expired() and self._token:
            return self._token.access_token
        async with self._lock:
            if not self._is_expired() and self._token:
                return self._token.access_token
            self._token = await self._refresh_access_token()
            return self._token.access_token

    async def close(self) -> None:
        if self._http_client and not self._http_client.is_closed:
            await self._http_client.aclose()
