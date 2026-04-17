"""
Rate Limiter baseado em Token Bucket para a API do RD Station.

Respeita os limites por endpoint e por entidade (lead individual),
conforme documentação: https://developers.rdstation.com/reference/limite-de-requisicoes-da-api

Uso:
    limiter = RateLimiter(plan="pro")
    await limiter.acquire("contacts_account")   # espera se necessário
    await limiter.acquire("contacts_per_lead", entity_id="lead@email.com")
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuração de limites por plano
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class BucketConfig:
    """Configuração de um bucket de rate limit."""
    max_tokens: int
    refill_tokens: int
    refill_seconds: float   # intervalo de refill

    @property
    def refill_rate(self) -> float:
        """Tokens por segundo."""
        return self.refill_tokens / self.refill_seconds


# Limites documentados, mapeados por (recurso, plano)
# Chave do recurso → { plano → BucketConfig }
RATE_LIMITS: dict[str, dict[str, BucketConfig]] = {
    # --- Contatos (por conta) ---
    "contacts_account": {
        "light":    BucketConfig(max_tokens=120, refill_tokens=120, refill_seconds=60),
        "basic":    BucketConfig(max_tokens=120, refill_tokens=120, refill_seconds=60),
        "pro":      BucketConfig(max_tokens=120, refill_tokens=120, refill_seconds=60),
        "advanced": BucketConfig(max_tokens=500, refill_tokens=500, refill_seconds=60),
    },
    # --- Contatos (por lead individual — PATCH) ---
    "contacts_per_lead": {
        "light":    BucketConfig(max_tokens=24, refill_tokens=24, refill_seconds=86400),
        "basic":    BucketConfig(max_tokens=24, refill_tokens=24, refill_seconds=86400),
        "pro":      BucketConfig(max_tokens=24, refill_tokens=24, refill_seconds=86400),
        "advanced": BucketConfig(max_tokens=24, refill_tokens=24, refill_seconds=86400),
    },
    # --- Tags (por conta) ---
    "tags_account": {
        "light":    BucketConfig(max_tokens=15000, refill_tokens=15000, refill_seconds=86400),
        "basic":    BucketConfig(max_tokens=15000, refill_tokens=15000, refill_seconds=86400),
        "pro":      BucketConfig(max_tokens=15000, refill_tokens=15000, refill_seconds=86400),
        "advanced": BucketConfig(max_tokens=15000, refill_tokens=15000, refill_seconds=86400),
    },
    # --- Tags (por lead) ---
    "tags_per_lead": {
        "light":    BucketConfig(max_tokens=24, refill_tokens=24, refill_seconds=86400),
        "basic":    BucketConfig(max_tokens=24, refill_tokens=24, refill_seconds=86400),
        "pro":      BucketConfig(max_tokens=24, refill_tokens=24, refill_seconds=86400),
        "advanced": BucketConfig(max_tokens=24, refill_tokens=24, refill_seconds=86400),
    },
    # --- Eventos (por conta) ---
    "events_account": {
        "light":    BucketConfig(max_tokens=120, refill_tokens=120, refill_seconds=60),
        "basic":    BucketConfig(max_tokens=120, refill_tokens=120, refill_seconds=60),
        "pro":      BucketConfig(max_tokens=120, refill_tokens=120, refill_seconds=60),
        "advanced": BucketConfig(max_tokens=500, refill_tokens=500, refill_seconds=60),
    },
    # --- Eventos (por lead) ---
    "events_per_lead": {
        "light":    BucketConfig(max_tokens=120, refill_tokens=120, refill_seconds=86400),
        "basic":    BucketConfig(max_tokens=120, refill_tokens=120, refill_seconds=86400),
        "pro":      BucketConfig(max_tokens=120, refill_tokens=120, refill_seconds=86400),
        "advanced": BucketConfig(max_tokens=120, refill_tokens=120, refill_seconds=86400),
    },
    # --- Funis (por conta) ---
    "funnels_account": {
        "light":    BucketConfig(max_tokens=1_000_000, refill_tokens=1_000_000, refill_seconds=86400),
        "basic":    BucketConfig(max_tokens=1_000_000, refill_tokens=1_000_000, refill_seconds=86400),
        "pro":      BucketConfig(max_tokens=1_000_000, refill_tokens=1_000_000, refill_seconds=86400),
        "advanced": BucketConfig(max_tokens=1_000_000, refill_tokens=1_000_000, refill_seconds=86400),
    },
    # --- Funis (por lead) ---
    "funnels_per_lead": {
        "light":    BucketConfig(max_tokens=24, refill_tokens=24, refill_seconds=86400),
        "basic":    BucketConfig(max_tokens=24, refill_tokens=24, refill_seconds=86400),
        "pro":      BucketConfig(max_tokens=24, refill_tokens=24, refill_seconds=86400),
        "advanced": BucketConfig(max_tokens=24, refill_tokens=24, refill_seconds=86400),
    },
    # --- Segmentações ---
    "segmentations": {
        "light":    BucketConfig(max_tokens=120, refill_tokens=120, refill_seconds=60),
        "basic":    BucketConfig(max_tokens=120, refill_tokens=120, refill_seconds=60),
        "pro":      BucketConfig(max_tokens=120, refill_tokens=120, refill_seconds=60),
        "advanced": BucketConfig(max_tokens=240, refill_tokens=240, refill_seconds=60),
    },
    # --- Analytics ---
    "analytics": {
        "pro":      BucketConfig(max_tokens=60, refill_tokens=60, refill_seconds=3600),
        "advanced": BucketConfig(max_tokens=60, refill_tokens=60, refill_seconds=3600),
    },
    # --- Fluxos de Automação (inserir leads) ---
    "flows_insert": {
        "light":    BucketConfig(max_tokens=1,   refill_tokens=1,   refill_seconds=3600),
        "basic":    BucketConfig(max_tokens=1,   refill_tokens=1,   refill_seconds=3600),
        "pro":      BucketConfig(max_tokens=10,  refill_tokens=10,  refill_seconds=3600),
        "advanced": BucketConfig(max_tokens=100, refill_tokens=100, refill_seconds=3600),
    },
    # --- Fluxos de Automação (consultar) ---
    "flows_query": {
        "light":    BucketConfig(max_tokens=1,  refill_tokens=1,  refill_seconds=3600),
        "basic":    BucketConfig(max_tokens=1,  refill_tokens=1,  refill_seconds=3600),
        "pro":      BucketConfig(max_tokens=3,  refill_tokens=3,  refill_seconds=3600),
        "advanced": BucketConfig(max_tokens=12, refill_tokens=12, refill_seconds=3600),
    },
    # --- Fluxos de Automação (listar) ---
    "flows_list": {
        "light":    BucketConfig(max_tokens=40, refill_tokens=40, refill_seconds=3600),
        "basic":    BucketConfig(max_tokens=40, refill_tokens=40, refill_seconds=3600),
        "pro":      BucketConfig(max_tokens=40, refill_tokens=40, refill_seconds=3600),
        "advanced": BucketConfig(max_tokens=40, refill_tokens=40, refill_seconds=3600),
    },
}


# ---------------------------------------------------------------------------
# Token Bucket
# ---------------------------------------------------------------------------

class TokenBucket:
    """Token bucket com refill contínuo."""

    def __init__(self, config: BucketConfig) -> None:
        self.config = config
        self.tokens: float = config.max_tokens
        self.last_refill: float = time.monotonic()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self.last_refill
        added = elapsed * self.config.refill_rate
        self.tokens = min(self.config.max_tokens, self.tokens + added)
        self.last_refill = now

    async def acquire(self, tokens: int = 1) -> float:
        """
        Adquire tokens. Se não houver suficiente, aguarda.
        Retorna o tempo (em segundos) que esperou.
        """
        waited = 0.0
        async with self._lock:
            self._refill()
            while self.tokens < tokens:
                deficit = tokens - self.tokens
                wait_time = deficit / self.config.refill_rate
                # Margem de segurança de 50ms
                wait_time += 0.05
                logger.debug(
                    "Rate limit: aguardando %.2fs (tokens=%.1f, precisa=%d)",
                    wait_time, self.tokens, tokens,
                )
                self._lock.release()
                await asyncio.sleep(wait_time)
                waited += wait_time
                await self._lock.acquire()
                self._refill()
            self.tokens -= tokens
        return waited

    @property
    def available(self) -> float:
        self._refill()
        return self.tokens


# ---------------------------------------------------------------------------
# Rate Limiter (gerencia múltiplos buckets)
# ---------------------------------------------------------------------------

class RateLimiter:
    """
    Gerencia rate limiting para todos os endpoints da API do RD Station.

    Uso:
        limiter = RateLimiter(plan="pro")

        # Antes de chamar GET /contacts
        await limiter.acquire("contacts_account")

        # Antes de chamar PATCH /contacts (por lead)
        await limiter.acquire("contacts_per_lead", entity_id="lead@email.com")
    """

    def __init__(self, plan: str = "pro") -> None:
        self.plan = plan.lower()
        # Buckets por conta (um por recurso)
        self._account_buckets: dict[str, TokenBucket] = {}
        # Buckets por entidade (recurso → entity_id → bucket)
        self._entity_buckets: dict[str, dict[str, TokenBucket]] = {}

    def _get_config(self, resource: str) -> BucketConfig:
        limits = RATE_LIMITS.get(resource)
        if not limits:
            raise ValueError(f"Recurso desconhecido: {resource}")
        config = limits.get(self.plan)
        if not config:
            raise ValueError(
                f"Plano '{self.plan}' não suportado para recurso '{resource}'"
            )
        return config

    def _get_bucket(
        self, resource: str, entity_id: Optional[str] = None
    ) -> TokenBucket:
        if entity_id:
            if resource not in self._entity_buckets:
                self._entity_buckets[resource] = {}
            buckets = self._entity_buckets[resource]
            if entity_id not in buckets:
                buckets[entity_id] = TokenBucket(self._get_config(resource))
            return buckets[entity_id]
        else:
            if resource not in self._account_buckets:
                self._account_buckets[resource] = TokenBucket(
                    self._get_config(resource)
                )
            return self._account_buckets[resource]

    async def acquire(
        self,
        resource: str,
        entity_id: Optional[str] = None,
        tokens: int = 1,
    ) -> float:
        """
        Adquire permissão para fazer uma requisição.
        Bloqueia se necessário até o rate limit permitir.

        Args:
            resource: Nome do recurso (ex: "contacts_account")
            entity_id: ID da entidade para limites per-lead (email ou uuid)
            tokens: Número de tokens a consumir (normalmente 1)

        Returns:
            Tempo esperado em segundos (0.0 se não precisou esperar)
        """
        bucket = self._get_bucket(resource, entity_id)
        waited = await bucket.acquire(tokens)
        if waited > 0:
            logger.info(
                "Rate limit aplicado: recurso=%s entity=%s aguardou=%.2fs",
                resource, entity_id or "conta", waited,
            )
        return waited

    def cleanup_entity_buckets(self, max_age_seconds: float = 86400) -> int:
        """
        Remove buckets de entidades que não são usadas há muito tempo.
        Evita memory leak em processos long-running.
        """
        now = time.monotonic()
        removed = 0
        for resource in list(self._entity_buckets.keys()):
            buckets = self._entity_buckets[resource]
            for eid in list(buckets.keys()):
                age = now - buckets[eid].last_refill
                if age > max_age_seconds:
                    del buckets[eid]
                    removed += 1
        return removed
