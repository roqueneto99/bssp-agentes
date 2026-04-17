"""
Classe base para agentes de IA da BSSP.

Interface genérica de LLM — permite trocar entre Claude, GPT, ou qualquer
outro provider sem alterar a lógica dos agentes.

Uso:
    # Com Claude
    provider = LLMProvider(provider="anthropic", api_key="...", model="claude-sonnet-4-20250514")

    # Com OpenAI
    provider = LLMProvider(provider="openai", api_key="...", model="gpt-4o-mini")

    # O agente usa o provider de forma transparente
    agent = EnrichmentAgent(llm=provider, rdstation=client)
    result = await agent.run(contact_email="lead@email.com")
"""

from __future__ import annotations

import json
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# LLM Provider (interface genérica)
# ---------------------------------------------------------------------------

class LLMProviderType(str, Enum):
    ANTHROPIC = "anthropic"
    OPENAI = "openai"


@dataclass
class LLMMessage:
    role: str  # "system", "user", "assistant"
    content: str


@dataclass
class LLMResponse:
    content: str
    model: str
    usage: dict = field(default_factory=dict)  # tokens usados
    latency_ms: float = 0.0


class LLMProvider:
    """
    Provider genérico de LLM. Encapsula a chamada à API do modelo
    para que os agentes não dependam de um provider específico.
    """

    def __init__(
        self,
        provider: str = "anthropic",
        api_key: str = "",
        model: str = "",
        max_tokens: int = 2048,
        temperature: float = 0.3,
    ) -> None:
        self.provider_type = LLMProviderType(provider.lower())
        self.api_key = api_key
        self.max_tokens = max_tokens
        self.temperature = temperature
        self._http: Optional[httpx.AsyncClient] = None

        # Defaults por provider
        if not model:
            if self.provider_type == LLMProviderType.ANTHROPIC:
                self.model = "claude-sonnet-4-20250514"
            else:
                self.model = "gpt-4o-mini"
        else:
            self.model = model

    @property
    def http(self) -> httpx.AsyncClient:
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(timeout=60.0)
        return self._http

    async def complete(
        self,
        messages: list[LLMMessage],
        *,
        system: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        response_format: Optional[str] = None,  # "json" para forçar JSON
    ) -> LLMResponse:
        """
        Envia mensagens ao LLM e retorna a resposta.
        Funciona com Anthropic e OpenAI de forma transparente.
        """
        temp = temperature if temperature is not None else self.temperature
        tokens = max_tokens or self.max_tokens
        start = time.monotonic()

        if self.provider_type == LLMProviderType.ANTHROPIC:
            result = await self._call_anthropic(messages, system, temp, tokens)
        else:
            result = await self._call_openai(messages, system, temp, tokens, response_format)

        result.latency_ms = (time.monotonic() - start) * 1000
        logger.info(
            "LLM call: provider=%s model=%s tokens_in=%s tokens_out=%s latency=%.0fms",
            self.provider_type.value, self.model,
            result.usage.get("input", "?"), result.usage.get("output", "?"),
            result.latency_ms,
        )
        return result

    async def complete_json(
        self,
        messages: list[LLMMessage],
        *,
        system: Optional[str] = None,
        temperature: Optional[float] = None,
    ) -> dict:
        """Convenience: chama o LLM e parseia a resposta como JSON."""
        response = await self.complete(
            messages, system=system, temperature=temperature, response_format="json"
        )
        # Tenta extrair JSON da resposta
        text = response.content.strip()
        # Remove markdown code blocks se presentes
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(lines[1:-1]) if len(lines) > 2 else text
        return json.loads(text)

    async def _call_anthropic(
        self,
        messages: list[LLMMessage],
        system: Optional[str],
        temperature: float,
        max_tokens: int,
    ) -> LLMResponse:
        """Chamada à API da Anthropic (Claude)."""
        api_messages = [{"role": m.role, "content": m.content} for m in messages if m.role != "system"]

        body: dict[str, Any] = {
            "model": self.model,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "messages": api_messages,
        }
        if system:
            body["system"] = system

        resp = await self.http.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": self.api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json=body,
        )
        resp.raise_for_status()
        data = resp.json()

        content = ""
        for block in data.get("content", []):
            if block.get("type") == "text":
                content += block["text"]

        return LLMResponse(
            content=content,
            model=data.get("model", self.model),
            usage={
                "input": data.get("usage", {}).get("input_tokens", 0),
                "output": data.get("usage", {}).get("output_tokens", 0),
            },
        )

    async def _call_openai(
        self,
        messages: list[LLMMessage],
        system: Optional[str],
        temperature: float,
        max_tokens: int,
        response_format: Optional[str] = None,
    ) -> LLMResponse:
        """Chamada à API da OpenAI (GPT)."""
        api_messages = []
        if system:
            api_messages.append({"role": "system", "content": system})
        for m in messages:
            if m.role != "system":
                api_messages.append({"role": m.role, "content": m.content})

        body: dict[str, Any] = {
            "model": self.model,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "messages": api_messages,
        }
        if response_format == "json":
            body["response_format"] = {"type": "json_object"}

        resp = await self.http.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            json=body,
        )
        resp.raise_for_status()
        data = resp.json()

        choice = data["choices"][0]
        return LLMResponse(
            content=choice["message"]["content"],
            model=data.get("model", self.model),
            usage={
                "input": data.get("usage", {}).get("prompt_tokens", 0),
                "output": data.get("usage", {}).get("completion_tokens", 0),
            },
        )

    async def close(self) -> None:
        if self._http and not self._http.is_closed:
            await self._http.aclose()


# ---------------------------------------------------------------------------
# Agent Base
# ---------------------------------------------------------------------------

@dataclass
class AgentResult:
    """Resultado de uma execução de agente."""
    success: bool
    agent_name: str
    contact_email: str
    data: dict = field(default_factory=dict)
    error: Optional[str] = None
    duration_ms: float = 0.0
    llm_calls: int = 0

    def __repr__(self) -> str:
        status = "OK" if self.success else f"ERRO: {self.error}"
        return f"AgentResult({self.agent_name}, {self.contact_email}, {status})"


class BaseAgent(ABC):
    """
    Classe base para todos os agentes da BSSP.

    Cada agente (squad) herda desta classe e implementa:
    - analyze(): lógica principal do agente
    - get_system_prompt(): prompt de sistema para o LLM

    O método run() cuida do ciclo de vida:
    1. Busca dados do contato no RD Station
    2. Chama analyze() (implementado pelo squad)
    3. Registra resultado e métricas
    """

    agent_name: str = "base"

    def __init__(
        self,
        llm: LLMProvider,
        rdstation: Any,  # RDStationClient (import circular se tipar)
    ) -> None:
        self.llm = llm
        self.rdstation = rdstation

    @abstractmethod
    async def analyze(self, contact_data: dict) -> dict:
        """
        Lógica principal do agente. Recebe os dados do contato
        e retorna um dict com as ações/decisões tomadas.
        """
        ...

    @abstractmethod
    def get_system_prompt(self) -> str:
        """Retorna o system prompt específico deste agente."""
        ...

    async def run(self, contact_email: str) -> AgentResult:
        """
        Executa o agente para um contato específico.

        Fluxo:
        1. Busca contato no RD Station
        2. Executa analyze() (lógica do squad)
        3. Retorna resultado estruturado
        """
        start = time.monotonic()
        try:
            # Busca dados completos do contato
            contact = await self.rdstation.get_contact(email=contact_email)
            contact_data = contact.to_api_payload()
            contact_data["uuid"] = contact.uuid
            contact_data["email"] = contact.email

            # Busca estágio do funil
            try:
                funnel = await self.rdstation.get_funnel_stage(contact_email)
                contact_data["funnel"] = funnel
            except Exception:
                contact_data["funnel"] = {}

            # Executa lógica do agente
            result_data = await self.analyze(contact_data)

            duration = (time.monotonic() - start) * 1000
            logger.info(
                "Agent %s concluído para %s em %.0fms",
                self.agent_name, contact_email, duration,
            )

            return AgentResult(
                success=True,
                agent_name=self.agent_name,
                contact_email=contact_email,
                data=result_data,
                duration_ms=duration,
            )

        except Exception as e:
            duration = (time.monotonic() - start) * 1000
            logger.error(
                "Agent %s falhou para %s: %s",
                self.agent_name, contact_email, e,
            )
            return AgentResult(
                success=False,
                agent_name=self.agent_name,
                contact_email=contact_email,
                error=str(e),
                duration_ms=duration,
            )
