"""
Queries e repositório para as tabelas do Squad 3.

Tabelas operadas:
    - mensagens_squad3: cada mensagem disparada (ou agendada).
    - respostas_lead:   mensagens recebidas do lead.

Convenção do projeto: SQL via SQLAlchemy core async (não ORM),
seguindo o que já existe em src/database/queries.py. Mantém a
biblioteca leve para queries simples; ORM fica reservado para
quando o domínio crescer.

Uso típico (na MulticanalAgent):

    repo = MensagensRepository(session_factory)
    msg_id = await repo.criar_mensagem(...)
    # ... despacho ao SendGrid ...
    await repo.marcar_enviada(msg_id, external_id="...")

Para testes, use a interface MensagensRepoLike (Protocol) — todos
os métodos do MulticanalAgent dependem dessa interface, não da
implementação SQLAlchemy.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional, Protocol

from sqlalchemy import text

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Interface (Protocol) — usada pelos agentes para permitir mock em testes
# ---------------------------------------------------------------------------

class MensagensRepoLike(Protocol):
    async def criar_mensagem(
        self,
        *,
        email: str,
        canal: str,
        cadencia_nome: Optional[str],
        passo: int,
        nudge: Optional[str],
        template_id: Optional[str],
        template_versao: Optional[str],
        assunto: Optional[str],
        corpo: Optional[str],
        modelo_llm: Optional[str],
        prompt_hash: Optional[str],
        razao: Optional[str],
    ) -> Optional[int]:
        ...

    async def marcar_enviada(
        self, mensagem_id: int, *, external_id: Optional[str]
    ) -> None:
        ...

    async def marcar_falha(
        self, mensagem_id: int, *, erro: str
    ) -> None:
        ...

    async def marcar_skipped(
        self, mensagem_id: int, *, razao_skip: str
    ) -> None:
        ...

    async def aplicar_evento_externo(
        self,
        *,
        external_id: str,
        evento: str,
        ocorrido_em: datetime,
        razao: Optional[str] = None,
    ) -> bool:
        """
        Aplica um evento de webhook (delivered/open/click/bounce/etc.)
        atualizando timestamps e status. Retorna True se encontrou a linha.
        """
        ...


# ---------------------------------------------------------------------------
# Implementação SQLAlchemy
# ---------------------------------------------------------------------------

# Mapa de evento da SendGrid → coluna de timestamp + novo status
EVENTO_PARA_COLUNA = {
    "delivered":   ("delivered_at", "delivered"),
    "open":        ("opened_at",    "opened"),
    "click":       ("clicked_at",   "clicked"),
    "bounce":      ("bounced_at",   "bounced"),
    "dropped":     ("bounced_at",   "bounced"),
    "spamreport":  ("bounced_at",   "bounced"),
    "deferred":    (None,           None),       # ignorado (transitório)
    "processed":   (None,           None),       # ignorado (recebido pela SendGrid)
    "unsubscribe": (None,           "skipped"),  # apenas muda status
    # Resposta inbound: tratado em respostas_lead, não aqui.
}


class MensagensRepository:
    """Repositório real, baseado em SQLAlchemy core async."""

    def __init__(self, session_factory: Any) -> None:
        # session_factory: async_sessionmaker[AsyncSession] (de connection.py)
        self.session_factory = session_factory

    async def criar_mensagem(
        self,
        *,
        email: str,
        canal: str,
        cadencia_nome: Optional[str],
        passo: int,
        nudge: Optional[str],
        template_id: Optional[str],
        template_versao: Optional[str],
        assunto: Optional[str],
        corpo: Optional[str],
        modelo_llm: Optional[str],
        prompt_hash: Optional[str],
        razao: Optional[str],
    ) -> Optional[int]:
        """
        Cria uma linha em mensagens_squad3 com status='pending'.
        Retorna o id da linha. Se a chave única (email, cadencia, passo)
        já existir, retorna None (não dispara duas vezes a mesma mensagem).
        """
        sql = text("""
            INSERT INTO mensagens_squad3 (
                email, canal, cadencia_nome, passo, nudge, template_id,
                template_versao, assunto, corpo, modelo_llm, prompt_hash,
                razao, status
            )
            VALUES (
                :email, :canal, :cadencia_nome, :passo, :nudge, :template_id,
                :template_versao, :assunto, :corpo, :modelo_llm, :prompt_hash,
                :razao, 'pending'
            )
            ON CONFLICT (email, cadencia_id, passo) DO NOTHING
            RETURNING id
        """)
        async with self.session_factory() as session:
            try:
                result = await session.execute(sql, {
                    "email": email, "canal": canal,
                    "cadencia_nome": cadencia_nome, "passo": passo,
                    "nudge": nudge, "template_id": template_id,
                    "template_versao": template_versao,
                    "assunto": assunto, "corpo": corpo,
                    "modelo_llm": modelo_llm, "prompt_hash": prompt_hash,
                    "razao": razao,
                })
                row = result.first()
                await session.commit()
                return row[0] if row else None
            except Exception as e:
                await session.rollback()
                logger.error("criar_mensagem falhou para %s: %s", email, e)
                raise

    async def marcar_enviada(
        self, mensagem_id: int, *, external_id: Optional[str]
    ) -> None:
        sql = text("""
            UPDATE mensagens_squad3
               SET status = 'sent',
                   external_id = :external_id,
                   sent_at = NOW()
             WHERE id = :id
        """)
        async with self.session_factory() as session:
            await session.execute(sql, {"id": mensagem_id, "external_id": external_id})
            await session.commit()

    async def marcar_falha(
        self, mensagem_id: int, *, erro: str
    ) -> None:
        sql = text("""
            UPDATE mensagens_squad3
               SET status = 'failed',
                   erro = :erro
             WHERE id = :id
        """)
        async with self.session_factory() as session:
            await session.execute(sql, {"id": mensagem_id, "erro": erro})
            await session.commit()

    async def marcar_skipped(
        self, mensagem_id: int, *, razao_skip: str
    ) -> None:
        sql = text("""
            UPDATE mensagens_squad3
               SET status = 'skipped',
                   erro = :razao
             WHERE id = :id
        """)
        async with self.session_factory() as session:
            await session.execute(sql, {"id": mensagem_id, "razao": razao_skip})
            await session.commit()

    async def aplicar_evento_externo(
        self,
        *,
        external_id: str,
        evento: str,
        ocorrido_em: datetime,
        razao: Optional[str] = None,
    ) -> bool:
        coluna, novo_status = EVENTO_PARA_COLUNA.get(evento, (None, None))
        if coluna is None and novo_status is None:
            logger.debug("Evento ignorado: %s (external_id=%s)", evento, external_id)
            return False

        sets: list[str] = []
        params: dict[str, Any] = {"external_id": external_id}

        if coluna:
            sets.append(f"{coluna} = COALESCE({coluna}, :ts)")
            params["ts"] = ocorrido_em

        if novo_status:
            # Não regredir: nunca sobrescrever 'replied' por 'opened', por exemplo.
            sets.append("status = CASE "
                       "  WHEN status IN ('replied') THEN status "
                       "  WHEN :novo_status = 'bounced' THEN :novo_status "
                       "  ELSE :novo_status "
                       "END")
            params["novo_status"] = novo_status

        if razao:
            sets.append("erro = COALESCE(erro, :razao)")
            params["razao"] = razao

        if not sets:
            return False

        sql = text(f"""
            UPDATE mensagens_squad3
               SET {", ".join(sets)}
             WHERE external_id = :external_id
        """)

        async with self.session_factory() as session:
            result = await session.execute(sql, params)
            await session.commit()
            updated = result.rowcount or 0
            if updated:
                logger.info(
                    "SendGrid event aplicado: external_id=%s evento=%s linhas=%d",
                    external_id, evento, updated,
                )
            return updated > 0


# ---------------------------------------------------------------------------
# Repositório nulo (para dry-run quando DB não está disponível)
# ---------------------------------------------------------------------------

class NullMensagensRepo:
    """
    Implementação no-op. Usada quando DATABASE_URL não está configurada
    (ex.: rodando localmente sem Postgres). Retorna sucesso sem persistir.
    """

    async def criar_mensagem(self, **kwargs) -> Optional[int]:
        return None

    async def marcar_enviada(self, mensagem_id, *, external_id) -> None:
        return None

    async def marcar_falha(self, mensagem_id, *, erro) -> None:
        return None

    async def marcar_skipped(self, mensagem_id, *, razao_skip) -> None:
        return None

    async def aplicar_evento_externo(self, **kwargs) -> bool:
        return False
