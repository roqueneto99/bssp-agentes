"""
Modelos SQLAlchemy — tabelas leads e execucoes.

Projetado para armazenar os ~363K leads do RD Station
e os resultados de processamento dos Squads 1 e 2.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Lead(Base):
    """
    Espelho local dos contatos do RD Station.

    Campos core vindos da API + campos calculados pelos agentes.
    O campo `raw_data` guarda o JSON completo original para
    referencia futura sem perda de dados.
    """

    __tablename__ = "leads"

    # --- Identificacao ---
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    uuid: Mapped[Optional[str]] = mapped_column(String(64), unique=True, index=True)
    email: Mapped[str] = mapped_column(String(320), unique=True, nullable=False)
    name: Mapped[Optional[str]] = mapped_column(String(512))

    # --- Dados de contato ---
    personal_phone: Mapped[Optional[str]] = mapped_column(String(32))
    mobile_phone: Mapped[Optional[str]] = mapped_column(String(32))
    job_title: Mapped[Optional[str]] = mapped_column(String(256))
    company_name: Mapped[Optional[str]] = mapped_column(String(512))
    city: Mapped[Optional[str]] = mapped_column(String(128))
    state: Mapped[Optional[str]] = mapped_column(String(64))
    country: Mapped[Optional[str]] = mapped_column(String(64))
    linkedin: Mapped[Optional[str]] = mapped_column(String(512))
    website: Mapped[Optional[str]] = mapped_column(String(512))

    # --- Datas do RD Station ---
    rd_created_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_conversion_date: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    first_conversion_date: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # --- Tags e bases legais ---
    tags: Mapped[Optional[dict]] = mapped_column(JSONB, default=list)
    legal_bases: Mapped[Optional[dict]] = mapped_column(JSONB, default=list)

    # --- Custom fields RD Station (relevantes para o funil) ---
    lifecycle_stage: Mapped[Optional[str]] = mapped_column(String(64))
    fit_score: Mapped[Optional[str]] = mapped_column(String(16))
    interest_score: Mapped[Optional[str]] = mapped_column(String(16))

    # --- Resultados Squad 1 ---
    s1_temperatura: Mapped[Optional[str]] = mapped_column(String(32))
    s1_prioridade: Mapped[Optional[str]] = mapped_column(String(32))
    s1_area_principal: Mapped[Optional[str]] = mapped_column(String(128))
    s1_compliance: Mapped[Optional[str]] = mapped_column(String(32))
    s1_duplicados: Mapped[Optional[int]] = mapped_column(Integer, default=0)
    s1_pode_seguir_squad2: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)
    s1_processado_em: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # --- Resultados Squad 2 ---
    s2_score: Mapped[Optional[float]] = mapped_column(Float)
    s2_classificacao: Mapped[Optional[str]] = mapped_column(String(16))
    s2_rota: Mapped[Optional[str]] = mapped_column(String(64))
    s2_acoes: Mapped[Optional[dict]] = mapped_column(JSONB)
    s2_dimensoes: Mapped[Optional[dict]] = mapped_column(JSONB)
    s2_briefing: Mapped[Optional[str]] = mapped_column(Text)
    s2_tags: Mapped[Optional[dict]] = mapped_column(JSONB, default=list)
    s2_pode_seguir_squad3: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)
    s2_processado_em: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # --- JSON bruto original (backup completo) ---
    raw_data: Mapped[Optional[dict]] = mapped_column(JSONB)

    # --- Controle de sincronizacao ---
    synced_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    __table_args__ = (
        Index("ix_leads_rd_created_at", "rd_created_at"),
        Index("ix_leads_last_conversion", "last_conversion_date"),
        Index("ix_leads_s1_temperatura", "s1_temperatura"),
        Index("ix_leads_s2_classificacao", "s2_classificacao"),
        Index("ix_leads_s2_score", "s2_score"),
        Index("ix_leads_synced_at", "synced_at"),
    )

    def __repr__(self) -> str:
        return f"<Lead {self.email}>"


class Execucao(Base):
    """
    Historico de execucoes dos agentes por lead.

    Cada vez que um Squad roda para um lead, uma linha e inserida
    aqui com o resultado completo (JSON nos campos de dados).
    """

    __tablename__ = "execucoes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    email: Mapped[str] = mapped_column(String(320), nullable=False, index=True)
    tipo: Mapped[str] = mapped_column(String(32), nullable=False, default="squad1")
    # tipo: "squad1", "pipeline_completo", "squad2", etc.

    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    total_ms: Mapped[Optional[int]] = mapped_column(Integer)

    # Resultado completo dos agentes (JSON)
    agentes: Mapped[Optional[dict]] = mapped_column(JSONB)
    resumo_squad1: Mapped[Optional[dict]] = mapped_column(JSONB)
    resumo_squad2: Mapped[Optional[dict]] = mapped_column(JSONB)

    # Campos denormalizados para queries rapidas
    temperatura: Mapped[Optional[str]] = mapped_column(String(32))
    score: Mapped[Optional[float]] = mapped_column(Float)
    classificacao: Mapped[Optional[str]] = mapped_column(String(16))
    success: Mapped[bool] = mapped_column(Boolean, default=True)

    __table_args__ = (
        Index("ix_exec_email_ts", "email", "timestamp"),
        Index("ix_exec_tipo", "tipo"),
    )

    def __repr__(self) -> str:
        return f"<Execucao {self.email} {self.tipo} {self.timestamp}>"
