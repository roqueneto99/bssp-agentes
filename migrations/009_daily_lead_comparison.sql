-- =================================================================
-- Migration 009 — Comparativo diário RD Station x Hablla
--
-- Cria:
--   1. leads_hablla_only      → leads encontrados no Hablla que NÃO casaram com nenhuma
--                               linha de `leads` (hoje invisíveis no painel).
--   2. daily_lead_comparison  → snapshot diário das divergências entre RD e Hablla
--                               por dimensão (stage, classification, last_interaction, origin).
--   3. daily_comparison_summary → totais agregados por dia/bucket (cache p/ a tela).
--   4. Colunas auxiliares em `leads`: chave usada no match + nível de confiança.
--
-- Tudo idempotente (IF NOT EXISTS).
-- =================================================================

BEGIN;

-- 1) Match metadata em leads --------------------------------------------------
ALTER TABLE leads
    ADD COLUMN IF NOT EXISTS hablla_match_key    VARCHAR(16),  -- 'external_id'|'email'|'phone'|'name'
    ADD COLUMN IF NOT EXISTS hablla_match_score  NUMERIC(4,3);  -- 0.000..1.000

CREATE INDEX IF NOT EXISTS ix_leads_hablla_match_key
    ON leads (hablla_match_key)
    WHERE hablla_match_key IS NOT NULL;

-- 2) Leads que SÓ existem no Hablla (não casaram com leads.id) ----------------
CREATE TABLE IF NOT EXISTS leads_hablla_only (
    id                  BIGSERIAL PRIMARY KEY,
    hablla_person_id    VARCHAR(64)  NOT NULL UNIQUE,
    email               VARCHAR(320),
    phone_e164          VARCHAR(32),
    name                VARCHAR(512),

    hablla_card_status      VARCHAR(16),
    hablla_em_atendimento   BOOLEAN,
    hablla_board_id         VARCHAR(64),
    hablla_list_id          VARCHAR(64),
    canal_preferido         VARCHAR(16),
    ultima_msg_em           TIMESTAMPTZ,

    first_seen_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw_data            JSONB
);

CREATE INDEX IF NOT EXISTS ix_hablla_only_email      ON leads_hablla_only (email);
CREATE INDEX IF NOT EXISTS ix_hablla_only_phone      ON leads_hablla_only (phone_e164);
CREATE INDEX IF NOT EXISTS ix_hablla_only_last_seen  ON leads_hablla_only (last_seen_at DESC);

-- 3) Snapshot diário das comparações ------------------------------------------
CREATE TABLE IF NOT EXISTS daily_lead_comparison (
    id                  BIGSERIAL PRIMARY KEY,
    snapshot_date       DATE        NOT NULL,
    lead_id             INTEGER     REFERENCES leads(id) ON DELETE CASCADE,
    hablla_only_id      BIGINT      REFERENCES leads_hablla_only(id) ON DELETE CASCADE,
    hablla_person_id    VARCHAR(64),

    bucket              VARCHAR(24) NOT NULL,
    -- 'matched_aligned' | 'matched_divergent' | 'only_rd' | 'only_hablla'

    match_key           VARCHAR(16),
    match_score         NUMERIC(4,3),

    diff_stage            BOOLEAN NOT NULL DEFAULT FALSE,
    diff_classification   BOOLEAN NOT NULL DEFAULT FALSE,
    diff_last_interaction BOOLEAN NOT NULL DEFAULT FALSE,
    diff_origin           BOOLEAN NOT NULL DEFAULT FALSE,

    rd_stage              VARCHAR(64),
    hablla_stage          VARCHAR(64),
    rd_classification     VARCHAR(32),
    hablla_classification VARCHAR(32),
    rd_last_interaction   TIMESTAMPTZ,
    hablla_last_interaction TIMESTAMPTZ,
    rd_origin             VARCHAR(255),
    hablla_origin         VARCHAR(255),

    updated_in_last_24h BOOLEAN NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT ck_dlc_bucket
        CHECK (bucket IN ('matched_aligned','matched_divergent','only_rd','only_hablla')),
    CONSTRAINT ck_dlc_target
        CHECK ((lead_id IS NOT NULL) OR (hablla_only_id IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS ix_dlc_date_bucket
    ON daily_lead_comparison (snapshot_date DESC, bucket);

CREATE INDEX IF NOT EXISTS ix_dlc_lead
    ON daily_lead_comparison (lead_id);

CREATE INDEX IF NOT EXISTS ix_dlc_updated
    ON daily_lead_comparison (snapshot_date DESC)
    WHERE updated_in_last_24h;

CREATE UNIQUE INDEX IF NOT EXISTS uq_dlc_unique_pair
    ON daily_lead_comparison (
        snapshot_date,
        COALESCE(lead_id, 0),
        COALESCE(hablla_only_id, 0)
    );

-- 4) Agregado diário ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS daily_comparison_summary (
    snapshot_date              DATE PRIMARY KEY,
    total                      INTEGER NOT NULL,
    matched_aligned            INTEGER NOT NULL,
    matched_divergent          INTEGER NOT NULL,
    only_rd                    INTEGER NOT NULL,
    only_hablla                INTEGER NOT NULL,
    divergent_stage            INTEGER NOT NULL DEFAULT 0,
    divergent_classification   INTEGER NOT NULL DEFAULT 0,
    divergent_last_interaction INTEGER NOT NULL DEFAULT 0,
    divergent_origin           INTEGER NOT NULL DEFAULT 0,
    matched_by_external_id     INTEGER NOT NULL DEFAULT 0,
    matched_by_email           INTEGER NOT NULL DEFAULT 0,
    matched_by_phone           INTEGER NOT NULL DEFAULT 0,
    matched_by_name            INTEGER NOT NULL DEFAULT 0,
    novos_24h                  INTEGER NOT NULL DEFAULT 0,
    atualizados_24h            INTEGER NOT NULL DEFAULT 0,
    computed_at                TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMIT;
