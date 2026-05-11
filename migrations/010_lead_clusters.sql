-- =================================================================
-- Migration 010 — Clusterização de leads perdidos (only_rd)
--
-- Persiste resultado da análise feita por src/analytics/cluster_lost_leads.py.
-- Cada execução cria um run_id; os clusters daquele run e os assignments
-- ficam ligados. Múltiplos runs convivem (histórico de análises).
-- =================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS cluster_runs (
    id               BIGSERIAL PRIMARY KEY,
    since_date       DATE,                       -- recorte usado
    n_leads          INTEGER NOT NULL,
    n_clusters       INTEGER NOT NULL,
    silhouette_score NUMERIC(5,3),
    executado_em     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_cluster_runs_recent
    ON cluster_runs (executado_em DESC);

CREATE TABLE IF NOT EXISTS lead_clusters (
    id                  BIGSERIAL PRIMARY KEY,
    run_id              BIGINT NOT NULL REFERENCES cluster_runs(id) ON DELETE CASCADE,
    cluster_index       INTEGER NOT NULL,        -- 0..K-1 dentro do run
    label               VARCHAR(64) NOT NULL,    -- nome curto gerado pelo LLM
    hipotese            TEXT,                    -- explicação do comportamento
    copy_whatsapp       TEXT,                    -- mensagem sugerida WhatsApp
    copy_email_assunto  VARCHAR(255),
    copy_email_corpo    TEXT,
    n_leads             INTEGER NOT NULL,
    score_medio         NUMERIC(6,2),
    score_mediano       NUMERIC(6,2),
    lifecycle_dominante VARCHAR(64),
    dias_sem_interacao_mediana INTEGER,
    metadados           JSONB,                   -- features médias, originem, etc.
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_cluster_run_idx UNIQUE (run_id, cluster_index)
);

CREATE INDEX IF NOT EXISTS ix_lead_clusters_run
    ON lead_clusters (run_id);

CREATE TABLE IF NOT EXISTS lead_cluster_assignments (
    id          BIGSERIAL PRIMARY KEY,
    run_id      BIGINT NOT NULL REFERENCES cluster_runs(id) ON DELETE CASCADE,
    cluster_id  BIGINT NOT NULL REFERENCES lead_clusters(id) ON DELETE CASCADE,
    lead_id     INTEGER NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
    distance    NUMERIC(8,4),                    -- distância ao centróide
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_assignment_run_lead UNIQUE (run_id, lead_id)
);

CREATE INDEX IF NOT EXISTS ix_assign_cluster
    ON lead_cluster_assignments (cluster_id);

CREATE INDEX IF NOT EXISTS ix_assign_run_lead
    ON lead_cluster_assignments (run_id, lead_id);

COMMIT;
