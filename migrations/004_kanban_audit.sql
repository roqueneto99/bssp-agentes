-- Suporte ao Kanban pipeline-first (drag-and-drop com auditoria)
ALTER TABLE leads ADD COLUMN IF NOT EXISTS classificacao_origem TEXT
    DEFAULT 'automatico' CHECK (classificacao_origem IN ('automatico','manual'));
ALTER TABLE leads ADD COLUMN IF NOT EXISTS classificacao_atualizada_em TIMESTAMPTZ;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS classificacao_atualizada_por UUID;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS origem TEXT;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS origem_label TEXT;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS lgpd_conforme BOOLEAN DEFAULT false;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS consultor TEXT;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS matricula_curso TEXT;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS ultima_interacao_em TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS leads_auditoria (
    id BIGSERIAL PRIMARY KEY,
    lead_id UUID NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
    evento TEXT NOT NULL,
    dados_antes JSONB,
    dados_depois JSONB,
    motivo TEXT,
    autor_id UUID,
    autor_email TEXT,
    ocorreu_em TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_leads_auditoria_lead_id ON leads_auditoria(lead_id, ocorreu_em DESC);
CREATE INDEX IF NOT EXISTS idx_leads_classificacao_interacao
    ON leads (cf_classificacao, ultima_interacao_em DESC NULLS LAST);
