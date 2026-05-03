-- Suporte ao scoring automático (Squad 2 batch)
ALTER TABLE leads ADD COLUMN IF NOT EXISTS metodo_scoring TEXT;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS ultima_pontuacao_em TIMESTAMPTZ;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS cf_classificacao TEXT;

CREATE INDEX IF NOT EXISTS idx_leads_pontuacao_pendente
  ON leads (ultima_pontuacao_em NULLS FIRST, ultima_conversao_em DESC);
