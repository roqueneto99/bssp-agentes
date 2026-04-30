-- =========================================================
-- BSSP Agentes — Migration 002: Squad 3 (Comunicação Inteligente)
-- PostgreSQL 15+
--
-- Cria as tabelas operacionais do Squad 3 e estende a tabela
-- execucoes para registrar resumo_squad3 + tipo='squad3'.
--
-- Idempotente: pode rodar várias vezes sem erro.
--
-- Uso:
--   psql $DATABASE_URL -f migrations/002_squad3_communication.sql
-- =========================================================

BEGIN;

-- -------------------------------------------------------
-- Tabela: cadencias
-- Sequência de mensagens parametrizada por rota do Squad 2.
-- Cada cadência tem um array JSONB de passos com tempo, canal,
-- nudge primário e template_id sugerido.
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS cadencias (
    id              SERIAL PRIMARY KEY,
    nome            VARCHAR(64)  NOT NULL UNIQUE,
    rota            VARCHAR(64),                 -- rota do Squad 2 que aciona esta cadência
    descricao       TEXT,
    passos          JSONB        NOT NULL,       -- [{ordem, dia, canal, nudge, template_id, escala}]
    janela_total_d  INTEGER,                     -- duração total prevista, em dias
    ativo           BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_cadencias_rota  ON cadencias (rota);
CREATE INDEX IF NOT EXISTS ix_cadencias_ativo ON cadencias (ativo);

-- -------------------------------------------------------
-- Tabela: mensagens_squad3
-- Cada mensagem disparada (ou agendada) pelo Squad 3.
-- Inclui auditoria de prompt para reproduzir o que foi enviado.
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS mensagens_squad3 (
    id              SERIAL PRIMARY KEY,
    email           VARCHAR(320) NOT NULL,
    lead_id         INTEGER REFERENCES leads(id) ON DELETE SET NULL,

    cadencia_id     INTEGER REFERENCES cadencias(id) ON DELETE SET NULL,
    cadencia_nome   VARCHAR(64),                 -- denormalizado para queries rápidas
    passo           INTEGER NOT NULL,            -- ordem dentro da cadência (0..N)
    canal           VARCHAR(16) NOT NULL,        -- 'email' | 'whatsapp' | 'sms'
    nudge           VARCHAR(32),                 -- 'prova_social', 'escassez', 'ancoragem', etc.
    template_id     VARCHAR(64),
    template_versao VARCHAR(16),

    -- Conteúdo final enviado (assunto + corpo)
    assunto         VARCHAR(512),
    corpo           TEXT,

    -- Auditoria do prompt usado para gerar a mensagem
    modelo_llm      VARCHAR(64),
    prompt_hash     VARCHAR(64),                 -- hash do prompt+inputs (reprodutibilidade)
    razao           TEXT,                        -- explicação textual de por que este nudge

    -- Telemetria do envio
    status          VARCHAR(16) NOT NULL DEFAULT 'pending',
                    -- 'pending'|'sent'|'delivered'|'opened'|'clicked'|'replied'|'bounced'|'failed'|'skipped'
    external_id     VARCHAR(128),                -- SendGrid/Hablla message_id

    -- Timestamps de cada evento
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at         TIMESTAMPTZ,
    delivered_at    TIMESTAMPTZ,
    opened_at       TIMESTAMPTZ,
    clicked_at      TIMESTAMPTZ,
    replied_at      TIMESTAMPTZ,
    bounced_at      TIMESTAMPTZ,

    erro            TEXT
);

CREATE INDEX IF NOT EXISTS ix_msg3_email_ts        ON mensagens_squad3 (email, created_at);
CREATE INDEX IF NOT EXISTS ix_msg3_status          ON mensagens_squad3 (status);
CREATE INDEX IF NOT EXISTS ix_msg3_canal           ON mensagens_squad3 (canal);
CREATE INDEX IF NOT EXISTS ix_msg3_cadencia        ON mensagens_squad3 (cadencia_id);
CREATE INDEX IF NOT EXISTS ix_msg3_external_id     ON mensagens_squad3 (external_id);

-- Chave de idempotência: impede envios duplicados (mesmo email, cadência, passo)
CREATE UNIQUE INDEX IF NOT EXISTS ux_msg3_email_cad_passo
    ON mensagens_squad3 (email, cadencia_id, passo);

-- -------------------------------------------------------
-- Tabela: respostas_lead
-- Mensagens recebidas do lead (e-mail respondido, WhatsApp recebido).
-- Alimentam o Agente de Qualificação Conversacional.
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS respostas_lead (
    id              SERIAL PRIMARY KEY,
    email           VARCHAR(320) NOT NULL,
    lead_id         INTEGER REFERENCES leads(id) ON DELETE SET NULL,
    mensagem_id     INTEGER REFERENCES mensagens_squad3(id) ON DELETE SET NULL,

    canal           VARCHAR(16) NOT NULL,        -- 'email' | 'whatsapp' | 'sms'
    conteudo        TEXT,

    -- Análise NLU/BANT do conteúdo
    intencao            VARCHAR(64),             -- 'duvida_curso' | 'objecao_preco' | 'pediu_humano' | etc.
    maturidade_bant     INTEGER,                 -- 1..5 (BANT adaptado)
    sinais_extraidos    JSONB DEFAULT '[]'::jsonb,

    -- Trigger de reciclagem para Squad 2
    requer_rescoring    BOOLEAN NOT NULL DEFAULT FALSE,

    recebida_em     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processada_em   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_resp_email_ts      ON respostas_lead (email, recebida_em);
CREATE INDEX IF NOT EXISTS ix_resp_intencao      ON respostas_lead (intencao);
CREATE INDEX IF NOT EXISTS ix_resp_rescoring     ON respostas_lead (requer_rescoring);

-- -------------------------------------------------------
-- Estender tabela execucoes para o Squad 3
-- -------------------------------------------------------
ALTER TABLE execucoes
    ADD COLUMN IF NOT EXISTS resumo_squad3 JSONB;

-- -------------------------------------------------------
-- Estender tabela leads com campos denormalizados do Squad 3
-- (mesmo padrão dos campos s1_*, s2_* já existentes)
-- -------------------------------------------------------
ALTER TABLE leads
    ADD COLUMN IF NOT EXISTS s3_estagio          VARCHAR(64),
    ADD COLUMN IF NOT EXISTS s3_cadencia_atual   VARCHAR(64),
    ADD COLUMN IF NOT EXISTS s3_ultimo_nudge     VARCHAR(32),
    ADD COLUMN IF NOT EXISTS s3_canal_preferido  VARCHAR(16),
    ADD COLUMN IF NOT EXISTS s3_status           VARCHAR(32),
                            -- 'ativo'|'pausado'|'recuperacao'|'desistente'|'concluido'
    ADD COLUMN IF NOT EXISTS s3_msgs_enviadas    INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS s3_ultima_msg_em    TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS s3_ultima_resposta_em TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS s3_processado_em    TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS ix_leads_s3_status   ON leads (s3_status);
CREATE INDEX IF NOT EXISTS ix_leads_s3_cadencia ON leads (s3_cadencia_atual);

-- -------------------------------------------------------
-- Trigger updated_at para cadencias
-- -------------------------------------------------------
DROP TRIGGER IF EXISTS trg_cadencias_updated_at ON cadencias;
CREATE TRIGGER trg_cadencias_updated_at
    BEFORE UPDATE ON cadencias
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();

-- -------------------------------------------------------
-- Seeds: cadências base
-- (passos são rascunhos — serão refinados nas sprints S3 e S4)
-- -------------------------------------------------------
INSERT INTO cadencias (nome, rota, descricao, passos, janela_total_d)
VALUES
    ('mql_nurture',
     'mql_nurture',
     'Cadência de nutrição avançada para MQL (5 mensagens em 10 dias).',
     $JSON$[
        {"ordem": 0, "dia": 0,  "canal": "email",    "nudge": "boas_vindas",   "template_id": "mql_v1_step0"},
        {"ordem": 1, "dia": 2,  "canal": "email",    "nudge": "prova_social",  "template_id": "mql_v1_step1"},
        {"ordem": 2, "dia": 4,  "canal": "whatsapp", "nudge": "depoimento",    "template_id": "mql_v1_step2"},
        {"ordem": 3, "dia": 7,  "canal": "email",    "nudge": "escassez",      "template_id": "mql_v1_step3"},
        {"ordem": 4, "dia": 10, "canal": "whatsapp", "nudge": "fechamento",    "template_id": "mql_v1_step4"}
     ]$JSON$::jsonb,
     10),

    ('sal_nurture',
     'sal_nurture',
     'Cadência educativa para SAL (4 mensagens em 21 dias).',
     $JSON$[
        {"ordem": 0, "dia": 0,  "canal": "email", "nudge": "educativa",    "template_id": "sal_v1_step0"},
        {"ordem": 1, "dia": 7,  "canal": "email", "nudge": "prova_social", "template_id": "sal_v1_step1"},
        {"ordem": 2, "dia": 14, "canal": "email", "nudge": "ancoragem",    "template_id": "sal_v1_step2"},
        {"ordem": 3, "dia": 21, "canal": "email", "nudge": "escassez",     "template_id": "sal_v1_step3"}
     ]$JSON$::jsonb,
     21),

    ('cold_recycle',
     'cold_recycle',
     'Re-engajamento leve para COLD (2 mensagens em 30-60 dias).',
     $JSON$[
        {"ordem": 0, "dia": 30, "canal": "email", "nudge": "loss_aversion", "template_id": "cold_v1_step0"},
        {"ordem": 1, "dia": 60, "canal": "email", "nudge": "ancoragem",     "template_id": "cold_v1_step1"}
     ]$JSON$::jsonb,
     60),

    ('recuperacao_default',
     NULL,
     'Sequência de retomada após abandono (3 mensagens em 7 dias, cross-canal).',
     $JSON$[
        {"ordem": 0, "dia": 1, "canal": "whatsapp", "nudge": "fricao",        "template_id": "rec_v1_step0"},
        {"ordem": 1, "dia": 3, "canal": "email",    "nudge": "loss_aversion", "template_id": "rec_v1_step1"},
        {"ordem": 2, "dia": 7, "canal": "whatsapp", "nudge": "ancoragem",     "template_id": "rec_v1_step2"}
     ]$JSON$::jsonb,
     7)
ON CONFLICT (nome) DO NOTHING;

COMMIT;
