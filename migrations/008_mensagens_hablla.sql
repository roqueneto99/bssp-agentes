-- =================================================================
-- Migration 008 — Mensagens trocadas via Hablla (webhook)
--
-- Armazena cada mensagem (in / out) recebida via webhook do Hablla.
-- Match com leads por hablla_person_id quando possível; fallback raw_payload.
-- Idempotência via UNIQUE em hablla_message_id.
-- =================================================================

CREATE TABLE IF NOT EXISTS mensagens_hablla (
    id                BIGSERIAL PRIMARY KEY,
    hablla_message_id VARCHAR(64) UNIQUE,         -- id da mensagem na Hablla
    hablla_service_id VARCHAR(64),                -- id do atendimento/service
    hablla_person_id  VARCHAR(64),                -- id da pessoa
    lead_id           BIGINT,                     -- FK para leads.id (resolvido por person_id)
    canal             VARCHAR(16),                -- whatsapp | email | telegram | instagram | webchat | ...
    direcao           VARCHAR(8),                 -- 'in' (cliente -> BSSP) | 'out' (BSSP -> cliente)
    autor_user_id     VARCHAR(64),                -- user da Hablla que enviou (se direcao=out)
    autor_nome        VARCHAR(255),               -- nome humano (resolvido via list_users)
    conteudo          TEXT,                       -- texto da msg (truncado se vier muito longo)
    midia_tipo        VARCHAR(32),                -- 'text' | 'image' | 'audio' | 'video' | 'document' | 'sticker'
    midia_url         TEXT,                       -- url da mídia (se houver)
    enviado_em        TIMESTAMPTZ,                -- data da msg na Hablla
    raw_payload       JSONB,                      -- payload completo, pra debugar shape
    received_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_ok      BOOLEAN NOT NULL DEFAULT TRUE,
    error_msg         TEXT,
    CONSTRAINT fk_msg_lead FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE SET NULL
);

-- Índice principal: render da conversa de um lead
CREATE INDEX IF NOT EXISTS ix_msgs_hablla_lead_enviado
    ON mensagens_hablla (lead_id, enviado_em DESC);

-- Inbox: últimas mensagens por person (latest_per_person)
CREATE INDEX IF NOT EXISTS ix_msgs_hablla_person_enviado
    ON mensagens_hablla (hablla_person_id, enviado_em DESC);

-- Por service (caso queira agrupar por atendimento)
CREATE INDEX IF NOT EXISTS ix_msgs_hablla_service
    ON mensagens_hablla (hablla_service_id);

-- Pra observabilidade do receiver
CREATE INDEX IF NOT EXISTS ix_msgs_hablla_received
    ON mensagens_hablla (received_at DESC);
