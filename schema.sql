-- ============================================================
-- Supabase / PostgreSQL schema for Telegram Auto-Reaction Bot
-- Run this ONCE in the Supabase SQL Editor.
-- ============================================================

CREATE TABLE IF NOT EXISTS accounts (
    id          UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    owner_id    BIGINT       NOT NULL,
    name        TEXT         NOT NULL,
    phone       TEXT,
    api_id      TEXT         NOT NULL,
    api_hash    TEXT         NOT NULL,
    session_str TEXT         NOT NULL,
    status      TEXT         NOT NULL DEFAULT 'active'
                             CHECK (status IN ('active', 'failed')),
    is_master   BOOLEAN      NOT NULL DEFAULT false,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Ensure at most one master account at a time.
CREATE UNIQUE INDEX IF NOT EXISTS accounts_single_master
    ON accounts (is_master) WHERE is_master = true;

CREATE TABLE IF NOT EXISTS channels (
    id            UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    channel_id    BIGINT       NOT NULL UNIQUE,
    channel_name  TEXT         NOT NULL,
    added_by      BIGINT       NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS emojis (
    id          UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    emoji       TEXT         NOT NULL UNIQUE,
    added_by    BIGINT       NOT NULL,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS users_roles (
    id          UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    user_id     BIGINT       NOT NULL UNIQUE,
    role        TEXT         NOT NULL DEFAULT 'admin'
                             CHECK (role IN ('owner', 'admin', 'moderator')),
    added_by    BIGINT       NOT NULL,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS reaction_logs (
    id            UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    channel_id    BIGINT       NOT NULL,
    channel_name  TEXT         NOT NULL DEFAULT '',
    post_id       BIGINT       NOT NULL,
    account_id    TEXT         NOT NULL,
    account_name  TEXT         NOT NULL DEFAULT '',
    status        TEXT         NOT NULL CHECK (status IN ('success', 'failed')),
    error_message TEXT,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS reaction_logs_created_at_idx
    ON reaction_logs (created_at DESC);

CREATE INDEX IF NOT EXISTS reaction_logs_status_idx
    ON reaction_logs (status);

-- ============================================================
-- MIGRATION from v6 → v7 (add is_master column):
--
--   ALTER TABLE accounts
--       ADD COLUMN IF NOT EXISTS is_master BOOLEAN NOT NULL DEFAULT false;
--
--   CREATE UNIQUE INDEX IF NOT EXISTS accounts_single_master
--       ON accounts (is_master) WHERE is_master = true;
-- ============================================================

-- ============================================================
-- MIGRATION from v8 → v9 (add reaction_logs table):
--
--   CREATE TABLE IF NOT EXISTS reaction_logs (
--       id            UUID DEFAULT gen_random_uuid() PRIMARY KEY,
--       channel_id    BIGINT       NOT NULL,
--       channel_name  TEXT         NOT NULL DEFAULT '',
--       post_id       BIGINT       NOT NULL,
--       account_id    TEXT         NOT NULL,
--       account_name  TEXT         NOT NULL DEFAULT '',
--       status        TEXT         NOT NULL CHECK (status IN ('success', 'failed')),
--       error_message TEXT,
--       created_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
--   );
--
--   CREATE INDEX IF NOT EXISTS reaction_logs_created_at_idx
--       ON reaction_logs (created_at DESC);
--
--   CREATE INDEX IF NOT EXISTS reaction_logs_status_idx
--       ON reaction_logs (status);
-- ============================================================
