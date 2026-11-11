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
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

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

-- ============================================================
-- MIGRATION from v2 (single 'admin' role):
--
--   ALTER TABLE users_roles DROP CONSTRAINT users_roles_role_check;
--   ALTER TABLE users_roles ADD CONSTRAINT users_roles_role_check
--       CHECK (role IN ('owner', 'admin', 'moderator'));
-- ============================================================
