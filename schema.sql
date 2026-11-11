-- ============================================================
-- Supabase / PostgreSQL schema for Telegram Auto-Reaction Bot
-- Run this in the Supabase SQL Editor to create the tables.
-- ============================================================

-- Registered Telethon user-accounts
CREATE TABLE IF NOT EXISTS accounts (
    id          UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    owner_id    BIGINT       NOT NULL,          -- Telegram user-id of the owner
    name        TEXT         NOT NULL,           -- Friendly display name
    phone       TEXT,                            -- Phone number (optional)
    api_id      TEXT         NOT NULL,
    api_hash    TEXT         NOT NULL,
    session_str TEXT         NOT NULL,           -- Telethon StringSession
    status      TEXT         NOT NULL DEFAULT 'active'
                             CHECK (status IN ('active', 'failed')),
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Target channels the bot should auto-react in
CREATE TABLE IF NOT EXISTS channels (
    id            UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    channel_id    BIGINT       NOT NULL UNIQUE,  -- Telegram channel / chat id
    channel_name  TEXT         NOT NULL,
    added_by      BIGINT       NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Allowed reaction emojis
CREATE TABLE IF NOT EXISTS emojis (
    id          UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    emoji       TEXT         NOT NULL UNIQUE,
    added_by    BIGINT       NOT NULL,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
