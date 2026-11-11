# Telegram Auto-Reaction Bot

A fully inline-keyboard-driven Telegram bot that automatically reacts to messages in target channels using multiple Telethon user-accounts, with a Supabase-backed database and role-based admin management.

## Features

| Area | Details |
|---|---|
| **UI** | 100 % Inline Keyboard – no text commands needed for navigation |
| **Role-Based Access** | Permanent Owner (full control + admin management) and Admins (accounts, channels, emojis) |
| **Admin Panel** | 👑 Owner-only panel to add/revoke admins via inline buttons |
| **Help Guide** | 📖 In-app step-by-step user manual for all authorized users |
| **Accounts** | Register multiple Telethon user-accounts (name, phone, API ID, API Hash, session string) |
| **Channels** | Add / remove target channels dynamically |
| **Emojis** | Manage the pool of allowed reaction emojis |
| **Reactor (v2)** | Non-blocking engine with peer caching, per-account flood cooldowns, fire-and-forget sweeps, and concurrent startup |
| **DB Caching** | TTL cache on channels, emojis, and admin lookups to eliminate response lag |
| **Health** | Lightweight `/healthz` endpoint for Render / Railway uptime checks |

---

## Architecture

```
main.py                 Entry point – wires everything together
├── config.py           Env-var loader (BOT_TOKEN, SUPABASE_*, PERMANENT_OWNER_ID)
├── database.py         Supabase CRUD + in-memory TTL cache
├── bot_handlers.py     python-telegram-bot inline-keyboard UI + admin panel + help
├── reactor.py          Telethon auto-reaction engine (v2, fully async)
└── health.py           aiohttp /healthz server
```

---

## Quick Start

### 1. Supabase Setup

1. Create a new Supabase project.
2. Open the **SQL Editor** and run [`schema.sql`](schema.sql) to create the four tables (`accounts`, `channels`, `emojis`, `users_roles`).
3. Copy your **Project URL** and **anon/service key**.

### 2. Environment Variables

```bash
cp .env.example .env
```

| Variable | Description |
|---|---|
| `BOT_TOKEN` | Telegram bot token from [@BotFather](https://t.me/BotFather) |
| `SUPABASE_URL` | Supabase project URL |
| `SUPABASE_KEY` | Supabase anon or service-role key |
| `PERMANENT_OWNER_ID` | Telegram user ID of the super-admin (default `6580282480`) |
| `PORT` | Port for the health-check endpoint (default `8080`) |

### 3. Install & Run Locally

```bash
pip install -r requirements.txt
python main.py
```

### 4. Deploy with Docker (Render / Railway)

```bash
docker build -t auto-reaction-bot .
docker run --env-file .env -p 8080:8080 auto-reaction-bot
```

---

## Role-Based Access

| Role | Accounts | Channels | Emojis | Admin Panel | Help |
|---|---|---|---|---|---|
| **Permanent Owner** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Admin** | ✅ | ✅ | ✅ | ❌ | ✅ |
| **Unauthorised** | ❌ | ❌ | ❌ | ❌ | ❌ |

- The **Permanent Owner** is set via `PERMANENT_OWNER_ID` and is always authorized.
- **Admins** are stored in the `users_roles` Supabase table and managed via the 👑 Admins panel.

---

## Bot Usage

1. **Start** → Send `/start` to your bot.
2. **Main menu**: Accounts · Channels · Emojis · (Admins – owner only) · Help Guide.

### Adding an Account
Tap **📱 Accounts → ➕ Add Account** and follow the 5-step wizard.

### Managing Channels
Tap **📡 Channels → ➕ Add Channel** and send: `-1001234567890 My Channel`

### Managing Emojis
Tap **😀 Emojis → ➕ Add Emoji** and send a reaction emoji.

### Managing Admins (Owner Only)
Tap **👑 Admins → ➕ Add Admin** and send the Telegram user ID.

### Help Guide
Tap **📖 Help Guide** for a detailed in-app user manual.

---

## Reactor v2 – Performance Optimizations

| Optimization | How |
|---|---|
| **Non-blocking sweeps** | Reaction sweeps run via `asyncio.create_task()` – the listener never blocks |
| **Peer caching** | Resolved `InputPeer` objects cached per (account, channel) to skip repeated API calls |
| **Flood cooldowns** | Per-account `FloodWaitError` cooldown tracking – no global sleep |
| **Concurrent startup** | All accounts connect in parallel via `asyncio.gather()` |
| **Bounded concurrency** | `asyncio.Semaphore` caps concurrent sweep tasks (default 5) |
| **RPC timeouts** | Every Telethon call wrapped in `asyncio.wait_for()` (15 s default) |
| **DB TTL cache** | Channels, emojis, and admin IDs cached for 30 s to eliminate Supabase round-trips |
| **FIFO dedup** | `OrderedDict` with 10 K cap and FIFO eviction |

---

## Generating a Telethon Session String

```python
from telethon.sync import TelegramClient
from telethon.sessions import StringSession

with TelegramClient(StringSession(), API_ID, API_HASH) as client:
    print(client.session.save())
```

---

## License

MIT
