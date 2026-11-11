# Telegram Auto-Reaction Bot

A production-ready Telegram bot that automatically reacts to messages in target
channels using multiple Telethon user-accounts. Fully driven by inline keyboard
buttons — no text commands needed for navigation.

## Features

| Feature | Description |
|---|---|
| 📱 **Multi-Account Management** | Register multiple Telethon StringSession accounts via a 5-step wizard |
| 📡 **Target Channels** | Define channels where auto-reactions are sent |
| 😀 **Emoji Management** | Configure which reaction emojis the bot randomly picks |
| 🔄 **Auto-Reaction Engine** | Listens for new messages, reacts with random emojis, 1s delay between accounts |
| 🩺 **Health Monitor** | Periodic background probe of all sessions; alerts owner on failure |
| 👑 **3-Tier RBAC** | Owner → Admin → Moderator with dynamic UI gating |
| 📖 **In-App Help Guide** | Step-by-step manual accessible via inline button |
| `/healthz` **Endpoint** | Lightweight keep-alive for Render / Railway |

## Role Permissions

| Action | Owner | Admin | Moderator |
|---|---|---|---|
| Manage roles (add/revoke users) | ✅ | ❌ | ❌ |
| Add / Delete / Check accounts | ✅ (all) | ✅ (own) | ❌ |
| View accounts | ✅ (all) | ✅ (own) | ✅ (read-only) |
| Channels & Emojis | ✅ | ✅ | ✅ |

## Quick Start

### 1. Create the database

Run [`schema.sql`](schema.sql) in the **Supabase SQL Editor** (once).

### 2. Configure environment

Copy `.env.example` → `.env` and fill in your values:

```
BOT_TOKEN=<from @BotFather>
SUPABASE_URL=https://xxxxx.supabase.co
SUPABASE_KEY=eyJ...
PERMANENT_OWNER_ID=6580282480
PORT=8080
HEALTH_CHECK_INTERVAL=300
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Run

```bash
python main.py
```

On startup the bot:
1. Upserts the permanent owner into `users_roles` (idempotent).
2. Connects all active Telethon accounts.
3. Starts the health-monitor loop.
4. Starts the `/healthz` HTTP server.
5. Begins polling Telegram for bot updates.

## Commands

| Command | Who | Description |
|---|---|---|
| `/start` | Everyone | Open the main inline menu |
| `/addadmin <user_id> [admin\|moderator]` | Owner | Quick-add a user role from chat |

Everything else is button-driven.

## File Structure

```
├── schema.sql          # Supabase table definitions
├── config.py           # Environment variables
├── database.py         # Supabase CRUD + TTL cache + ensure_owner_role()
├── reactor.py          # Telethon client pool, reaction engine, health monitor
├── bot_handlers.py     # Inline keyboard UI, /start, /addadmin, RBAC gating
├── main.py             # Entry point — wires everything together
├── health.py           # /healthz aiohttp keep-alive server
├── .env.example
├── requirements.txt
├── Dockerfile
└── README.md
```

## Deployment (Render / Railway)

1. Push to a Git repo.
2. Set environment variables in the dashboard.
3. Build command: `pip install -r requirements.txt`
4. Start command: `python main.py`
5. Health check path: `/healthz` on port `8080`.

## Health Monitor

Every `HEALTH_CHECK_INTERVAL` seconds (default 300) the reactor:
- Probes each connected Telethon session with `client.get_me()`.
- If a session is revoked or banned → marks it `failed` in Supabase and sends
  a 🔴 DM alert to the owner.
- Attempts to reconnect any `active`-in-DB-but-disconnected accounts.

## License

MIT
