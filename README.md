# Telegram Auto-Reaction Bot

A fully inline-keyboard-driven Telegram bot that automatically reacts to messages in target channels using multiple Telethon user-accounts, with a Supabase-backed database.

## Features

| Area | Details |
|---|---|
| **UI** | 100 % Inline Keyboard – no text commands needed for navigation |
| **Accounts** | Register multiple Telethon user-accounts (name, phone, API ID, API Hash, session string) |
| **Channels** | Add / remove target channels dynamically |
| **Emojis** | Manage the pool of allowed reaction emojis |
| **Reactor** | Listens for new messages in target channels and reacts from every active account with a **1-second delay** between accounts |
| **Health** | Lightweight `/healthz` endpoint for Render / Railway uptime checks |

---

## Architecture

```
main.py                 Entry point – wires everything together
├── config.py           Env-var loader
├── database.py         Supabase CRUD (accounts, channels, emojis)
├── bot_handlers.py     python-telegram-bot inline-keyboard UI
├── reactor.py          Telethon auto-reaction engine
└── health.py           aiohttp /healthz server
```

---

## Quick Start

### 1. Supabase Setup

1. Create a new Supabase project.
2. Open the **SQL Editor** and run [`schema.sql`](schema.sql) to create the three tables.
3. Copy your **Project URL** and **anon/service key**.

### 2. Environment Variables

Copy `.env.example` to `.env` and fill in the values:

```bash
cp .env.example .env
```

| Variable | Description |
|---|---|
| `BOT_TOKEN` | Telegram bot token from [@BotFather](https://t.me/BotFather) |
| `SUPABASE_URL` | Supabase project URL |
| `SUPABASE_KEY` | Supabase anon or service-role key |
| `OWNER_IDS` | Comma-separated Telegram user IDs allowed to manage the bot |
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

On **Render** or **Railway**, set the environment variables in the dashboard and use the `Dockerfile` for the build. The `/healthz` endpoint will keep the service alive.

---

## Bot Usage

1. **Start the bot** → Send `/start` to your bot in Telegram.
2. **Main menu** appears with three buttons: **Accounts**, **Channels**, **Emojis**.

### Adding an Account

1. Tap **📱 Accounts** → **➕ Add Account**.
2. Follow the 5-step flow:
   - **Account Name** (required)
   - **Phone Number** (optional – tap *Skip ⏭️*)
   - **API ID**
   - **API Hash**
   - **Session String** (Telethon `StringSession`)
3. The bot saves the account and tries to connect immediately.

### Managing Channels

1. Tap **📡 Channels** → **➕ Add Channel**.
2. Send the channel ID and name: `-1001234567890 My Channel`
3. The reactor begins listening immediately.

### Managing Emojis

1. Tap **😀 Emojis** → **➕ Add Emoji**.
2. Send the reaction emoji (e.g. `👍`).
3. Tap any existing emoji button to remove it.

### Checking an Account

1. Open an account's detail view.
2. Tap **Check 🔍** and send a post link (e.g. `https://t.me/channel/123`).
3. The bot sends a test reaction and reports the result.

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
