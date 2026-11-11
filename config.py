"""Configuration loaded from environment variables."""

import os
from dotenv import load_dotenv

load_dotenv()

# Telegram Bot token (from @BotFather)
BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")

# Supabase credentials
SUPABASE_URL: str = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY: str = os.getenv("SUPABASE_KEY", "")

# Comma-separated Telegram user IDs allowed to manage the bot
OWNER_IDS: list[int] = [
    int(x) for x in os.getenv("OWNER_IDS", "").split(",") if x.strip()
]

# Health-check server port (for Render / Railway)
PORT: int = int(os.getenv("PORT", "8080"))
