"""Configuration loaded from environment variables."""

import os
from dotenv import load_dotenv

load_dotenv()

# Telegram Bot token (from @BotFather)
BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")

# Supabase credentials
SUPABASE_URL: str = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY: str = os.getenv("SUPABASE_KEY", "")

# Permanent owner – the single super-admin who can manage other admins.
# This ID is ALWAYS authorised regardless of the users_roles table.
PERMANENT_OWNER_ID: int = int(os.getenv("PERMANENT_OWNER_ID", "6580282480"))

# Health-check server port (for Render / Railway)
PORT: int = int(os.getenv("PORT", "8080"))

# Account health-monitor interval in seconds (default: 5 minutes).
HEALTH_CHECK_INTERVAL: int = int(os.getenv("HEALTH_CHECK_INTERVAL", "300"))
