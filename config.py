"""Configuration loaded from environment variables."""

import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
SUPABASE_URL: str = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY: str = os.getenv("SUPABASE_KEY", "")
PERMANENT_OWNER_ID: int = int(os.getenv("PERMANENT_OWNER_ID", "6580282480"))
PORT: int = int(os.getenv("PORT", "8080"))
HEALTH_CHECK_INTERVAL: int = int(os.getenv("HEALTH_CHECK_INTERVAL", "300"))
