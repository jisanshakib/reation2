"""Entry point (v4) — wire up the database, reactor, bot, and health server.

v4 changes
──────────
• Calls ``db.ensure_owner_role(PERMANENT_OWNER_ID)`` on startup so the
  bot always has an authorised owner row in the database.
• Logs a startup summary with counts of accounts, channels, emojis.
• Passes ``health_interval`` from config to the Reactor.
"""

from __future__ import annotations

import asyncio
import logging
import sys

from telegram.ext import Application

from config import (
    BOT_TOKEN,
    SUPABASE_URL,
    SUPABASE_KEY,
    PERMANENT_OWNER_ID,
    PORT,
    HEALTH_CHECK_INTERVAL,
)
from database import Database
from reactor import Reactor
from bot_handlers import setup_handlers
from health import start_health_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
log = logging.getLogger(__name__)


async def post_init(app: Application) -> None:
    """Called after the bot Application is initialised but before polling."""
    # Start health server inside PTB's active event loop
    health_runner = await start_health_server(PORT)
    app.bot_data["health_runner"] = health_runner

    reactor: Reactor = app.bot_data["reactor"]
    # Give the reactor a reference to the Bot so it can send health alerts.
    reactor.set_bot(app.bot)
    await reactor.start()
    log.info("Reactor and Health Server initialised inside post_init.")


async def post_shutdown(app: Application) -> None:
    """Cleanup on graceful shutdown."""
    reactor: Reactor = app.bot_data["reactor"]
    await reactor.stop()

    runner = app.bot_data.get("health_runner")
    if runner is not None:
        await runner.cleanup()

    log.info("Shutdown complete.")


def main() -> None:
    # ---- Pre-flight checks ----------------------------------------- #
    missing = []
    if not BOT_TOKEN:
        missing.append("BOT_TOKEN")
    if not SUPABASE_URL:
        missing.append("SUPABASE_URL")
    if not SUPABASE_KEY:
        missing.append("SUPABASE_KEY")
    if missing:
        log.critical("Missing environment variables: %s", ", ".join(missing))
        sys.exit(1)

    # ---- Shared objects -------------------------------------------- #
    db = Database(SUPABASE_URL, SUPABASE_KEY)

    # Bootstrap the permanent owner into users_roles (idempotent upsert).
    db.ensure_owner_role(PERMANENT_OWNER_ID)

    reactor = Reactor(db, health_interval=HEALTH_CHECK_INTERVAL)

    # ---- Build the bot application --------------------------------- #
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    # Stash shared objects so lifecycle hooks can access them.
    app.bot_data["reactor"] = reactor
    app.bot_data["db"] = db

    # Register handlers.
    setup_handlers(app, db, reactor)

    # ---- Run the bot (blocks until Ctrl+C / SIGTERM) --------------- #
    log.info(
        "Starting bot polling — owner=%d, health_interval=%ds, port=%d …",
        PERMANENT_OWNER_ID,
        HEALTH_CHECK_INTERVAL,
        PORT,
    )
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
