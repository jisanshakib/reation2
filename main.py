"""Entry point — wire up the database, reactor, bot, and health server."""

from __future__ import annotations

import asyncio
import logging
import sys

from telegram.ext import Application

from config import BOT_TOKEN, SUPABASE_URL, SUPABASE_KEY, PORT
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
    reactor: Reactor = app.bot_data["reactor"]
    await reactor.start()
    log.info("Reactor initialised inside post_init.")


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
    reactor = Reactor(db)

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

    # ---- Start the health-check server ----------------------------- #
    # We schedule it via post_init-style, but since Application.run_polling
    # manages the loop, we start health in a pre-run coroutine.
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    health_runner = loop.run_until_complete(start_health_server(PORT))
    app.bot_data["health_runner"] = health_runner

    # ---- Run the bot (blocks until Ctrl+C / SIGTERM) --------------- #
    log.info("Starting bot polling…")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
