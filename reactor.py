"""Auto-Reaction Engine (v3) – fully non-blocking, with peer caching,
per-account flood-cooldown tracking, fire-and-forget reaction sweeps,
and periodic account-health monitoring with owner alerts.
"""

from __future__ import annotations

import asyncio
import logging
import random
import re
import time
from collections import OrderedDict
from typing import TYPE_CHECKING, Any

from telethon import TelegramClient, events
from telethon.errors import (
    AuthKeyUnregisteredError,
    ReactionInvalidError,
    ChannelPrivateError,
    ChatWriteForbiddenError,
    FloodWaitError,
    PeerIdInvalidError,
    UserDeactivatedBanError,
)
from telethon.sessions import StringSession
from telethon.tl.functions.messages import SendReactionRequest
from telethon.tl.types import ReactionEmoji

if TYPE_CHECKING:
    from telegram import Bot
    from database import Database

log = logging.getLogger(__name__)

# Maximum processed-message keys kept in memory (FIFO eviction).
_MAX_PROCESSED = 10_000

# Upper bound on concurrent reaction-sweep tasks.
_MAX_CONCURRENT_SWEEPS = 5

# Timeout (seconds) for a single Telethon RPC call.
_RPC_TIMEOUT = 15.0

# Telethon errors that mean the session is dead / revoked.
_AUTH_FATAL_ERRORS = (AuthKeyUnregisteredError, UserDeactivatedBanError)


class Reactor:
    """Manages Telethon user-clients, listens for new messages in target
    channels, dispatches emoji reactions, and monitors account health."""

    def __init__(self, db: Database, health_interval: int = 300) -> None:
        self.db = db

        # account_id → TelegramClient
        self.clients: dict[str, TelegramClient] = {}

        # Fast-path copies of DB state (refreshed via reload_*).
        self.target_channels: set[int] = set()
        self.allowed_emojis: list[str] = []

        # De-duplication of (chat_id, msg_id) across all listener clients.
        self._processed: OrderedDict[tuple[int, int], None] = OrderedDict()
        self._dedup_lock = asyncio.Lock()

        # Semaphore that caps concurrent sweep tasks.
        self._sweep_sem = asyncio.Semaphore(_MAX_CONCURRENT_SWEEPS)

        # Per-account flood cooldown: account_id → earliest-allowed monotonic ts.
        self._cooldowns: dict[str, float] = {}

        # Peer cache: (account_id, chat_id) → resolved InputPeer.
        self._peer_cache: dict[tuple[str, int], object] = {}

        # Background tasks we need to cancel on stop().
        self._bg_tasks: set[asyncio.Task] = set()

        # Health monitor settings.
        self._health_interval = health_interval
        self._health_task: asyncio.Task | None = None

        # Telegram Bot reference – set via set_bot(), used to send alerts.
        self._bot: Bot | None = None

    # ================================================================ #
    #  Bot reference (for sending owner alerts)                        #
    # ================================================================ #

    def set_bot(self, bot: Bot) -> None:
        """Store the python-telegram-bot ``Bot`` instance so the health
        monitor can send DM alerts to the owner."""
        self._bot = bot

    # ================================================================ #
    #  Lifecycle                                                       #
    # ================================================================ #

    async def start(self) -> None:
        """Load state from the DB, connect active accounts, and start
        the health-monitor loop."""
        await self.reload_channels()
        await self.reload_emojis()

        accounts = self.db.get_active_accounts()
        results = await asyncio.gather(
            *(self._safe_add_client(acc) for acc in accounts),
            return_exceptions=True,
        )
        for acc, res in zip(accounts, results):
            if isinstance(res, Exception):
                log.error("Startup connect failed for %s: %s", acc["name"], res)

        # Launch the periodic health-check loop.
        self._health_task = asyncio.create_task(self._health_check_loop())

        log.info(
            "Reactor started – %d clients, %d channels, %d emojis, "
            "health interval %ds",
            len(self.clients),
            len(self.target_channels),
            len(self.allowed_emojis),
            self._health_interval,
        )

    async def stop(self) -> None:
        """Cancel the health monitor, background sweeps, and disconnect
        every Telethon client."""
        # Cancel health task.
        if self._health_task is not None:
            self._health_task.cancel()
            try:
                await self._health_task
            except (asyncio.CancelledError, Exception):
                pass
            self._health_task = None

        # Cancel sweep tasks.
        for task in self._bg_tasks:
            task.cancel()
        if self._bg_tasks:
            await asyncio.gather(*self._bg_tasks, return_exceptions=True)
        self._bg_tasks.clear()

        for acc_id in list(self.clients):
            await self.remove_client(acc_id)

        self._peer_cache.clear()
        log.info("Reactor stopped.")

    # ================================================================ #
    #  Client management                                               #
    # ================================================================ #

    async def _safe_add_client(self, account: dict) -> None:
        """Wrapper that marks the account *failed* on any exception."""
        try:
            await self.add_client(account)
        except Exception:
            log.exception(
                "Failed to connect account %s (%s)", account["name"], account["id"]
            )
            self.db.update_account_status(account["id"], "failed")

    async def add_client(self, account: dict) -> None:
        """Create a Telethon client, connect, and register the new-message
        handler.  Raises on failure."""
        acc_id = account["id"]
        session = StringSession(account["session_str"])
        client = TelegramClient(
            session,
            int(account["api_id"]),
            account["api_hash"],
            receive_updates=True,
        )

        await asyncio.wait_for(client.connect(), timeout=_RPC_TIMEOUT)

        if not await asyncio.wait_for(
            client.is_user_authorized(), timeout=_RPC_TIMEOUT
        ):
            log.warning("Account %s (%s) is not authorised.", account["name"], acc_id)
            self.db.update_account_status(acc_id, "failed")
            await client.disconnect()
            return

        @client.on(events.NewMessage())
        async def _on_new_message(event: events.NewMessage.Event) -> None:
            await self._maybe_dispatch_sweep(event)

        self.clients[acc_id] = client
        self.db.update_account_status(acc_id, "active")
        log.info("Connected account %s (%s)", account["name"], acc_id)

    async def remove_client(self, account_id: str) -> None:
        """Disconnect and forget a client."""
        client = self.clients.pop(account_id, None)
        if client is not None:
            try:
                await asyncio.wait_for(client.disconnect(), timeout=5.0)
            except Exception:
                pass
        stale = [k for k in self._peer_cache if k[0] == account_id]
        for k in stale:
            del self._peer_cache[k]

    # ================================================================ #
    #  Reload helpers                                                  #
    # ================================================================ #

    async def reload_channels(self) -> None:
        rows = self.db.get_channels()
        self.target_channels = {int(r["channel_id"]) for r in rows}

    async def reload_emojis(self) -> None:
        rows = self.db.get_emojis()
        self.allowed_emojis = [r["emoji"] for r in rows]

    async def reload_accounts(self) -> None:
        """Synchronise running clients with the DB."""
        db_accounts = {a["id"]: a for a in self.db.get_active_accounts()}

        for acc_id in list(self.clients):
            if acc_id not in db_accounts:
                await self.remove_client(acc_id)

        to_add = [acc for aid, acc in db_accounts.items() if aid not in self.clients]
        if to_add:
            await asyncio.gather(
                *(self._safe_add_client(acc) for acc in to_add),
                return_exceptions=True,
            )

    # ================================================================ #
    #  Health monitor (Feature 4)                                      #
    # ================================================================ #

    async def _health_check_loop(self) -> None:
        """Runs forever, checking every ``_health_interval`` seconds."""
        while True:
            await asyncio.sleep(self._health_interval)
            try:
                await self._run_health_check()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Health-check iteration failed")

    async def _run_health_check(self) -> None:
        """Probe each connected client and try to reconnect DB-active but
        disconnected accounts."""
        log.info("Health check – probing %d connected clients …", len(self.clients))

        # 1. Probe every connected client.
        for acc_id, client in list(self.clients.items()):
            try:
                if not client.is_connected():
                    raise ConnectionError("Client disconnected")
                # Lightweight RPC – validates session & connection.
                await asyncio.wait_for(client.get_me(), timeout=_RPC_TIMEOUT)
            except _AUTH_FATAL_ERRORS as e:
                log.warning("Session revoked/banned for %s: %s", acc_id[:8], e)
                self.db.update_account_status(acc_id, "failed")
                await self.remove_client(acc_id)
                await self._send_health_alert(acc_id, str(e))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("Health probe failed for %s: %s", acc_id[:8], e)
                # Attempt an in-place reconnect before declaring failure.
                acc = self.db.get_account(acc_id)
                await self.remove_client(acc_id)
                if acc:
                    try:
                        await self.add_client(acc)
                        log.info("Reconnected %s during health check.", acc["name"])
                        continue
                    except Exception:
                        pass
                self.db.update_account_status(acc_id, "failed")
                await self._send_health_alert(acc_id, str(e))

        # 2. Try to bring up accounts that are 'active' in DB but not running.
        for acc in self.db.get_active_accounts():
            if acc["id"] not in self.clients:
                try:
                    await self.add_client(acc)
                    log.info("Recovered account %s during health check.", acc["name"])
                except _AUTH_FATAL_ERRORS as e:
                    self.db.update_account_status(acc["id"], "failed")
                    await self._send_health_alert(acc["id"], str(e))
                except Exception as e:
                    log.warning("Recovery failed for %s: %s", acc["name"], e)
                    self.db.update_account_status(acc["id"], "failed")
                    await self._send_health_alert(acc["id"], str(e))

    async def _send_health_alert(self, account_id: str, error: str) -> None:
        """Send a private DM to the Permanent Owner about a failed account."""
        if self._bot is None:
            log.warning("Cannot send health alert – bot reference not set.")
            return

        from config import PERMANENT_OWNER_ID

        acc = self.db.get_account(account_id)
        name = acc["name"] if acc else account_id[:8]
        phone = (acc.get("phone") or "N/A") if acc else "N/A"

        text = (
            "🔴 *Account Health Alert*\n\n"
            f"• *Account:* {name}\n"
            f"• *Phone:* {phone}\n"
            f"• *Error:* `{error}`\n\n"
            "⚠️ Session may be revoked or expired.\n"
            "Please re-authenticate or replace the session string.\n"
            "Tap /start → 📱 Accounts → select → Delete 🗑️ and re-register."
        )
        try:
            await self._bot.send_message(
                chat_id=PERMANENT_OWNER_ID,
                text=text,
                parse_mode="Markdown",
            )
        except Exception:
            log.exception("Failed to send health alert for %s", name)

    # ================================================================ #
    #  Core reaction logic                                             #
    # ================================================================ #

    async def _maybe_dispatch_sweep(self, event: events.NewMessage.Event) -> None:
        chat_id = event.chat_id
        msg_id = event.message.id

        if chat_id not in self.target_channels:
            return

        key = (chat_id, msg_id)
        async with self._dedup_lock:
            if key in self._processed:
                return
            self._processed[key] = None
            while len(self._processed) > _MAX_PROCESSED:
                self._processed.popitem(last=False)

        if not self.allowed_emojis:
            return

        task = asyncio.create_task(self._sweep_reactions(chat_id, msg_id))
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)

    async def _sweep_reactions(self, chat_id: int, msg_id: int) -> None:
        async with self._sweep_sem:
            log.info(
                "Sweep %s/%s – %d accounts", chat_id, msg_id, len(self.clients)
            )

            for acc_id, client in list(self.clients.items()):
                cooldown_until = self._cooldowns.get(acc_id, 0)
                now = time.monotonic()
                if now < cooldown_until:
                    wait = cooldown_until - now
                    log.debug("Account %s in cooldown for %.1fs", acc_id[:8], wait)
                    await asyncio.sleep(wait)

                emoji = random.choice(self.allowed_emojis)

                try:
                    peer = await self._resolve_peer(acc_id, client, chat_id)
                    await asyncio.wait_for(
                        client(
                            SendReactionRequest(
                                peer=peer,
                                msg_id=msg_id,
                                reaction=[ReactionEmoji(emoticon=emoji)],
                            )
                        ),
                        timeout=_RPC_TIMEOUT,
                    )
                    log.debug("  %s reacted with %s", acc_id[:8], emoji)

                except FloodWaitError as e:
                    self._cooldowns[acc_id] = time.monotonic() + e.seconds
                    log.warning(
                        "FloodWait %ds for %s – cooldown set",
                        e.seconds,
                        acc_id[:8],
                    )

                except _AUTH_FATAL_ERRORS as e:
                    log.warning("Session dead for %s during sweep: %s", acc_id[:8], e)
                    self.db.update_account_status(acc_id, "failed")
                    await self.remove_client(acc_id)
                    await self._send_health_alert(acc_id, str(e))
                    continue  # skip the 1-s delay for a removed client

                except (
                    ReactionInvalidError,
                    ChannelPrivateError,
                    ChatWriteForbiddenError,
                    PeerIdInvalidError,
                ) as e:
                    log.warning("Reaction error for %s: %s", acc_id[:8], e)

                except asyncio.TimeoutError:
                    log.warning(
                        "RPC timeout for %s on %s/%s", acc_id[:8], chat_id, msg_id
                    )

                except Exception:
                    log.exception("Unexpected error for %s", acc_id[:8])

                # Strict 1-second delay between accounts.
                await asyncio.sleep(1)

    async def _resolve_peer(
        self, acc_id: str, client: TelegramClient, chat_id: int
    ) -> object:
        key = (acc_id, chat_id)
        peer = self._peer_cache.get(key)
        if peer is not None:
            return peer
        peer = await asyncio.wait_for(
            client.get_input_entity(chat_id), timeout=_RPC_TIMEOUT
        )
        self._peer_cache[key] = peer
        return peer

    # ================================================================ #
    #  Check helper (bot "Check 🔍" button)                            #
    # ================================================================ #

    async def check_react(self, account_id: str, post_link: str) -> str:
        client = self.clients.get(account_id)
        if client is None:
            return "❌ Account is not connected."

        match = re.match(
            r"https?://t\.me/(?:c/(\d+)|([a-zA-Z0-9_]+))/(\d+)", post_link
        )
        if not match:
            return "❌ Invalid post link. Expected format:\n`https://t.me/channel/123`"

        raw_id, username, msg_id_str = match.groups()
        msg_id = int(msg_id_str)

        try:
            if raw_id:
                chat_id = int(f"-100{raw_id}")
                peer = await asyncio.wait_for(
                    client.get_input_entity(chat_id), timeout=_RPC_TIMEOUT
                )
            else:
                peer = await asyncio.wait_for(
                    client.get_input_entity(username), timeout=_RPC_TIMEOUT
                )

            emoji = random.choice(self.allowed_emojis) if self.allowed_emojis else "👍"
            await asyncio.wait_for(
                client(
                    SendReactionRequest(
                        peer=peer,
                        msg_id=msg_id,
                        reaction=[ReactionEmoji(emoticon=emoji)],
                    )
                ),
                timeout=_RPC_TIMEOUT,
            )
            return f"✅ Reacted with {emoji} successfully!"
        except asyncio.TimeoutError:
            return "❌ Request timed out."
        except Exception as e:
            return f"❌ Reaction failed: `{e}`"
