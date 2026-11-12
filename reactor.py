"""Auto-Reaction Engine (v6) – fully non-blocking, with peer caching,
per-account flood-cooldown tracking, fire-and-forget reaction sweeps,
periodic account-health monitoring with owner alerts, instant reaction
for arbitrary post links, entity resolution for channels/usernames,
emoji fallback/multi-try, and a reliable background event listener.

v6 changes
──────────
• **Fixed background event listener**: each Telethon client now calls
  ``client.catch_up()`` after connect so the update loop is actually
  running and events.NewMessage fires.  Handler is stored as a bound
  reference per-client so it can be reliably removed / re-added.
  A dedicated ``_client_keepalive_loop`` keeps every client's update
  receiver alive and reconnects if it drops.
• **Emoji fallback & multi-try**: ``_try_react_with_fallback()`` loops
  through all configured emojis in shuffled order.  If one emoji
  fails with ReactionInvalidError / MsgIdInvalidError, it immediately
  tries the next.  Stops on first success.  Used by both background
  sweeps and instant reactions.
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
    MsgIdInvalidError,
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

# How often (seconds) the keepalive loop checks client connectivity.
_KEEPALIVE_INTERVAL = 60

# Telethon errors that mean the session is dead / revoked.
_AUTH_FATAL_ERRORS = (AuthKeyUnregisteredError, UserDeactivatedBanError)

# Telethon errors that mean "this emoji is not valid for this channel
# or the message ID is wrong" — the next emoji should be tried.
_EMOJI_RETRY_ERRORS = (ReactionInvalidError, MsgIdInvalidError)

# Regex for parsing Telegram post links.
_POST_LINK_RE = re.compile(
    r"https?://t\.me/(?:c/(\d+)|([a-zA-Z0-9_]+))/(\d+)"
)

# Regex for parsing channel links / usernames during channel addition.
_CHANNEL_LINK_RE = re.compile(
    r"https?://t\.me/(?:c/(\d+)|([a-zA-Z0-9_]+))(?:/\d+)?$"
)


class Reactor:
    """Manages Telethon user-clients, listens for new messages in target
    channels, dispatches emoji reactions, and monitors account health."""

    def __init__(self, db: Database, health_interval: int = 300) -> None:
        self.db = db

        # account_id → TelegramClient
        self.clients: dict[str, TelegramClient] = {}

        # account_id → handler callback (so we can remove it reliably).
        self._handlers: dict[str, Any] = {}

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

        # Keepalive loop task.
        self._keepalive_task: asyncio.Task | None = None

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

        if not accounts:
            log.warning(
                "No active accounts found in the database. "
                "The reactor will idle until accounts are added via the bot UI."
            )
        else:
            results = await asyncio.gather(
                *(self._safe_add_client(acc) for acc in accounts),
                return_exceptions=True,
            )
            for acc, res in zip(accounts, results):
                if isinstance(res, Exception):
                    log.error("Startup connect failed for %s: %s", acc["name"], res)

        # Launch the periodic health-check loop.
        self._health_task = asyncio.create_task(self._health_check_loop())
        # Launch the keepalive loop.
        self._keepalive_task = asyncio.create_task(self._client_keepalive_loop())

        log.info(
            "Reactor started – %d clients, %d channels, %d emojis, "
            "health interval %ds",
            len(self.clients),
            len(self.target_channels),
            len(self.allowed_emojis),
            self._health_interval,
        )

    async def stop(self) -> None:
        """Cancel the health monitor, keepalive, background sweeps, and
        disconnect every Telethon client."""
        # Cancel health task.
        if self._health_task is not None:
            self._health_task.cancel()
            try:
                await self._health_task
            except (asyncio.CancelledError, Exception):
                pass
            self._health_task = None

        # Cancel keepalive task.
        if self._keepalive_task is not None:
            self._keepalive_task.cancel()
            try:
                await self._keepalive_task
            except (asyncio.CancelledError, Exception):
                pass
            self._keepalive_task = None

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
        """Create a Telethon client, connect, register the new-message
        handler, and start receiving updates.  Raises on failure."""
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

        # Register the new-message event handler.
        self._register_event_handler(acc_id, client)

        # ── CRITICAL: kick-start the update receiver ─────────────
        # Without catch_up() Telethon connects but never processes
        # incoming updates, so events.NewMessage never fires.
        try:
            await asyncio.wait_for(client.catch_up(), timeout=_RPC_TIMEOUT)
            log.info(
                "Account %s (%s): catch_up complete – update receiver active.",
                account["name"],
                acc_id,
            )
        except Exception as e:
            # catch_up can fail on fresh sessions – not fatal, the
            # update loop may still work for *new* messages.
            log.warning(
                "Account %s (%s): catch_up raised %s – continuing anyway.",
                account["name"],
                acc_id,
                e,
            )

        self.clients[acc_id] = client
        self.db.update_account_status(acc_id, "active")
        log.info("Connected account %s (%s) – event listener registered.", account["name"], acc_id)

    def _register_event_handler(self, acc_id: str, client: TelegramClient) -> None:
        """Register (or re-register) the NewMessage handler on *client*.

        We store one handler per account so we can cleanly remove it
        later.  The handler is a catch-all — channel filtering happens
        at runtime inside ``_maybe_dispatch_sweep`` so that adding or
        removing target channels takes effect immediately without having
        to touch event registrations.
        """
        # Remove the old handler for this account if it exists.
        old = self._handlers.pop(acc_id, None)
        if old is not None:
            try:
                client.remove_event_handler(old, events.NewMessage)
            except Exception:
                pass

        # Build a per-account handler that carries the acc_id.
        async def _handler(event: events.NewMessage.Event) -> None:
            try:
                await self._maybe_dispatch_sweep(event)
            except Exception:
                log.exception(
                    "Unhandled error in NewMessage handler (account %s)",
                    acc_id[:8],
                )

        client.add_event_handler(_handler, events.NewMessage())
        self._handlers[acc_id] = _handler
        log.debug("Event handler registered for account %s", acc_id[:8])

    async def remove_client(self, account_id: str) -> None:
        """Disconnect and forget a client."""
        client = self.clients.pop(account_id, None)
        # Remove handler reference.
        self._handlers.pop(account_id, None)
        if client is not None:
            try:
                await asyncio.wait_for(client.disconnect(), timeout=5.0)
            except Exception:
                pass
        stale = [k for k in self._peer_cache if k[0] == account_id]
        for k in stale:
            del self._peer_cache[k]

    # ================================================================ #
    #  Keepalive loop – ensures event receivers stay alive              #
    # ================================================================ #

    async def _client_keepalive_loop(self) -> None:
        """Periodically verify every client is still connected and its
        update-receive task hasn't silently died.  If a client dropped,
        reconnect it in-place so the NewMessage handler keeps firing."""
        try:
            while True:
                await asyncio.sleep(_KEEPALIVE_INTERVAL)
                for acc_id, client in list(self.clients.items()):
                    try:
                        if not client.is_connected():
                            log.warning(
                                "Keepalive: client %s disconnected – reconnecting.",
                                acc_id[:8],
                            )
                            acc = self.db.get_account(acc_id)
                            await self.remove_client(acc_id)
                            if acc and acc["status"] == "active":
                                await self._safe_add_client(acc)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        log.exception("Keepalive: error for %s", acc_id[:8])
        except asyncio.CancelledError:
            log.debug("Keepalive loop cancelled.")
            raise

    # ================================================================ #
    #  Reload helpers                                                  #
    # ================================================================ #

    async def reload_channels(self) -> None:
        rows = self.db.get_channels()
        self.target_channels = {int(r["channel_id"]) for r in rows}
        log.info("Channels reloaded: %d targets", len(self.target_channels))

    async def reload_emojis(self) -> None:
        rows = self.db.get_emojis()
        self.allowed_emojis = [r["emoji"] for r in rows]
        log.info("Emojis reloaded: %d configured", len(self.allowed_emojis))

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
    #  Health monitor                                                  #
    # ================================================================ #

    async def _health_check_loop(self) -> None:
        """Runs forever, checking every ``_health_interval`` seconds."""
        try:
            while True:
                await asyncio.sleep(self._health_interval)
                try:
                    await self._run_health_check()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("Health-check iteration failed")
        except asyncio.CancelledError:
            log.debug("Health-check loop cancelled.")
            raise

    async def _run_health_check(self) -> None:
        """Probe each connected client and try to reconnect DB-active but
        disconnected accounts."""
        if not self.clients:
            log.info("Health check – no clients to probe, skipping.")
            return

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
    #  Emoji fallback helper                                           #
    # ================================================================ #

    async def _try_react_with_fallback(
        self,
        client: TelegramClient,
        peer: object,
        msg_id: int,
        emojis: list[str],
        acc_label: str,
    ) -> str | None:
        """Try to send a reaction using each emoji in *emojis* (shuffled)
        until one succeeds.

        Returns the successful emoji string, or ``None`` if all failed.

        Raises:
            FloodWaitError — caller must handle cooldown.
            _AUTH_FATAL_ERRORS — caller must handle session death.
            ChannelPrivateError, ChatWriteForbiddenError, PeerIdInvalidError
                — caller must handle (retrying emojis won't help).
        """
        # Shuffle so we don't always hammer the same emoji first.
        candidates = list(emojis)
        random.shuffle(candidates)

        last_error: Exception | None = None

        for emoji in candidates:
            try:
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
                log.debug("  %s reacted with %s", acc_label, emoji)
                return emoji  # ✅ success — stop trying

            except _EMOJI_RETRY_ERRORS as e:
                # This emoji is not valid for this channel / message.
                # Try the next one.
                log.debug(
                    "  %s: emoji %s rejected (%s) – trying next",
                    acc_label,
                    emoji,
                    type(e).__name__,
                )
                last_error = e
                continue

            except asyncio.TimeoutError as e:
                log.debug(
                    "  %s: emoji %s timed out – trying next", acc_label, emoji
                )
                last_error = e
                continue

            # All other errors propagate to caller (FloodWait, AuthKey, etc.).

        # All emojis exhausted.
        log.warning(
            "  %s: all %d emojis failed. Last error: %s",
            acc_label,
            len(candidates),
            last_error,
        )
        return None

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
            log.debug(
                "New post %s/%s ignored – no emojis configured.", chat_id, msg_id
            )
            return

        log.info(
            "New post detected in channel %s, message %d – dispatching sweep",
            chat_id,
            msg_id,
        )

        task = asyncio.create_task(self._sweep_reactions(chat_id, msg_id))
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)

    async def _sweep_reactions(self, chat_id: int, msg_id: int) -> None:
        """React to a post across all connected accounts with emoji
        fallback: if the first emoji fails, try the next, and so on."""
        async with self._sweep_sem:
            log.info(
                "Sweep %s/%s – %d accounts, %d emojis",
                chat_id,
                msg_id,
                len(self.clients),
                len(self.allowed_emojis),
            )

            for acc_id, client in list(self.clients.items()):
                # ── Cooldown check ───────────────────────────────
                cooldown_until = self._cooldowns.get(acc_id, 0)
                now = time.monotonic()
                if now < cooldown_until:
                    wait = cooldown_until - now
                    log.debug("Account %s in cooldown for %.1fs", acc_id[:8], wait)
                    await asyncio.sleep(wait)

                acc_label = acc_id[:8]

                try:
                    peer = await self._resolve_peer(acc_id, client, chat_id)

                    result = await self._try_react_with_fallback(
                        client=client,
                        peer=peer,
                        msg_id=msg_id,
                        emojis=self.allowed_emojis,
                        acc_label=acc_label,
                    )
                    if result is None:
                        log.warning(
                            "Sweep: %s failed all emojis for %s/%s",
                            acc_label,
                            chat_id,
                            msg_id,
                        )

                except FloodWaitError as e:
                    self._cooldowns[acc_id] = time.monotonic() + e.seconds
                    log.warning(
                        "FloodWait %ds for %s – cooldown set",
                        e.seconds,
                        acc_label,
                    )

                except _AUTH_FATAL_ERRORS as e:
                    log.warning("Session dead for %s during sweep: %s", acc_label, e)
                    self.db.update_account_status(acc_id, "failed")
                    await self.remove_client(acc_id)
                    await self._send_health_alert(acc_id, str(e))
                    continue  # skip the 1-s delay for a removed client

                except (
                    ChannelPrivateError,
                    ChatWriteForbiddenError,
                    PeerIdInvalidError,
                ) as e:
                    log.warning("Reaction error for %s: %s", acc_label, e)

                except asyncio.TimeoutError:
                    log.warning(
                        "RPC timeout for %s on %s/%s (peer resolve)",
                        acc_label,
                        chat_id,
                        msg_id,
                    )

                except Exception:
                    log.exception("Unexpected error for %s", acc_label)

                # Strict 1-second delay between accounts.
                await asyncio.sleep(1)

    async def _resolve_peer(
        self, acc_id: str, client: TelegramClient, chat_id: int | str
    ) -> object:
        key = (acc_id, chat_id if isinstance(chat_id, int) else hash(chat_id))
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
        """Single-account check with emoji fallback."""
        client = self.clients.get(account_id)
        if client is None:
            return "❌ Account is not connected."

        match = _POST_LINK_RE.match(post_link)
        if not match:
            return "❌ Invalid post link. Expected format:\n`https://t.me/channel/123`"

        raw_id, username, msg_id_str = match.groups()
        msg_id = int(msg_id_str)

        emojis = list(self.allowed_emojis) if self.allowed_emojis else ["👍"]

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

            result = await self._try_react_with_fallback(
                client=client,
                peer=peer,
                msg_id=msg_id,
                emojis=emojis,
                acc_label=account_id[:8],
            )
            if result:
                return f"✅ Reacted with {result} successfully!"
            return "❌ All emojis failed for this post."

        except FloodWaitError as e:
            self._cooldowns[account_id] = time.monotonic() + e.seconds
            return f"❌ FloodWait: please wait {e.seconds}s."
        except asyncio.TimeoutError:
            return "❌ Request timed out."
        except Exception as e:
            return f"❌ Reaction failed: `{e}`"

    # ================================================================ #
    #  Instant Reaction – ALL active accounts react to any post link   #
    # ================================================================ #

    async def instant_react(self, post_link: str) -> str:
        """Parse a Telegram post link, then trigger ALL active/connected
        accounts to react to it with emoji fallback.

        The channel does NOT need to be in the target-channels list.
        Returns a human-readable status summary.
        """
        match = _POST_LINK_RE.match(post_link.strip())
        if not match:
            return "❌ Invalid post link.\nExpected: `https://t.me/channel/123`"

        raw_id, username, msg_id_str = match.groups()
        msg_id = int(msg_id_str)

        if not self.clients:
            return "❌ No active accounts connected."

        if not self.allowed_emojis:
            return "❌ No reaction emojis configured. Add emojis first."

        success = 0
        failed = 0
        errors: list[str] = []

        for acc_id, client in list(self.clients.items()):
            # Respect per-account flood cooldowns.
            cooldown_until = self._cooldowns.get(acc_id, 0)
            now = time.monotonic()
            if now < cooldown_until:
                wait = cooldown_until - now
                log.debug(
                    "Instant: account %s in cooldown for %.1fs", acc_id[:8], wait
                )
                await asyncio.sleep(wait)

            try:
                # Resolve the entity for this specific account.
                if raw_id:
                    chat_id: int | str = int(f"-100{raw_id}")
                    peer = await asyncio.wait_for(
                        client.get_input_entity(chat_id), timeout=_RPC_TIMEOUT
                    )
                else:
                    peer = await asyncio.wait_for(
                        client.get_input_entity(username), timeout=_RPC_TIMEOUT
                    )

                result = await self._try_react_with_fallback(
                    client=client,
                    peer=peer,
                    msg_id=msg_id,
                    emojis=self.allowed_emojis,
                    acc_label=acc_id[:8],
                )
                if result:
                    log.info("Instant: %s reacted with %s", acc_id[:8], result)
                    success += 1
                else:
                    failed += 1
                    errors.append("All emojis rejected")

            except FloodWaitError as e:
                self._cooldowns[acc_id] = time.monotonic() + e.seconds
                log.warning(
                    "Instant: FloodWait %ds for %s", e.seconds, acc_id[:8]
                )
                failed += 1
                errors.append(f"FloodWait {e.seconds}s")

            except _AUTH_FATAL_ERRORS as e:
                log.warning("Instant: session dead for %s: %s", acc_id[:8], e)
                self.db.update_account_status(acc_id, "failed")
                await self.remove_client(acc_id)
                await self._send_health_alert(acc_id, str(e))
                failed += 1
                errors.append("Session revoked")
                continue  # skip delay

            except (
                ChannelPrivateError,
                ChatWriteForbiddenError,
                PeerIdInvalidError,
            ) as e:
                log.warning("Instant: reaction error for %s: %s", acc_id[:8], e)
                failed += 1
                errors.append(str(e))

            except asyncio.TimeoutError:
                log.warning("Instant: timeout for %s", acc_id[:8])
                failed += 1
                errors.append("Timeout")

            except Exception as e:
                log.exception("Instant: unexpected error for %s", acc_id[:8])
                failed += 1
                errors.append(str(e))

            # 1-second delay between accounts.
            await asyncio.sleep(1)

        # Build summary.
        lines = ["⚡ *Instant Reaction Complete*\n"]
        lines.append(f"✅ Success: {success}  |  ❌ Failed: {failed}")
        if errors:
            # Show unique errors, max 5.
            unique = list(dict.fromkeys(errors))[:5]
            lines.append("\n*Errors:*")
            for err in unique:
                lines.append(f"• `{err}`")
        return "\n".join(lines)

    # ================================================================ #
    #  Entity resolution helpers (for channel/username → ID)           #
    # ================================================================ #

    def _get_any_client(self) -> TelegramClient | None:
        """Return the first available connected client, or None."""
        for client in self.clients.values():
            if client.is_connected():
                return client
        return None

    async def resolve_channel_entity(
        self, link_or_username: str
    ) -> tuple[int, str] | str:
        """Resolve a Telegram channel link (https://t.me/...) or @username
        to a ``(channel_id, channel_title)`` tuple.

        Returns a string error message on failure.
        """
        client = self._get_any_client()
        if client is None:
            return "❌ No active accounts to resolve entity. Add an account first."

        # Try to extract from link.
        username = link_or_username.strip()
        m = _CHANNEL_LINK_RE.match(username)
        if m:
            raw_id, uname = m.groups()
            if raw_id:
                # Private-style link: t.me/c/123456/...
                entity_ref: int | str = int(f"-100{raw_id}")
            else:
                entity_ref = uname
        elif username.startswith("@"):
            entity_ref = username[1:]  # strip @
        else:
            entity_ref = username

        try:
            entity = await asyncio.wait_for(
                client.get_entity(entity_ref), timeout=_RPC_TIMEOUT
            )
            # entity can be Channel, Chat, etc.
            title = getattr(entity, "title", None) or getattr(entity, "username", None) or str(entity_ref)
            # Build the -100-prefixed ID for channels/supergroups.
            entity_id = entity.id
            if hasattr(entity, "broadcast") or hasattr(entity, "megagroup"):
                # Channel or supergroup — need -100 prefix.
                if entity_id > 0:
                    entity_id = int(f"-100{entity_id}")
            return (entity_id, title)
        except asyncio.TimeoutError:
            return "❌ Timed out while resolving entity."
        except Exception as e:
            return f"❌ Could not resolve: `{e}`"

    async def resolve_user_id(self, username: str) -> int | str:
        """Resolve a Telegram @username to a numeric user ID.

        Returns the int user-ID on success, or a string error message on failure.
        """
        client = self._get_any_client()
        if client is None:
            return "❌ No active accounts to resolve username. Add an account first."

        clean = username.strip()
        if clean.startswith("@"):
            clean = clean[1:]

        try:
            entity = await asyncio.wait_for(
                client.get_entity(clean), timeout=_RPC_TIMEOUT
            )
            return entity.id
        except asyncio.TimeoutError:
            return "❌ Timed out while resolving username."
        except Exception as e:
            return f"❌ Could not resolve @{clean}: `{e}`"
