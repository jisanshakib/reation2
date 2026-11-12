"""Auto-Reaction Engine (v8) – Guaranteed Multi-Account Delivery.

Only the **Master Account** monitors target channels (event listener +
fast 15-second fallback poll).  When a new post is detected the master
dispatches reactions across ALL connected accounts (master + workers).
Worker accounts stay idle until dispatched — no event handlers, no
catch_up — which minimises Telegram rate-limit exposure.

v8 changes
──────────
• **Guaranteed delivery**: ``_sweep_reactions`` now tracks per-account
  success/failure.  Failed accounts are retried with exponential
  backoff (5 → 10 → 20 → … capped at 60 s) until every active account
  has reacted, up to ``_MAX_SWEEP_RETRIES`` rounds (default 10).
  The task stays alive on the ``asyncio.Queue`` until 100 %% delivery
  or the retry limit is exhausted.
• Fatal errors (``AuthKeyUnregistered``, ``UserDeactivatedBan``) and
  permanent channel errors (``ChannelPrivate``, ``ChatWriteForbidden``,
  ``PeerIdInvalid``) are not retried — the account/channel is marked
  and skipped.
• Sweep summary log now includes per-account outcome.

v7 changes (preserved)
──────────────────────
• Master-Worker split, fallback poll loop, entity pre-caching,
  auto-join, reload_master, keepalive priority.

Preserved from v6
──────────────────
• Emoji fallback / multi-try (``_try_react_with_fallback``).
• Instant reaction for arbitrary post links.
• Entity resolution for channels / usernames / user IDs.
• Health-check loop with owner DM alerts.
• Peer caching, per-account flood cooldowns, FIFO dedup.
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
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import (
    ImportChatInviteRequest,
    SendReactionRequest,
)
from telethon.tl.types import ReactionEmoji

if TYPE_CHECKING:
    from telegram import Bot
    from database import Database

log = logging.getLogger(__name__)

# Maximum processed-message keys kept in memory (FIFO eviction).
_MAX_PROCESSED = 10_000

# Upper bound on concurrent reaction-sweep tasks.
_MAX_CONCURRENT_SWEEPS = 5

# Maximum retry rounds per sweep before giving up.
_MAX_SWEEP_RETRIES = 10

# Maximum transient-failure attempts per account per post before the
# account is auto-deactivated (circuit breaker).
_MAX_ACCOUNT_ATTEMPTS = 3

# Initial backoff (seconds) between retry rounds; doubles each round,
# capped at _MAX_RETRY_BACKOFF.
_INITIAL_RETRY_BACKOFF = 5.0
_MAX_RETRY_BACKOFF = 60.0

# Timeout (seconds) for a single Telethon RPC call.
_RPC_TIMEOUT = 15.0

# How often (seconds) the keepalive loop checks client connectivity.
_KEEPALIVE_INTERVAL = 60

# How often (seconds) the master fallback poll checks for new posts.
_POLL_INTERVAL = 15

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

# Regex for extracting invite hashes from private channel links.
_INVITE_HASH_RE = re.compile(
    r"https?://t\.me/(?:\+|joinchat/)([a-zA-Z0-9_-]+)"
)


class Reactor:
    """Manages Telethon user-clients with a Master-Worker architecture.

    The Master Account monitors target channels and dispatches reactions
    across all connected accounts (master + workers).
    """

    def __init__(self, db: Database, health_interval: int = 300) -> None:
        self.db = db

        # account_id → TelegramClient
        self.clients: dict[str, TelegramClient] = {}

        # Master account tracking.
        self.master_id: str | None = None

        # account_id → handler callback (only master gets one).
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

        # Master poll loop task.
        self._poll_task: asyncio.Task | None = None

        # Per-channel last-seen message ID for fast poll comparison.
        self._last_seen: dict[int, int] = {}

        # Telegram Bot reference – set via set_bot(), used to send alerts.
        self._bot: Bot | None = None

    # ================================================================ #
    #  Master client property                                          #
    # ================================================================ #

    @property
    def master_client(self) -> TelegramClient | None:
        """Return the master TelegramClient, or None."""
        if self.master_id is None:
            return None
        return self.clients.get(self.master_id)

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

        # Identify the master account from DB.
        master_row = self.db.get_master_account()
        if master_row:
            self.master_id = master_row["id"]
            log.info("Master account identified: %s (%s)", master_row["name"], self.master_id)
        else:
            self.master_id = None
            log.warning(
                "No master account configured. Background monitoring "
                "will not work until a master is set."
            )

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

        # Prime the master's entity cache so Telethon subscribes to
        # channel updates and the poll loop can resolve entities.
        await self._prime_master_channels()

        # Launch the master fallback poll loop.
        self._start_poll_loop()

        # Launch the periodic health-check loop.
        self._health_task = asyncio.create_task(self._health_check_loop())
        # Launch the keepalive loop.
        self._keepalive_task = asyncio.create_task(self._client_keepalive_loop())

        log.info(
            "Reactor started – %d clients, master=%s, %d channels, "
            "%d emojis, health interval %ds",
            len(self.clients),
            self.master_id[:8] if self.master_id else "NONE",
            len(self.target_channels),
            len(self.allowed_emojis),
            self._health_interval,
        )

    async def stop(self) -> None:
        """Cancel the health monitor, keepalive, poll loop, background
        sweeps, and disconnect every Telethon client."""
        # Cancel poll task.
        if self._poll_task is not None:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except (asyncio.CancelledError, Exception):
                pass
            self._poll_task = None

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
        """Create a Telethon client, connect it.

        If the account is the master, register the new-message handler
        and call catch_up().  Workers only connect — no handler, no
        catch_up — they react only when dispatched by the master.
        """
        acc_id = account["id"]
        is_master = account.get("is_master", False)
        session = StringSession(account["session_str"])
        client = TelegramClient(
            session,
            int(account["api_id"]),
            account["api_hash"],
            receive_updates=is_master,  # only master needs updates
        )

        await asyncio.wait_for(client.connect(), timeout=_RPC_TIMEOUT)

        if not await asyncio.wait_for(
            client.is_user_authorized(), timeout=_RPC_TIMEOUT
        ):
            log.warning("Account %s (%s) is not authorised.", account["name"], acc_id)
            self.db.update_account_status(acc_id, "failed")
            await client.disconnect()
            return

        # Master-specific setup.
        if is_master:
            self.master_id = acc_id
            self._register_event_handler(acc_id, client)

            try:
                await asyncio.wait_for(client.catch_up(), timeout=_RPC_TIMEOUT)
                log.info(
                    "Master %s (%s): catch_up complete – update receiver active.",
                    account["name"],
                    acc_id,
                )
            except Exception as e:
                log.warning(
                    "Master %s (%s): catch_up raised %s – continuing anyway.",
                    account["name"],
                    acc_id,
                    e,
                )
        else:
            log.debug(
                "Worker %s (%s): connected (no event handler).",
                account["name"],
                acc_id,
            )

        self.clients[acc_id] = client
        self.db.update_account_status(acc_id, "active")
        role_label = "MASTER" if is_master else "worker"
        log.info("Connected account %s (%s) as %s.", account["name"], acc_id, role_label)

    def _register_event_handler(self, acc_id: str, client: TelegramClient) -> None:
        """Register (or re-register) the NewMessage handler on *client*.

        Only the master account should have this handler.  The handler
        is a catch-all — channel filtering happens at runtime inside
        ``_maybe_dispatch_sweep``.
        """
        old = self._handlers.pop(acc_id, None)
        if old is not None:
            try:
                client.remove_event_handler(old, events.NewMessage)
            except Exception:
                pass

        async def _handler(event: events.NewMessage.Event) -> None:
            try:
                await self._maybe_dispatch_sweep(event)
            except Exception:
                log.exception(
                    "Unhandled error in NewMessage handler (master %s)",
                    acc_id[:8],
                )

        client.add_event_handler(_handler, events.NewMessage())
        self._handlers[acc_id] = _handler
        log.debug("Event handler registered for master %s", acc_id[:8])

    async def remove_client(self, account_id: str) -> None:
        """Disconnect and forget a client."""
        # If removing the master, clear the reference.
        if account_id == self.master_id:
            self.master_id = None
            log.warning("Master account %s removed — no master active.", account_id[:8])

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
    #  Entity pre-caching — force Telethon to subscribe to channels   #
    # ================================================================ #

    async def _prime_master_channels(self) -> None:
        """Ensure the master client has fully resolved and cached every
        target channel entity.

        Telethon only delivers ``events.NewMessage`` for chats whose
        entities it has seen.  Calling ``get_dialogs()`` populates the
        internal entity cache, and per-channel ``get_entity()`` calls
        ensure each target is individually resolved.  Finally, a quick
        ``get_messages(limit=1)`` seeds ``_last_seen`` so the poll loop
        knows the current high-water mark and won't false-fire on old
        posts.
        """
        mc = self.master_client
        if mc is None or not mc.is_connected():
            log.debug("_prime_master_channels: master not available, skipping.")
            return

        if not self.target_channels:
            log.debug("_prime_master_channels: no target channels, skipping.")
            return

        # Step 1: fetch full dialog list so Telethon populates its
        # internal entity cache and subscribes to update channels.
        try:
            await asyncio.wait_for(mc.get_dialogs(), timeout=30.0)
            log.info(
                "Master dialogs fetched – Telethon entity cache populated."
            )
        except Exception as e:
            log.warning("_prime_master_channels: get_dialogs failed: %s", e)

        # Step 2: explicitly resolve each target channel entity and
        # seed the _last_seen high-water mark.
        for ch_id in list(self.target_channels):
            try:
                entity = await asyncio.wait_for(
                    mc.get_entity(ch_id), timeout=_RPC_TIMEOUT
                )
                title = getattr(entity, "title", None) or str(ch_id)
                log.info(
                    "Primed channel entity: %s (%s)", title, ch_id
                )
            except Exception as e:
                log.warning(
                    "_prime_master_channels: get_entity failed for %s: %s",
                    ch_id, e,
                )

            # Seed _last_seen so the poll loop doesn't react to
            # already-existing messages on first cycle.
            try:
                msgs = await asyncio.wait_for(
                    mc.get_messages(ch_id, limit=1), timeout=_RPC_TIMEOUT
                )
                if msgs and msgs[0] is not None:
                    self._last_seen[ch_id] = msgs[0].id
                    log.debug(
                        "Seeded _last_seen[%s] = %d", ch_id, msgs[0].id
                    )
            except Exception as e:
                log.debug(
                    "_prime_master_channels: seed get_messages failed for %s: %s",
                    ch_id, e,
                )

    # ================================================================ #
    #  Master poll loop — fallback that catches missed events          #
    # ================================================================ #

    def _start_poll_loop(self) -> None:
        """(Re)start the master fallback poll task."""
        if self._poll_task is not None:
            self._poll_task.cancel()
        self._poll_task = asyncio.create_task(self._master_poll_loop())

    async def _master_poll_loop(self) -> None:
        """Every ``_POLL_INTERVAL`` seconds, use the master client to
        fetch the latest messages from each target channel and dispatch
        sweeps for any that are newer than ``_last_seen[channel]``.

        Uses a per-channel high-water mark (``_last_seen``) for fast
        comparison instead of the global ``_processed`` dedup set, so
        even if the event handler already processed a message the poll
        loop won't re-fire.  New detections are also recorded in
        ``_processed`` to prevent the event handler from double-firing.
        """
        try:
            while True:
                await asyncio.sleep(_POLL_INTERVAL)

                mc = self.master_client
                if mc is None or not mc.is_connected():
                    log.debug("Poll loop: master not available, skipping cycle.")
                    continue

                if not self.target_channels:
                    continue

                for chat_id in list(self.target_channels):
                    try:
                        messages = await asyncio.wait_for(
                            mc.get_messages(chat_id, limit=3),
                            timeout=_RPC_TIMEOUT,
                        )
                        if not messages:
                            continue

                        prev_last = self._last_seen.get(chat_id, 0)
                        new_last = prev_last

                        for m in messages:
                            if m is None:
                                continue
                            # Track the highest ID we've seen.
                            if m.id > new_last:
                                new_last = m.id

                            # Skip messages we already know about.
                            if m.id <= prev_last:
                                continue

                            key = (chat_id, m.id)
                            async with self._dedup_lock:
                                if key in self._processed:
                                    continue
                                self._processed[key] = None
                                while len(self._processed) > _MAX_PROCESSED:
                                    self._processed.popitem(last=False)

                            if not self.allowed_emojis:
                                continue

                            log.info(
                                "Poll: new post detected %s/%s – dispatching sweep",
                                chat_id,
                                m.id,
                            )
                            task = asyncio.create_task(
                                self._sweep_reactions(chat_id, m.id)
                            )
                            self._bg_tasks.add(task)
                            task.add_done_callback(self._bg_tasks.discard)

                        # Update the high-water mark.
                        if new_last > prev_last:
                            self._last_seen[chat_id] = new_last

                    except asyncio.CancelledError:
                        raise
                    except _AUTH_FATAL_ERRORS as e:
                        log.warning(
                            "Poll: master session dead: %s", e
                        )
                        break
                    except Exception:
                        log.debug(
                            "Poll: error fetching messages for %s",
                            chat_id,
                            exc_info=True,
                        )

        except asyncio.CancelledError:
            log.debug("Master poll loop cancelled.")
            raise

    # ================================================================ #
    #  Auto-join — master joins a channel automatically                #
    # ================================================================ #

    async def auto_join_channel(self, channel_entity: int | str) -> None:
        """Make the master account (or any available client) join a
        channel so it can monitor it for new posts.

        *channel_entity* can be a numeric channel ID, a username, or
        a ``t.me/+HASH`` / ``t.me/joinchat/HASH`` private invite link.
        """
        client = self.master_client
        if client is None or not client.is_connected():
            client = self._get_any_client()
        if client is None:
            log.warning("auto_join_channel: no client available to join %s", channel_entity)
            return

        entity_str = str(channel_entity)

        # Check if it's a private invite link.
        invite_match = _INVITE_HASH_RE.match(entity_str)
        if invite_match:
            invite_hash = invite_match.group(1)
            try:
                await asyncio.wait_for(
                    client(ImportChatInviteRequest(hash=invite_hash)),
                    timeout=_RPC_TIMEOUT,
                )
                log.info("Auto-joined private channel via invite hash: %s", invite_hash[:8])
            except Exception as e:
                log.warning(
                    "auto_join_channel: ImportChatInviteRequest failed for hash %s: %s",
                    invite_hash[:8],
                    e,
                )
            return

        # Public channel — resolve entity and join.
        try:
            entity = await asyncio.wait_for(
                client.get_entity(channel_entity),
                timeout=_RPC_TIMEOUT,
            )
            await asyncio.wait_for(
                client(JoinChannelRequest(channel=entity)),
                timeout=_RPC_TIMEOUT,
            )
            title = getattr(entity, "title", None) or str(channel_entity)
            log.info("Auto-joined channel: %s", title)
        except Exception as e:
            # Not fatal — the account may already be a member, or it's
            # a private channel that requires an invite link.
            log.warning(
                "auto_join_channel: JoinChannelRequest failed for %s: %s",
                channel_entity,
                e,
            )

    # ================================================================ #
    #  Reload helpers                                                  #
    # ================================================================ #

    async def reload_channels(self) -> None:
        old_channels = set(self.target_channels)
        rows = self.db.get_channels()
        self.target_channels = {int(r["channel_id"]) for r in rows}
        log.info("Channels reloaded: %d targets", len(self.target_channels))

        # Auto-join any newly added channels.
        new_channels = self.target_channels - old_channels
        for ch_id in new_channels:
            await self.auto_join_channel(ch_id)

        # Re-prime entity cache if channels changed so Telethon
        # subscribes to the new channel's updates.
        if new_channels:
            await self._prime_master_channels()

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

    async def reload_master(self) -> None:
        """Hot-swap the master account at runtime.

        Called from the bot UI when the user promotes a different
        account to master.
        """
        master_row = self.db.get_master_account()

        old_master_id = self.master_id

        if master_row is None:
            # No master set — remove handler from old master if any.
            if old_master_id and old_master_id in self.clients:
                old_handler = self._handlers.pop(old_master_id, None)
                if old_handler:
                    try:
                        self.clients[old_master_id].remove_event_handler(
                            old_handler, events.NewMessage
                        )
                    except Exception:
                        pass
            self.master_id = None
            log.warning("reload_master: no master configured.")
            self._start_poll_loop()
            return

        new_master_id = master_row["id"]

        # Remove event handler from old master if it changed.
        if old_master_id and old_master_id != new_master_id:
            old_handler = self._handlers.pop(old_master_id, None)
            if old_handler and old_master_id in self.clients:
                try:
                    self.clients[old_master_id].remove_event_handler(
                        old_handler, events.NewMessage
                    )
                except Exception:
                    pass
                log.info(
                    "Removed event handler from old master %s",
                    old_master_id[:8],
                )

        self.master_id = new_master_id

        # Register handler on new master if it's connected.
        if new_master_id in self.clients:
            client = self.clients[new_master_id]
            self._register_event_handler(new_master_id, client)
            try:
                await asyncio.wait_for(client.catch_up(), timeout=_RPC_TIMEOUT)
            except Exception as e:
                log.warning("reload_master: catch_up raised %s", e)
            log.info("Master switched to %s", new_master_id[:8])
        else:
            # Master account isn't connected yet — connect it.
            acc = self.db.get_account(new_master_id)
            if acc:
                await self._safe_add_client(acc)
                log.info("Connected new master %s during reload", new_master_id[:8])

        # Restart the poll loop with the new master.
        self._start_poll_loop()

        # Prime entity cache and auto-join all target channels.
        await self._prime_master_channels()
        for ch_id in list(self.target_channels):
            await self.auto_join_channel(ch_id)

    # ================================================================ #
    #  Keepalive loop — ensures clients stay alive                     #
    # ================================================================ #

    async def _client_keepalive_loop(self) -> None:
        """Periodically verify every client is still connected.

        Prioritises the master account — if the master dropped, it is
        reconnected first.
        """
        try:
            while True:
                await asyncio.sleep(_KEEPALIVE_INTERVAL)

                # Prioritise master reconnection.
                if self.master_id and self.master_id in self.clients:
                    mc = self.clients[self.master_id]
                    try:
                        if not mc.is_connected():
                            log.warning(
                                "Keepalive: MASTER %s disconnected – reconnecting.",
                                self.master_id[:8],
                            )
                            acc = self.db.get_account(self.master_id)
                            await self.remove_client(self.master_id)
                            if acc and acc["status"] == "active":
                                await self._safe_add_client(acc)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        log.exception("Keepalive: error for master %s", self.master_id[:8])

                # Check workers.
                for acc_id, client in list(self.clients.items()):
                    if acc_id == self.master_id:
                        continue  # already handled above
                    try:
                        if not client.is_connected():
                            log.warning(
                                "Keepalive: worker %s disconnected – reconnecting.",
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

        for acc_id, client in list(self.clients.items()):
            try:
                if not client.is_connected():
                    raise ConnectionError("Client disconnected")
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

        # Try to bring up accounts that are 'active' in DB but not running.
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
                return emoji

            except _EMOJI_RETRY_ERRORS as e:
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
        """React to a post across all connected accounts with guaranteed
        multi-account delivery and per-account circuit breaker.

        Tracks per-account success/failure with an attempt counter.
        Failed accounts (due to network timeout, temporary FloodWait,
        or connection drop) are retried with exponential backoff.

        Circuit breaker: if an account hits ``_MAX_ACCOUNT_ATTEMPTS``
        transient failures for this post, it is auto-marked as 'failed'
        in the database, disconnected, and an alert is sent to the admin.
        This prevents the queue from getting stuck on a broken account.

        Fatal errors (session revoked, account banned) and permanent
        channel errors (private/forbidden/invalid-peer) trip the breaker
        immediately on the first occurrence.
        """
        async with self._sweep_sem:
            log.info(
                "Sweep %s/%s – %d accounts, %d emojis",
                chat_id,
                msg_id,
                len(self.clients),
                len(self.allowed_emojis),
            )

            # Per-account attempt counter for transient failures.
            attempt_counts: dict[str, int] = {}

            # Build the list of account IDs that need to react.
            pending: set[str] = set(self.clients.keys())
            succeeded: set[str] = set()
            permanently_failed: set[str] = set()
            circuit_broken: set[str] = set()  # auto-deactivated accounts
            retry_round = 0
            backoff = _INITIAL_RETRY_BACKOFF

            while pending and retry_round <= _MAX_SWEEP_RETRIES:
                if retry_round > 0:
                    log.info(
                        "Sweep %s/%s retry round %d – %d accounts remaining, "
                        "backoff %.1fs",
                        chat_id,
                        msg_id,
                        retry_round,
                        len(pending),
                        backoff,
                    )
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, _MAX_RETRY_BACKOFF)

                round_targets = list(pending)
                round_failures: set[str] = set()

                for acc_id in round_targets:
                    client = self.clients.get(acc_id)
                    if client is None or not client.is_connected():
                        attempt_counts[acc_id] = attempt_counts.get(acc_id, 0) + 1
                        if attempt_counts[acc_id] >= _MAX_ACCOUNT_ATTEMPTS:
                            log.warning(
                                "Circuit breaker: %s disconnected %d times "
                                "– auto-deactivating",
                                acc_id[:8],
                                attempt_counts[acc_id],
                            )
                            self.db.update_account_status(acc_id, "failed")
                            await self.remove_client(acc_id)
                            await self._send_health_alert(
                                acc_id,
                                f"Auto-deactivated: disconnected {attempt_counts[acc_id]} "
                                f"times during sweep {chat_id}/{msg_id}",
                            )
                            circuit_broken.add(acc_id)
                        else:
                            log.debug(
                                "Sweep: %s not connected (attempt %d/%d), will retry.",
                                acc_id[:8],
                                attempt_counts[acc_id],
                                _MAX_ACCOUNT_ATTEMPTS,
                            )
                            round_failures.add(acc_id)
                        continue

                    # Cooldown check.
                    cooldown_until = self._cooldowns.get(acc_id, 0)
                    now = time.monotonic()
                    if now < cooldown_until:
                        wait = cooldown_until - now
                        log.debug(
                            "Account %s in cooldown for %.1fs",
                            acc_id[:8],
                            wait,
                        )
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
                        if result is not None:
                            succeeded.add(acc_id)
                        else:
                            # All emojis rejected — permanent for this post.
                            log.warning(
                                "Sweep: %s all emojis rejected for %s/%s "
                                "– marking permanent failure",
                                acc_label,
                                chat_id,
                                msg_id,
                            )
                            permanently_failed.add(acc_id)

                    except FloodWaitError as e:
                        self._cooldowns[acc_id] = time.monotonic() + e.seconds
                        attempt_counts[acc_id] = attempt_counts.get(acc_id, 0) + 1
                        if attempt_counts[acc_id] >= _MAX_ACCOUNT_ATTEMPTS:
                            log.warning(
                                "Circuit breaker: %s hit %d FloodWaits "
                                "– auto-deactivating",
                                acc_label,
                                attempt_counts[acc_id],
                            )
                            self.db.update_account_status(acc_id, "failed")
                            await self.remove_client(acc_id)
                            await self._send_health_alert(
                                acc_id,
                                f"Auto-deactivated: {attempt_counts[acc_id]} "
                                f"FloodWait errors during sweep {chat_id}/{msg_id}",
                            )
                            circuit_broken.add(acc_id)
                        else:
                            log.warning(
                                "FloodWait %ds for %s (attempt %d/%d) – will retry",
                                e.seconds,
                                acc_label,
                                attempt_counts[acc_id],
                                _MAX_ACCOUNT_ATTEMPTS,
                            )
                            round_failures.add(acc_id)

                    except _AUTH_FATAL_ERRORS as e:
                        log.warning(
                            "Session dead for %s during sweep: %s",
                            acc_label,
                            e,
                        )
                        self.db.update_account_status(acc_id, "failed")
                        await self.remove_client(acc_id)
                        await self._send_health_alert(acc_id, str(e))
                        permanently_failed.add(acc_id)

                    except (
                        ChannelPrivateError,
                        ChatWriteForbiddenError,
                        PeerIdInvalidError,
                    ) as e:
                        log.warning(
                            "Permanent channel error for %s: %s – "
                            "skipping retries",
                            acc_label,
                            e,
                        )
                        permanently_failed.add(acc_id)

                    except (asyncio.TimeoutError, ConnectionError, OSError) as e:
                        attempt_counts[acc_id] = attempt_counts.get(acc_id, 0) + 1
                        if attempt_counts[acc_id] >= _MAX_ACCOUNT_ATTEMPTS:
                            log.warning(
                                "Circuit breaker: %s hit %d transient errors "
                                "– auto-deactivating",
                                acc_label,
                                attempt_counts[acc_id],
                            )
                            self.db.update_account_status(acc_id, "failed")
                            await self.remove_client(acc_id)
                            await self._send_health_alert(
                                acc_id,
                                f"Auto-deactivated: {attempt_counts[acc_id]} transient "
                                f"errors ({type(e).__name__}) during sweep "
                                f"{chat_id}/{msg_id}",
                            )
                            circuit_broken.add(acc_id)
                        else:
                            log.warning(
                                "Transient error for %s on %s/%s: %s "
                                "(attempt %d/%d) – will retry",
                                acc_label,
                                chat_id,
                                msg_id,
                                e,
                                attempt_counts[acc_id],
                                _MAX_ACCOUNT_ATTEMPTS,
                            )
                            round_failures.add(acc_id)

                    except Exception:
                        attempt_counts[acc_id] = attempt_counts.get(acc_id, 0) + 1
                        if attempt_counts[acc_id] >= _MAX_ACCOUNT_ATTEMPTS:
                            log.warning(
                                "Circuit breaker: %s hit %d unexpected errors "
                                "– auto-deactivating",
                                acc_id[:8],
                                attempt_counts[acc_id],
                            )
                            self.db.update_account_status(acc_id, "failed")
                            await self.remove_client(acc_id)
                            await self._send_health_alert(
                                acc_id,
                                f"Auto-deactivated: {attempt_counts[acc_id]} unexpected "
                                f"errors during sweep {chat_id}/{msg_id}",
                            )
                            circuit_broken.add(acc_id)
                        else:
                            log.exception(
                                "Unexpected error for %s (attempt %d/%d) "
                                "– will retry",
                                acc_id[:8],
                                attempt_counts[acc_id],
                                _MAX_ACCOUNT_ATTEMPTS,
                            )
                            round_failures.add(acc_id)

                    # 1-second delay between accounts.
                    await asyncio.sleep(1)

                # Update the pending set for the next round.
                excluded = succeeded | permanently_failed | circuit_broken
                pending = round_failures - excluded
                retry_round += 1

            # ── Sweep summary ────────────────────────────────────────
            total = (
                len(succeeded) + len(permanently_failed)
                + len(circuit_broken) + len(pending)
            )
            if pending:
                log.warning(
                    "Sweep %s/%s INCOMPLETE after %d retries – "
                    "succeeded=%d, permanent_fail=%d, circuit_broken=%d, "
                    "still_pending=%d",
                    chat_id,
                    msg_id,
                    _MAX_SWEEP_RETRIES,
                    len(succeeded),
                    len(permanently_failed),
                    len(circuit_broken),
                    len(pending),
                )
            else:
                log.info(
                    "Sweep %s/%s COMPLETE – succeeded=%d, "
                    "permanent_fail=%d, circuit_broken=%d "
                    "(total=%d, retries=%d)",
                    chat_id,
                    msg_id,
                    len(succeeded),
                    len(permanently_failed),
                    len(circuit_broken),
                    total,
                    max(0, retry_round - 1),
                )

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
        accounts to react with guaranteed delivery + circuit breaker.

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

        attempt_counts: dict[str, int] = {}
        pending: set[str] = set(self.clients.keys())
        succeeded: set[str] = set()
        permanently_failed: dict[str, str] = {}  # acc_id → reason
        circuit_broken: dict[str, str] = {}      # acc_id → reason
        retry_round = 0
        backoff = _INITIAL_RETRY_BACKOFF

        while pending and retry_round <= _MAX_SWEEP_RETRIES:
            if retry_round > 0:
                log.info(
                    "Instant retry round %d – %d accounts remaining",
                    retry_round,
                    len(pending),
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _MAX_RETRY_BACKOFF)

            round_targets = list(pending)
            round_failures: set[str] = set()

            for acc_id in round_targets:
                client = self.clients.get(acc_id)
                if client is None or not client.is_connected():
                    attempt_counts[acc_id] = attempt_counts.get(acc_id, 0) + 1
                    if attempt_counts[acc_id] >= _MAX_ACCOUNT_ATTEMPTS:
                        self.db.update_account_status(acc_id, "failed")
                        await self.remove_client(acc_id)
                        await self._send_health_alert(
                            acc_id,
                            f"Auto-deactivated: disconnected "
                            f"{attempt_counts[acc_id]} times (instant react)",
                        )
                        circuit_broken[acc_id] = "Disconnected (circuit breaker)"
                    else:
                        round_failures.add(acc_id)
                    continue

                cooldown_until = self._cooldowns.get(acc_id, 0)
                now = time.monotonic()
                if now < cooldown_until:
                    wait = cooldown_until - now
                    log.debug(
                        "Instant: account %s in cooldown for %.1fs",
                        acc_id[:8],
                        wait,
                    )
                    await asyncio.sleep(wait)

                try:
                    if raw_id:
                        chat_id: int | str = int(f"-100{raw_id}")
                        peer = await asyncio.wait_for(
                            client.get_input_entity(chat_id),
                            timeout=_RPC_TIMEOUT,
                        )
                    else:
                        peer = await asyncio.wait_for(
                            client.get_input_entity(username),
                            timeout=_RPC_TIMEOUT,
                        )

                    result = await self._try_react_with_fallback(
                        client=client,
                        peer=peer,
                        msg_id=msg_id,
                        emojis=self.allowed_emojis,
                        acc_label=acc_id[:8],
                    )
                    if result:
                        log.info(
                            "Instant: %s reacted with %s", acc_id[:8], result
                        )
                        succeeded.add(acc_id)
                    else:
                        permanently_failed[acc_id] = "All emojis rejected"

                except FloodWaitError as e:
                    self._cooldowns[acc_id] = time.monotonic() + e.seconds
                    attempt_counts[acc_id] = attempt_counts.get(acc_id, 0) + 1
                    if attempt_counts[acc_id] >= _MAX_ACCOUNT_ATTEMPTS:
                        self.db.update_account_status(acc_id, "failed")
                        await self.remove_client(acc_id)
                        await self._send_health_alert(
                            acc_id,
                            f"Auto-deactivated: {attempt_counts[acc_id]} "
                            f"FloodWait errors (instant react)",
                        )
                        circuit_broken[acc_id] = "FloodWait (circuit breaker)"
                    else:
                        log.warning(
                            "Instant: FloodWait %ds for %s (attempt %d/%d)",
                            e.seconds,
                            acc_id[:8],
                            attempt_counts[acc_id],
                            _MAX_ACCOUNT_ATTEMPTS,
                        )
                        round_failures.add(acc_id)

                except _AUTH_FATAL_ERRORS as e:
                    log.warning(
                        "Instant: session dead for %s: %s",
                        acc_id[:8],
                        e,
                    )
                    self.db.update_account_status(acc_id, "failed")
                    await self.remove_client(acc_id)
                    await self._send_health_alert(acc_id, str(e))
                    permanently_failed[acc_id] = "Session revoked"

                except (
                    ChannelPrivateError,
                    ChatWriteForbiddenError,
                    PeerIdInvalidError,
                ) as e:
                    log.warning(
                        "Instant: channel error for %s: %s",
                        acc_id[:8],
                        e,
                    )
                    permanently_failed[acc_id] = str(e)

                except (asyncio.TimeoutError, ConnectionError, OSError) as e:
                    attempt_counts[acc_id] = attempt_counts.get(acc_id, 0) + 1
                    if attempt_counts[acc_id] >= _MAX_ACCOUNT_ATTEMPTS:
                        self.db.update_account_status(acc_id, "failed")
                        await self.remove_client(acc_id)
                        await self._send_health_alert(
                            acc_id,
                            f"Auto-deactivated: {attempt_counts[acc_id]} "
                            f"transient errors (instant react)",
                        )
                        circuit_broken[acc_id] = f"{type(e).__name__} (circuit breaker)"
                    else:
                        log.warning(
                            "Instant: transient error for %s: %s "
                            "(attempt %d/%d) – will retry",
                            acc_id[:8],
                            e,
                            attempt_counts[acc_id],
                            _MAX_ACCOUNT_ATTEMPTS,
                        )
                        round_failures.add(acc_id)

                except Exception as e:
                    attempt_counts[acc_id] = attempt_counts.get(acc_id, 0) + 1
                    if attempt_counts[acc_id] >= _MAX_ACCOUNT_ATTEMPTS:
                        self.db.update_account_status(acc_id, "failed")
                        await self.remove_client(acc_id)
                        await self._send_health_alert(
                            acc_id,
                            f"Auto-deactivated: {attempt_counts[acc_id]} "
                            f"unexpected errors (instant react)",
                        )
                        circuit_broken[acc_id] = f"{type(e).__name__} (circuit breaker)"
                    else:
                        log.exception(
                            "Instant: unexpected error for %s "
                            "(attempt %d/%d) – will retry",
                            acc_id[:8],
                            attempt_counts[acc_id],
                            _MAX_ACCOUNT_ATTEMPTS,
                        )
                        round_failures.add(acc_id)

                # 1-second delay between accounts.
                await asyncio.sleep(1)

            excluded = (
                succeeded
                | set(permanently_failed.keys())
                | set(circuit_broken.keys())
            )
            pending = round_failures - excluded
            retry_round += 1

        # Build summary.
        total_failed = len(permanently_failed) + len(circuit_broken)
        lines = ["⚡ *Instant Reaction Complete*\n"]
        lines.append(
            f"✅ Success: {len(succeeded)}  |  "
            f"❌ Failed: {total_failed}  |  "
            f"⏳ Pending: {len(pending)}"
        )
        retries_used = max(0, retry_round - 1)
        if retries_used > 0:
            lines.append(f"🔄 Retry rounds used: {retries_used}")
        if circuit_broken:
            lines.append(f"\n🔌 *Circuit Breaker ({len(circuit_broken)}):*")
            for reason in list(dict.fromkeys(circuit_broken.values()))[:3]:
                lines.append(f"• `{reason}`")
        if permanently_failed:
            unique_errors = list(dict.fromkeys(permanently_failed.values()))[:5]
            lines.append("\n*Permanent Errors:*")
            for err in unique_errors:
                lines.append(f"• `{err}`")
        if pending:
            lines.append(
                f"\n⚠️ {len(pending)} account(s) still failed after "
                f"{_MAX_SWEEP_RETRIES} retries."
            )
        return "\n".join(lines)

    # ================================================================ #
    #  Entity resolution helpers (for channel/username → ID)           #
    # ================================================================ #

    def _get_any_client(self) -> TelegramClient | None:
        """Return the first available connected client.

        Prefers the master client if available.
        """
        # Prefer master.
        mc = self.master_client
        if mc is not None and mc.is_connected():
            return mc
        # Fall back to any connected client.
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

        username = link_or_username.strip()
        m = _CHANNEL_LINK_RE.match(username)
        if m:
            raw_id, uname = m.groups()
            if raw_id:
                entity_ref: int | str = int(f"-100{raw_id}")
            else:
                entity_ref = uname
        elif username.startswith("@"):
            entity_ref = username[1:]
        else:
            entity_ref = username

        try:
            entity = await asyncio.wait_for(
                client.get_entity(entity_ref), timeout=_RPC_TIMEOUT
            )
            title = getattr(entity, "title", None) or getattr(entity, "username", None) or str(entity_ref)
            entity_id = entity.id
            if hasattr(entity, "broadcast") or hasattr(entity, "megagroup"):
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
