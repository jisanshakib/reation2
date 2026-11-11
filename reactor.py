"""Auto-Reaction Engine – Telethon-based reactor that listens to target
channels and sends reactions from every registered account with a strict
1-second inter-account delay.
"""

from __future__ import annotations

import asyncio
import logging
import random
import re
from collections import OrderedDict
from typing import TYPE_CHECKING

from telethon import TelegramClient, events
from telethon.errors import (
    ReactionInvalidError,
    ChannelPrivateError,
    ChatWriteForbiddenError,
    FloodWaitError,
)
from telethon.sessions import StringSession
from telethon.tl.functions.messages import SendReactionRequest
from telethon.tl.types import ReactionEmoji

if TYPE_CHECKING:
    from database import Database

log = logging.getLogger(__name__)

# Maximum number of processed message keys to keep in memory.
_MAX_PROCESSED = 5_000


class Reactor:
    """Manages Telethon user-clients, listens for new messages in target
    channels, and dispatches emoji reactions."""

    def __init__(self, db: Database) -> None:
        self.db = db
        self.clients: dict[str, TelegramClient] = {}       # account_id -> client
        self.target_channels: set[int] = set()
        self.allowed_emojis: list[str] = []
        self._processed: OrderedDict[tuple[int, int], None] = OrderedDict()
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------ #
    #  Lifecycle                                                         #
    # ------------------------------------------------------------------ #

    async def start(self) -> None:
        """Load state from the DB and connect all active accounts."""
        await self.reload_channels()
        await self.reload_emojis()

        for acc in self.db.get_active_accounts():
            try:
                await self.add_client(acc)
            except Exception:
                log.exception("Failed to connect account %s (%s)", acc["name"], acc["id"])
                self.db.update_account_status(acc["id"], "failed")

        log.info(
            "Reactor started – %d clients, %d channels, %d emojis",
            len(self.clients),
            len(self.target_channels),
            len(self.allowed_emojis),
        )

    async def stop(self) -> None:
        """Disconnect every Telethon client."""
        for acc_id in list(self.clients):
            await self.remove_client(acc_id)
        log.info("Reactor stopped.")

    # ------------------------------------------------------------------ #
    #  Client management                                                 #
    # ------------------------------------------------------------------ #

    async def add_client(self, account: dict) -> None:
        """Create a Telethon client from *account* row, connect, and register
        the new-message handler."""
        acc_id = account["id"]
        session = StringSession(account["session_str"])
        client = TelegramClient(session, int(account["api_id"]), account["api_hash"])

        await client.connect()
        if not await client.is_user_authorized():
            log.warning("Account %s (%s) is not authorised.", account["name"], acc_id)
            self.db.update_account_status(acc_id, "failed")
            await client.disconnect()
            return

        # Register the listener on this client.
        @client.on(events.NewMessage())
        async def _on_new_message(event: events.NewMessage.Event) -> None:
            await self._handle_message(event)

        self.clients[acc_id] = client
        self.db.update_account_status(acc_id, "active")
        log.info("Connected account %s (%s)", account["name"], acc_id)

    async def remove_client(self, account_id: str) -> None:
        """Disconnect and forget a client."""
        client = self.clients.pop(account_id, None)
        if client is not None:
            try:
                await client.disconnect()
            except Exception:
                pass

    # ------------------------------------------------------------------ #
    #  Reload helpers (called by bot handlers after DB changes)          #
    # ------------------------------------------------------------------ #

    async def reload_channels(self) -> None:
        rows = self.db.get_channels()
        self.target_channels = {int(r["channel_id"]) for r in rows}

    async def reload_emojis(self) -> None:
        rows = self.db.get_emojis()
        self.allowed_emojis = [r["emoji"] for r in rows]

    async def reload_accounts(self) -> None:
        """Synchronise running clients with the DB (add new / remove deleted)."""
        db_accounts = {a["id"]: a for a in self.db.get_active_accounts()}

        # Remove clients no longer in DB.
        for acc_id in list(self.clients):
            if acc_id not in db_accounts:
                await self.remove_client(acc_id)

        # Add new clients.
        for acc_id, acc in db_accounts.items():
            if acc_id not in self.clients:
                try:
                    await self.add_client(acc)
                except Exception:
                    log.exception("reload_accounts: failed to add %s", acc_id)
                    self.db.update_account_status(acc_id, "failed")

    # ------------------------------------------------------------------ #
    #  Core reaction logic                                               #
    # ------------------------------------------------------------------ #

    async def _handle_message(self, event: events.NewMessage.Event) -> None:
        """Called on every new message received by *any* connected client."""
        chat_id = event.chat_id
        msg_id = event.message.id

        if chat_id not in self.target_channels:
            return

        # De-duplicate: only one client should trigger the reaction sweep.
        key = (chat_id, msg_id)
        async with self._lock:
            if key in self._processed:
                return
            self._processed[key] = None
            # Evict old entries to prevent unbounded memory growth.
            while len(self._processed) > _MAX_PROCESSED:
                self._processed.popitem(last=False)

        if not self.allowed_emojis:
            return

        log.info("New message in %s / %s – reacting with %d accounts", chat_id, msg_id, len(self.clients))

        for acc_id, client in list(self.clients.items()):
            emoji = random.choice(self.allowed_emojis)
            try:
                peer = await client.get_input_entity(chat_id)
                await client(
                    SendReactionRequest(
                        peer=peer,
                        msg_id=msg_id,
                        reaction=[ReactionEmoji(emoticon=emoji)],
                    )
                )
                log.debug("  %s reacted with %s", acc_id[:8], emoji)
            except FloodWaitError as e:
                log.warning("FloodWait %ds for account %s", e.seconds, acc_id[:8])
                await asyncio.sleep(e.seconds)
            except (ReactionInvalidError, ChannelPrivateError, ChatWriteForbiddenError) as e:
                log.warning("Reaction error for %s: %s", acc_id[:8], e)
            except Exception:
                log.exception("Unexpected reaction error for %s", acc_id[:8])

            # Strict 1-second delay between accounts
            await asyncio.sleep(1)

    # ------------------------------------------------------------------ #
    #  Check helper (used by the bot's "Check 🔍" button)               #
    # ------------------------------------------------------------------ #

    async def check_react(self, account_id: str, post_link: str) -> str:
        """Send a test reaction to *post_link* using a single account.

        Returns a human-readable status string.
        """
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
                # Private channel numeric ID (need to prepend -100)
                chat_id = int(f"-100{raw_id}")
                peer = await client.get_input_entity(chat_id)
            else:
                peer = await client.get_input_entity(username)

            emoji = random.choice(self.allowed_emojis) if self.allowed_emojis else "👍"
            await client(
                SendReactionRequest(
                    peer=peer,
                    msg_id=msg_id,
                    reaction=[ReactionEmoji(emoticon=emoji)],
                )
            )
            return f"✅ Reacted with {emoji} successfully!"
        except Exception as e:
            return f"❌ Reaction failed: `{e}`"
