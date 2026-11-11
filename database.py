"""Supabase database helper – CRUD for accounts, channels, and emojis."""

from __future__ import annotations

import logging
from typing import Any

from supabase import create_client, Client

log = logging.getLogger(__name__)


class Database:
    """Thin wrapper around ``supabase-py`` for the three bot tables."""

    def __init__(self, url: str, key: str) -> None:
        self.client: Client = create_client(url, key)

    # ------------------------------------------------------------------ #
    #  Accounts                                                          #
    # ------------------------------------------------------------------ #

    def add_account(
        self,
        owner_id: int,
        name: str,
        phone: str | None,
        api_id: str,
        api_hash: str,
        session_str: str,
    ) -> dict[str, Any]:
        """Insert a new account and return its row."""
        row = {
            "owner_id": owner_id,
            "name": name,
            "phone": phone,
            "api_id": api_id,
            "api_hash": api_hash,
            "session_str": session_str,
            "status": "active",
        }
        res = self.client.table("accounts").insert(row).execute()
        return res.data[0]

    def get_accounts(self, owner_id: int | None = None) -> list[dict[str, Any]]:
        """Return all accounts, optionally filtered by *owner_id*."""
        q = self.client.table("accounts").select("*").order("created_at")
        if owner_id is not None:
            q = q.eq("owner_id", owner_id)
        return q.execute().data

    def get_account(self, account_id: str) -> dict[str, Any] | None:
        """Return a single account by UUID, or ``None``."""
        res = (
            self.client.table("accounts")
            .select("*")
            .eq("id", account_id)
            .execute()
        )
        return res.data[0] if res.data else None

    def get_active_accounts(self) -> list[dict[str, Any]]:
        """Return only accounts with status ``'active'``."""
        return (
            self.client.table("accounts")
            .select("*")
            .eq("status", "active")
            .execute()
            .data
        )

    def update_account_status(self, account_id: str, status: str) -> None:
        (
            self.client.table("accounts")
            .update({"status": status})
            .eq("id", account_id)
            .execute()
        )

    def delete_account(self, account_id: str) -> None:
        self.client.table("accounts").delete().eq("id", account_id).execute()

    # ------------------------------------------------------------------ #
    #  Channels                                                          #
    # ------------------------------------------------------------------ #

    def add_channel(
        self, channel_id: int, channel_name: str, added_by: int
    ) -> dict[str, Any]:
        row = {
            "channel_id": channel_id,
            "channel_name": channel_name,
            "added_by": added_by,
        }
        res = self.client.table("channels").insert(row).execute()
        return res.data[0]

    def get_channels(self) -> list[dict[str, Any]]:
        return (
            self.client.table("channels")
            .select("*")
            .order("created_at")
            .execute()
            .data
        )

    def get_channel(self, channel_db_id: str) -> dict[str, Any] | None:
        res = (
            self.client.table("channels")
            .select("*")
            .eq("id", channel_db_id)
            .execute()
        )
        return res.data[0] if res.data else None

    def delete_channel(self, channel_db_id: str) -> None:
        self.client.table("channels").delete().eq("id", channel_db_id).execute()

    # ------------------------------------------------------------------ #
    #  Emojis                                                            #
    # ------------------------------------------------------------------ #

    def add_emoji(self, emoji: str, added_by: int) -> dict[str, Any]:
        row = {"emoji": emoji, "added_by": added_by}
        res = self.client.table("emojis").insert(row).execute()
        return res.data[0]

    def get_emojis(self) -> list[dict[str, Any]]:
        return (
            self.client.table("emojis")
            .select("*")
            .order("created_at")
            .execute()
            .data
        )

    def get_emoji(self, emoji_id: str) -> dict[str, Any] | None:
        res = (
            self.client.table("emojis")
            .select("*")
            .eq("id", emoji_id)
            .execute()
        )
        return res.data[0] if res.data else None

    def delete_emoji(self, emoji_id: str) -> None:
        self.client.table("emojis").delete().eq("id", emoji_id).execute()
