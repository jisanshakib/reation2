"""Supabase database helper – CRUD for accounts, channels, emojis, and
user roles, with a lightweight in-memory TTL cache to eliminate response
lag on hot-path reads (channels, emojis, role lookups).
"""

from __future__ import annotations

import logging
import time
from typing import Any

from supabase import create_client, Client

log = logging.getLogger(__name__)


# ------------------------------------------------------------------ #
#  Tiny TTL cache                                                    #
# ------------------------------------------------------------------ #

class _TTLCache:
    """Key → (value, expires_at) store with configurable default TTL."""

    def __init__(self, default_ttl: float = 30.0) -> None:
        self._store: dict[str, tuple[Any, float]] = {}
        self._default_ttl = default_ttl

    def get(self, key: str) -> Any | None:
        entry = self._store.get(key)
        if entry is None:
            return None
        value, expires_at = entry
        if time.monotonic() > expires_at:
            self._store.pop(key, None)
            return None
        return value

    def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        self._store[key] = (value, time.monotonic() + (ttl or self._default_ttl))

    def invalidate(self, *keys: str) -> None:
        for k in keys:
            self._store.pop(k, None)

    def clear(self) -> None:
        self._store.clear()


# ------------------------------------------------------------------ #
#  Database                                                          #
# ------------------------------------------------------------------ #

class Database:
    """Thin wrapper around ``supabase-py`` for the four bot tables."""

    def __init__(self, url: str, key: str) -> None:
        self.client: Client = create_client(url, key)
        self._cache = _TTLCache(default_ttl=30.0)

    # ================================================================ #
    #  Accounts                                                        #
    # ================================================================ #

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

    def get_all_accounts(self) -> list[dict[str, Any]]:
        """Return every account (all owners)."""
        return (
            self.client.table("accounts")
            .select("*")
            .order("created_at")
            .execute()
            .data
        )

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

    # ================================================================ #
    #  Channels  (cached)                                              #
    # ================================================================ #

    def add_channel(
        self, channel_id: int, channel_name: str, added_by: int
    ) -> dict[str, Any]:
        row = {
            "channel_id": channel_id,
            "channel_name": channel_name,
            "added_by": added_by,
        }
        res = self.client.table("channels").insert(row).execute()
        self._cache.invalidate("channels")
        return res.data[0]

    def get_channels(self) -> list[dict[str, Any]]:
        cached = self._cache.get("channels")
        if cached is not None:
            return cached
        rows = (
            self.client.table("channels")
            .select("*")
            .order("created_at")
            .execute()
            .data
        )
        self._cache.set("channels", rows)
        return rows

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
        self._cache.invalidate("channels")

    # ================================================================ #
    #  Emojis  (cached)                                                #
    # ================================================================ #

    def add_emoji(self, emoji: str, added_by: int) -> dict[str, Any]:
        row = {"emoji": emoji, "added_by": added_by}
        res = self.client.table("emojis").insert(row).execute()
        self._cache.invalidate("emojis")
        return res.data[0]

    def get_emojis(self) -> list[dict[str, Any]]:
        cached = self._cache.get("emojis")
        if cached is not None:
            return cached
        rows = (
            self.client.table("emojis")
            .select("*")
            .order("created_at")
            .execute()
            .data
        )
        self._cache.set("emojis", rows)
        return rows

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
        self._cache.invalidate("emojis")

    # ================================================================ #
    #  User roles  (cached, multi-tier: admin / moderator)             #
    # ================================================================ #

    _ROLE_CACHE_KEYS = ("user_roles", "authorized_ids")

    def add_user_role(self, user_id: int, role: str, added_by: int) -> dict[str, Any]:
        row = {"user_id": user_id, "role": role, "added_by": added_by}
        res = self.client.table("users_roles").insert(row).execute()
        self._cache.invalidate(*self._ROLE_CACHE_KEYS)
        return res.data[0]

    def get_user_roles(self) -> list[dict[str, Any]]:
        """Return all rows from users_roles."""
        cached = self._cache.get("user_roles")
        if cached is not None:
            return cached
        rows = (
            self.client.table("users_roles")
            .select("*")
            .order("created_at")
            .execute()
            .data
        )
        self._cache.set("user_roles", rows)
        return rows

    def get_user_role(self, role_id: str) -> dict[str, Any] | None:
        res = (
            self.client.table("users_roles")
            .select("*")
            .eq("id", role_id)
            .execute()
        )
        return res.data[0] if res.data else None

    def get_role_for_user(self, user_id: int) -> str | None:
        """Return the role string ('admin' or 'moderator') for a user,
        or ``None`` if the user has no role."""
        for r in self.get_user_roles():
            if int(r["user_id"]) == user_id:
                return r["role"]
        return None

    def get_authorized_ids(self) -> set[int]:
        """Return a cached set of ALL authorised user-IDs (admin + moderator)."""
        cached = self._cache.get("authorized_ids")
        if cached is not None:
            return cached
        rows = self.get_user_roles()
        ids = {int(r["user_id"]) for r in rows}
        self._cache.set("authorized_ids", ids)
        return ids

    def delete_user_role(self, role_id: str) -> None:
        self.client.table("users_roles").delete().eq("id", role_id).execute()
        self._cache.invalidate(*self._ROLE_CACHE_KEYS)

    def delete_user_role_by_user_id(self, user_id: int) -> None:
        self.client.table("users_roles").delete().eq("user_id", user_id).execute()
        self._cache.invalidate(*self._ROLE_CACHE_KEYS)
