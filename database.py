"""Supabase database helper (v7) – CRUD for accounts, channels, emojis,
and user roles, with a lightweight in-memory TTL cache to eliminate
response lag on hot-path reads (channels, emojis, role lookups).

v7 changes
──────────
• ``is_master`` column support: ``set_master_account()``,
  ``get_master_account()``, ``clear_master_account()``.
• ``add_account()`` now accepts optional ``is_master`` flag.
• All public methods wrapped in try/except with logging so callers
  never see unhandled DB exceptions.
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
    #  Owner bootstrap                                                 #
    # ================================================================ #

    def ensure_owner_role(self, owner_id: int) -> None:
        """Upsert the permanent owner into ``users_roles`` so the bot
        always has at least one authorised user after startup.

        Uses ``on_conflict="user_id"`` — if the row already exists the
        role is silently updated to ``'owner'`` (idempotent).
        """
        try:
            self.client.table("users_roles").upsert(
                {
                    "user_id": owner_id,
                    "role": "owner",
                    "added_by": owner_id,
                },
                on_conflict="user_id",
            ).execute()
            self._cache.invalidate(*self._ROLE_CACHE_KEYS)
            log.info("Owner role ensured for user %d.", owner_id)
        except Exception:
            log.exception(
                "Failed to upsert owner role for %d – the bot will "
                "still recognise this ID via the hardcoded check.",
                owner_id,
            )

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
        is_master: bool = False,
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
            "is_master": is_master,
        }
        # If registering as master, clear the old master first.
        if is_master:
            self.clear_master_account()
        res = self.client.table("accounts").insert(row).execute()
        return res.data[0]

    def get_accounts(self, owner_id: int | None = None) -> list[dict[str, Any]]:
        """Return all accounts, optionally filtered by *owner_id*."""
        try:
            q = self.client.table("accounts").select("*").order("created_at")
            if owner_id is not None:
                q = q.eq("owner_id", owner_id)
            return q.execute().data
        except Exception:
            log.exception("get_accounts failed")
            return []

    def get_all_accounts(self) -> list[dict[str, Any]]:
        """Return every account (all owners)."""
        try:
            return (
                self.client.table("accounts")
                .select("*")
                .order("created_at")
                .execute()
                .data
            )
        except Exception:
            log.exception("get_all_accounts failed")
            return []

    def get_account(self, account_id: str) -> dict[str, Any] | None:
        """Return a single account by UUID, or ``None``."""
        try:
            res = (
                self.client.table("accounts")
                .select("*")
                .eq("id", account_id)
                .execute()
            )
            return res.data[0] if res.data else None
        except Exception:
            log.exception("get_account failed for %s", account_id)
            return None

    def get_active_accounts(self) -> list[dict[str, Any]]:
        """Return only accounts with status ``'active'``."""
        try:
            return (
                self.client.table("accounts")
                .select("*")
                .eq("status", "active")
                .execute()
                .data
            )
        except Exception:
            log.exception("get_active_accounts failed")
            return []

    def update_account_status(self, account_id: str, status: str) -> None:
        try:
            (
                self.client.table("accounts")
                .update({"status": status})
                .eq("id", account_id)
                .execute()
            )
        except Exception:
            log.exception("update_account_status failed for %s", account_id)

    def delete_account(self, account_id: str) -> None:
        try:
            self.client.table("accounts").delete().eq("id", account_id).execute()
        except Exception:
            log.exception("delete_account failed for %s", account_id)

    # ── Master account management ─────────────────────────────────── #

    def get_master_account(self) -> dict[str, Any] | None:
        """Return the account row with ``is_master=true``, or ``None``."""
        try:
            res = (
                self.client.table("accounts")
                .select("*")
                .eq("is_master", True)
                .limit(1)
                .execute()
            )
            return res.data[0] if res.data else None
        except Exception:
            log.exception("get_master_account failed")
            return None

    def set_master_account(self, account_id: str) -> None:
        """Promote *account_id* to master, demoting any previous master."""
        try:
            # Clear old master(s).
            self.clear_master_account()
            # Set the new one.
            (
                self.client.table("accounts")
                .update({"is_master": True})
                .eq("id", account_id)
                .execute()
            )
            log.info("Master account set to %s", account_id)
        except Exception:
            log.exception("set_master_account failed for %s", account_id)

    def clear_master_account(self) -> None:
        """Demote all master accounts back to worker."""
        try:
            (
                self.client.table("accounts")
                .update({"is_master": False})
                .eq("is_master", True)
                .execute()
            )
        except Exception:
            log.exception("clear_master_account failed")

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
        try:
            rows = (
                self.client.table("channels")
                .select("*")
                .order("created_at")
                .execute()
                .data
            )
        except Exception:
            log.exception("get_channels failed")
            return []
        self._cache.set("channels", rows)
        return rows

    def get_channel(self, channel_db_id: str) -> dict[str, Any] | None:
        try:
            res = (
                self.client.table("channels")
                .select("*")
                .eq("id", channel_db_id)
                .execute()
            )
            return res.data[0] if res.data else None
        except Exception:
            log.exception("get_channel failed for %s", channel_db_id)
            return None

    def delete_channel(self, channel_db_id: str) -> None:
        try:
            self.client.table("channels").delete().eq("id", channel_db_id).execute()
            self._cache.invalidate("channels")
        except Exception:
            log.exception("delete_channel failed for %s", channel_db_id)

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
        try:
            rows = (
                self.client.table("emojis")
                .select("*")
                .order("created_at")
                .execute()
                .data
            )
        except Exception:
            log.exception("get_emojis failed")
            return []
        self._cache.set("emojis", rows)
        return rows

    def get_emoji(self, emoji_id: str) -> dict[str, Any] | None:
        try:
            res = (
                self.client.table("emojis")
                .select("*")
                .eq("id", emoji_id)
                .execute()
            )
            return res.data[0] if res.data else None
        except Exception:
            log.exception("get_emoji failed for %s", emoji_id)
            return None

    def delete_emoji(self, emoji_id: str) -> None:
        try:
            self.client.table("emojis").delete().eq("id", emoji_id).execute()
            self._cache.invalidate("emojis")
        except Exception:
            log.exception("delete_emoji failed for %s", emoji_id)

    # ================================================================ #
    #  User roles  (cached, multi-tier: owner / admin / moderator)     #
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
        try:
            rows = (
                self.client.table("users_roles")
                .select("*")
                .order("created_at")
                .execute()
                .data
            )
        except Exception:
            log.exception("get_user_roles failed")
            return []
        self._cache.set("user_roles", rows)
        return rows

    def get_user_role(self, role_id: str) -> dict[str, Any] | None:
        try:
            res = (
                self.client.table("users_roles")
                .select("*")
                .eq("id", role_id)
                .execute()
            )
            return res.data[0] if res.data else None
        except Exception:
            log.exception("get_user_role failed for %s", role_id)
            return None

    def get_role_for_user(self, user_id: int) -> str | None:
        """Return the role string ('owner', 'admin' or 'moderator') for
        a user, or ``None`` if the user has no role."""
        for r in self.get_user_roles():
            if int(r["user_id"]) == user_id:
                return r["role"]
        return None

    def get_authorized_ids(self) -> set[int]:
        """Return a cached set of ALL authorised user-IDs."""
        cached = self._cache.get("authorized_ids")
        if cached is not None:
            return cached
        rows = self.get_user_roles()
        ids = {int(r["user_id"]) for r in rows}
        self._cache.set("authorized_ids", ids)
        return ids

    def delete_user_role(self, role_id: str) -> None:
        try:
            self.client.table("users_roles").delete().eq("id", role_id).execute()
            self._cache.invalidate(*self._ROLE_CACHE_KEYS)
        except Exception:
            log.exception("delete_user_role failed for %s", role_id)

    def delete_user_role_by_user_id(self, user_id: int) -> None:
        try:
            self.client.table("users_roles").delete().eq("user_id", user_id).execute()
            self._cache.invalidate(*self._ROLE_CACHE_KEYS)
        except Exception:
            log.exception("delete_user_role_by_user_id failed for %d", user_id)
