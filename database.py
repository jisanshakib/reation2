"""Supabase database helper (v9) – CRUD for accounts, channels, emojis,
user roles, and reaction logs, with a lightweight in-memory TTL cache to
eliminate response lag on hot-path reads (channels, emojis, role lookups).

v9 changes
──────────
• ``reaction_logs`` table support: ``log_reaction()``,
  ``get_reaction_summary()``, ``get_error_logs()``,
  ``clear_old_logs()``.

v8 changes
──────────
• ``update_account_session()`` for rebind flow.

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

    def get_failed_accounts(self) -> list[dict[str, Any]]:
        """Return only accounts with status ``'failed'``."""
        try:
            return (
                self.client.table("accounts")
                .select("*")
                .eq("status", "failed")
                .execute()
                .data
            )
        except Exception:
            log.exception("get_failed_accounts failed")
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

    def update_account_session(self, account_id: str, session_str: str) -> None:
        """Replace the session string for an existing account and mark it active."""
        try:
            (
                self.client.table("accounts")
                .update({"session_str": session_str, "status": "active"})
                .eq("id", account_id)
                .execute()
            )
        except Exception:
            log.exception("update_account_session failed for %s", account_id)

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
        self,
        channel_id: int,
        channel_name: str,
        added_by: int,
        username: str | None = None,
        invite_link: str | None = None,
    ) -> dict[str, Any]:
        """Add a channel or reactivate a ghost record.

        Stores optional ``username`` (@channelname) or ``invite_link``
        (https://t.me/+) alongside ``channel_id`` so isolated worker sessions
        can resolve and join the channel without a cached access_hash.
        """
        row: dict[str, Any] = {
            "channel_id": channel_id,
            "channel_name": channel_name,
            "added_by": added_by,
        }
        if username:
            row["username"] = username.lstrip("@").strip()
        if invite_link:
            row["invite_link"] = invite_link.strip()

        try:
            res = (
                self.client.table("channels")
                .upsert(row, on_conflict="channel_id")
                .execute()
            )
        except Exception as e:
            err_msg = str(e).lower()
            if "username" in err_msg or "invite_link" in err_msg or "pgrst204" in err_msg or "column" in err_msg:
                log.warning(
                    "channels table missing username/invite_link columns; falling back to base columns. "
                    "Run migration in schema.sql to enable worker channel routing."
                )
                res = (
                    self.client.table("channels")
                    .upsert(
                        {
                            "channel_id": channel_id,
                            "channel_name": channel_name,
                            "added_by": added_by,
                        },
                        on_conflict="channel_id",
                    )
                    .execute()
                )
            else:
                raise

        self._cache.invalidate("channels")
        return res.data[0]

    def update_channel_metadata(
        self,
        channel_id: int,
        username: str | None = None,
        invite_link: str | None = None,
    ) -> bool:
        """Update username and/or invite_link for an existing channel record."""
        update_data: dict[str, Any] = {}
        if username is not None:
            update_data["username"] = username.lstrip("@").strip()
        if invite_link is not None:
            update_data["invite_link"] = invite_link.strip()
        if not update_data:
            return False
        try:
            self.client.table("channels").update(update_data).eq("channel_id", channel_id).execute()
            self._cache.invalidate("channels")
            log.info("Updated channel metadata for %d: %s", channel_id, update_data)
            return True
        except Exception:
            log.exception("Failed to update channel metadata for %d", channel_id)
            return False

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
        """Hard-delete a channel by its UUID primary key.

        Also removes any ghost rows that share the same ``channel_id``
        (numeric Telegram ID) to prevent duplicate-key errors on
        future re-addition.
        """
        try:
            # First, grab the Telegram channel_id so we can nuke ghosts.
            ch = self.get_channel(channel_db_id)
            tg_id = int(ch["channel_id"]) if ch else None

            # Delete the target row by UUID.
            self.client.table("channels").delete().eq("id", channel_db_id).execute()

            # Safety sweep: delete any remaining rows with the same
            # Telegram channel_id (ghost / orphan records).
            if tg_id is not None:
                self.client.table("channels").delete().eq("channel_id", tg_id).execute()

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

    # ================================================================ #
    #  Reaction logs                                                    #
    # ================================================================ #

    def log_reaction(
        self,
        channel_id: int,
        channel_name: str,
        post_id: int,
        account_id: str,
        account_name: str,
        status: str,
        error_message: str | None = None,
    ) -> None:
        """Insert a single reaction outcome into ``reaction_logs``.

        *status* must be ``'success'`` or ``'failed'``.
        """
        try:
            self.client.table("reaction_logs").insert(
                {
                    "channel_id": channel_id,
                    "channel_name": channel_name,
                    "post_id": post_id,
                    "account_id": account_id,
                    "account_name": account_name,
                    "status": status,
                    "error_message": error_message,
                }
            ).execute()
        except Exception:
            log.exception("log_reaction failed for %s/%s", channel_id, post_id)

    def get_reaction_summary(self, today_only: bool = True) -> dict[str, Any]:
        """Return aggregated summary data for the analytics dashboard.

        Returns a dict with keys:
        - ``total_success``: int
        - ``total_failed``: int
        - ``total_posts``: int  (unique channel_id+post_id pairs)
        - ``channels``: list of {channel_name, success_count, failed_count}
        """
        try:
            q = self.client.table("reaction_logs").select("*")
            if today_only:
                q = q.gte("created_at", _today_start())
            rows = q.execute().data

            total_success = 0
            total_failed = 0
            posts_seen: set[str] = set()
            channel_stats: dict[str, dict[str, int]] = {}

            for r in rows:
                post_key = f"{r['channel_id']}:{r['post_id']}"
                posts_seen.add(post_key)

                ch_name = r.get("channel_name") or str(r["channel_id"])
                if ch_name not in channel_stats:
                    channel_stats[ch_name] = {"success": 0, "failed": 0}

                if r["status"] == "success":
                    total_success += 1
                    channel_stats[ch_name]["success"] += 1
                else:
                    total_failed += 1
                    channel_stats[ch_name]["failed"] += 1

            channels = [
                {
                    "channel_name": name,
                    "success_count": stats["success"],
                    "failed_count": stats["failed"],
                }
                for name, stats in channel_stats.items()
            ]

            return {
                "total_success": total_success,
                "total_failed": total_failed,
                "total_posts": len(posts_seen),
                "channels": channels,
            }
        except Exception:
            log.exception("get_reaction_summary failed")
            return {
                "total_success": 0,
                "total_failed": 0,
                "total_posts": 0,
                "channels": [],
            }

    def get_error_logs(self, limit: int = 20) -> list[dict[str, Any]]:
        """Return the most recent failed reaction logs for the error view."""
        try:
            return (
                self.client.table("reaction_logs")
                .select("*")
                .eq("status", "failed")
                .order("created_at", desc=True)
                .limit(limit)
                .execute()
                .data
            )
        except Exception:
            log.exception("get_error_logs failed")
            return []

    def clear_old_logs(self, days: int = 7) -> int:
        """Delete reaction logs older than *days* days.

        Returns the number of rows deleted (best-effort).
        """
        try:
            from datetime import datetime, timedelta, timezone

            cutoff = (
                datetime.now(timezone.utc) - timedelta(days=days)
            ).isoformat()
            res = (
                self.client.table("reaction_logs")
                .delete()
                .lt("created_at", cutoff)
                .execute()
            )
            count = len(res.data) if res.data else 0
            log.info("Cleared %d old reaction logs (older than %d days).", count, days)
            return count
        except Exception:
            log.exception("clear_old_logs failed")
            return 0


def _today_start() -> str:
    """Return the start of today (UTC) as an ISO-8601 string."""
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    return now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
