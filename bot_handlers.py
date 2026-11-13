"""100 % Inline-Keyboard-driven Telegram bot UI  (v8).

v8 changes
──────────
• **Smart Account Add with Dual Session Options**: after entering name,
  API ID, and API Hash the wizard presents a choice:
  – 📋 *Manual Session Paste* (legacy string-session input).
  – ⚡ *Auto Session Generator* (built-in: phone → OTP → optional 2FA
    → extracts StringSession automatically).
• **Interactive Account Rebind** (``🔄 Rebind`` button): shows stored
  details (never the session string), tests the existing session, and
  if it's dead re-authenticates in-place with OTP / 2FA flow.
• Temporary Telethon clients used during auto-gen and rebind are cleaned
  up on cancel, error, or success via ``_cleanup_temp_client()``.

Preserved from v7
──────────────────
• 👑 Master Account UI, ⚡ Instant Reaction, channel link / @username
  resolution, admin @username resolution, emoji management, admin
  panel, role management, /addadmin command.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from telethon import TelegramClient
from telethon.errors import (
    SessionPasswordNeededError,
    PhoneCodeInvalidError,
    PhoneCodeExpiredError,
    PasswordHashInvalidError,
    FloodWaitError,
    AuthKeyUnregisteredError,
    UserDeactivatedBanError,
)
from telethon.sessions import StringSession

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from config import PERMANENT_OWNER_ID
from database import Database
from reactor import Reactor

log = logging.getLogger(__name__)

# Timeout for Telethon RPC inside the bot flows.
_TEL_TIMEOUT = 20.0

# ------------------------------------------------------------------ #
#  In-memory user states  (keyed by ``user_id``)                     #
# ------------------------------------------------------------------ #
# States: reg_name, reg_api_id, reg_api_hash, reg_session_choice,
#         reg_session, reg_auto_phone, reg_auto_otp, reg_auto_2fa,
#         rebind_otp, rebind_2fa,
#         check, add_channel, add_emoji, add_role_user, add_role_select,
#         instant_react

user_states: dict[int, dict[str, Any]] = {}

# Module-level references – set once by ``setup_handlers``.
db: Database = None  # type: ignore[assignment]
reactor: Reactor = None  # type: ignore[assignment]


# ================================================================== #
#  Temp-client cleanup helper                                         #
# ================================================================== #

async def _cleanup_temp_client(user_id: int) -> None:
    """Disconnect and discard any temporary Telethon client stored in
    the user's state (used during auto-gen and rebind flows)."""
    state = user_states.get(user_id)
    if state is None:
        return
    client: TelegramClient | None = state["data"].pop("_client", None)
    if client is not None:
        try:
            if client.is_connected():
                await client.disconnect()
        except Exception:
            pass


# ================================================================== #
#  Role helpers                                                       #
# ================================================================== #

_ROLE_ICON = {"owner": "👑", "admin": "🛡️", "moderator": "👁️"}


def _is_permanent_owner(user_id: int) -> bool:
    return user_id == PERMANENT_OWNER_ID


def _get_role(user_id: int) -> str | None:
    """Return ``'owner'``, ``'admin'``, ``'moderator'``, or ``None``."""
    if _is_permanent_owner(user_id):
        return "owner"
    return db.get_role_for_user(user_id)


def _is_authorized(user_id: int) -> bool:
    return _get_role(user_id) is not None


def _can_manage_roles(user_id: int) -> bool:
    return _get_role(user_id) == "owner"


def _can_manage_accounts(user_id: int) -> bool:
    """Owner and Admin can add / delete / check accounts."""
    return _get_role(user_id) in ("owner", "admin")


# Moderator+ can manage channels and emojis.
def _can_manage_channels(user_id: int) -> bool:
    return _get_role(user_id) in ("owner", "admin", "moderator")


# ================================================================== #
#  Keyboard builders                                                  #
# ================================================================== #

def _back_button(target: str = "main") -> InlineKeyboardButton:
    return InlineKeyboardButton("⬅️ Back", callback_data=target)


def _main_menu_kb(user_id: int) -> InlineKeyboardMarkup:
    role = _get_role(user_id)
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton("📱 Accounts", callback_data="accounts")],
        [InlineKeyboardButton("📡 Channels", callback_data="channels")],
        [InlineKeyboardButton("😀 Emojis", callback_data="emojis")],
        [InlineKeyboardButton("⚡ Instant Reaction", callback_data="instant_react")],
        [InlineKeyboardButton("📊 Analytics", callback_data="stats")],
    ]
    if role == "owner":
        rows.append([InlineKeyboardButton("👑 Admins", callback_data="admins")])
    rows.append([InlineKeyboardButton("📖 Help Guide", callback_data="help")])
    return InlineKeyboardMarkup(rows)


# ── Accounts ────────────────────────────────────────────────────── #

def _accounts_kb(accounts: list[dict], can_add: bool) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for acc in accounts:
        if acc.get("is_master"):
            icon = "👑"
        elif acc["status"] == "active":
            icon = "🟢"
        else:
            icon = "🔴"
        rows.append(
            [InlineKeyboardButton(f"{icon} {acc['name']}", callback_data=f"acc:{acc['id']}")]
        )
    if can_add:
        rows.append([InlineKeyboardButton("➕ Add Account", callback_data="acc_add")])
    if can_add:
        rows.append([InlineKeyboardButton("⚡ Rebind All Accounts", callback_data="acc_rebind_all")])
    rows.append([_back_button()])
    return InlineKeyboardMarkup(rows)


def _account_detail_kb(
    acc_id: str, can_manage: bool, is_master: bool = False
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if can_manage:
        rows.append(
            [
                InlineKeyboardButton("Check 🔍", callback_data=f"acc_chk:{acc_id}"),
                InlineKeyboardButton("Delete 🗑️", callback_data=f"acc_del:{acc_id}"),
            ]
        )
        rows.append(
            [InlineKeyboardButton("🔄 Rebind", callback_data=f"acc_rebind:{acc_id}")]
        )
        if not is_master:
            rows.append(
                [InlineKeyboardButton("👑 Set as Master", callback_data=f"acc_master:{acc_id}")]
            )
    rows.append([_back_button("accounts")])
    return InlineKeyboardMarkup(rows)


# ── Channels ────────────────────────────────────────────────────── #

def _channels_kb(channels: list[dict]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for ch in channels:
        rows.append(
            [InlineKeyboardButton(f"📡 {ch['channel_name']}", callback_data=f"ch:{ch['id']}")]
        )
    rows.append([InlineKeyboardButton("➕ Add Channel", callback_data="ch_add")])
    rows.append([InlineKeyboardButton("🔗 Join All Accounts", callback_data="ch_join_all")])
    rows.append([_back_button()])
    return InlineKeyboardMarkup(rows)


def _channel_detail_kb(ch_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🎛️ Control View & Reaction", callback_data=f"ch_ctrl:{ch_id}")],
            [InlineKeyboardButton("🗑️ Remove Channel", callback_data=f"ch_del:{ch_id}")],
            [_back_button("channels")],
        ]
    )


def _channel_ctrl_kb(ch_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("👁️ Control View Menu", callback_data=f"ch_vmenu:{ch_id}")],
            [InlineKeyboardButton("⚡ Control Reaction Menu", callback_data=f"ch_rmenu:{ch_id}")],
            [_back_button(f"ch:{ch_id}")],
        ]
    )


def _channel_view_menu_kb(ch_id: str, current_setting: str) -> InlineKeyboardMarkup:
    cur = str(current_setting or "random").lower()
    b70 = f"{'✅ ' if cur == '70' else ''}70% View"
    b80 = f"{'✅ ' if cur == '80' else ''}80% View"
    b90 = f"{'✅ ' if cur == '90' else ''}90% View"
    brand = f"{'✅ ' if cur == 'random' else ''}🎲 Random View"
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(b70, callback_data=f"ch_vset:{ch_id}:70"),
                InlineKeyboardButton(b80, callback_data=f"ch_vset:{ch_id}:80"),
            ],
            [
                InlineKeyboardButton(b90, callback_data=f"ch_vset:{ch_id}:90"),
                InlineKeyboardButton(brand, callback_data=f"ch_vset:{ch_id}:random"),
            ],
            [_back_button(f"ch_ctrl:{ch_id}")],
        ]
    )


def _channel_reaction_menu_kb(ch_id: str, current_setting: str, custom_val: int | None = None) -> InlineKeyboardMarkup:
    cur = str(current_setting or "40").lower()
    b30 = f"{'✅ ' if cur == '30' else ''}30% Reaction"
    b40 = f"{'✅ ' if cur == '40' else ''}40% Reaction"
    b50 = f"{'✅ ' if cur == '50' else ''}50% Reaction"
    if cur == "custom" and custom_val:
        bcustom = f"✅ Custom Reaction ({custom_val}%)"
    else:
        bcustom = "✏️ Custom Reaction (1-100%)"

    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(b30, callback_data=f"ch_rset:{ch_id}:30"),
                InlineKeyboardButton(b40, callback_data=f"ch_rset:{ch_id}:40"),
            ],
            [
                InlineKeyboardButton(b50, callback_data=f"ch_rset:{ch_id}:50"),
            ],
            [
                InlineKeyboardButton(bcustom, callback_data=f"ch_rset:{ch_id}:custom"),
            ],
            [_back_button(f"ch_ctrl:{ch_id}")],
        ]
    )


# ── Emojis ──────────────────────────────────────────────────────── #

def _emojis_kb(emojis: list[dict]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for em in emojis:
        row.append(InlineKeyboardButton(em["emoji"], callback_data=f"em:{em['id']}"))
        if len(row) == 5:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("➕ Add Emoji", callback_data="em_add")])
    rows.append([_back_button()])
    return InlineKeyboardMarkup(rows)


# ── Confirm ─────────────────────────────────────────────────────── #

def _confirm_delete_kb(prefix: str, item_id: str, back_target: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Yes, delete", callback_data=f"{prefix}_y:{item_id}"),
                InlineKeyboardButton("❌ Cancel", callback_data=back_target),
            ]
        ]
    )


# ── Admin panel ─────────────────────────────────────────────────── #

def _admins_kb(roles: list[dict]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for r in roles:
        icon = _ROLE_ICON.get(r["role"], "👤")
        rows.append(
            [InlineKeyboardButton(f"{icon} {r['user_id']}  ({r['role']})", callback_data=f"adm:{r['id']}")]
        )
    rows.append([InlineKeyboardButton("➕ Add User", callback_data="adm_add")])
    rows.append([_back_button()])
    return InlineKeyboardMarkup(rows)


def _admin_detail_kb(role_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🗑️ Revoke", callback_data=f"adm_del:{role_id}")],
            [_back_button("admins")],
        ]
    )


def _role_select_kb() -> InlineKeyboardMarkup:
    """Shown after the owner enters a user ID – pick admin or moderator."""
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🛡️ Admin", callback_data="set_role:admin"),
                InlineKeyboardButton("👁️ Moderator", callback_data="set_role:moderator"),
            ],
            [InlineKeyboardButton("❌ Cancel", callback_data="admins")],
        ]
    )


# ── Analytics / Stats ──────────────────────────────────────────── #

def _stats_main_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📊 Summary", callback_data="stats_summary"),
                InlineKeyboardButton("🛠️ Error Logs", callback_data="stats_errors"),
            ],
            [
                InlineKeyboardButton("🔄 Refresh", callback_data="stats"),
                InlineKeyboardButton("🗑️ Clear Old Logs", callback_data="stats_clear"),
            ],
            [_back_button()],
        ]
    )


def _stats_nav_kb(current: str) -> InlineKeyboardMarkup:
    """Navigation bar shown inside summary / error views."""
    rows: list[list[InlineKeyboardButton]] = []
    if current == "summary":
        rows.append([InlineKeyboardButton("🛠️ Switch to Error Logs", callback_data="stats_errors")])
        rows.append([InlineKeyboardButton("🔄 Refresh", callback_data="stats_summary")])
    else:
        rows.append([InlineKeyboardButton("📊 Switch to Summary", callback_data="stats_summary")])
        rows.append([InlineKeyboardButton("🔄 Refresh", callback_data="stats_errors")])
    rows.append([_back_button("stats")])
    return InlineKeyboardMarkup(rows)


# ── Session choice (during registration) ───────────────────────── #

def _session_choice_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📋 Manual Session Paste", callback_data="acc_add_manual")],
            [InlineKeyboardButton("⚡ Auto Session Generator", callback_data="acc_add_auto")],
            [InlineKeyboardButton("❌ Cancel", callback_data="accounts")],
        ]
    )


# ================================================================== #
#  Help text                                                          #
# ================================================================== #

_HELP_TEXT = (
    "📖 *Auto-Reaction Bot — User Guide*\n"
    "━━━━━━━━━━━━━━━━━━━━\n\n"

    "*👑 Role Permissions*\n"
    "• *Owner* — full control, manage admins & moderators.\n"
    "• *Admin* 🛡️ — manage accounts, channels, emojis.\n"
    "• *Moderator* 👁️ — add channels & emojis, view accounts (read-only).\n\n"

    "*👑 Master / Worker Accounts*\n"
    "• Set one account as *Master* via the account detail page.\n"
    "• The Master account monitors target channels for new posts.\n"
    "• When a new post is detected, ALL accounts (Master + Workers) react.\n"
    "• Only one Master can be active at a time.\n"
    "• The Master auto-joins new channels when they are added.\n\n"

    "*📱 Adding Accounts*\n"
    "1. Go to https://my.telegram.org → log in → create an app.\n"
    "2. Note your *API ID* and *API Hash*.\n"
    "3. Tap *📱 Accounts → ➕ Add Account* and enter Name, API ID, API Hash.\n"
    "4. Choose a session method:\n"
    "   • *📋 Manual Paste* — paste an existing StringSession.\n"
    "   • *⚡ Auto Generator* — enter your phone number, receive OTP "
    "in Telegram, and the bot generates the session for you.\n\n"

    "*🔄 Account Rebind*\n"
    "• If an account's session expires, tap its name → *🔄 Rebind*.\n"
    "• The bot tests the existing session and, if expired, walks you "
    "through a fresh OTP / 2FA login without deleting the account.\n\n"

    "*📡 Adding Target Channels*\n"
    "1. Tap *📡 Channels → ➕ Add Channel*.\n"
    "2. Send a channel link, @username, or numeric ID:\n"
    "   • `https://t.me/mychannel`\n"
    "   • `@mychannel`\n"
    "   • `-1001234567890 My Channel`\n"
    "3. The bot resolves the entity automatically.\n"
    "4. The Master account auto-joins the channel.\n\n"

    "*⚡ Instant Reaction*\n"
    "• Tap *⚡ Instant Reaction* in the main menu.\n"
    "• Send any post link (e.g. `https://t.me/channel/123`).\n"
    "• ALL active accounts will instantly react — no pre-registration needed.\n\n"

    "*😀 Managing Reaction Emojis*\n"
    "• Tap *😀 Emojis → ➕ Add Emoji* and send a single emoji.\n"
    "• Tap any listed emoji to remove it.\n"
    "• Only emojis that Telegram allows as reactions will work "
    "(👍 ❤️ 🔥 🎉 😢 etc.).\n\n"

    "*🔒 Security & Rate-Limit Tips*\n"
    "• Never share your Session String.\n"
    "• Keep fewer than 5 accounts to stay under Telegram's radar.\n"
    "• 1-second gap between accounts is enforced — do *not* lower it.\n"
    "• FloodWait → account auto-cools down, no action needed.\n"
    "• 🔴 Health alerts are sent to the Owner if a session expires.\n\n"

    "*⌨️ Quick Commands*\n"
    "• /start — open the main menu.\n"
    "• /addadmin `<user_id or @username>` `[admin|moderator]` — (Owner only) assign a role."
)


# ================================================================== #
#  /start and /cancel commands                                        #
# ================================================================== #

async def cmd_start(update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id  # type: ignore[union-attr]
    if not _is_authorized(user_id):
        await update.message.reply_text("⛔ You are not authorised to use this bot.")  # type: ignore[union-attr]
        return
    await _cleanup_temp_client(user_id)
    user_states.pop(user_id, None)
    await update.message.reply_text(  # type: ignore[union-attr]
        "🤖 *Auto-Reaction Bot*\n\nSelect an option:",
        reply_markup=_main_menu_kb(user_id),
        parse_mode="Markdown",
    )


async def cmd_cancel(update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Cancel any active flow and return to main menu."""
    user_id = update.effective_user.id  # type: ignore[union-attr]
    await _cleanup_temp_client(user_id)
    user_states.pop(user_id, None)
    await update.message.reply_text(  # type: ignore[union-attr]
        "❌ Cancelled. Tap /start to return to the main menu.",
    )


async def cmd_stats(update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """/stats — open the analytics dashboard."""
    user_id = update.effective_user.id  # type: ignore[union-attr]
    if not _is_authorized(user_id):
        await update.message.reply_text("⛔ You are not authorised to use this bot.")  # type: ignore[union-attr]
        return
    summary = db.get_reaction_summary(today_only=True)
    active_count = len(reactor.clients)
    total = summary["total_success"] + summary["total_failed"]
    rate = (
        f"{summary['total_success'] * 100 / total:.1f}%"
        if total > 0
        else "N/A"
    )
    text = (
        "📊 *Analytics Dashboard*\n\n"
        f"📌 Posts handled today: *{summary['total_posts']}*\n"
        f"✅ Success: *{summary['total_success']}*  |  "
        f"❌ Failed: *{summary['total_failed']}*\n"
        f"📈 Success rate: *{rate}*\n"
        f"👥 Active accounts: *{active_count}*\n\n"
        "Choose a view:"
    )
    await update.message.reply_text(  # type: ignore[union-attr]
        text,
        reply_markup=_stats_main_kb(),
        parse_mode="Markdown",
    )


# ================================================================== #
#  /addadmin command  (owner only – accepts @username)                #
# ================================================================== #

async def cmd_addadmin(update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Usage: ``/addadmin <user_id or @username> [admin|moderator]``"""
    msg = update.message
    assert msg is not None
    user_id = msg.from_user.id  # type: ignore[union-attr]

    if not _can_manage_roles(user_id):
        await msg.reply_text("⛔ Owner only.")
        return

    parts = (msg.text or "").split()
    if len(parts) < 2:
        await msg.reply_text(
            "Usage: `/addadmin <user_id or @username> [admin|moderator]`\n"
            "Default role is *admin*.",
            parse_mode="Markdown",
        )
        return

    raw_target = parts[1]

    role = "admin"
    if len(parts) >= 3 and parts[2].lower() in ("admin", "moderator"):
        role = parts[2].lower()

    if raw_target.startswith("@"):
        await msg.reply_text(f"🔄 Resolving `{raw_target}`…", parse_mode="Markdown")
        resolved = await reactor.resolve_user_id(raw_target)
        if isinstance(resolved, str):
            await msg.reply_text(resolved, parse_mode="Markdown")
            return
        target_uid = resolved
    else:
        try:
            target_uid = int(raw_target)
        except ValueError:
            await msg.reply_text(
                "❗ Provide a numeric User ID or @username.",
                parse_mode="Markdown",
            )
            return

    if _is_permanent_owner(target_uid):
        await msg.reply_text("ℹ️ The permanent owner is always authorised.")
        return

    if target_uid in db.get_authorized_ids():
        await msg.reply_text(
            "ℹ️ This user already has a role. Revoke first via the Admins panel."
        )
        return

    try:
        db.add_user_role(target_uid, role, user_id)
        icon = _ROLE_ICON.get(role, "👤")
        await msg.reply_text(
            f"✅ User `{target_uid}` is now {icon} *{role}*!",
            parse_mode="Markdown",
        )
    except Exception as e:
        log.exception("Failed to add role via /addadmin")
        await msg.reply_text(f"❌ Failed: `{e}`", parse_mode="Markdown")


# ================================================================== #
#  Callback-query dispatcher                                          #
# ================================================================== #

async def callback_handler(update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> None:  # noqa: C901
    query = update.callback_query
    assert query is not None

    user_id = query.from_user.id
    if not _is_authorized(user_id):
        try:
            await query.answer("⛔ Unauthorised", show_alert=True)
        except Exception:
            pass
        return

    data: str = query.data or ""
    role = _get_role(user_id)

    try:
        await _route_callback(query, user_id, role, data)
    except BadRequest as e:
        if "Message is not modified" in str(e):
            log.debug("Ignored: Message is not modified (user_id=%d)", user_id)
        else:
            log.exception("BadRequest in callback_handler for user %d", user_id)
            try:
                await query.edit_message_text(
                    "❌ Something went wrong. Tap /start to return.",
                    reply_markup=InlineKeyboardMarkup([[_back_button()]]),
                )
            except Exception:
                pass
    except Exception:
        log.exception("Unhandled error in callback_handler for user %d", user_id)
        try:
            await query.edit_message_text(
                "❌ Something went wrong. Tap /start to return.",
                reply_markup=InlineKeyboardMarkup([[_back_button()]]),
            )
        except Exception:
            pass
    finally:
        # Acknowledge callback query if it wasn't already answered to dismiss button spinner
        try:
            if hasattr(query, "answered") and not query.answered:
                await query.answer()
            elif not hasattr(query, "answered"):
                await query.answer()
        except Exception:
            pass


async def _route_callback(query: Any, user_id: int, role: str | None, data: str) -> None:  # noqa: C901
    """Inner dispatch – separated so the caller can catch exceptions."""

    # Keep state during active input flows.
    keep = ("acc_add", "acc_master", "acc_rebind", "skip_phone",
            "ch_add", "em_add", "adm_add",
            "set_role", "cancel", "instant_react", "stats")
    if not data.startswith(keep):
        await _cleanup_temp_client(user_id)
        user_states.pop(user_id, None)

    # ─── Main menu ─────────────────────────────────────────────── #
    if data == "main":
        await _cleanup_temp_client(user_id)
        user_states.pop(user_id, None)
        await query.edit_message_text(
            "🤖 *Auto-Reaction Bot*\n\nSelect an option:",
            reply_markup=_main_menu_kb(user_id),
            parse_mode="Markdown",
        )

    # ─── Help ──────────────────────────────────────────────────── #
    elif data == "help":
        await query.edit_message_text(
            _HELP_TEXT,
            reply_markup=InlineKeyboardMarkup([[_back_button()]]),
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )

    # ─── Instant Reaction ─────────────────────────────────────── #
    elif data == "instant_react":
        user_states[user_id] = {"step": "instant_react", "data": {}}
        await query.edit_message_text(
            "⚡ *Instant Reaction*\n\n"
            "Send a Telegram *post link* and ALL active accounts "
            "will react to it immediately.\n\n"
            "✅ No need to pre-add the channel!\n\n"
            "Example: `https://t.me/channelname/123`",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("❌ Cancel", callback_data="main")]]
            ),
            parse_mode="Markdown",
        )

    # ─── Analytics Dashboard ──────────────────────────────────── #
    elif data == "stats":
        summary = db.get_reaction_summary(today_only=True)
        active_count = len(reactor.clients)
        total = summary["total_success"] + summary["total_failed"]
        rate = (
            f"{summary['total_success'] * 100 / total:.1f}%"
            if total > 0
            else "N/A"
        )
        text = (
            "📊 *Analytics Dashboard*\n\n"
            f"📌 Posts handled today: *{summary['total_posts']}*\n"
            f"✅ Success: *{summary['total_success']}*  |  "
            f"❌ Failed: *{summary['total_failed']}*\n"
            f"📈 Success rate: *{rate}*\n"
            f"👥 Active accounts: *{active_count}*\n\n"
            "Choose a view:"
        )
        await query.edit_message_text(
            text,
            reply_markup=_stats_main_kb(),
            parse_mode="Markdown",
        )

    elif data == "stats_summary":
        summary = db.get_reaction_summary(today_only=True)
        active_count = len(reactor.clients)
        total = summary["total_success"] + summary["total_failed"]
        rate = (
            f"{summary['total_success'] * 100 / total:.1f}%"
            if total > 0
            else "N/A"
        )
        lines = [
            "📊 *Summary — Today*\n",
            f"📌 Unique posts: *{summary['total_posts']}*",
            f"✅ Success: *{summary['total_success']}*  |  "
            f"❌ Failed: *{summary['total_failed']}*",
            f"📈 Success rate: *{rate}*",
            f"👥 Active accounts: *{active_count}*",
        ]
        if summary["channels"]:
            lines.append("\n*Channel Breakdown:*")
            for ch in summary["channels"]:
                lines.append(
                    f"• {ch['channel_name']}  —  "
                    f"✅ {ch['success_count']}  ❌ {ch['failed_count']}"
                )
        else:
            lines.append("\n_No reaction data for today yet._")

        await query.edit_message_text(
            "\n".join(lines),
            reply_markup=_stats_nav_kb("summary"),
            parse_mode="Markdown",
        )

    elif data == "stats_errors":
        errors = db.get_error_logs(limit=15)
        if errors:
            lines = ["🛠️ *Recent Error Logs*\n"]
            for i, err in enumerate(errors, 1):
                ts = err.get("created_at", "?")
                if isinstance(ts, str) and len(ts) > 16:
                    ts = ts[:16].replace("T", " ")
                lines.append(
                    f"*{i}.* `{ts}`\n"
                    f"   📡 {err.get('channel_name', '?')}  •  "
                    f"Post `{err.get('post_id', '?')}`\n"
                    f"   👤 {err.get('account_name', '?')}\n"
                    f"   ⚠️ `{err.get('error_message', 'Unknown')}`\n"
                )
        else:
            lines = ["🛠️ *Error Logs*\n\n_No errors recorded._ 🎉"]

        await query.edit_message_text(
            "\n".join(lines),
            reply_markup=_stats_nav_kb("errors"),
            parse_mode="Markdown",
        )

    elif data == "stats_clear":
        count = db.clear_old_logs(days=7)
        await query.edit_message_text(
            f"🗑️ *Cleared {count} log(s)* older than 7 days.",
            reply_markup=_stats_main_kb(),
            parse_mode="Markdown",
        )

    # ─── Accounts ──────────────────────────────────────────────── #
    elif data == "accounts":
        can_manage = _can_manage_accounts(user_id)
        if role == "owner":
            accs = db.get_all_accounts()
            title = "📱 *All Accounts*"
        elif role == "admin":
            accs = db.get_accounts(owner_id=user_id)
            title = "📱 *Your Accounts*"
        else:
            accs = db.get_all_accounts()
            title = "📱 *Accounts* _(read-only)_"

        if accs:
            body = f"Select an account{' or add a new one' if can_manage else ''}:"
        else:
            body = "_No accounts registered yet._"

        await query.edit_message_text(
            f"{title}\n\n{body}",
            reply_markup=_accounts_kb(accs, can_add=can_manage),
            parse_mode="Markdown",
        )

    # ─── Add Account: Step 1 (name) ────────────────────────────── #
    elif data == "acc_add":
        if not _can_manage_accounts(user_id):
            await query.answer("⛔ Admins and Owner only", show_alert=True)
            return
        user_states[user_id] = {"step": "reg_name", "data": {}}
        await query.edit_message_text(
            "📝 *Register Account — Step 1/4*\n\nSend the *Account Name/Label*:",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("❌ Cancel", callback_data="accounts")]]
            ),
            parse_mode="Markdown",
        )

    # ─── Add Account: Session choice callbacks ─────────────────── #
    elif data == "acc_add_manual":
        state = user_states.get(user_id)
        if not state or state["step"] != "reg_session_choice":
            await query.answer("Session expired. Start again.", show_alert=True)
            return
        state["step"] = "reg_session"
        await query.edit_message_text(
            "📋 *Manual Session Paste*\n\n"
            "Send the *Telethon StringSession* string:",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("❌ Cancel", callback_data="accounts")]]
            ),
            parse_mode="Markdown",
        )

    elif data == "acc_add_auto":
        state = user_states.get(user_id)
        if not state or state["step"] != "reg_session_choice":
            await query.answer("Session expired. Start again.", show_alert=True)
            return
        state["step"] = "reg_auto_phone"
        await query.edit_message_text(
            "⚡ *Auto Session Generator — Phone*\n\n"
            "Send the *phone number* with country code.\n"
            "Example: `+8801712345678`",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("❌ Cancel", callback_data="accounts")]]
            ),
            parse_mode="Markdown",
        )

    # ─── Account detail ───────────────────────────────────────── #
    elif data.startswith("acc:"):
        acc_id = data.split(":", 1)[1]
        acc = db.get_account(acc_id)
        if acc is None:
            await query.edit_message_text(
                "Account not found.",
                reply_markup=InlineKeyboardMarkup([[_back_button("accounts")]]),
            )
            return

        can_manage = _can_manage_accounts(user_id)
        if role == "admin" and acc["owner_id"] != user_id:
            await query.answer("⛔ Not your account", show_alert=True)
            return

        icon = "🟢 Active" if acc["status"] == "active" else "🔴 Failed"
        is_master = acc.get("is_master", False)
        badge = "👑 Master Account" if is_master else "⚙️ Worker Account"
        text = (
            f"📱 *Account Detail*\n\n"
            f"• *Name:* {acc['name']}\n"
            f"• *Phone:* {acc.get('phone') or 'N/A'}\n"
            f"• *API ID:* `{acc['api_id']}`\n"
            f"• *API Hash:* `{acc['api_hash'][:8]}…`\n"
            f"• *Status:* {icon}\n"
            f"• *Role:* {badge}"
        )
        await query.edit_message_text(
            text,
            reply_markup=_account_detail_kb(acc_id, can_manage=can_manage, is_master=is_master),
            parse_mode="Markdown",
        )

    elif data.startswith("acc_chk:"):
        if not _can_manage_accounts(user_id):
            await query.answer("⛔ Admins and Owner only", show_alert=True)
            return
        acc_id = data.split(":", 1)[1]
        user_states[user_id] = {"step": "check", "data": {"account_id": acc_id}}
        await query.edit_message_text(
            "🔍 *Check Account*\n\nSend a *post link* to test a reaction:\n"
            "Example: `https://t.me/channel/123`",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("❌ Cancel", callback_data=f"acc:{acc_id}")]]
            ),
            parse_mode="Markdown",
        )

    elif data.startswith("acc_del:"):
        if not _can_manage_accounts(user_id):
            await query.answer("⛔ Admins and Owner only", show_alert=True)
            return
        acc_id = data.split(":", 1)[1]
        acc = db.get_account(acc_id)
        if acc and role == "admin" and acc["owner_id"] != user_id:
            await query.answer("⛔ Not your account", show_alert=True)
            return
        name = acc["name"] if acc else "?"
        await query.edit_message_text(
            f"⚠️ *Delete account* `{name}`?",
            reply_markup=_confirm_delete_kb("acc_del", acc_id, f"acc:{acc_id}"),
            parse_mode="Markdown",
        )

    elif data.startswith("acc_del_y:"):
        if not _can_manage_accounts(user_id):
            await query.answer("⛔ Admins and Owner only", show_alert=True)
            return
        acc_id = data.split(":", 1)[1]
        acc = db.get_account(acc_id)
        if acc and role == "admin" and acc["owner_id"] != user_id:
            await query.answer("⛔ Not your account", show_alert=True)
            return
        await reactor.remove_client(acc_id)
        db.delete_account(acc_id)
        await query.edit_message_text(
            "✅ Account deleted.",
            reply_markup=InlineKeyboardMarkup([[_back_button("accounts")]]),
        )

    # ─── Set Master ────────────────────────────────────────────── #
    elif data.startswith("acc_master:"):
        if not _can_manage_accounts(user_id):
            await query.answer("⛔ Admins and Owner only", show_alert=True)
            return
        acc_id = data.split(":", 1)[1]
        acc = db.get_account(acc_id)
        if acc is None:
            await query.edit_message_text(
                "Account not found.",
                reply_markup=InlineKeyboardMarkup([[_back_button("accounts")]]),
            )
            return
        name = acc["name"]
        try:
            db.set_master_account(acc_id)
            await reactor.reload_master()
            await query.edit_message_text(
                f"👑 *{name}* is now the Master Account!\n\n"
                "It will monitor all target channels and dispatch "
                "reactions to all accounts when new posts are detected.",
                reply_markup=InlineKeyboardMarkup([[_back_button("accounts")]]),
                parse_mode="Markdown",
            )
        except Exception as e:
            log.exception("Failed to set master account")
            await query.edit_message_text(
                f"❌ Failed to set master: `{e}`",
                reply_markup=InlineKeyboardMarkup([[_back_button("accounts")]]),
                parse_mode="Markdown",
            )

    # ─── Rebind: preview + start ───────────────────────────────── #
    elif data.startswith("acc_rebind:"):
        if not _can_manage_accounts(user_id):
            await query.answer("⛔ Admins and Owner only", show_alert=True)
            return
        acc_id = data.split(":", 1)[1]
        acc = db.get_account(acc_id)
        if acc is None:
            await query.edit_message_text(
                "Account not found.",
                reply_markup=InlineKeyboardMarkup([[_back_button("accounts")]]),
            )
            return
        if role == "admin" and acc["owner_id"] != user_id:
            await query.answer("⛔ Not your account", show_alert=True)
            return
        text = (
            "🔄 *Account Rebind*\n\n"
            f"• *Name:* {acc['name']}\n"
            f"• *Phone:* {acc.get('phone') or 'N/A'}\n"
            f"• *API ID:* `{acc['api_id']}`\n"
            f"• *API Hash:* `{acc['api_hash'][:8]}…`\n"
            f"• *Status:* {'🟢 Active' if acc['status'] == 'active' else '🔴 Failed'}\n\n"
            "🔒 _Session string is never displayed._\n\n"
            "Tap *🚀 Start Re-authentication* to test the current session "
            "and refresh it if expired."
        )
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🚀 Start Re-authentication", callback_data=f"acc_rebind_go:{acc_id}")],
                [_back_button(f"acc:{acc_id}")],
            ]),
            parse_mode="Markdown",
        )

    # ─── Rebind: test & re-auth ────────────────────────────────── #
    elif data.startswith("acc_rebind_go:"):
        if not _can_manage_accounts(user_id):
            await query.answer("⛔ Admins and Owner only", show_alert=True)
            return
        acc_id = data.split(":", 1)[1]
        acc = db.get_account(acc_id)
        if acc is None:
            await query.edit_message_text(
                "Account not found.",
                reply_markup=InlineKeyboardMarkup([[_back_button("accounts")]]),
            )
            return

        await query.edit_message_text(
            "🔄 *Testing existing session…*\nPlease wait ⏳",
            parse_mode="Markdown",
        )

        # Test the existing session.
        try:
            test_client = TelegramClient(
                StringSession(acc["session_str"]),
                int(acc["api_id"]),
                acc["api_hash"],
            )
            await asyncio.wait_for(test_client.connect(), timeout=_TEL_TIMEOUT)
            authorized = await asyncio.wait_for(
                test_client.is_user_authorized(), timeout=_TEL_TIMEOUT
            )
            if authorized:
                # Session is still valid!
                await test_client.disconnect()
                db.update_account_status(acc_id, "active")
                # Reconnect in reactor if not already connected.
                if acc_id not in reactor.clients:
                    refreshed = db.get_account(acc_id)
                    if refreshed:
                        try:
                            await reactor.add_client(refreshed)
                        except Exception:
                            pass
                await query.edit_message_text(
                    "✅ *Session is valid!*\n\n"
                    f"Account *{acc['name']}* is active — no re-authentication needed.",
                    reply_markup=InlineKeyboardMarkup([[_back_button(f"acc:{acc_id}")]]),
                    parse_mode="Markdown",
                )
                return
            # Not authorized — need fresh login.
            await test_client.disconnect()
        except Exception as e:
            log.debug("Rebind: existing session test failed: %s", e)
            try:
                await test_client.disconnect()  # type: ignore[possibly-undefined]
            except Exception:
                pass

        # Session is dead. Check if we have a phone number to re-auth.
        phone = acc.get("phone")
        if not phone:
            await query.edit_message_text(
                "❌ *Session expired* but no phone number is stored for this account.\n\n"
                "Please delete and re-register the account, or use *🔄 Rebind* "
                "after adding a phone number.",
                reply_markup=InlineKeyboardMarkup([[_back_button(f"acc:{acc_id}")]]),
                parse_mode="Markdown",
            )
            return

        # Start fresh login flow.
        try:
            fresh_client = TelegramClient(
                StringSession(),
                int(acc["api_id"]),
                acc["api_hash"],
            )
            await asyncio.wait_for(fresh_client.connect(), timeout=_TEL_TIMEOUT)
            await asyncio.wait_for(
                fresh_client.send_code_request(phone), timeout=_TEL_TIMEOUT
            )

            user_states[user_id] = {
                "step": "rebind_otp",
                "data": {
                    "account_id": acc_id,
                    "phone": phone,
                    "_client": fresh_client,
                },
            }
            await query.edit_message_text(
                "🔐 *OTP Sent!*\n\n"
                f"A verification code has been sent to `{phone}`.\n"
                "Please enter the *OTP code*:\n\n"
                "💡 _Type /cancel to abort._",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("❌ Cancel", callback_data=f"acc:{acc_id}")]]
                ),
                parse_mode="Markdown",
            )
        except FloodWaitError as e:
            await query.edit_message_text(
                f"❌ *FloodWait:* please wait {e.seconds} seconds before retrying.",
                reply_markup=InlineKeyboardMarkup([[_back_button(f"acc:{acc_id}")]]),
                parse_mode="Markdown",
            )
        except Exception as e:
            log.exception("Rebind: send_code_request failed")
            await query.edit_message_text(
                f"❌ Failed to send OTP: `{e}`",
                reply_markup=InlineKeyboardMarkup([[_back_button(f"acc:{acc_id}")]]),
                parse_mode="Markdown",
            )

    # ─── Rebind All Accounts ──────────────────────────────────── #
    elif data == "acc_rebind_all":
        if not _can_manage_accounts(user_id):
            await query.answer("⛔ Admins and Owner only", show_alert=True)
            return

        await query.edit_message_text(
            "⚡ *Bulk Rebind — Starting…*\n\n"
            "Testing and reconnecting all accounts.\n"
            "Please wait ⏳",
            parse_mode="Markdown",
        )

        progress_msg = query.message

        async def _progress(done: int, total: int, name: str, ok: bool) -> None:
            icon = "✅" if ok else "❌"
            try:
                await progress_msg.edit_text(
                    f"⚡ *Bulk Rebind — Processing…*\n\n"
                    f"{icon} `{name}`\n"
                    f"Progress: *{done}/{total}* completed\n\n"
                    f"⏳ Please wait…",
                    parse_mode="Markdown",
                )
            except BadRequest:
                pass  # "Message is not modified"
            except Exception:
                pass

        result = await reactor.rebind_all_accounts(
            failed_only=False,
            progress_callback=_progress,
        )

        # Build final summary.
        s_count = len(result["success"])
        f_count = len(result["failed"])
        lines = [
            "⚡ *Bulk Rebind Complete!*\n",
            f"✅ Success: *{s_count}*",
            f"❌ Failed: *{f_count}*",
            f"📊 Total: *{result['total']}*",
        ]
        if result["failed"]:
            lines.append("\n*Needs Manual Attention:*")
            for f in result["failed"]:
                lines.append(f"• 🔴 `{f['name']}` — {f['reason']}")

        await progress_msg.edit_text(
            "\n".join(lines),
            reply_markup=InlineKeyboardMarkup([[_back_button("accounts")]]),
            parse_mode="Markdown",
        )

    # ─── Skip phone (removed — no longer needed) ──────────────── #
    elif data == "skip_phone":
        # Legacy compatibility — ignore.
        pass

    # ─── Channels ──────────────────────────────────────────────── #
    elif data == "channels":
        channels = db.get_channels()
        if channels:
            body = "Select a channel or add a new one:"
        else:
            body = "_No target channels yet._ Tap ➕ to add one."
        await query.edit_message_text(
            f"📡 *Target Channels*\n\n{body}",
            reply_markup=_channels_kb(channels),
            parse_mode="Markdown",
        )

    elif data == "ch_add":
        user_states[user_id] = {"step": "add_channel", "data": {}}
        await query.edit_message_text(
            "📡 *Add Channel*\n\n"
            "Send the channel in any of these formats:\n\n"
            "• *Link:* `https://t.me/channelname`\n"
            "• *Username:* `@channelname`\n"
            "• *Numeric ID:* `-1001234567890 My Channel`",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("❌ Cancel", callback_data="channels")]]
            ),
            parse_mode="Markdown",
        )

    elif data.startswith("ch:"):
        ch_id = data.split(":", 1)[1]
        ch = db.get_channel(ch_id)
        if ch is None:
            await query.edit_message_text(
                "Channel not found.",
                reply_markup=InlineKeyboardMarkup([[_back_button("channels")]]),
            )
            return

        v_setting = ch.get("view_setting") or "random"
        r_setting = ch.get("reaction_setting") or "40"
        c_val = ch.get("custom_reaction_val")

        v_desc = "🎲 Random (70%/80%/90%)" if v_setting == "random" else f"{v_setting}%"
        if r_setting == "custom":
            r_desc = f"✏️ Custom ({c_val or 40}%)"
        else:
            r_desc = f"{r_setting}%"

        uname_line = f"\n• *Username:* @{ch['username']}" if ch.get("username") else ""
        link_line = f"\n• *Invite Link:* `{ch['invite_link'][:30]}…`" if ch.get("invite_link") else ""

        text = (
            f"📡 *Channel Detail*\n\n"
            f"• *Name:* {ch['channel_name']}\n"
            f"• *ID:* `{ch['channel_id']}`"
            f"{uname_line}"
            f"{link_line}\n"
            f"• *Target Views:* `{v_desc}`\n"
            f"• *Target Reactions:* `{r_desc}`"
        )
        await query.edit_message_text(
            text, reply_markup=_channel_detail_kb(ch_id), parse_mode="Markdown"
        )

    # ─── Control View & Reaction Main Menu ─────────────────────── #
    elif data.startswith("ch_ctrl:"):
        ch_id = data.split(":", 1)[1]
        if not _can_manage_channels(user_id):
            await query.answer("⛔ Not authorized", show_alert=True)
            return
        ch = db.get_channel(ch_id)
        if not ch:
            await query.edit_message_text(
                "Channel not found.",
                reply_markup=InlineKeyboardMarkup([[_back_button("channels")]]),
            )
            return

        v_setting = ch.get("view_setting") or "random"
        r_setting = ch.get("reaction_setting") or "40"
        c_val = ch.get("custom_reaction_val")
        v_desc = "🎲 Random (70%/80%/90%)" if v_setting == "random" else f"{v_setting}%"
        r_desc = f"✏️ Custom ({c_val or 40}%)" if r_setting == "custom" else f"{r_setting}%"

        text = (
            f"🎛️ *Per-Channel View & Reaction Controls*\n\n"
            f"Channel: *{ch['channel_name']}*\n"
            f"Channel ID: `{ch['channel_id']}`\n\n"
            f"👁️ *Current View Setting:* `{v_desc}`\n"
            f"⚡ *Current Reaction Setting:* `{r_desc}`\n\n"
            f"Select a setting below to customize:"
        )
        await query.edit_message_text(
            text, reply_markup=_channel_ctrl_kb(ch_id), parse_mode="Markdown"
        )

    # ─── Control View Menu ─────────────────────────────────────── #
    elif data.startswith("ch_vmenu:"):
        ch_id = data.split(":", 1)[1]
        if not _can_manage_channels(user_id):
            await query.answer("⛔ Not authorized", show_alert=True)
            return
        ch = db.get_channel(ch_id)
        if not ch:
            await query.edit_message_text(
                "Channel not found.",
                reply_markup=InlineKeyboardMarkup([[_back_button("channels")]]),
            )
            return

        v_setting = ch.get("view_setting") or "random"
        v_desc = "🎲 Random (70%/80%/90%)" if v_setting == "random" else f"{v_setting}%"

        text = (
            f"👁️ *Control View Menu*\n\n"
            f"Channel: *{ch['channel_name']}*\n"
            f"Current Setting: *{v_desc}*\n\n"
            f"Select target percentage of worker accounts that will view each new post in this channel:"
        )
        await query.edit_message_text(
            text,
            reply_markup=_channel_view_menu_kb(ch_id, v_setting),
            parse_mode="Markdown",
        )

    # ─── Set View Setting ──────────────────────────────────────── #
    elif data.startswith("ch_vset:"):
        if not _can_manage_channels(user_id):
            await query.answer("⛔ Not authorized", show_alert=True)
            return
        parts = data.split(":")
        ch_id = parts[1]
        val = parts[2]
        ch = db.get_channel(ch_id)
        if not ch:
            await query.answer("Channel not found.", show_alert=True)
            return

        db.update_channel_view_setting(ch["channel_id"], val)
        await reactor.reload_channels()

        v_desc = "🎲 Random (70%/80%/90%)" if val == "random" else f"{val}%"
        try:
            await query.answer(f"✅ View target set to {v_desc} for {ch['channel_name']}!", show_alert=False)
        except Exception:
            pass

        text = (
            f"👁️ *Control View Menu*\n\n"
            f"Channel: *{ch['channel_name']}*\n"
            f"Current Setting: *{v_desc}*\n\n"
            f"Select target percentage of worker accounts that will view each new post in this channel:"
        )
        await query.edit_message_text(
            text,
            reply_markup=_channel_view_menu_kb(ch_id, val),
            parse_mode="Markdown",
        )

    # ─── Control Reaction Menu ─────────────────────────────────── #
    elif data.startswith("ch_rmenu:"):
        ch_id = data.split(":", 1)[1]
        if not _can_manage_channels(user_id):
            await query.answer("⛔ Not authorized", show_alert=True)
            return
        ch = db.get_channel(ch_id)
        if not ch:
            await query.edit_message_text(
                "Channel not found.",
                reply_markup=InlineKeyboardMarkup([[_back_button("channels")]]),
            )
            return

        r_setting = ch.get("reaction_setting") or "40"
        c_val = ch.get("custom_reaction_val")
        r_desc = f"✏️ Custom ({c_val or 40}%)" if r_setting == "custom" else f"{r_setting}%"

        text = (
            f"⚡ *Control Reaction Menu*\n\n"
            f"Channel: *{ch['channel_name']}*\n"
            f"Current Setting: *{r_desc}*\n\n"
            f"Select target percentage of worker accounts that will react to each new post in this channel (selected strictly from workers who viewed):"
        )
        await query.edit_message_text(
            text,
            reply_markup=_channel_reaction_menu_kb(ch_id, r_setting, c_val),
            parse_mode="Markdown",
        )

    # ─── Set Reaction Setting ───────────────────────────────────── #
    elif data.startswith("ch_rset:"):
        if not _can_manage_channels(user_id):
            await query.answer("⛔ Not authorized", show_alert=True)
            return
        parts = data.split(":")
        ch_id = parts[1]
        val = parts[2]
        ch = db.get_channel(ch_id)
        if not ch:
            await query.answer("Channel not found.", show_alert=True)
            return

        if val in ("30", "40", "50"):
            db.update_channel_reaction_setting(ch["channel_id"], val)
            await reactor.reload_channels()
            try:
                await query.answer(f"✅ Reaction target set to {val}% for {ch['channel_name']}!", show_alert=False)
            except Exception:
                pass

            text = (
                f"⚡ *Control Reaction Menu*\n\n"
                f"Channel: *{ch['channel_name']}*\n"
                f"Current Setting: *{val}%*\n\n"
                f"Select target percentage of worker accounts that will react to each new post in this channel (selected strictly from workers who viewed):"
            )
            await query.edit_message_text(
                text,
                reply_markup=_channel_reaction_menu_kb(ch_id, val),
                parse_mode="Markdown",
            )
        elif val == "custom":
            user_states[user_id] = {
                "step": "custom_reaction_val",
                "data": {
                    "ch_db_id": ch_id,
                    "channel_id": ch["channel_id"],
                    "channel_name": ch["channel_name"],
                },
            }
            await query.edit_message_text(
                f"✏️ *Custom Reaction Percentage*\n\n"
                f"Channel: *{ch['channel_name']}*\n\n"
                f"Please reply in chat with an integer between *1 and 100* for the reaction percentage:\n"
                f"_(e.g. `25` or `60`)_",
                reply_markup=InlineKeyboardMarkup([[_back_button(f"ch_rmenu:{ch_id}")]]),
                parse_mode="Markdown",
            )

    elif data.startswith("ch_del:"):
        ch_id = data.split(":", 1)[1]
        ch = db.get_channel(ch_id)
        name = ch["channel_name"] if ch else "?"
        await query.edit_message_text(
            f"⚠️ *Remove channel* `{name}`?",
            reply_markup=_confirm_delete_kb("ch_del", ch_id, f"ch:{ch_id}"),
            parse_mode="Markdown",
        )

    elif data.startswith("ch_del_y:"):
        ch_id = data.split(":", 1)[1]
        ch = db.get_channel(ch_id)
        ch_tg_id = int(ch["channel_id"]) if ch else None
        db.delete_channel(ch_id)
        await reactor.reload_channels()
        if ch_tg_id is not None:
            await reactor.auto_leave_channel(ch_tg_id)
        await query.edit_message_text(
            "✅ Channel removed. All accounts auto-left.",
            reply_markup=InlineKeyboardMarkup([[_back_button("channels")]]),
        )

    # ─── Join All Accounts to All Channels ─────────────────────── #
    elif data == "ch_join_all":
        if not _can_manage_channels(user_id):
            await query.answer("⛔ Not authorized", show_alert=True)
            return

        channels = db.get_channels()
        if not channels:
            await query.answer("No channels saved.", show_alert=True)
            return

        await query.edit_message_text(
            "🔗 *Join All Accounts — Starting…*\n\n"
            "Joining all active accounts to all saved channels.\n"
            "Please wait ⏳",
            parse_mode="Markdown",
        )

        progress_msg = query.message

        async def _join_progress(done: int, total: int, info: str) -> None:
            try:
                await progress_msg.edit_text(
                    f"🔗 *Join All Accounts — Processing…*\n\n"
                    f"{info}\n"
                    f"Progress: *{done}/{total}*\n\n"
                    f"⏳ Please wait…",
                    parse_mode="Markdown",
                )
            except BadRequest:
                pass
            except Exception:
                pass

        result = await reactor.join_all_accounts_all_channels(
            progress_callback=_join_progress,
        )

        lines = [
            "🔗 *Join All Accounts — Complete!*\n",
            f"✅ Newly Joined: *{result['joined']}*",
            f"ℹ️ Already Joined: *{result['already']}*",
            f"❌ Failed: *{result['failed_count']}*",
            f"📊 Total Pairs: *{result['total']}*",
        ]
        if result["failed_details"]:
            lines.append("\n*Failed (after 3 retries):*")
            for f in result["failed_details"]:
                lines.append(f"• 🔴 `{f['account']}` → `{f['channel']}`")

        await progress_msg.edit_text(
            "\n".join(lines),
            reply_markup=InlineKeyboardMarkup([[_back_button("channels")]]),
            parse_mode="Markdown",
        )

    # ─── Emojis ────────────────────────────────────────────────── #
    elif data == "emojis":
        emojis = db.get_emojis()
        if emojis:
            body = "Tap an emoji to remove it, or add a new one:"
        else:
            body = "_No emojis configured yet._ Tap ➕ to add one."
        await query.edit_message_text(
            f"😀 *Allowed Emojis*\n\n{body}",
            reply_markup=_emojis_kb(emojis),
            parse_mode="Markdown",
        )

    elif data == "em_add":
        user_states[user_id] = {"step": "add_emoji", "data": {}}
        await query.edit_message_text(
            "😀 *Add Emoji*\n\nSend the reaction emoji (e.g. 👍 🔥 ❤️):",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("❌ Cancel", callback_data="emojis")]]
            ),
            parse_mode="Markdown",
        )

    elif data.startswith("em:"):
        em_id = data.split(":", 1)[1]
        em = db.get_emoji(em_id)
        emoji_char = em["emoji"] if em else "?"
        await query.edit_message_text(
            f"⚠️ *Remove emoji* {emoji_char}?",
            reply_markup=_confirm_delete_kb("em_del", em_id, "emojis"),
            parse_mode="Markdown",
        )

    elif data.startswith("em_del_y:"):
        em_id = data.split(":", 1)[1]
        db.delete_emoji(em_id)
        await reactor.reload_emojis()
        await query.edit_message_text(
            "✅ Emoji removed.",
            reply_markup=InlineKeyboardMarkup([[_back_button("emojis")]]),
        )

    # ─── Admins panel  (owner only) ────────────────────────────── #
    elif data == "admins":
        if not _can_manage_roles(user_id):
            await query.answer("⛔ Owner only", show_alert=True)
            return
        roles = db.get_user_roles()
        if roles:
            body = (
                "🛡️ Admin — accounts, channels, emojis\n"
                "👁️ Moderator — channels, emojis, view accounts\n"
            )
        else:
            body = "_No users added yet._ Tap ➕ to add one."
        await query.edit_message_text(
            f"👑 *User Role Management*\n\n{body}",
            reply_markup=_admins_kb(roles),
            parse_mode="Markdown",
        )

    elif data == "adm_add":
        if not _can_manage_roles(user_id):
            await query.answer("⛔ Owner only", show_alert=True)
            return
        user_states[user_id] = {"step": "add_role_user", "data": {}}
        await query.edit_message_text(
            "👑 *Add User*\n\nSend the Telegram *User ID* or *@username*:",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("❌ Cancel", callback_data="admins")]]
            ),
            parse_mode="Markdown",
        )

    elif data.startswith("adm:"):
        if not _can_manage_roles(user_id):
            await query.answer("⛔ Owner only", show_alert=True)
            return
        role_id = data.split(":", 1)[1]
        r = db.get_user_role(role_id)
        if r is None:
            await query.edit_message_text(
                "User not found.",
                reply_markup=InlineKeyboardMarkup([[_back_button("admins")]]),
            )
            return
        icon = _ROLE_ICON.get(r["role"], "👤")
        text = (
            f"{icon} *User Detail*\n\n"
            f"• *User ID:* `{r['user_id']}`\n"
            f"• *Role:* {r['role']}\n"
            f"• *Added by:* `{r['added_by']}`"
        )
        await query.edit_message_text(
            text, reply_markup=_admin_detail_kb(role_id), parse_mode="Markdown"
        )

    elif data.startswith("adm_del:"):
        if not _can_manage_roles(user_id):
            await query.answer("⛔ Owner only", show_alert=True)
            return
        role_id = data.split(":", 1)[1]
        r = db.get_user_role(role_id)
        uid = r["user_id"] if r else "?"
        role_name = r["role"] if r else "?"
        await query.edit_message_text(
            f"⚠️ *Revoke {role_name}* `{uid}`?",
            reply_markup=_confirm_delete_kb("adm_del", role_id, f"adm:{role_id}"),
            parse_mode="Markdown",
        )

    elif data.startswith("adm_del_y:"):
        if not _can_manage_roles(user_id):
            await query.answer("⛔ Owner only", show_alert=True)
            return
        role_id = data.split(":", 1)[1]
        db.delete_user_role(role_id)
        await query.edit_message_text(
            "✅ User role revoked.",
            reply_markup=InlineKeyboardMarkup([[_back_button("admins")]]),
        )

    # ─── Role selector (two-step from add_role_user) ───────────── #
    elif data.startswith("set_role:"):
        if not _can_manage_roles(user_id):
            await query.answer("⛔ Owner only", show_alert=True)
            return
        state = user_states.get(user_id)
        if not state or state["step"] != "add_role_select":
            await query.answer("No pending user. Start again.", show_alert=True)
            return
        chosen_role = data.split(":", 1)[1]
        target_uid = state["data"]["target_user_id"]
        user_states.pop(user_id, None)
        try:
            db.add_user_role(target_uid, chosen_role, user_id)
            icon = _ROLE_ICON.get(chosen_role, "👤")
            await query.edit_message_text(
                f"✅ User `{target_uid}` is now {icon} *{chosen_role}*!",
                reply_markup=InlineKeyboardMarkup([[_back_button("admins")]]),
                parse_mode="Markdown",
            )
        except Exception as e:
            log.exception("Failed to add user role")
            await query.edit_message_text(
                f"❌ Failed: `{e}`",
                reply_markup=InlineKeyboardMarkup([[_back_button("admins")]]),
                parse_mode="Markdown",
            )

    # ─── Cancel ────────────────────────────────────────────────── #
    elif data == "cancel":
        await _cleanup_temp_client(user_id)
        user_states.pop(user_id, None)
        await query.edit_message_text(
            "🤖 *Auto-Reaction Bot*\n\nSelect an option:",
            reply_markup=_main_menu_kb(user_id),
            parse_mode="Markdown",
        )


# ================================================================== #
#  Text-message handler  (state-driven input)                         #
# ================================================================== #

async def text_handler(update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> None:  # noqa: C901
    msg = update.message
    assert msg is not None
    user_id = msg.from_user.id  # type: ignore[union-attr]

    if not _is_authorized(user_id):
        return

    state = user_states.get(user_id)
    if state is None:
        await msg.reply_text(
            "Use the buttons to navigate. Tap /start if you don't see them.",
        )
        return

    try:
        await _route_text(msg, user_id, state)
    except Exception:
        log.exception("Unhandled error in text_handler for user %d", user_id)
        await _cleanup_temp_client(user_id)
        user_states.pop(user_id, None)
        try:
            await msg.reply_text(
                "❌ Something went wrong. Tap /start to return.",
                reply_markup=InlineKeyboardMarkup([[_back_button()]]),
            )
        except Exception:
            pass


async def _route_text(msg: Any, user_id: int, state: dict) -> None:  # noqa: C901
    """Inner dispatch – separated so the caller can catch exceptions."""
    step = state["step"]
    text = msg.text or ""

    # ── Registration: Account Name ─────────────────────────────── #
    if step == "reg_name":
        if not _can_manage_accounts(user_id):
            user_states.pop(user_id, None)
            return
        if not text.strip():
            await msg.reply_text("❗ Account name cannot be empty. Please try again.")
            return
        state["data"]["name"] = text.strip()
        state["step"] = "reg_api_id"
        await msg.reply_text(
            "📝 *Register Account — Step 2/4*\n\nSend the *API ID*:",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("❌ Cancel", callback_data="accounts")]]
            ),
            parse_mode="Markdown",
        )

    # ── Registration: API ID ───────────────────────────────────── #
    elif step == "reg_api_id":
        if not text.strip().isdigit():
            await msg.reply_text("❗ API ID must be numeric. Please try again.")
            return
        state["data"]["api_id"] = text.strip()
        state["step"] = "reg_api_hash"
        await msg.reply_text(
            "📝 *Register Account — Step 3/4*\n\nSend the *API Hash*:",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("❌ Cancel", callback_data="accounts")]]
            ),
            parse_mode="Markdown",
        )

    # ── Registration: API Hash → Session Choice ────────────────── #
    elif step == "reg_api_hash":
        if not text.strip():
            await msg.reply_text("❗ API Hash cannot be empty. Please try again.")
            return
        state["data"]["api_hash"] = text.strip()
        state["step"] = "reg_session_choice"
        await msg.reply_text(
            "📝 *Register Account — Step 4/4*\n\n"
            "How would you like to provide the session?\n\n"
            "📋 *Manual Paste* — paste a pre-generated StringSession.\n"
            "⚡ *Auto Generator* — log in via phone OTP right here.",
            reply_markup=_session_choice_kb(),
            parse_mode="Markdown",
        )

    # ── Registration: Manual Session Paste ─────────────────────── #
    elif step == "reg_session":
        if not text.strip():
            await msg.reply_text("❗ Session string cannot be empty. Please try again.")
            return
        state["data"]["session_str"] = text.strip()
        d = state["data"]
        user_states.pop(user_id, None)
        try:
            acc = db.add_account(
                owner_id=user_id,
                name=d["name"],
                phone=d.get("phone"),
                api_id=d["api_id"],
                api_hash=d["api_hash"],
                session_str=d["session_str"],
            )
            try:
                await reactor.add_client(acc)
                status_msg = "🟢 Connected successfully!"
            except Exception as e:
                log.exception("Failed to connect new account %s", acc["id"])
                db.update_account_status(acc["id"], "failed")
                status_msg = f"🔴 Saved but connection failed: `{e}`"
            await msg.reply_text(
                f"✅ *Account registered!*\n\n{status_msg}",
                reply_markup=InlineKeyboardMarkup([[_back_button("accounts")]]),
                parse_mode="Markdown",
            )
        except Exception as e:
            log.exception("Failed to save account")
            await msg.reply_text(
                f"❌ Failed to save: `{e}`",
                reply_markup=InlineKeyboardMarkup([[_back_button("accounts")]]),
                parse_mode="Markdown",
            )

    # ── Registration: Auto Generator — Phone ───────────────────── #
    elif step == "reg_auto_phone":
        phone = text.strip()
        if not phone:
            await msg.reply_text("❗ Phone number cannot be empty.")
            return
        d = state["data"]
        progress = await msg.reply_text(
            f"📲 *Sending OTP to* `{phone}`…\nPlease wait ⏳",
            parse_mode="Markdown",
        )
        try:
            client = TelegramClient(
                StringSession(),
                int(d["api_id"]),
                d["api_hash"],
            )
            await asyncio.wait_for(client.connect(), timeout=_TEL_TIMEOUT)
            await asyncio.wait_for(
                client.send_code_request(phone), timeout=_TEL_TIMEOUT
            )
            state["data"]["phone"] = phone
            state["data"]["_client"] = client
            state["step"] = "reg_auto_otp"
            try:
                await progress.edit_text(
                    "🔐 *OTP Sent!*\n\n"
                    f"A verification code has been sent to `{phone}`.\n"
                    "Please enter the *OTP code*:\n\n"
                    "💡 _Type /cancel to abort._",
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("❌ Cancel", callback_data="accounts")]]
                    ),
                    parse_mode="Markdown",
                )
            except Exception:
                await msg.reply_text(
                    "🔐 *OTP Sent!* Enter the *OTP code*:",
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("❌ Cancel", callback_data="accounts")]]
                    ),
                    parse_mode="Markdown",
                )
        except FloodWaitError as e:
            await progress.edit_text(
                f"❌ *FloodWait:* please wait {e.seconds}s before retrying.",
                reply_markup=InlineKeyboardMarkup([[_back_button("accounts")]]),
                parse_mode="Markdown",
            )
            user_states.pop(user_id, None)
        except Exception as e:
            log.exception("Auto-gen: send_code_request failed")
            try:
                await progress.edit_text(
                    f"❌ Failed to send OTP: `{e}`",
                    reply_markup=InlineKeyboardMarkup([[_back_button("accounts")]]),
                    parse_mode="Markdown",
                )
            except Exception:
                await msg.reply_text(
                    f"❌ Failed to send OTP: `{e}`",
                    reply_markup=InlineKeyboardMarkup([[_back_button("accounts")]]),
                    parse_mode="Markdown",
                )
            user_states.pop(user_id, None)

    # ── Registration: Auto Generator — OTP ─────────────────────── #
    elif step == "reg_auto_otp":
        code = text.strip()
        if not code:
            await msg.reply_text("❗ OTP code cannot be empty.")
            return
        d = state["data"]
        client: TelegramClient = d["_client"]
        phone = d["phone"]
        try:
            await asyncio.wait_for(
                client.sign_in(phone=phone, code=code),
                timeout=_TEL_TIMEOUT,
            )
            # Success — extract session.
            session_str = client.session.save()
            await client.disconnect()
            state["data"].pop("_client", None)
            user_states.pop(user_id, None)
            acc = db.add_account(
                owner_id=user_id,
                name=d["name"],
                phone=phone,
                api_id=d["api_id"],
                api_hash=d["api_hash"],
                session_str=session_str,
            )
            try:
                await reactor.add_client(acc)
                status_msg = "🟢 Connected successfully!"
            except Exception as e:
                log.exception("Failed to connect auto-gen account %s", acc["id"])
                db.update_account_status(acc["id"], "failed")
                status_msg = f"🔴 Saved but connection failed: `{e}`"
            await msg.reply_text(
                f"✅ *Account registered via Auto Generator!*\n\n{status_msg}",
                reply_markup=InlineKeyboardMarkup([[_back_button("accounts")]]),
                parse_mode="Markdown",
            )
        except SessionPasswordNeededError:
            # 2FA required.
            state["step"] = "reg_auto_2fa"
            await msg.reply_text(
                "🔒 *Two-Step Verification Required*\n\n"
                "This account has a cloud password enabled.\n"
                "Please enter your *2FA password*:\n\n"
                "💡 _Type /cancel to abort._",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("❌ Cancel", callback_data="accounts")]]
                ),
                parse_mode="Markdown",
            )
        except (PhoneCodeInvalidError, PhoneCodeExpiredError) as e:
            await msg.reply_text(
                f"❌ *Invalid or expired OTP code.*\n`{e}`\n\nPlease try again:",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("❌ Cancel", callback_data="accounts")]]
                ),
                parse_mode="Markdown",
            )
            # Stay on reg_auto_otp step so user can re-enter.
        except Exception as e:
            log.exception("Auto-gen: sign_in failed")
            await _cleanup_temp_client(user_id)
            user_states.pop(user_id, None)
            await msg.reply_text(
                f"❌ Login failed: `{e}`",
                reply_markup=InlineKeyboardMarkup([[_back_button("accounts")]]),
                parse_mode="Markdown",
            )

    # ── Registration: Auto Generator — 2FA ─────────────────────── #
    elif step == "reg_auto_2fa":
        password = text.strip()
        if not password:
            await msg.reply_text("❗ Password cannot be empty.")
            return
        d = state["data"]
        client = d["_client"]
        try:
            await asyncio.wait_for(
                client.sign_in(password=password),
                timeout=_TEL_TIMEOUT,
            )
            session_str = client.session.save()
            await client.disconnect()
            state["data"].pop("_client", None)
            user_states.pop(user_id, None)
            acc = db.add_account(
                owner_id=user_id,
                name=d["name"],
                phone=d["phone"],
                api_id=d["api_id"],
                api_hash=d["api_hash"],
                session_str=session_str,
            )
            try:
                await reactor.add_client(acc)
                status_msg = "🟢 Connected successfully!"
            except Exception as e:
                log.exception("Failed to connect auto-gen account %s", acc["id"])
                db.update_account_status(acc["id"], "failed")
                status_msg = f"🔴 Saved but connection failed: `{e}`"
            await msg.reply_text(
                f"✅ *Account registered via Auto Generator!*\n\n{status_msg}",
                reply_markup=InlineKeyboardMarkup([[_back_button("accounts")]]),
                parse_mode="Markdown",
            )
        except PasswordHashInvalidError:
            await msg.reply_text(
                "❌ *Wrong password.* Please try again:",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("❌ Cancel", callback_data="accounts")]]
                ),
                parse_mode="Markdown",
            )
            # Stay on reg_auto_2fa step.
        except Exception as e:
            log.exception("Auto-gen: 2FA sign_in failed")
            await _cleanup_temp_client(user_id)
            user_states.pop(user_id, None)
            await msg.reply_text(
                f"❌ 2FA login failed: `{e}`",
                reply_markup=InlineKeyboardMarkup([[_back_button("accounts")]]),
                parse_mode="Markdown",
            )

    # ── Rebind: OTP ────────────────────────────────────────────── #
    elif step == "rebind_otp":
        code = text.strip()
        if not code:
            await msg.reply_text("❗ OTP code cannot be empty.")
            return
        d = state["data"]
        client = d["_client"]
        phone = d["phone"]
        acc_id = d["account_id"]
        try:
            await asyncio.wait_for(
                client.sign_in(phone=phone, code=code),
                timeout=_TEL_TIMEOUT,
            )
            # Success — extract session.
            session_str = client.session.save()
            await client.disconnect()
            state["data"].pop("_client", None)
            user_states.pop(user_id, None)
            db.update_account_session(acc_id, session_str)
            # Reconnect in reactor.
            await reactor.remove_client(acc_id)
            refreshed = db.get_account(acc_id)
            if refreshed:
                try:
                    await reactor.add_client(refreshed)
                except Exception:
                    pass
            await msg.reply_text(
                "✅ *Account rebound successfully!*\n\n"
                "🟢 Fresh session saved and reactor reloaded.",
                reply_markup=InlineKeyboardMarkup([[_back_button(f"acc:{acc_id}")]]),
                parse_mode="Markdown",
            )
        except SessionPasswordNeededError:
            state["step"] = "rebind_2fa"
            await msg.reply_text(
                "🔒 *Two-Step Verification Required*\n\n"
                "Please enter your *2FA password*:\n\n"
                "💡 _Type /cancel to abort._",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("❌ Cancel", callback_data=f"acc:{acc_id}")]]
                ),
                parse_mode="Markdown",
            )
        except (PhoneCodeInvalidError, PhoneCodeExpiredError) as e:
            await msg.reply_text(
                f"❌ *Invalid or expired OTP code.*\n`{e}`\n\nPlease try again:",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("❌ Cancel", callback_data=f"acc:{acc_id}")]]
                ),
                parse_mode="Markdown",
            )
        except Exception as e:
            log.exception("Rebind: sign_in failed")
            await _cleanup_temp_client(user_id)
            user_states.pop(user_id, None)
            await msg.reply_text(
                f"❌ Login failed: `{e}`",
                reply_markup=InlineKeyboardMarkup([[_back_button(f"acc:{acc_id}")]]),
                parse_mode="Markdown",
            )

    # ── Rebind: 2FA ────────────────────────────────────────────── #
    elif step == "rebind_2fa":
        password = text.strip()
        if not password:
            await msg.reply_text("❗ Password cannot be empty.")
            return
        d = state["data"]
        client = d["_client"]
        acc_id = d["account_id"]
        try:
            await asyncio.wait_for(
                client.sign_in(password=password),
                timeout=_TEL_TIMEOUT,
            )
            session_str = client.session.save()
            await client.disconnect()
            state["data"].pop("_client", None)
            user_states.pop(user_id, None)
            db.update_account_session(acc_id, session_str)
            await reactor.remove_client(acc_id)
            refreshed = db.get_account(acc_id)
            if refreshed:
                try:
                    await reactor.add_client(refreshed)
                except Exception:
                    pass
            await msg.reply_text(
                "✅ *Account rebound successfully!*\n\n"
                "🟢 Fresh session saved and reactor reloaded.",
                reply_markup=InlineKeyboardMarkup([[_back_button(f"acc:{acc_id}")]]),
                parse_mode="Markdown",
            )
        except PasswordHashInvalidError:
            await msg.reply_text(
                "❌ *Wrong password.* Please try again:",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("❌ Cancel", callback_data=f"acc:{acc_id}")]]
                ),
                parse_mode="Markdown",
            )
        except Exception as e:
            log.exception("Rebind: 2FA sign_in failed")
            await _cleanup_temp_client(user_id)
            user_states.pop(user_id, None)
            await msg.reply_text(
                f"❌ 2FA login failed: `{e}`",
                reply_markup=InlineKeyboardMarkup([[_back_button(f"acc:{acc_id}")]]),
                parse_mode="Markdown",
            )

    # ── Check mode ─────────────────────────────────────────────── #
    elif step == "check":
        acc_id = state["data"]["account_id"]
        user_states.pop(user_id, None)
        result = await reactor.check_react(acc_id, text.strip())
        await msg.reply_text(
            result,
            reply_markup=InlineKeyboardMarkup([[_back_button(f"acc:{acc_id}")]]),
            parse_mode="Markdown",
        )

    # ── Instant Reaction ───────────────────────────────────────── #
    elif step == "instant_react":
        user_states.pop(user_id, None)
        post_link = text.strip()
        progress = await msg.reply_text(
            "⚡ *Triggering reactions across all accounts…*\n"
            "Please wait ⏳",
            parse_mode="Markdown",
        )
        result = await reactor.instant_react(post_link)
        try:
            await progress.edit_text(
                result,
                reply_markup=InlineKeyboardMarkup([[_back_button()]]),
                parse_mode="Markdown",
            )
        except Exception:
            await msg.reply_text(
                result,
                reply_markup=InlineKeyboardMarkup([[_back_button()]]),
                parse_mode="Markdown",
            )

    # ── Add channel (link / @username / numeric ID) ────────────── #
    elif step == "add_channel":
        user_states.pop(user_id, None)
        raw = text.strip()

        if raw.startswith("https://t.me/") or raw.startswith("http://t.me/") or raw.startswith("@") or raw.startswith("t.me/"):
            progress = await msg.reply_text(
                "🔄 Resolving channel…",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("❌ Cancel", callback_data="channels")]]
                ),
            )
            result = await reactor.resolve_channel_entity(raw)
            if isinstance(result, str):
                try:
                    await progress.edit_text(
                        result,
                        reply_markup=InlineKeyboardMarkup([[_back_button("channels")]]),
                        parse_mode="Markdown",
                    )
                except Exception:
                    await msg.reply_text(
                        result,
                        reply_markup=InlineKeyboardMarkup([[_back_button("channels")]]),
                        parse_mode="Markdown",
                    )
                return

            if len(result) == 4:
                channel_id, channel_name, username, invite_link = result
            else:
                channel_id, channel_name = result[:2]
                username, invite_link = None, None

            try:
                db.add_channel(
                    channel_id, channel_name, user_id,
                    username=username, invite_link=invite_link,
                )
                await reactor.reload_channels()
                ch_dict = {
                    "channel_id": channel_id,
                    "channel_name": channel_name,
                    "username": username,
                    "invite_link": invite_link,
                }
                await reactor.auto_join_all_accounts(ch_dict)
                route_desc = f"@{username}" if username else ("Private Invite Link" if invite_link else f"`{channel_id}`")
                try:
                    await progress.edit_text(
                        f"✅ Channel *{channel_name}* (`{channel_id}`) added!\n"
                        f"Routing: `{route_desc}`\n"
                        f"All accounts auto-joined.",
                        reply_markup=InlineKeyboardMarkup([[_back_button("channels")]]),
                        parse_mode="Markdown",
                    )
                except Exception:
                    await msg.reply_text(
                        f"✅ Channel *{channel_name}* (`{channel_id}`) added!\n"
                        f"Routing: `{route_desc}`\n"
                        f"All accounts auto-joined.",
                        reply_markup=InlineKeyboardMarkup([[_back_button("channels")]]),
                        parse_mode="Markdown",
                    )
            except Exception as e:
                log.exception("Failed to add channel")
                try:
                    await progress.edit_text(
                        f"❌ Failed: `{e}`",
                        reply_markup=InlineKeyboardMarkup([[_back_button("channels")]]),
                        parse_mode="Markdown",
                    )
                except Exception:
                    await msg.reply_text(
                        f"❌ Failed: `{e}`",
                        reply_markup=InlineKeyboardMarkup([[_back_button("channels")]]),
                        parse_mode="Markdown",
                    )

        else:
            parts = raw.split(maxsplit=1)
            if len(parts) < 2:
                await msg.reply_text(
                    "❗ Send a channel link, @username, or numeric ID + name.\n"
                    "Examples:\n"
                    "• `https://t.me/channelname`\n"
                    "• `@channelname`\n"
                    "• `https://t.me/+invitehash`\n"
                    "• `-1001234567890 My Channel`",
                    reply_markup=InlineKeyboardMarkup([[_back_button("channels")]]),
                    parse_mode="Markdown",
                )
                return
            try:
                channel_id = int(parts[0])
            except ValueError:
                await msg.reply_text(
                    "❗ Channel ID must be numeric, or use a link / @username.",
                    reply_markup=InlineKeyboardMarkup([[_back_button("channels")]]),
                )
                return
            channel_name = parts[1]
            try:
                db.add_channel(channel_id, channel_name, user_id)
                await reactor.reload_channels()
                # Master account fallback backfill
                metadata = await reactor.ensure_channel_metadata(channel_id)
                ch_dict = {
                    "channel_id": channel_id,
                    "channel_name": channel_name,
                    "username": metadata.get("username") if metadata else None,
                    "invite_link": metadata.get("invite_link") if metadata else None,
                }
                await reactor.auto_join_all_accounts(ch_dict)
                await msg.reply_text(
                    f"✅ Channel *{channel_name}* added!\n"
                    f"All accounts auto-joined.",
                    reply_markup=InlineKeyboardMarkup([[_back_button("channels")]]),
                    parse_mode="Markdown",
                )
            except Exception as e:
                log.exception("Failed to add channel")
                await msg.reply_text(
                    f"❌ Failed: `{e}`",
                    reply_markup=InlineKeyboardMarkup([[_back_button("channels")]]),
                    parse_mode="Markdown",
                )

    # ── Add emoji ──────────────────────────────────────────────── #
    elif step == "add_emoji":
        user_states.pop(user_id, None)
        emoji = text.strip()
        if not emoji:
            await msg.reply_text(
                "❗ Send a valid emoji.",
                reply_markup=InlineKeyboardMarkup([[_back_button("emojis")]]),
            )
            return
        try:
            db.add_emoji(emoji, user_id)
            await reactor.reload_emojis()
            await msg.reply_text(
                f"✅ Emoji {emoji} added!",
                reply_markup=InlineKeyboardMarkup([[_back_button("emojis")]]),
            )
        except Exception as e:
            log.exception("Failed to add emoji")
            await msg.reply_text(
                f"❌ Failed: `{e}`",
                reply_markup=InlineKeyboardMarkup([[_back_button("emojis")]]),
                parse_mode="Markdown",
            )

    # ── Custom reaction percentage (1-100%) ───────────────────── #
    elif step == "custom_reaction_val":
        data_ctx = state.get("data", {})
        raw_val = text.strip().rstrip("%")
        try:
            num = int(raw_val)
            if not (1 <= num <= 100):
                raise ValueError
        except ValueError:
            ch_db_id = data_ctx.get("ch_db_id")
            bk = InlineKeyboardMarkup([[_back_button(f"ch_rmenu:{ch_db_id}")]]) if ch_db_id else None
            await msg.reply_text(
                "❗ Invalid percentage. Please enter a whole number between *1 and 100* (e.g. `25` or `60`):\n"
                "_(Or tap Back / type /cancel to abort)_",
                reply_markup=bk,
                parse_mode="Markdown",
            )
            return

        # Input is valid – consume state
        user_states.pop(user_id, None)

        ch_tg_id = data_ctx.get("channel_id")
        ch_name = data_ctx.get("channel_name", "Channel")
        ch_db_id = data_ctx.get("ch_db_id")

        db.update_channel_reaction_setting(ch_tg_id, "custom", custom_reaction_val=num)
        await reactor.reload_channels()

        await msg.reply_text(
            f"✅ Custom reaction percentage set to *{num}%* for channel *{ch_name}*!\n\n"
            f"For every new post in this channel, {num}% of the worker accounts (from those who viewed) will react with the single configured emoji.",
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("⚡ Return to Reaction Menu", callback_data=f"ch_rmenu:{ch_db_id}")],
                    [InlineKeyboardButton("📡 Return to Channel", callback_data=f"ch:{ch_db_id}")],
                ]
            ),
            parse_mode="Markdown",
        )

    # ── Add role – step 1: receive user ID or @username ────────── #
    elif step == "add_role_user":
        if not _can_manage_roles(user_id):
            user_states.pop(user_id, None)
            return
        raw = text.strip()

        if raw.startswith("@"):
            progress = await msg.reply_text(
                f"🔄 Resolving `{raw}`…", parse_mode="Markdown"
            )
            resolved = await reactor.resolve_user_id(raw)
            if isinstance(resolved, str):
                user_states.pop(user_id, None)
                try:
                    await progress.edit_text(
                        resolved,
                        reply_markup=InlineKeyboardMarkup([[_back_button("admins")]]),
                        parse_mode="Markdown",
                    )
                except Exception:
                    await msg.reply_text(
                        resolved,
                        reply_markup=InlineKeyboardMarkup([[_back_button("admins")]]),
                        parse_mode="Markdown",
                    )
                return
            target_uid = resolved
        elif raw.isdigit() or (raw.startswith("-") and raw[1:].isdigit()):
            target_uid = int(raw)
        else:
            await msg.reply_text(
                "❗ Send a numeric User ID or @username.",
                reply_markup=InlineKeyboardMarkup([[_back_button("admins")]]),
            )
            return

        if _is_permanent_owner(target_uid):
            await msg.reply_text(
                "ℹ️ The permanent owner is always authorised — no need to add.",
                reply_markup=InlineKeyboardMarkup([[_back_button("admins")]]),
            )
            user_states.pop(user_id, None)
            return
        if target_uid in db.get_authorized_ids():
            await msg.reply_text(
                "ℹ️ This user already has a role. Revoke first to change it.",
                reply_markup=InlineKeyboardMarkup([[_back_button("admins")]]),
            )
            user_states.pop(user_id, None)
            return
        state["step"] = "add_role_select"
        state["data"]["target_user_id"] = target_uid
        await msg.reply_text(
            f"Choose a role for `{target_uid}`:",
            reply_markup=_role_select_kb(),
            parse_mode="Markdown",
        )


# ================================================================== #
#  Application wiring                                                 #
# ================================================================== #

def setup_handlers(app: Application, database: Database, react: Reactor) -> None:
    """Register all handlers and inject shared dependencies."""
    global db, reactor  # noqa: PLW0603
    db = database
    reactor = react

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("cancel", cmd_cancel))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("addadmin", cmd_addadmin))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
