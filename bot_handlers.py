"""100 % Inline-Keyboard-driven Telegram bot UI  (v4).

v4 changes
──────────
• Top-level try/except in ``callback_handler`` and ``text_handler`` so
  no unhandled exception can crash a callback or leave the UI hanging.
• ``telegram.error.BadRequest: Message is not modified`` silently ignored
  (user double-clicking the same button).
• ``/addadmin <user_id> [admin|moderator]`` command handler (owner only).
• Friendly "No items yet" messages when accounts/channels/emojis are empty.
• All DB look-ups in callback_handler wrapped for safety.
"""

from __future__ import annotations

import logging
from typing import Any

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

# ------------------------------------------------------------------ #
#  In-memory user states  (keyed by ``user_id``)                     #
# ------------------------------------------------------------------ #
# States: reg_name, reg_phone, reg_api_id, reg_api_hash, reg_session,
#         check, add_channel, add_emoji, add_role_user, add_role_select

user_states: dict[int, dict[str, Any]] = {}

# Module-level references – set once by ``setup_handlers``.
db: Database = None  # type: ignore[assignment]
reactor: Reactor = None  # type: ignore[assignment]


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
    ]
    if role == "owner":
        rows.append([InlineKeyboardButton("👑 Admins", callback_data="admins")])
    rows.append([InlineKeyboardButton("📖 Help Guide", callback_data="help")])
    return InlineKeyboardMarkup(rows)


# ── Accounts ────────────────────────────────────────────────────── #

def _accounts_kb(accounts: list[dict], can_add: bool) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for acc in accounts:
        icon = "🟢" if acc["status"] == "active" else "🔴"
        rows.append(
            [InlineKeyboardButton(f"{icon} {acc['name']}", callback_data=f"acc:{acc['id']}")]
        )
    if can_add:
        rows.append([InlineKeyboardButton("➕ Add Account", callback_data="acc_add")])
    rows.append([_back_button()])
    return InlineKeyboardMarkup(rows)


def _account_detail_kb(acc_id: str, can_manage: bool) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if can_manage:
        rows.append(
            [
                InlineKeyboardButton("Check 🔍", callback_data=f"acc_chk:{acc_id}"),
                InlineKeyboardButton("Delete 🗑️", callback_data=f"acc_del:{acc_id}"),
            ]
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
    rows.append([_back_button()])
    return InlineKeyboardMarkup(rows)


def _channel_detail_kb(ch_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🗑️ Remove Channel", callback_data=f"ch_del:{ch_id}")],
            [_back_button("channels")],
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

    "*📱 Adding Accounts (StringSession)*\n"
    "1. Go to https://my.telegram.org → log in → create an app.\n"
    "2. Note your *API ID* and *API Hash*.\n"
    "3. Generate a Session String:\n"
    "`from telethon.sync import TelegramClient`\n"
    "`from telethon.sessions import StringSession`\n"
    "`with TelegramClient(StringSession(), API_ID, API_HASH) as c:`\n"
    "`    print(c.session.save())`\n"
    "4. In the bot tap *📱 Accounts → ➕ Add Account* and follow the 5-step wizard "
    "(Name → Phone → API ID → API Hash → Session String).\n\n"

    "*📡 Adding Target Channels*\n"
    "1. Find the channel's numeric ID (e.g. forward a msg to @userinfobot).\n"
    "2. Tap *📡 Channels → ➕ Add Channel*.\n"
    "3. Send the ID and name: `-1001234567890 My Channel`\n"
    "4. Make sure every registered account has *joined* the channel.\n\n"

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
    "• /addadmin `<user_id>` `[admin|moderator]` — (Owner only) assign a role."
)


# ================================================================== #
#  /start command                                                     #
# ================================================================== #

async def cmd_start(update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id  # type: ignore[union-attr]
    if not _is_authorized(user_id):
        await update.message.reply_text("⛔ You are not authorised to use this bot.")  # type: ignore[union-attr]
        return
    user_states.pop(user_id, None)
    await update.message.reply_text(  # type: ignore[union-attr]
        "🤖 *Auto-Reaction Bot*\n\nSelect an option:",
        reply_markup=_main_menu_kb(user_id),
        parse_mode="Markdown",
    )


# ================================================================== #
#  /addadmin command  (owner only, v4)                                #
# ================================================================== #

async def cmd_addadmin(update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Usage: ``/addadmin <user_id> [admin|moderator]``"""
    msg = update.message
    assert msg is not None
    user_id = msg.from_user.id  # type: ignore[union-attr]

    if not _can_manage_roles(user_id):
        await msg.reply_text("⛔ Owner only.")
        return

    parts = (msg.text or "").split()
    if len(parts) < 2:
        await msg.reply_text(
            "Usage: `/addadmin <user_id> [admin|moderator]`\n"
            "Default role is *admin*.",
            parse_mode="Markdown",
        )
        return

    try:
        target_uid = int(parts[1])
    except ValueError:
        await msg.reply_text("❗ User ID must be numeric.")
        return

    role = "admin"
    if len(parts) >= 3 and parts[2].lower() in ("admin", "moderator"):
        role = parts[2].lower()

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
    await query.answer()

    user_id = query.from_user.id
    if not _is_authorized(user_id):
        await query.answer("⛔ Unauthorised", show_alert=True)
        return

    data: str = query.data or ""
    role = _get_role(user_id)

    try:
        await _route_callback(query, user_id, role, data)
    except BadRequest as e:
        # "Message is not modified" happens when user double-clicks a
        # button that leads to the same content — harmless, ignore.
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


async def _route_callback(query: Any, user_id: int, role: str | None, data: str) -> None:  # noqa: C901
    """Inner dispatch – separated so the caller can catch exceptions."""

    # Keep state during active input flows.
    keep = ("acc_add", "skip_phone", "ch_add", "em_add", "adm_add", "set_role", "cancel")
    if not data.startswith(keep):
        user_states.pop(user_id, None)

    # ─── Main menu ─────────────────────────────────────────────── #
    if data == "main":
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

    # ─── Accounts ──────────────────────────────────────────────── #
    elif data == "accounts":
        can_manage = _can_manage_accounts(user_id)
        # Owner sees ALL accounts; Admin sees own; Moderator sees all (read-only).
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

    elif data == "acc_add":
        if not _can_manage_accounts(user_id):
            await query.answer("⛔ Admins and Owner only", show_alert=True)
            return
        user_states[user_id] = {"step": "reg_name", "data": {}}
        await query.edit_message_text(
            "📝 *Register Account — Step 1/5*\n\nSend the *Account Name*:",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("❌ Cancel", callback_data="accounts")]]
            ),
            parse_mode="Markdown",
        )

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
        # Ownership check: owner can view any; admin own only; moderator any (view).
        if role == "admin" and acc["owner_id"] != user_id:
            await query.answer("⛔ Not your account", show_alert=True)
            return

        icon = "🟢 Active" if acc["status"] == "active" else "🔴 Failed"
        text = (
            f"📱 *Account Detail*\n\n"
            f"• *Name:* {acc['name']}\n"
            f"• *Phone:* {acc.get('phone') or 'N/A'}\n"
            f"• *API ID:* `{acc['api_id']}`\n"
            f"• *API Hash:* `{acc['api_hash'][:8]}…`\n"
            f"• *Status:* {icon}"
        )
        await query.edit_message_text(
            text,
            reply_markup=_account_detail_kb(acc_id, can_manage=can_manage),
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
        # Admin can only delete own; owner can delete any.
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

    # ─── Skip phone ────────────────────────────────────────────── #
    elif data == "skip_phone":
        state = user_states.get(user_id)
        if state and state["step"] == "reg_phone":
            state["data"]["phone"] = None
            state["step"] = "reg_api_id"
            await query.edit_message_text(
                "📝 *Register Account — Step 3/5*\n\nSend the *API ID*:",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("❌ Cancel", callback_data="accounts")]]
                ),
                parse_mode="Markdown",
            )

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
            "📡 *Add Channel*\n\nSend the *channel ID* (numeric, e.g. `-1001234567890`)\n"
            "and *channel name* separated by a space.\n\n"
            "Example: `-1001234567890 My Channel`",
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
        text = f"📡 *Channel*\n\n• *Name:* {ch['channel_name']}\n• *ID:* `{ch['channel_id']}`"
        await query.edit_message_text(
            text, reply_markup=_channel_detail_kb(ch_id), parse_mode="Markdown"
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
        db.delete_channel(ch_id)
        await reactor.reload_channels()
        await query.edit_message_text(
            "✅ Channel removed.",
            reply_markup=InlineKeyboardMarkup([[_back_button("channels")]]),
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
            "👑 *Add User*\n\nSend the Telegram *User ID*:",
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
        chosen_role = data.split(":", 1)[1]  # "admin" or "moderator"
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

    # ── Registration flow (owner / admin only) ────────────────── #
    if step == "reg_name":
        if not _can_manage_accounts(user_id):
            user_states.pop(user_id, None)
            return
        if not text.strip():
            await msg.reply_text("❗ Account name cannot be empty. Please try again.")
            return
        state["data"]["name"] = text.strip()
        state["step"] = "reg_phone"
        await msg.reply_text(
            "📝 *Register Account — Step 2/5*\n\nSend the *Phone Number* or tap Skip:",
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("Skip ⏭️", callback_data="skip_phone")],
                    [InlineKeyboardButton("❌ Cancel", callback_data="accounts")],
                ]
            ),
            parse_mode="Markdown",
        )

    elif step == "reg_phone":
        state["data"]["phone"] = text.strip() or None
        state["step"] = "reg_api_id"
        await msg.reply_text(
            "📝 *Register Account — Step 3/5*\n\nSend the *API ID*:",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("❌ Cancel", callback_data="accounts")]]
            ),
            parse_mode="Markdown",
        )

    elif step == "reg_api_id":
        if not text.strip().isdigit():
            await msg.reply_text("❗ API ID must be numeric. Please try again.")
            return
        state["data"]["api_id"] = text.strip()
        state["step"] = "reg_api_hash"
        await msg.reply_text(
            "📝 *Register Account — Step 4/5*\n\nSend the *API Hash*:",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("❌ Cancel", callback_data="accounts")]]
            ),
            parse_mode="Markdown",
        )

    elif step == "reg_api_hash":
        if not text.strip():
            await msg.reply_text("❗ API Hash cannot be empty. Please try again.")
            return
        state["data"]["api_hash"] = text.strip()
        state["step"] = "reg_session"
        await msg.reply_text(
            "📝 *Register Account — Step 5/5*\n\nSend the *Telethon Session String*:",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("❌ Cancel", callback_data="accounts")]]
            ),
            parse_mode="Markdown",
        )

    elif step == "reg_session":
        if not text.strip():
            await msg.reply_text("❗ Session string cannot be empty. Please try again.")
            return
        state["data"]["session_str"] = text.strip()
        d = state["data"]
        try:
            acc = db.add_account(
                owner_id=user_id,
                name=d["name"],
                phone=d.get("phone"),
                api_id=d["api_id"],
                api_hash=d["api_hash"],
                session_str=d["session_str"],
            )
            user_states.pop(user_id, None)
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
            user_states.pop(user_id, None)
            await msg.reply_text(
                f"❌ Failed to save: `{e}`",
                reply_markup=InlineKeyboardMarkup([[_back_button("accounts")]]),
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

    # ── Add channel ────────────────────────────────────────────── #
    elif step == "add_channel":
        user_states.pop(user_id, None)
        parts = text.strip().split(maxsplit=1)
        if len(parts) < 2:
            await msg.reply_text(
                "❗ Send the channel ID and name separated by a space.\n"
                "Example: `-1001234567890 My Channel`",
                reply_markup=InlineKeyboardMarkup([[_back_button("channels")]]),
            )
            return
        try:
            channel_id = int(parts[0])
        except ValueError:
            await msg.reply_text(
                "❗ Channel ID must be numeric.",
                reply_markup=InlineKeyboardMarkup([[_back_button("channels")]]),
            )
            return
        channel_name = parts[1]
        try:
            db.add_channel(channel_id, channel_name, user_id)
            await reactor.reload_channels()
            await msg.reply_text(
                f"✅ Channel *{channel_name}* added!",
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

    # ── Add role – step 1: receive user ID ─────────────────────── #
    elif step == "add_role_user":
        if not _can_manage_roles(user_id):
            user_states.pop(user_id, None)
            return
        raw = text.strip()
        if not raw.isdigit():
            await msg.reply_text(
                "❗ User ID must be numeric.",
                reply_markup=InlineKeyboardMarkup([[_back_button("admins")]]),
            )
            return
        target_uid = int(raw)
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
        # Transition to role selection.
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
    app.add_handler(CommandHandler("addadmin", cmd_addadmin))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
