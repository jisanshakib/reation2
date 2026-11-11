"""100 % Inline-Keyboard-driven Telegram bot UI.

Every navigation action is a callback-query button press.  Text input from
the user is only requested during state-based flows (registration, check,
add-channel, add-emoji) and is always preceded by an inline-keyboard prompt.
"""

from __future__ import annotations

import logging
from typing import Any

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from config import BOT_TOKEN, OWNER_IDS
from database import Database
from reactor import Reactor

log = logging.getLogger(__name__)

# ------------------------------------------------------------------ #
#  In-memory user states (keyed by ``user_id``)                      #
# ------------------------------------------------------------------ #
# States: reg_name, reg_phone, reg_api_id, reg_api_hash, reg_session,
#          check, add_channel, add_emoji

user_states: dict[int, dict[str, Any]] = {}

# Module-level references set by ``setup_handlers``.
db: Database = None  # type: ignore[assignment]
reactor: Reactor = None  # type: ignore[assignment]


def _is_owner(user_id: int) -> bool:
    """Return True if *user_id* is in the configured owner list."""
    return user_id in OWNER_IDS


# =================================================================== #
#  Keyboard builders                                                  #
# =================================================================== #

def _back_button(target: str = "main") -> InlineKeyboardButton:
    return InlineKeyboardButton("⬅️ Back", callback_data=target)


def _main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📱 Accounts", callback_data="accounts")],
            [InlineKeyboardButton("📡 Channels", callback_data="channels")],
            [InlineKeyboardButton("😀 Emojis", callback_data="emojis")],
        ]
    )


def _accounts_kb(accounts: list[dict]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for acc in accounts:
        icon = "🟢" if acc["status"] == "active" else "🔴"
        label = f"{icon} {acc['name']}"
        rows.append([InlineKeyboardButton(label, callback_data=f"acc:{acc['id']}")])
    rows.append([InlineKeyboardButton("➕ Add Account", callback_data="acc_add")])
    rows.append([_back_button()])
    return InlineKeyboardMarkup(rows)


def _account_detail_kb(acc_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Check 🔍", callback_data=f"acc_chk:{acc_id}"),
                InlineKeyboardButton("Delete 🗑️", callback_data=f"acc_del:{acc_id}"),
            ],
            [_back_button("accounts")],
        ]
    )


def _channels_kb(channels: list[dict]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for ch in channels:
        label = f"📡 {ch['channel_name']}"
        rows.append([InlineKeyboardButton(label, callback_data=f"ch:{ch['id']}")])
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


def _emojis_kb(emojis: list[dict]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    # Lay emojis out in rows of 5 buttons.
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


def _confirm_delete_kb(prefix: str, item_id: str, back_target: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Yes, delete", callback_data=f"{prefix}_y:{item_id}"),
                InlineKeyboardButton("❌ Cancel", callback_data=back_target),
            ]
        ]
    )


# =================================================================== #
#  /start – the only text-command; shows the main menu                #
# =================================================================== #

async def cmd_start(update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id  # type: ignore[union-attr]
    if not _is_owner(user_id):
        await update.message.reply_text("⛔ You are not authorised to use this bot.")  # type: ignore[union-attr]
        return
    user_states.pop(user_id, None)
    await update.message.reply_text(  # type: ignore[union-attr]
        "🤖 *Auto-Reaction Bot*\n\nSelect an option:",
        reply_markup=_main_menu_kb(),
        parse_mode="Markdown",
    )


# =================================================================== #
#  Callback-query dispatcher                                          #
# =================================================================== #

async def callback_handler(update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    assert query is not None
    await query.answer()

    user_id = query.from_user.id
    if not _is_owner(user_id):
        await query.answer("⛔ Unauthorised", show_alert=True)
        return

    data: str = query.data or ""

    # Clear any pending text-input state unless we're in a registration flow
    # that the user intentionally entered.
    if not data.startswith(("acc_add", "skip_phone", "ch_add", "em_add", "cancel")):
        user_states.pop(user_id, None)

    # ---- Navigation ------------------------------------------------ #
    if data == "main":
        user_states.pop(user_id, None)
        await query.edit_message_text(
            "🤖 *Auto-Reaction Bot*\n\nSelect an option:",
            reply_markup=_main_menu_kb(),
            parse_mode="Markdown",
        )

    # ---- Accounts -------------------------------------------------- #
    elif data == "accounts":
        accs = db.get_accounts(owner_id=user_id)
        text = "📱 *Your Accounts*\n\nSelect an account or add a new one:"
        await query.edit_message_text(text, reply_markup=_accounts_kb(accs), parse_mode="Markdown")

    elif data == "acc_add":
        user_states[user_id] = {"step": "reg_name", "data": {}}
        await query.edit_message_text(
            "📝 *Register Account — Step 1/5*\n\nSend the *Account Name*:",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="accounts")]]),
            parse_mode="Markdown",
        )

    elif data.startswith("acc:"):
        acc_id = data.split(":", 1)[1]
        acc = db.get_account(acc_id)
        if acc is None:
            await query.edit_message_text("Account not found.", reply_markup=InlineKeyboardMarkup([[_back_button("accounts")]]))
            return
        if acc["owner_id"] != user_id:
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
        await query.edit_message_text(text, reply_markup=_account_detail_kb(acc_id), parse_mode="Markdown")

    elif data.startswith("acc_chk:"):
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
        acc_id = data.split(":", 1)[1]
        acc = db.get_account(acc_id)
        name = acc["name"] if acc else "?"
        await query.edit_message_text(
            f"⚠️ *Delete account* `{name}`?",
            reply_markup=_confirm_delete_kb("acc_del", acc_id, f"acc:{acc_id}"),
            parse_mode="Markdown",
        )

    elif data.startswith("acc_del_y:"):
        acc_id = data.split(":", 1)[1]
        acc = db.get_account(acc_id)
        if acc and acc["owner_id"] != user_id:
            await query.answer("⛔ Not your account", show_alert=True)
            return
        await reactor.remove_client(acc_id)
        db.delete_account(acc_id)
        await query.edit_message_text(
            "✅ Account deleted.",
            reply_markup=InlineKeyboardMarkup([[_back_button("accounts")]]),
        )

    # ---- Skip phone ------------------------------------------------ #
    elif data == "skip_phone":
        state = user_states.get(user_id)
        if state and state["step"] == "reg_phone":
            state["data"]["phone"] = None
            state["step"] = "reg_api_id"
            await query.edit_message_text(
                "📝 *Register Account — Step 3/5*\n\nSend the *API ID*:",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="accounts")]]),
                parse_mode="Markdown",
            )

    # ---- Channels -------------------------------------------------- #
    elif data == "channels":
        channels = db.get_channels()
        text = "📡 *Target Channels*\n\nSelect a channel or add a new one:"
        await query.edit_message_text(text, reply_markup=_channels_kb(channels), parse_mode="Markdown")

    elif data == "ch_add":
        user_states[user_id] = {"step": "add_channel", "data": {}}
        await query.edit_message_text(
            "📡 *Add Channel*\n\nSend the *channel ID* (numeric, e.g. `-1001234567890`)\n"
            "and *channel name* separated by a space.\n\n"
            "Example: `-1001234567890 My Channel`",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="channels")]]),
            parse_mode="Markdown",
        )

    elif data.startswith("ch:"):
        ch_id = data.split(":", 1)[1]
        ch = db.get_channel(ch_id)
        if ch is None:
            await query.edit_message_text("Channel not found.", reply_markup=InlineKeyboardMarkup([[_back_button("channels")]]))
            return
        text = f"📡 *Channel*\n\n• *Name:* {ch['channel_name']}\n• *ID:* `{ch['channel_id']}`"
        await query.edit_message_text(text, reply_markup=_channel_detail_kb(ch_id), parse_mode="Markdown")

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

    # ---- Emojis ---------------------------------------------------- #
    elif data == "emojis":
        emojis = db.get_emojis()
        text = "😀 *Allowed Emojis*\n\nTap an emoji to remove it, or add a new one:"
        await query.edit_message_text(text, reply_markup=_emojis_kb(emojis), parse_mode="Markdown")

    elif data == "em_add":
        user_states[user_id] = {"step": "add_emoji", "data": {}}
        await query.edit_message_text(
            "😀 *Add Emoji*\n\nSend the reaction emoji (e.g. 👍 🔥 ❤️):",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="emojis")]]),
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

    # ---- Cancel (generic) ------------------------------------------ #
    elif data == "cancel":
        user_states.pop(user_id, None)
        await query.edit_message_text(
            "🤖 *Auto-Reaction Bot*\n\nSelect an option:",
            reply_markup=_main_menu_kb(),
            parse_mode="Markdown",
        )


# =================================================================== #
#  Text-message handler (state-driven input)                          #
# =================================================================== #

async def text_handler(update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    assert msg is not None
    user_id = msg.from_user.id  # type: ignore[union-attr]

    if not _is_owner(user_id):
        return

    state = user_states.get(user_id)
    if state is None:
        # No active state – show main menu hint.
        await msg.reply_text(
            "Use the buttons to navigate. Tap /start if you don't see them.",
        )
        return

    step = state["step"]
    text = msg.text or ""

    # ---- Registration flow ----------------------------------------- #
    if step == "reg_name":
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
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="accounts")]]),
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
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="accounts")]]),
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
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="accounts")]]),
            parse_mode="Markdown",
        )

    elif step == "reg_session":
        if not text.strip():
            await msg.reply_text("❗ Session string cannot be empty. Please try again.")
            return
        state["data"]["session_str"] = text.strip()

        # Persist to DB and connect in the reactor.
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

            # Try to connect immediately.
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

    # ---- Check mode ------------------------------------------------ #
    elif step == "check":
        acc_id = state["data"]["account_id"]
        user_states.pop(user_id, None)
        result = await reactor.check_react(acc_id, text.strip())
        await msg.reply_text(
            result,
            reply_markup=InlineKeyboardMarkup([[_back_button(f"acc:{acc_id}")]]),
            parse_mode="Markdown",
        )

    # ---- Add channel ----------------------------------------------- #
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

    # ---- Add emoji ------------------------------------------------- #
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


# =================================================================== #
#  Application builder                                                #
# =================================================================== #

def setup_handlers(app: Application, database: Database, react: Reactor) -> None:
    """Register all handlers and inject shared dependencies."""
    global db, reactor  # noqa: PLW0603
    db = database
    reactor = react

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
