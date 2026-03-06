import asyncio
import math
import re
import time
from datetime import datetime
from pathlib import Path

from aiogram import Bot, Router, F
from aiogram.dispatcher.middlewares.base import BaseMiddleware
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    KeyboardButton,
    KeyboardButtonRequestChat,
    ChatAdministratorRights,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    ReplyKeyboardMarkup,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder

from config import Config
from db import Database
from services import (
    CryptoPayClient,
    build_topup_success_text,
    check_bot_permissions,
    check_sponsors,
    ensure_user_achievements,
    format_achievements,
    format_permission_error,
    get_achievement_progress,
    grant_task_reward,
    hold_until_ts,
    is_member,
    next_level_target,
    topup_bonus_seconds,
    verify_task_for_resume,
)
from utils import escape, format_coins, now_ts, parse_referral_arg

router = Router()

routers: list[Router] = []

NEW_USER_NOTIFY_SETTING = "notify_new_user"


class GetIdState(StatesGroup):
    waiting_for_chat = State()


class CreateTaskState(StatesGroup):
    choose_type = State()
    channel = State()
    post = State()
    reaction = State()
    reward = State()
    quantity = State()
    confirm = State()


class TopUpState(StatesGroup):
    amount = State()
    asset = State()


class PromoState(StatesGroup):
    enter_code = State()


class AdminState(StatesGroup):
    action = State()
    user_lookup = State()
    promo_code = State()
    promo_amount = State()
    promo_max_uses = State()
    promo_expires = State()
    promo_delete_code = State()
    task_delete_id = State()
    task_info_id = State()
    admin_user_id = State()
    admin_task_pause = State()
    admin_task_resume = State()
    admin_task_delete = State()
    admin_task_edit = State()
    admin_task_edit_title = State()
    admin_task_edit_reward = State()
    admin_task_edit_desc = State()
    admin_task_block_reason = State()
    admin_create_task_owner = State()
    sponsor_action = State()
    sponsor_chat = State()
    sponsor_link = State()
    broadcast_text = State()
    broadcast_button = State()
    broadcast_button_url = State()
    balance_user = State()
    balance_amount = State()
    balance_reason = State()
    block_user = State()
    block_duration = State()
    unblock_user = State()


class ReviewState(StatesGroup):
    note = State()


class ManageTaskState(StatesGroup):
    amount = State()


class AppealState(StatesGroup):
    reason = State()


def main_menu_kb(is_admin: bool) -> ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    builder.button(text="🧩 Задания")
    builder.button(text="📢 Рекламировать")
    builder.button(text="👤 Профиль")
    builder.button(text="💳 Баланс")
    builder.button(text="🧷 ОП")
    builder.button(text="📊 Статистика")
    builder.button(text="❓ Помощь")
    if is_admin:
        builder.button(text="🛠 Админ")
        builder.adjust(2, 2, 2, 2)
    else:
        builder.adjust(2, 2, 2, 1)
    return builder.as_markup(resize_keyboard=True)


def profile_menu_kb() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="🏅 Достижения", callback_data="menu:achievements")
    builder.button(text="🤝 Рефералы", callback_data="menu:referrals")
    builder.button(text="🎁 Промокод", callback_data="menu:promo")
    builder.button(text="📈 Мои задания", callback_data="menu:ads")
    builder.button(text="⬅️ Меню", callback_data="menu:home")
    builder.adjust(2, 2, 1)
    return builder.as_markup()


def onboarding_kb(is_admin: bool) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="🧩 Задания", callback_data="menu:tasks")
    builder.button(text="📢 Рекламировать", callback_data="menu:create")
    builder.button(text="💳 Баланс", callback_data="menu:balance")
    builder.button(text="❓ FAQ", callback_data="menu:faq")
    if is_admin:
        builder.button(text="🛠 Админ", callback_data="menu:admin")
    builder.button(text="🏠 Меню", callback_data="menu:home")
    builder.adjust(2, 2, 2)
    return builder.as_markup()


def help_menu_kb() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="📘 FAQ", callback_data="menu:faq")
    builder.button(text="💡 Подсказки", callback_data="menu:tips")
    builder.button(text="⬅️ Меню", callback_data="menu:home")
    builder.adjust(2, 1)
    return builder.as_markup()


def faq_menu_kb() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="💰 Заработок", callback_data="faq:earn")
    builder.button(text="📢 Реклама", callback_data="faq:advertise")
    builder.button(text="💳 Оплата", callback_data="faq:payments")
    builder.button(text="🧾 Комиссия", callback_data="faq:commission")
    builder.button(text="🆘 Поддержка", callback_data="faq:support")
    builder.button(text="⬅️ Помощь", callback_data="menu:help")
    builder.adjust(2, 2, 2)
    return builder.as_markup()


def cancel_kb() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ Отмена", callback_data="state:cancel")
    return builder.as_markup()


def quantity_kb(max_quantity: int | None = None) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    max_value = max(max_quantity or 0, 0)
    builder.button(
        text=f"{max_value} (Максимально для вашего баланса)",
        callback_data="create:qty:max",
    )
    builder.button(text="❌ Отмена", callback_data="state:cancel")
    builder.adjust(1)
    return builder.as_markup()


INVITE_RIGHTS = ChatAdministratorRights(
    is_anonymous=False,
    can_manage_chat=False,
    can_delete_messages=False,
    can_manage_video_chats=False,
    can_restrict_members=False,
    can_promote_members=False,
    can_change_info=False,
    can_invite_users=True,
    can_post_stories=False,
    can_edit_stories=False,
    can_delete_stories=False,
)

OWN_CHANNEL_REQUEST_ID = 101
OTHER_CHANNEL_REQUEST_ID = 102
OWN_GROUP_REQUEST_ID = 201
OTHER_GROUP_REQUEST_ID = 202
OTHER_CHAT_REQUEST_IDS = {OTHER_CHANNEL_REQUEST_ID, OTHER_GROUP_REQUEST_ID}


def chat_picker_kb(kind: str) -> ReplyKeyboardMarkup:
    is_channel = kind == "channel"
    own_text = "✅ Выбрать свой канал" if is_channel else "✅ Выбрать свой чат"
    other_text = "📌 Выберите другой канал" if is_channel else "📌 Выберите другой чат"
    own_request_id = OWN_CHANNEL_REQUEST_ID if is_channel else OWN_GROUP_REQUEST_ID
    other_request_id = OTHER_CHANNEL_REQUEST_ID if is_channel else OTHER_GROUP_REQUEST_ID
    builder = ReplyKeyboardBuilder()
    builder.add(
        KeyboardButton(
            text=own_text,
            request_chat=KeyboardButtonRequestChat(
                request_id=own_request_id,
                chat_is_channel=is_channel,
                user_administrator_rights=INVITE_RIGHTS,
                bot_administrator_rights=INVITE_RIGHTS,
            ),
        )
    )
    builder.add(
        KeyboardButton(
            text=other_text,
            request_chat=KeyboardButtonRequestChat(
                request_id=other_request_id,
                chat_is_channel=is_channel,
            ),
        )
    )
    builder.add(KeyboardButton(text="❌ Отмена"))
    builder.adjust(1, 1, 1)
    return builder.as_markup(resize_keyboard=True)


def id_picker_kb() -> ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    builder.add(
        KeyboardButton(
            text="📢 Выбрать канал",
            request_chat=KeyboardButtonRequestChat(
                request_id=301,
                chat_is_channel=True,
            ),
        )
    )
    builder.add(
        KeyboardButton(
            text="👥 Выбрать группу",
            request_chat=KeyboardButtonRequestChat(
                request_id=302,
                chat_is_channel=False,
            ),
        )
    )
    builder.add(KeyboardButton(text="❌ Отменить"))
    builder.adjust(1, 1, 1)
    return builder.as_markup(resize_keyboard=True)


def tasks_menu_kb() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="📢 Каналы", callback_data="tasks:channel")
    builder.button(text="👥 Группы", callback_data="tasks:group")
    builder.button(text="👀 Просмотр поста", callback_data="tasks:view")
    builder.button(text="👍 Реакция", callback_data="tasks:reaction")
    builder.button(text="⚡️ Бусты", callback_data="tasks:boost")
    builder.button(text="⬅️ Меню", callback_data="menu:home")
    builder.adjust(2, 2, 1, 1)
    return builder.as_markup()


def op_menu_kb() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="📢 Публичные каналы/чаты", callback_data="op:public")
    builder.button(text="🔒 Приватные каналы/чаты", callback_data="op:private")
    builder.button(text="🔗 ОП на пригласительную ссылку", callback_data="op:invite")
    builder.button(text="🤝 Реферальная ссылка AD GRAM", callback_data="op:ref")
    builder.button(text="⌛️ Автоудаление сообщений", callback_data="op:auto")
    builder.button(text="⬅️ Назад", callback_data="menu:home")
    builder.adjust(1)
    return builder.as_markup()


def op_back_kb() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="⬅️ Назад", callback_data="op:menu")
    return builder.as_markup()


def sponsor_gate_kb(sponsors: list[dict]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for sponsor in sponsors:
        title = sponsor.get("title") or sponsor.get("chat_username") or "Спонсор"
        link = sponsor.get("invite_link")
        if not link and sponsor.get("chat_username"):
            link = f"https://t.me/{sponsor['chat_username'].lstrip('@')}"
        if link:
            builder.row(InlineKeyboardButton(text=f"Подписаться: {title}", url=link))
    builder.button(text="✅ Проверить подписку", callback_data="sponsor:check")
    builder.adjust(1)
    return builder.as_markup()


def task_take_kb(task_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="Выполнить", callback_data=f"task:take:{task_id}")
    return builder.as_markup()


def task_check_subscribe_kb(claim_id: int, join_url: str | None) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if join_url:
        builder.row(InlineKeyboardButton(text="Перейти к каналу/группе", url=join_url))
    builder.button(text="✅ Я подписался", callback_data=f"task:check:{claim_id}")
    builder.adjust(1)
    return builder.as_markup()


def task_view_confirm_kb(claim_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Я посмотрел", callback_data=f"task:viewed:{claim_id}")
    return builder.as_markup()


def reaction_review_kb(claim_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Одобрить", callback_data=f"review:approve:{claim_id}")
    builder.button(text="❌ Отклонить", callback_data=f"review:reject:{claim_id}")
    builder.button(text="📝 На доработку", callback_data=f"review:revise:{claim_id}")
    builder.adjust(1)
    return builder.as_markup()


def admin_menu_kb() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="📊 Статистика", callback_data="admin:stats")
    builder.button(text="🏆 Топы", callback_data="admin:tops")
    builder.button(text="📋 Управление заданиями", callback_data="admin:tasks:manage")
    builder.button(text="📝 Создать задание от пользователя", callback_data="admin:create:as_user")
    builder.button(text="👤 Пользователь", callback_data="admin:user")
    builder.button(text="💰 Начислить", callback_data="admin:credit")
    builder.button(text="💸 Списать", callback_data="admin:debit")
    builder.button(text="🎁 Промокоды", callback_data="admin:promo")
    builder.button(text="📋 Список промокодов", callback_data="admin:promo:list")
    builder.button(text="🗑 Удалить промокод", callback_data="admin:promo:delete")
    builder.button(text="📝 Апелляции", callback_data="admin:appeals:page:1")
    builder.button(text="🤝 Спонсоры", callback_data="admin:sponsors")
    builder.button(text="📣 Рассылка", callback_data="admin:broadcast")
    builder.button(text="🔔 Уведомления", callback_data="admin:notify")
    builder.button(text="🧾 Транзакции", callback_data="admin:tx")
    builder.button(text="🧾 Счета", callback_data="admin:invoices")
    builder.button(text="🔎 Задание", callback_data="admin:task:info")
    builder.button(text="🗑 Удалить задание", callback_data="admin:task:delete")
    builder.button(text="🚫 Блокировки", callback_data="admin:blocks")
    builder.button(text="⛔️ Бан", callback_data="admin:block")
    builder.button(text="🔓 Разбан", callback_data="admin:unblock")
    builder.button(text="🗂 Дамп базы", callback_data="admin:dump")
    builder.button(text="⬅️ Назад", callback_data="admin:back")
    builder.adjust(2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1)
    return builder.as_markup()


def admin_tops_kb() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="💰 По балансу", callback_data="admin:tops:balance")
    builder.button(text="✅ По выполненным", callback_data="admin:tops:completed")
    builder.button(text="⬅️ Админ", callback_data="menu:admin")
    builder.adjust(2, 1)
    return builder.as_markup()


def admin_user_actions_kb(user_id: int, is_blocked: bool) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="🔄 Обновить", callback_data=f"admin:user:refresh:{user_id}")
    builder.button(text="🧩 Задания", callback_data=f"admin:user:tasks:{user_id}")
    if is_blocked:
        builder.button(text="🔓 Разбанить", callback_data=f"admin:user:unblock:{user_id}")
    else:
        builder.button(text="⛔️ Бан", callback_data=f"admin:user:block:{user_id}")
    builder.button(text="⬅️ Админ", callback_data="menu:admin")
    builder.adjust(2, 1)
    return builder.as_markup()


def admin_user_unblock_confirm_kb(user_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Разбанить", callback_data=f"admin:user:unblock:confirm:{user_id}")
    builder.button(text="❌ Отмена", callback_data=f"admin:user:refresh:{user_id}")
    builder.adjust(2)
    return builder.as_markup()


def admin_notify_kb(enabled: bool) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if enabled:
        builder.button(text="🔕 Выключить", callback_data="admin:notify:off")
    else:
        builder.button(text="🔔 Включить", callback_data="admin:notify:on")
    builder.button(text="⬅️ Админ", callback_data="menu:admin")
    builder.adjust(1)
    return builder.as_markup()


def reaction_choices_kb() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for emoji in ["👍", "❤️", "🔥", "👏", "😍", "🤩", "😮", "😂"]:
        builder.button(text=emoji, callback_data=f"reaction:{emoji}")
    builder.adjust(4)
    return builder.as_markup()


def add_bot_admin_url(kind: str | None = None) -> str:
    if kind == "channel":
        return (
            "https://t.me/adgramo_bot?startchannel&admin=post_messages+edit_messages+delete_messages+invite_users+manage_chat"
        )
    return (
        "https://t.me/adgramo_bot?startgroup&admin=post_messages+edit_messages+delete_messages+invite_users+manage_chat"
    )


def add_bot_admin_help_text(kind: str | None = None, missing_access: bool = True) -> str:
    link = add_bot_admin_url(kind)
    if missing_access:
        return (
            "❌ Чат не найден или бот не имеет доступа к этому чату.\n\n"
            "Нажмите кнопку ниже, чтобы добавить бота с нужными правами.\n"
            f"Если вы не можете сделать это сами, передайте <a href=\"{link}\">эту ссылку</a> администратору чата."
        )
    return (
        "❌ Бот не является администратором в этом чате!\n\n"
        "Нажмите кнопку ниже, чтобы добавить бота с нужными правами.\n"
        f"Если вы не можете сделать это сами, передайте <a href=\"{link}\">эту ссылку</a> администратору чата."
    )


def add_bot_admin_kb(kind: str | None = None) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(
        text="➕ Добавить бота",
        url=add_bot_admin_url(kind),
    )
    return builder.as_markup()


def add_bot_admin_cancel_kb(kind: str | None = None) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(
        text="➕ Добавить бота",
        url=add_bot_admin_url(kind),
    )
    builder.button(text="❌ Отмена", callback_data="state:cancel")
    builder.adjust(1, 1)
    return builder.as_markup()


def create_confirm_kb() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Создать", callback_data="create:confirm")
    builder.button(text="❌ Отмена", callback_data="create:cancel")
    builder.adjust(2)
    return builder.as_markup()


class BlockedUserMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        user = getattr(event, "from_user", None)
        if not user:
            return await handler(event, data)
        db: Database | None = data.get("db")
        if not db:
            return await handler(event, data)
        if await check_user_blocked(event.bot, db, user.id):
            if isinstance(event, CallbackQuery):
                try:
                    await event.answer()
                except Exception:
                    pass
            return
        return await handler(event, data)


class CallbackAntiFloodMiddleware(BaseMiddleware):
    def __init__(self, limit: float = 0.7) -> None:
        self._limit = limit
        self._last: dict[int, float] = {}

    async def __call__(self, handler, event, data):
        if isinstance(event, CallbackQuery):
            user = event.from_user
            if user:
                now = time.monotonic()
                last = self._last.get(user.id, 0.0)
                if now - last < self._limit:
                    try:
                        await event.answer("Не так быстро 😅", show_alert=True)
                    except Exception:
                        pass
                    return
                self._last[user.id] = now
        return await handler(event, data)


class MessageAntiFloodMiddleware(BaseMiddleware):
    def __init__(self, limit: float = 0.7) -> None:
        self._limit = limit
        self._last: dict[int, float] = {}

    async def __call__(self, handler, event, data):
        if isinstance(event, Message):
            user = event.from_user
            if user:
                now = time.monotonic()
                last = self._last.get(user.id, 0.0)
                if now - last < self._limit:
                    return
                self._last[user.id] = now
        return await handler(event, data)


# -- handlers sections will be appended below --

router = Router()
routers.append(router)

MAIN_MENU_TEXT = (
    "✨ <b>Главное меню</b>\n"
    "📌 Обязательная проверка подписки в ваших чатах.\n"
    "AD GRAM обеспечивает удобные и гибкие настройки для проверки подписок с широким выбором фильтров.\n\n"
    "📈 Рекламная система AD GRAM для роста живой аудитории позволяет эффективно продвигать:\n"
    "• 👥 Подписки на каналы и группы\n"
    "• 👁 Просмотры контента\n"
    "• ⚡️Премиум-бусты\n\n"
    "📘 <a href=\"https://teletype.in/@ameropro/help_ru\">Инструкция по продвижению</a>\n\n"
    "💡 Используя бота, вы автоматически соглашаетесь с нашей политикой конфиденциальности."
)

ONBOARDING_TEXT = (
    "👋 <b>Добро пожаловать в AD GRAM</b>\n"
    "Выберите, что хотите сделать прямо сейчас:\n"
    "• 💰 Зарабатывать на заданиях\n"
    "• 📢 Рекламировать канал/чат\n"
    "• 💳 Пополнить баланс\n\n"
    "Быстрые кнопки ниже, основное меню уже в клавиатуре."
)

HELP_MENU_TEXT = (
    "❓ <b>Помощь</b>\n"
    "Ответы на частые вопросы и подсказки."
)

TIPS_TEXT = (
    "💡 <b>Подсказки</b>\n\n"
    "1) Пополняйте баланс заранее, чтобы задания запускались без задержек.\n"
    "2) Если бот не видит канал — дайте ему право «Приглашение пользователей»."
)

FAQ_CONTENT = {
    "earn": (
        "Как зарабатывать",
        "Откройте «Задания», выберите тип, выполните условия и нажмите кнопку проверки.\n"
        "Награда начислится после подтверждения.",
    ),
    "advertise": (
        "Как рекламировать",
        "Нажмите «Рекламировать», выберите тип задания, укажите источник и награду.\n"
        "После подтверждения задание появится в ленте.",
    ),
    "payments": (
        "Оплата и счета",
        "Пополнение работает через CryptoBot. После оплаты нажмите «Проверить оплату»\n"
        "или дождитесь автопроверки (до 15 минут).",
    ),
    "commission": (
        "Комиссия",
        "Комиссия отключается на 24 часа за каждый оплаченный 1$ эквивалента.\n"
        "Статус отображается при создании задания.",
    ),
    "support": (
        "Поддержка",
        "Если возникли сложности, напишите в чат поддержки @ameropro.",
    ),
}

OP_PUBLIC_TEXT = (
    "✅Функция проверки подписки на канал/чат\n\n"
    "▸ Шаг 1. Добавьте бота в ваш чат с правом «Приглашение пользователей». "
    "Можно через <a href=\"https://t.me/adgramo_bot?startgroup&admin=post_messages+edit_messages+delete_messages+invite_users+manage_chat\">эту ссылку</a>.\n"
    "▸ Шаг 2. Добавьте бота в администраторы канала/чата, на который хотите установить проверку подписки, "
    "и оставьте право «Приглашение пользователей».\n"
    "Если нужно, передайте администратору канала/чата "
    "<a href=\"https://t.me/adgramo_bot?startchannel&admin=post_messages+edit_messages+delete_messages+invite_users+manage_chat\">эту ссылку</a>.\n"
    "Шаг 3: Чтобы включить подписку на канал/чат, напишите в вашем чате команду:\n"
    "<code>/setup ссылка</code>\n\n"
    "⛔️ Чтобы отключить функцию, вам нужно:\n"
    "▸ Написать команду:\n"
    "<code>/unsetup @channel</code> - отключить канал/чат\n"
    "Пример: <code>/unsetup @rove</code>\n\n"
    "➕Максимальное количество одновременной проверки - 5 каналов/чатов\n"
    "❌ Для отключения сразу всех установленных проверок на подписки используйте команду /unsetup\n\n"
    "💡 Напишите команду <code>/status</code> в вашем чате, чтобы получить перечень активных проверок на подписку, "
    "а также информацию о времени действия каждой проверки и ее отмене.\n\n"
    "🕒 Дополнительно вы можете установить таймер для автоматического отключения проверки подписки.\n"
    "пример:\n"
    "<code>/setup @rove 1d</code>\n"
    "Время можно указать в секундах, минутах, часах и днях.\n"
    "s - секунд\n"
    "m - минут\n"
    "h - часов\n"
    "d - дней\n\n"
    "Если возникли сложности, обращайтесь в чат поддержки @ameropro"
)

OP_PRIVATE_TEXT = (
    "📢 Проверка подписки для приватных каналов/чатов:\n"
    "Рекламируйте приватные каналы и группы через AD GRAM, устанавливая проверку подписки для приватных групп и каналов. "
    "Ниже приведена подробная инструкция, как это сделать.\n\n"
    "Шаг 1: Узнайте ID приватного канала. Вот как это можно сделать:\n"
    "▸Напишите команду /id в боте AD GRAM\n"
    "▸С помощью кнопки внизу экрана выберите нужный канал или группу в AD GRAM\n"
    "▸Скопируйте полученный id канала\n\n"
    "Шаг 2: Чтобы включить подписку на канал/чат, напишите в вашем чате команду, пример:\n"
    "/setup 1001994526641\n\n"
    "Чтобы отключить проверку подписки, воспользуйтесь командой:\n"
    "/unsetup 1001994526641\n\n"
    "Или напишите команду /status в вашем чате, чтобы открыть удобное меню для просмотра и редактирования "
    "всех установленных проверок на подписку."
)

OP_INVITE_TEXT = (
    "🔗 Проверка подписки на пригласительные ссылки.\n\n"
    "Шаг 1: Узнайте ID приватного канала. Для этого:\n"
    "▸ Напишите команду /id в боте AD GRAM\n"
    "▸ Используя кнопку внизу экрана, выберите нужный канал или группу в AD GRAM\n"
    "▸ Скопируйте полученный ID канала\n\n"
    "Шаг 2: Чтобы включить проверку подписки на канал/чат, отправьте команду в вашем чате, пример:\n"
    "/setup 1001994526641 https://t.me/+JKlfKLJnfjfn\n\n"
    "Чтобы отключить проверку подписки, используйте команду:\n"
    "/unsetup 1001994526641\n\n"
    "Можно установить проверку на определённое количество подписок — как только цель будет достигнута, "
    "проверка отключится автоматически, пример:\n"
    "/setup 1001994526641 https://t.me/+JKlfKLJnfjfn 100\n\n"
    "🕒 Дополнительно: Вы можете установить таймер для автоматического отключения проверки подписки. Пример:\n"
    "/setup 1001994526641 https://t.me/+JKlfKLJnfjfn 1d\n"
    "Время можно указать в секундах, минутах, часах и днях.\n"
    "s - секунды\n"
    "m - минуты\n"
    "h - часы\n"
    "d - дни\n\n"
    "💡Если вам нужно просмотреть или отредактировать все текущие настройки, используйте команду /status в вашем "
    "чате для удобного меню управления проверками подписки."
)

OP_REFERRAL_TEXT = (
    "🔗 Добавление реферальной ссылки AD GRAM к обязательной подписке в вашем чате!\n\n"
    "Это поможет привлекать новых пользователей в бот по вашей реферальной ссылке и получать бонусы.\n\n"
    "Вы будете получать:\n"
    "• 💰 3000 BIT – за каждого привлеченного реферала\n"
    "• 🎁 +10% – от суммы пополнений вашими рефералами\n"
    "• 👨‍💻 +3% до +15% – от выполнения заданий вашими рефералами (в зависимости от вашего уровня)\n\n"
    "Как настроить проверку подписки:\n"
    "▸ Шаг 1: Добавьте бота в ваш чат с правом «Приглашение пользователей». "
    "Можно через <a href=\"https://t.me/adgramo_bot?startgroup&admin=post_messages+edit_messages+delete_messages+invite_users+manage_chat\">эту ссылку</a>.\n"
    "▸ Шаг 2: Чтобы включить проверку подписки на вашу реферальную ссылку, напишите в вашем чате команду:\n"
    "<code>/setup &lt;ваш_ID&gt; [период]</code>\n"
    "ID - используйте ваш ID (можно найти в «Мой кабинет»)\n\n"
    "Пример:\n"
    "<code>/setup 7193701006</code>\n\n"
    "⛔️ Чтобы отключить проверку, вам нужно написать команду:\n"
    "<code>/unsetup bot</code>\n\n"
    "💡 Напишите команду <code>/status</code> в вашем чате, чтобы получить список активных проверок на подписку, "
    "а также информацию о времени действия каждой проверки и её отмене.\n\n"
    "🕒 Дополнительно вы можете установить таймер для автоматического отключения проверки подписки.\n"
    "Пример:\n"
    "<code>/setup 7193701006 1d</code>\n"
    "Время можно указать в секундах, минутах, часах и днях.\n"
    "s - секунд\n"
    "m - минут\n"
    "h - часов\n"
    "d - дней\n\n"
    "Если возникли сложности обращайтесь в чат поддержки @ameropro"
)

OP_AUTODELETE_TEXT = (
    "⌛️ Настройка автоудаления сообщений\n\n"
    "Установите время от 15 секунд до 5 минут. При активации все сообщения бота будут автоматически "
    "удаляться через указанный интервал времени.\n\n"
    "⚙️ Примеры настройки\n"
    "/autodelete 30s — бот будет удалять свои сообщения через 30 секунд\n"
    "/autodelete 2m — бот будет удалять свои сообщения через 2 минуты\n\n"
    "🛑 Чтобы отключить автоудаление сообщений, используйте команду:\n"
    "/autodelete off\n\n"
    "/get_autodelete — проверить статус команды\n\n"
    "💡 Примечание: Эта функция работает только для сообщений, отправленных ботом."
)


def _day_start_ts() -> int:
    now = datetime.now()
    start = datetime(now.year, now.month, now.day)
    return int(start.timestamp())


def build_tasks_menu_text(summary: dict) -> str:
    return (
        f"💰 <b>Вы можете заработать: {format_coins(summary['total_reward'])}</b>\n\n"
        f"📢 Задание на каналы: <b>{format_coins(summary['channels'])}</b>\n"
        f"👤 Задания на группы: <b>{format_coins(summary['groups'])}</b>\n"
        f"👁 Задания на просмотр: <b>{format_coins(summary['views'])}</b>\n"
        f"⚡️ Задания на бусты: <b>{format_coins(summary['boosts'])}</b>\n"
        f"🔥 Задания на реакции: <b>{format_coins(summary['reactions'])}</b>\n\n"
        "🔔 Выберите способ заработка👇"
    )


async def send_onboarding(bot: Bot, chat_id: int, is_admin: bool) -> None:
    await bot.send_message(chat_id, ONBOARDING_TEXT, reply_markup=onboarding_kb(is_admin))


def build_faq_text(key: str) -> str:
    title, body = FAQ_CONTENT.get(key, ("FAQ", "Раздел не найден."))
    return f"❓ <b>{title}</b>\n{body}"


async def send_main_menu(
    bot: Bot, chat_id: int, is_admin: bool, db: Database, prefix: str | None = None
) -> None:
    stats_text = (
        f"{MAIN_MENU_TEXT}"
    )
    text = f"{prefix}\n\n{stats_text}" if prefix else stats_text
    await bot.send_message(chat_id, text, reply_markup=main_menu_kb(is_admin))


async def send_profile(
    bot: Bot,
    chat_id: int,
    user_id: int,
    db: Database,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> None:
    await notify_new_achievements(bot, db, user_id)
    user = await db.get_user(user_id)
    if not user:
        await bot.send_message(chat_id, "Профиль не найден.")
        return
    bot_user = await bot.get_me()
    referral_link = f"https://t.me/{bot_user.username}?start=ref_{user['id']}"
    progress = await get_achievement_progress(db, user_id)
    achievements_count = sum(1 for item in progress if item["unlocked"])
    achievements_total = len(progress)
    next_target = next_level_target(int(user["completed_tasks"]))
    level_progress = ""
    if next_target:
        left = max(0, next_target - int(user["completed_tasks"]))
        level_progress = f"До следующего уровня: <b>{left}</b> заданий"
    next_achievement_line = ""
    best_remaining = None
    best_item = None
    for item in progress:
        if item["unlocked"]:
            continue
        achievement = item["achievement"]
        remaining = max(0, achievement.threshold - int(item["value"]))
        if best_remaining is None or remaining < best_remaining:
            best_remaining = remaining
            best_item = item
    if best_item:
        achievement = best_item["achievement"]
        reward_text = (
            f" (+{format_coins(achievement.reward)} BIT)" if achievement.reward > 0 else ""
        )
        next_achievement_line = (
            f"Следующее достижение: <b>{achievement.title}</b> — "
            f"{best_item['value']}/{achievement.threshold}{reward_text}"
        )
    now = now_ts()
    free_until = int(user.get("commission_free_until") or 0)
    if free_until > now:
        free_text = f"активна, осталось {_format_age_short(free_until - now)}"
    else:
        free_text = "нет"
    divider = "--------------------"
    lines = [
        "👤 <b>Профиль</b>",
        f"ID: <code>{user['id']}</code>",
        divider,
        f"💰 Баланс: <b>{format_coins(user['balance'])}</b> BIT",
        f"🎯 Уровень: <b>{user['level']}</b>",
    ]
    if level_progress:
        lines.append(level_progress)
    lines.extend(
        [
            f"✅ Выполнено: <b>{user['completed_tasks']}</b>",
            f"🏅 Достижения: <b>{achievements_count}/{achievements_total}</b>",
        ]
    )
    if next_achievement_line:
        lines.append(next_achievement_line)
    lines.extend(
        [
            f"⚡️ Комиссия 0%: <b>{free_text}</b>",
            divider,
            f"🤝 Рефералы: <b>{user['referral_count']}</b>",
            f"💸 Доход с реф.: <b>{format_coins(user['referral_earned'])}</b>",
            f"🔗 Рефссылка: {escape(referral_link)}",
        ]
    )
    text = "\n".join(lines)
    await bot.send_message(chat_id, text, reply_markup=reply_markup)


async def send_referrals(bot: Bot, chat_id: int, user_id: int, db: Database) -> None:
    user = await db.get_user(user_id)
    if not user:
        return
    bot_user = await bot.get_me()
    referral_link = f"https://t.me/{bot_user.username}?start=ref_{user['id']}"
    await bot.send_message(
        chat_id,
        "🤝 <b>Реферальная программа</b>\n"
        f"Ваша ссылка:\n{escape(referral_link)}\n\nС каждого выполненого задания от приглашенного вы получаете 15%\n\n"
        f"Всего рефералов: <b>{user['referral_count']}</b>\n"
        f"Заработано: <b>{format_coins(user['referral_earned'])}</b> BIT",
    )


async def ensure_sponsors(bot: Bot, user_id: int, db: Database) -> bool:
    sponsors = await db.list_sponsors()
    if not sponsors:
        return True
    if await check_sponsors(bot, user_id, sponsors):
        return True
    await bot.send_message(
        user_id,
        "Чтобы пользоваться ботом, подпишитесь на спонсоров:",
        reply_markup=sponsor_gate_kb(sponsors),
    )
    return False


async def is_new_user_notify_enabled(db: Database) -> bool:
    value = await db.get_setting(NEW_USER_NOTIFY_SETTING)
    return value == "1"


async def notify_admins_new_user(
    bot: Bot,
    db: Database,
    config: Config,
    user,
    referrer_id: int | None,
) -> None:
    if not config.admin_ids:
        return
    if not await is_new_user_notify_enabled(db):
        return
    username = f"@{escape(user.username)}" if user.username else "-"
    first_name = escape(user.first_name or "-")
    lines = [
        "🆕 <b>Новый пользователь</b>",
        f"ID: <code>{user.id}</code>",
        f"Имя: {first_name}",
        f"Username: {username}",
    ]
    if referrer_id:
        lines.append(f"Реферал: <code>{referrer_id}</code>")
    text = "\n".join(lines)
    for admin_id in config.admin_ids:
        try:
            await bot.send_message(admin_id, text)
        except (TelegramBadRequest, TelegramForbiddenError):
            pass


def is_admin(user_id: int, config: Config) -> bool:
    return user_id in config.admin_ids


@router.callback_query(F.data.startswith("admin:user:tasks:"))
async def admin_user_tasks(callback: CallbackQuery, db: Database, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    user_id = int(callback.data.split(":")[-1])
    tasks = await db.list_owner_tasks(user_id, 20, 0)
    if not tasks:
        await callback.message.answer("У пользователя нет рекламируемых заданий.")
        await callback.answer()
        return
    lines = [f"🧩 Задания пользователя <code>{user_id}</code>:"]
    for task in tasks:
        lines.append(f"ID: <b>{task['id']}</b> | {escape(task['title'])} | Статус: {task['type']}")
    await callback.message.answer("\n".join(lines))
    await callback.answer()


@router.message(CommandStart())
async def start_handler(message: Message, db: Database, config: Config) -> None:
    user_id = message.from_user.id
    existing = await db.get_user(user_id)
    is_new = existing is None
    await db.upsert_user(
        user_id,
        message.from_user.username,
        message.from_user.first_name,
    )
    ref_id = None
    if message.text:
        parts = message.text.split(maxsplit=1)
        if len(parts) == 2:
            ref_id = parse_referral_arg(parts[1])
    referrer_id = None
    if ref_id and ref_id != user_id:
        if await db.get_user(ref_id):
            if is_new:
                referrer_id = ref_id
                await db.set_referrer(user_id, ref_id)
                await db.add_referral_count(ref_id)
                await notify_new_achievements(message.bot, db, ref_id)
            else:
                user = await db.get_user(user_id)
                if user and not user.get("referrer_id"):
                    referrer_id = ref_id
                    await db.set_referrer(user_id, ref_id)
                    await db.add_referral_count(ref_id)
                    await notify_new_achievements(message.bot, db, ref_id)
            await db.authorize_bot_ref_user(ref_id, user_id)
    if is_new:
        await notify_admins_new_user(
            message.bot,
            db,
            config,
            message.from_user,
            referrer_id,
        )
    sponsors = await db.list_sponsors()
    if sponsors:
        ok = await check_sponsors(message.bot, user_id, sponsors)
        if not ok:
            await message.answer(
                "Чтобы пользоваться ботом, подпишитесь на спонсоров:",
                reply_markup=sponsor_gate_kb(sponsors),
            )
            return
    if is_new:
        await send_onboarding(
            message.bot,
            message.from_user.id,
            user_id in config.admin_ids,
        )
    await send_main_menu(
        message.bot,
        message.from_user.id,
        user_id in config.admin_ids,
        db,
        prefix="👋 Добро пожаловать!",
    )


@router.callback_query(F.data == "sponsor:check")
async def sponsor_check(callback: CallbackQuery, db: Database, config: Config) -> None:
    user_id = callback.from_user.id
    sponsors = await db.list_sponsors()
    if not sponsors:
        await callback.message.edit_text("Спонсоры не настроены.")
        await callback.answer()
        return
    ok = await check_sponsors(callback.bot, user_id, sponsors)
    if ok:
        await callback.message.answer(
            "✅ Подписка подтверждена. Доступ открыт.",
            reply_markup=main_menu_kb(user_id in config.admin_ids),
        )
    else:
        await callback.answer("Подпишитесь на всех спонсоров.", show_alert=True)


@router.message(Command("admin"))
async def admin_command(message: Message, config: Config) -> None:
    if message.from_user.id not in config.admin_ids:
        await message.answer("Нет доступа.")
        return
    await message.answer("🛠 <b>Админ-панель</b>", reply_markup=admin_menu_kb())


@router.message(F.text.in_(["Админ", "🛠 Админ"]))
async def admin_menu(message: Message, state: FSMContext, config: Config) -> None:
    await state.clear()
    if message.from_user.id not in config.admin_ids:
        await message.answer("Нет доступа.")
        return
    await message.answer("🛠 <b>Админ-панель</b>", reply_markup=admin_menu_kb())


@router.message(F.text.in_(["Отмена", "❌ Отмена"]))
async def cancel_state(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    await state.clear()
    await send_main_menu(
        message.bot,
        message.from_user.id,
        message.from_user.id in config.admin_ids,
        db,
        prefix="✅ Операция отменена.",
    )


@router.callback_query(F.data == "state:cancel")
async def cancel_state_inline(
    callback: CallbackQuery, state: FSMContext, db: Database, config: Config
) -> None:
    current = await state.get_state()
    await state.clear()
    prefix = "✅ Операция отменена."
    if current and current.startswith(CreateTaskState.__name__):
        prefix = "Создание задания отменено."
    await send_main_menu(
        callback.bot,
        callback.from_user.id,
        callback.from_user.id in config.admin_ids,
        db,
        prefix=prefix,
    )
    await callback.answer()


@router.message(F.text.in_(["Задания", "🧩 Задания"]))
async def tasks_menu(message: Message, state: FSMContext, db: Database) -> None:
    await state.clear()
    if not await ensure_sponsors(message.bot, message.from_user.id, db):
        return
    summary = await db.available_task_summary(message.from_user.id)
    await message.answer(
        build_tasks_menu_text(summary),
        reply_markup=tasks_menu_kb(),
    )


@router.message(F.text.in_(["Помощь", "❓ Помощь"]))
async def help_menu(message: Message, state: FSMContext, db: Database) -> None:
    await state.clear()
    if not await ensure_sponsors(message.bot, message.from_user.id, db):
        return
    await message.answer(HELP_MENU_TEXT, reply_markup=help_menu_kb())


@router.message(F.text.in_(["Профиль", "👤 Профиль"]))
async def profile(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    await state.clear()
    if not await ensure_sponsors(message.bot, message.from_user.id, db):
        return
    await send_profile(
        message.bot,
        message.from_user.id,
        message.from_user.id,
        db,
        reply_markup=profile_menu_kb(),
    )


@router.message(F.text.in_(["Рефералы", "🤝 Рефералы"]))
async def referrals(message: Message, state: FSMContext, db: Database) -> None:
    await state.clear()
    if not await ensure_sponsors(message.bot, message.from_user.id, db):
        return
    await send_referrals(message.bot, message.from_user.id, message.from_user.id, db)


@router.message(F.text.in_(["Промокод", "🎁 Промокод"]))
async def promo_start(message: Message, state: FSMContext, db: Database) -> None:
    await state.clear()
    if not await ensure_sponsors(message.bot, message.from_user.id, db):
        return
    await state.set_state(PromoState.enter_code)
    await message.answer("🎁 Введите промокод:", reply_markup=cancel_kb())


@router.message(F.text.in_(["🧷 ОП", "ОП"]))
async def menu_op_message(message: Message, state: FSMContext, db: Database) -> None:
    await state.clear()
    if not await ensure_sponsors(message.bot, message.from_user.id, db):
        return
    await message.answer(OP_PUBLIC_TEXT, reply_markup=op_menu_kb())


@router.message(F.text.in_(["📊 Статистика", "Статистика"]))
async def public_stats_message(message: Message, state: FSMContext, db: Database) -> None:
    await state.clear()
    if not await ensure_sponsors(message.bot, message.from_user.id, db):
        return
    stats = await db.global_stats(_day_start_ts())
    text = (
        "📊 <b>Статистика AD GRAM</b>\n\n"
        f"👤 Общее количество пользователей: <b>{format_coins(stats['users_total'])}</b>\n"
        f"🆕 Новых сегодня: <b>{format_coins(stats['new_today'])}</b>\n\n"
        f"📣 Подписались на каналы: <b>{format_coins(stats['channel_subs'])}</b>\n"
        f"👥 Подписались на группы: <b>{format_coins(stats['group_subs'])}</b>\n\n"
        f"👁️ Общее количество просмотров: <b>{format_coins(stats['views'])}</b>\n"
        f"🔥 Общее количество реакций: <b>{format_coins(stats['reactions'])}</b>"
    )
    await message.answer(text, reply_markup=menu_back_kb())


@router.message(F.text.in_(["🏅 Достижения", "Достижения"]))
async def achievements_message(message: Message, state: FSMContext, db: Database) -> None:
    await state.clear()
    if not await ensure_sponsors(message.bot, message.from_user.id, db):
        return
    await notify_new_achievements(message.bot, db, message.from_user.id)
    progress = await get_achievement_progress(db, message.from_user.id)
    text, page, total_pages = build_achievements_page(progress, 1)
    await message.answer(text, reply_markup=achievements_kb(page, total_pages))


@router.callback_query(F.data == "ach:noop")
async def achievements_noop(callback: CallbackQuery) -> None:
    await callback.answer()


@router.callback_query(F.data.startswith("ach:page:"))
async def achievements_page(callback: CallbackQuery, db: Database) -> None:
    if not await ensure_sponsors(callback.bot, callback.from_user.id, db):
        await callback.answer()
        return
    try:
        page = int(callback.data.split(":")[2])
    except (ValueError, IndexError):
        await callback.answer()
        return
    progress = await get_achievement_progress(db, callback.from_user.id)
    text, page, total_pages = build_achievements_page(progress, page)
    try:
        await callback.message.edit_text(text, reply_markup=achievements_kb(page, total_pages))
    except TelegramBadRequest:
        await callback.message.answer(text, reply_markup=achievements_kb(page, total_pages))
    await callback.answer()


@router.message(F.text.in_(["📈 Мои задания", "Мои задания"]))
async def menu_ads_message(message: Message, state: FSMContext, db: Database) -> None:
    await state.clear()
    if not await ensure_sponsors(message.bot, message.from_user.id, db):
        return
    await send_advertiser_tasks_page(message, db, page=1)


@router.callback_query(F.data == "menu:profile")
async def menu_profile(callback: CallbackQuery, db: Database, state: FSMContext) -> None:
    await state.clear()
    if not await ensure_sponsors(callback.bot, callback.from_user.id, db):
        await callback.answer()
        return
    await send_profile(
        callback.bot,
        callback.from_user.id,
        callback.from_user.id,
        db,
        reply_markup=profile_menu_kb(),
    )
    await callback.answer()


@router.callback_query(F.data == "menu:referrals")
async def menu_referrals(callback: CallbackQuery, db: Database, state: FSMContext) -> None:
    await state.clear()
    if not await ensure_sponsors(callback.bot, callback.from_user.id, db):
        await callback.answer()
        return
    await send_referrals(callback.bot, callback.from_user.id, callback.from_user.id, db)
    await callback.answer()


@router.callback_query(F.data == "menu:promo")
async def menu_promo(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    await state.clear()
    if not await ensure_sponsors(callback.bot, callback.from_user.id, db):
        await callback.answer()
        return
    await state.set_state(PromoState.enter_code)
    await callback.message.answer("🎁 Введите промокод:", reply_markup=cancel_kb())
    await callback.answer()


@router.callback_query(F.data == "menu:help")
async def menu_help(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    await state.clear()
    if not await ensure_sponsors(callback.bot, callback.from_user.id, db):
        await callback.answer()
        return
    await callback.message.answer(HELP_MENU_TEXT, reply_markup=help_menu_kb())
    await callback.answer()


@router.callback_query(F.data == "menu:faq")
async def menu_faq(callback: CallbackQuery, db: Database) -> None:
    if not await ensure_sponsors(callback.bot, callback.from_user.id, db):
        await callback.answer()
        return
    await callback.message.answer("❓ <b>FAQ</b>", reply_markup=faq_menu_kb())
    await callback.answer()


@router.callback_query(F.data.startswith("faq:"))
async def faq_topic(callback: CallbackQuery, db: Database) -> None:
    if not await ensure_sponsors(callback.bot, callback.from_user.id, db):
        await callback.answer()
        return
    topic = callback.data.split(":", 1)[1]
    text = build_faq_text(topic)
    await callback.message.answer(text, reply_markup=faq_menu_kb())
    await callback.answer()


@router.callback_query(F.data == "menu:tips")
async def menu_tips(callback: CallbackQuery, db: Database) -> None:
    if not await ensure_sponsors(callback.bot, callback.from_user.id, db):
        await callback.answer()
        return
    await callback.message.answer(TIPS_TEXT, reply_markup=help_menu_kb())
    await callback.answer()


@router.callback_query(F.data == "menu:tasks")
async def menu_tasks(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    await state.clear()
    if not await ensure_sponsors(callback.bot, callback.from_user.id, db):
        await callback.answer()
        return
    summary = await db.available_task_summary(callback.from_user.id)
    await callback.message.answer(
        build_tasks_menu_text(summary),
        reply_markup=tasks_menu_kb(),
    )
    await callback.answer()


@router.callback_query(F.data == "menu:home")
async def menu_home(
    callback: CallbackQuery, state: FSMContext, db: Database, config: Config
) -> None:
    await state.clear()
    if not await ensure_sponsors(callback.bot, callback.from_user.id, db):
        await callback.answer()
        return
    await send_main_menu(
        callback.bot,
        callback.from_user.id,
        callback.from_user.id in config.admin_ids,
        db,
    )
    await callback.answer()


@router.callback_query(F.data == "menu:op")
async def menu_op(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    await state.clear()
    if not await ensure_sponsors(callback.bot, callback.from_user.id, db):
        await callback.answer()
        return
    await callback.message.answer(OP_PUBLIC_TEXT, reply_markup=op_menu_kb())
    await callback.answer()


@router.callback_query(F.data == "op:menu")
async def op_menu_back(callback: CallbackQuery, db: Database) -> None:
    if not await ensure_sponsors(callback.bot, callback.from_user.id, db):
        await callback.answer()
        return
    await callback.message.edit_text(OP_PUBLIC_TEXT, reply_markup=op_menu_kb())
    await callback.answer()


@router.callback_query(F.data == "op:public")
async def op_public(callback: CallbackQuery, db: Database) -> None:
    if not await ensure_sponsors(callback.bot, callback.from_user.id, db):
        await callback.answer()
        return
    await callback.message.edit_text(OP_PUBLIC_TEXT, reply_markup=op_back_kb())
    await callback.answer()


@router.callback_query(F.data == "op:private")
async def op_private(callback: CallbackQuery, db: Database) -> None:
    if not await ensure_sponsors(callback.bot, callback.from_user.id, db):
        await callback.answer()
        return
    await callback.message.edit_text(escape(OP_PRIVATE_TEXT), reply_markup=op_back_kb())
    await callback.answer()


@router.callback_query(F.data == "op:invite")
async def op_invite(callback: CallbackQuery, db: Database) -> None:
    if not await ensure_sponsors(callback.bot, callback.from_user.id, db):
        await callback.answer()
        return
    await callback.message.edit_text(escape(OP_INVITE_TEXT), reply_markup=op_back_kb())
    await callback.answer()


@router.callback_query(F.data == "op:ref")
async def op_ref(callback: CallbackQuery, db: Database) -> None:
    if not await ensure_sponsors(callback.bot, callback.from_user.id, db):
        await callback.answer()
        return
    await callback.message.edit_text(OP_REFERRAL_TEXT, reply_markup=op_back_kb())
    await callback.answer()


@router.callback_query(F.data == "op:auto")
async def op_auto(callback: CallbackQuery, db: Database) -> None:
    if not await ensure_sponsors(callback.bot, callback.from_user.id, db):
        await callback.answer()
        return
    await callback.message.edit_text(escape(OP_AUTODELETE_TEXT), reply_markup=op_back_kb())
    await callback.answer()


@router.message(PromoState.enter_code)
async def promo_redeem(message: Message, state: FSMContext, db: Database) -> None:
    code = (message.text or "").strip()
    if not code:
        await message.answer("Промокод не распознан. Введите еще раз.")
        return
    amount = await db.redeem_promo(code, message.from_user.id)
    if amount is None:
        await message.answer("Промокод недействителен или уже использован.")
    else:
        await db.add_balance(message.from_user.id, amount, "promo", {"code": code})
        await message.answer(f"Промокод активирован. Начислено {format_coins(amount)} BIT.")
    await state.clear()


router = Router()
routers.append(router)

OP_MAX_RULES = 5
OP_BOT_LINK_MIN_WAIT = 15


def parse_duration_strict(value: str) -> int | None:
    text = value.strip().lower()
    if not text or len(text) < 2:
        return None
    unit = text[-1]
    if unit not in {"s", "m", "h", "d"}:
        return None
    number = text[:-1]
    if not number.isdigit():
        return None
    multiplier = {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]
    return int(number) * multiplier


def normalize_username(value: str) -> str:
    raw = value.strip()
    if "t.me/" in raw:
        raw = raw.split("t.me/")[-1].strip("/")
    if not raw.startswith("@"):
        raw = f"@{raw}"
    return raw


def normalize_chat_id(value: str) -> int | None:
    text = value.strip()
    if not text:
        return None
    if text.startswith("-100") and text[1:].isdigit():
        return int(text)
    if text.startswith("-") and text[1:].isdigit():
        return int(text)
    if text.isdigit():
        if len(text) >= 10:
            return int(f"-100{text}")
        return int(text)
    return None


async def create_invite_link_safe(bot: Bot, chat_id: int) -> str | None:
    try:
        invite = await bot.create_chat_invite_link(chat_id=chat_id)
    except (TelegramBadRequest, TelegramForbiddenError):
        return None
    return invite.invite_link


def format_rule_title(rule: dict) -> str:
    if rule.get("type") == "bot_ref":
        return f"Реф. ссылка #{rule.get('referrer_id')}"
    if rule.get("type") == "bot_link":
        return f"🤖 Бот"
    if rule.get("target_username"):
        return rule["target_username"]
    if rule.get("target_chat_id"):
        return str(rule["target_chat_id"])
    return "Цель"


def rule_link(rule: dict, bot_username: str | None) -> str | None:
    if rule.get("type") == "bot_ref":
        if not bot_username or not rule.get("referrer_id"):
            return None
        return f"https://t.me/{bot_username}?start=ref_{rule['referrer_id']}"
    if rule.get("type") == "bot_link":
        return rule.get("invite_link")
    if rule.get("invite_link"):
        return rule["invite_link"]
    if rule.get("target_username"):
        return f"https://t.me/{rule['target_username'].lstrip('@')}"
    return None


async def is_chat_admin(bot, chat_id: int, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id, user_id)
    except (TelegramBadRequest, TelegramForbiddenError):
        return False
    return member.status in {"administrator", "creator"}


async def schedule_delete(bot, chat_id: int, message_id: int, delay: int) -> None:
    await asyncio.sleep(delay)
    try:
        await bot.delete_message(chat_id, message_id)
    except (TelegramBadRequest, TelegramForbiddenError):
        return


async def send_with_autodelete(
    bot, db: Database, chat_id: int, text: str, reply_markup: InlineKeyboardMarkup | None = None
) -> None:
    msg = await bot.send_message(chat_id, text, reply_markup=reply_markup)
    delay = await db.get_chat_autodelete(chat_id)
    if delay and delay > 0:
        asyncio.create_task(schedule_delete(bot, chat_id, msg.message_id, delay))


async def cleanup_rule_if_needed(db: Database, rule: dict) -> bool:
    now = now_ts()
    if rule.get("expires_at") and now >= int(rule["expires_at"]):
        await db.deactivate_op_rule(rule["id"])
        return True
    if rule.get("target_count") and int(rule.get("completed_count") or 0) >= int(rule["target_count"]):
        await db.deactivate_op_rule(rule["id"])
        return True
    return False


async def bot_link_wait_remaining(db: Database, rule_id: int, user_id: int) -> int | None:
    row = await db.fetchone(
        "SELECT passed_at FROM op_passes WHERE rule_id = ? AND user_id = ?",
        (rule_id, user_id),
    )
    if not row:
        return None
    passed_at = int(row.get("passed_at") or 0)
    wait_left = OP_BOT_LINK_MIN_WAIT - (now_ts() - passed_at)
    if wait_left > 0:
        return wait_left
    return None


async def check_rule(bot, db: Database, user_id: int, rule: dict) -> bool:
    if rule.get("type") == "bot_ref":
        user = await db.get_user(user_id)
        if user and user.get("referrer_id") == rule.get("referrer_id"):
            return True
        op_pass = await db.fetchone(
            "SELECT 1 FROM op_passes WHERE rule_id = ? AND user_id = ?",
            (rule["id"], user_id),
        )
        return bool(op_pass)
    if rule.get("type") == "bot_link":
        wait_left = await bot_link_wait_remaining(db, rule["id"], user_id)
        if wait_left is not None:
            return False
        op_pass = await db.fetchone(
            "SELECT 1 FROM op_passes WHERE rule_id = ? AND user_id = ?",
            (rule["id"], user_id),
        )
        return bool(op_pass)
    target_chat_id = rule.get("target_chat_id")
    if not target_chat_id:
        return False
    try:
        member = await bot.get_chat_member(int(target_chat_id), user_id)
    except (TelegramBadRequest, TelegramForbiddenError):
        return False
    if getattr(member, "is_member", False):
        return True
    return member.status in {"member", "administrator", "creator", "restricted"}


async def evaluate_rules(
    bot, db: Database, chat_id: int, user_id: int
) -> tuple[list[dict], list[dict]]:
    missing: list[dict] = []
    passed: list[dict] = []
    rules = await db.list_op_rules(chat_id, active_only=True)
    if not rules:
        return missing, passed
    for rule in rules:
        if await cleanup_rule_if_needed(db, rule):
            continue
        ok = await check_rule(bot, db, user_id, rule)
        if ok:
            passed.append(rule)
            if await db.mark_op_pass(rule["id"], user_id):
                target_count = rule.get("target_count")
                if target_count and int(rule.get("completed_count") or 0) + 1 >= int(target_count):
                    await db.deactivate_op_rule(rule["id"])
        else:
            missing.append(rule)
    return missing, passed


async def rule_button_label(bot, rule: dict) -> str:
    if rule.get("type") == "bot_ref":
        return "➕Запустить"
    if rule.get("type") == "bot_link":
        return "🤖 Бот"
    target_chat_id = rule.get("target_chat_id")
    if target_chat_id:
        try:
            chat = await bot.get_chat(int(target_chat_id))
            if chat.type == "channel":
                return "➕Канал"
            return "➕Чат"
        except (TelegramBadRequest, TelegramForbiddenError):
            return "➕Канал"
    return "➕Канал"


async def build_check_kb(
    bot, rules: list[dict], bot_username: str | None, chat_id: int
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for rule in rules:
        label = await rule_button_label(bot, rule)
        if rule.get("type") == "bot_link":
            link = rule_link(rule, bot_username)
            if link:
                builder.button(text=label, callback_data=f"op:visit_bot:{rule['id']}:{chat_id}")
        else:
            link = rule_link(rule, bot_username)
            if link:
                builder.button(text=label, url=link)
    builder.adjust(2)
    builder.row(InlineKeyboardButton(text="✅ Проверить подписку", callback_data=f"op:check:{chat_id}"))
    return builder.as_markup()


@router.message(Command("setup"))
async def op_setup(message: Message, db: Database) -> None:
    if message.chat.type not in {"group", "supergroup"}:
        await message.answer("Эту команду нужно использовать в чате.")
        return
    if not await is_chat_admin(message.bot, message.chat.id, message.from_user.id):
        await message.answer("Команда доступна только администраторам.")
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        await send_with_autodelete(
            message.bot,
            db,
            message.chat.id,
            "Использование:\n"
            "/setup <ID> [1d] - по реф-ссылке\n"
            "/setup https://t.me/botname [1d] - по ссылке бота\n"
            "/setup @username [период] - по каналу/чату",
        )
        return
    if await db.count_active_op_rules(message.chat.id) >= OP_MAX_RULES:
        await send_with_autodelete(
            message.bot,
            db,
            message.chat.id,
            "Достигнут лимит активных проверок (5).",
        )
        return
    
    arg1 = parts[1]
    expires_at = None
    
    if "t.me/+" in arg1 or "joinchat" in arg1:
        await send_with_autodelete(
            message.bot,
            db,
            message.chat.id,
            "Это приватная ссылка. Используйте ID канала/чата — бот сам создаст пригласительную ссылку.",
        )
        return

    if arg1.startswith("https://t.me/"):
        bot_link = arg1
        if len(parts) > 2:
            duration = parse_duration_strict(parts[2])
            if duration:
                expires_at = now_ts() + duration
        rule_id = await db.create_op_rule(
            chat_id=message.chat.id,
            rule_type="bot_link",
            target_chat_id=None,
            target_username=None,
            invite_link=bot_link,
            referrer_id=None,
            expires_at=expires_at,
            target_count=None,
        )
        await send_with_autodelete(
            message.bot,
            db,
            message.chat.id,
            f"✅ Проверка по ссылке бота подключена. ID правила: <code>{rule_id}</code>",
        )
        return

    if arg1.isdigit():
        referrer_id = int(arg1)
        user_exists = await db.get_user(referrer_id)
        if not user_exists:
            await send_with_autodelete(
                message.bot,
                db,
                message.chat.id,
                f"❌ Пользователь с ID <code>{referrer_id}</code> не найден в боте. Пожалуйста, укажите правильный ID.",
            )
            return
        if len(parts) > 2:
            duration = parse_duration_strict(parts[2])
            if duration:
                expires_at = now_ts() + duration
        rule_id = await db.create_op_rule(
            chat_id=message.chat.id,
            rule_type="bot_ref",
            target_chat_id=None,
            target_username=None,
            invite_link=None,
            referrer_id=referrer_id,
            expires_at=expires_at,
            target_count=None,
        )
        await send_with_autodelete(
            message.bot,
            db,
            message.chat.id,
            f"✅ Проверка по рефссылке подключена. ID правила: <code>{rule_id}</code>",
        )
        return

    if arg1.startswith("@") or "t.me/" in arg1:
        username = normalize_username(arg1)
        try:
            chat = await message.bot.get_chat(username)
            bot_user = await message.bot.get_me()
            bot_member = await message.bot.get_chat_member(chat.id, bot_user.id)
        except (TelegramBadRequest, TelegramForbiddenError):
            await send_with_autodelete(
                message.bot,
                db,
                message.chat.id,
                "Бот не имеет доступа к целевому каналу/чату.",
            )
            return
        if bot_member.status not in {"administrator", "creator"}:
            await send_with_autodelete(
                message.bot,
                db,
                message.chat.id,
                "Добавьте бота админом в целевой канал/чат.",
            )
            return
        if len(parts) > 2:
            duration = parse_duration_strict(parts[2])
            if duration:
                expires_at = now_ts() + duration
        invite_link = None
        rule_type = "public"
        if not chat.username:
            invite_link = await create_invite_link_safe(message.bot, chat.id)
            if not invite_link:
                await send_with_autodelete(
                    message.bot,
                    db,
                    message.chat.id,
                    "Не удалось создать пригласительную ссылку. Проверьте права бота на приглашение.",
                )
                return
            rule_type = "invite"
        rule_id = await db.create_op_rule(
            chat_id=message.chat.id,
            rule_type=rule_type,
            target_chat_id=chat.id,
            target_username=chat.username,
            invite_link=invite_link,
            referrer_id=None,
            expires_at=expires_at,
            target_count=None,
        )
        target_label = "канала" if chat.type == "channel" else "чата"
        await send_with_autodelete(
            message.bot,
            db,
            message.chat.id,
            f"✅ Проверка подключена для {target_label}. ID правила: <code>{rule_id}</code>",
        )
        return

    target_chat_id = normalize_chat_id(arg1)
    if not target_chat_id:
        await send_with_autodelete(
            message.bot,
            db,
            message.chat.id,
            "Не удалось распознать ID канала/чата.",
        )
        return
    
    invite_link = None
    arg2 = parts[2] if len(parts) > 2 else None
    arg3 = parts[3] if len(parts) > 3 else None
    target_count = None

    if arg2 and (arg2.startswith("http") or "t.me/" in arg2):
        invite_link = arg2
        if arg3:
            duration = parse_duration_strict(arg3)
            if duration:
                expires_at = now_ts() + duration
            elif arg3.isdigit():
                target_count = int(arg3)
    elif arg2:
        duration = parse_duration_strict(arg2)
        if duration:
            expires_at = now_ts() + duration

    try:
        chat = await message.bot.get_chat(target_chat_id)
        bot_user = await message.bot.get_me()
        bot_member = await message.bot.get_chat_member(chat.id, bot_user.id)
    except (TelegramBadRequest, TelegramForbiddenError):
        await send_with_autodelete(
            message.bot,
            db,
            message.chat.id,
            "Бот не имеет доступа к целевому каналу/чату.",
        )
        return
    if bot_member.status not in {"administrator", "creator"}:
        await send_with_autodelete(
            message.bot,
            db,
            message.chat.id,
            "Добавьте бота админом в целевой канал/чат.",
        )
        return
    if not invite_link:
        invite_link = await create_invite_link_safe(message.bot, chat.id)
        if not invite_link:
            await send_with_autodelete(
                message.bot,
                db,
                message.chat.id,
                "Не удалось создать пригласительную ссылку. Проверьте права бота на приглашение.",
            )
            return
    rule_type = "invite" if invite_link else "private"
    rule_id = await db.create_op_rule(
        chat_id=message.chat.id,
        rule_type=rule_type,
        target_chat_id=chat.id,
        target_username=chat.username,
        invite_link=invite_link,
        referrer_id=None,
        expires_at=expires_at,
        target_count=target_count,
    )
    target_label = "канала" if chat.type == "channel" else "чата"
    await send_with_autodelete(
        message.bot,
        db,
        message.chat.id,
        f"✅ Проверка подключена для {target_label}. ID правила: <code>{rule_id}</code>",
    )


@router.message(Command("unsetup"))
async def op_unsetup(message: Message, db: Database) -> None:
    if message.chat.type not in {"group", "supergroup"}:
        await message.answer("Эту команду нужно использовать в чате.")
        return
    if not await is_chat_admin(message.bot, message.chat.id, message.from_user.id):
        await message.answer("Команда доступна только администраторам.")
        return
    parts = (message.text or "").split()
    
    if len(parts) == 1:
        count = await db.deactivate_all_op_rules(message.chat.id)
        await send_with_autodelete(
            message.bot,
            db,
            message.chat.id,
            f"✅ Отключено проверок: <b>{count}</b>",
        )
        return
    
    arg = parts[1]
    
    if arg.isdigit():
        arg_id = int(arg)
        if arg_id > 100000000:
            target_chat_id = normalize_chat_id(arg)
            if target_chat_id:
                count = await db.deactivate_op_rules_by_target(
                    message.chat.id, target_chat_id, None, None
                )
            else:
                await send_with_autodelete(
                    message.bot,
                    db,
                    message.chat.id,
                    "❌ Не удалось распознать ID.",
                )
                return
        else:
            count = await db.deactivate_op_rules_by_target(
                message.chat.id, None, None, "bot_ref"
            )
    elif arg.startswith("@") or "t.me/" in arg:
        target_username = normalize_username(arg)
        count = await db.deactivate_op_rules_by_target(
            message.chat.id, None, target_username, None
        )
    elif arg == "bot" or arg == "bot_ref":
        count = await db.deactivate_op_rules_by_target(
            message.chat.id, None, None, "bot_ref"
        )
    elif arg == "bot_link":
        count = await db.deactivate_op_rules_by_target(
            message.chat.id, None, None, "bot_link"
        )
    else:
        await send_with_autodelete(
            message.bot,
            db,
            message.chat.id,
            "Использование:\n"
            "/unsetup - отключить всё\n"
            "/unsetup bot - отключить реф-ссылки\n"
            "/unsetup bot_link - отключить ссылки ботов\n"
            "/unsetup @channel - отключить конкретный канал/чат\n"
            "/unsetup ID - отключить конкретный ID",
        )
        return
    
    await send_with_autodelete(
        message.bot,
        db,
        message.chat.id,
        f"✅ Отключено проверок: <b>{count}</b>",
    )


@router.message(F.text.startswith("/unsetup_"))
async def op_unsetup_underscore(message: Message, db: Database) -> None:
    if message.chat.type not in {"group", "supergroup"}:
        return
    if not await is_chat_admin(message.bot, message.chat.id, message.from_user.id):
        return
    raw = message.text.split()[0].replace("/unsetup_", "").strip()
    target_chat_id = normalize_chat_id(raw)
    count = await db.deactivate_op_rules_by_target(
        message.chat.id, target_chat_id, None, None
    )
    await send_with_autodelete(
        message.bot,
        db,
        message.chat.id,
        f"✅ Отключено проверок: <b>{count}</b>",
    )


@router.message(Command("status"))
async def op_status(message: Message, db: Database) -> None:
    if message.chat.type not in {"group", "supergroup"}:
        return
    if not await is_chat_admin(message.bot, message.chat.id, message.from_user.id):
        return
    rules = await db.list_op_rules(message.chat.id, active_only=True)
    if not rules:
        await send_with_autodelete(
            message.bot,
            db,
            message.chat.id,
            "Активных проверок нет.",
        )
        return
    lines = ["📌 <b>Активные проверки</b>"]
    now = now_ts()
    for rule in rules:
        title = format_rule_title(rule)
        parts = [f"#{rule['id']} • {escape(title)}"]
        if rule.get("expires_at"):
            left = max(0, int(rule["expires_at"]) - now)
            parts.append(f"{left} сек")
        if rule.get("target_count"):
            parts.append(
                f"{rule.get('completed_count', 0)}/{rule['target_count']}"
            )
        lines.append(" | ".join(parts))
    builder = InlineKeyboardBuilder()
    for rule in rules:
        builder.button(text=f"❌ Отключить #{rule['id']}", callback_data=f"op:disable:{rule['id']}")
    builder.adjust(1)
    await send_with_autodelete(
        message.bot,
        db,
        message.chat.id,
        "\n".join(lines),
        reply_markup=builder.as_markup(),
    )


@router.message(Command("id"))
async def op_id(message: Message, state: FSMContext) -> None:
    if message.chat.type == "private":
        await state.set_state(GetIdState.waiting_for_chat)
        await message.answer(
            "Выберите канал/чат для получения ID, с помощью кнопок ниже.",
            reply_markup=id_picker_kb(),
        )
        return
    
    if message.chat.type not in {"group", "supergroup"}:
        await message.answer("Команда доступна только в группах.")
        return
    
    parts = (message.text or "").split()
    if len(parts) > 1:
        ref = parts[1]
        if ref.startswith("@") or "t.me/" in ref:
            ref = normalize_username(ref)
        else:
            chat_id = normalize_chat_id(ref)
            if chat_id:
                ref = str(chat_id)
        try:
            chat = await message.bot.get_chat(ref)
        except (TelegramBadRequest, TelegramForbiddenError):
            await message.answer("Не удалось получить ID.")
            return
        await message.answer(
            f"Вот ID который нужно вставить в задание: <code>{chat.id}</code>\n"
            f"Тип: <b>{chat.type}</b>"
        )
        return
    
    await message.answer(
        f"Вот ID который нужно вставить в задание: <code>{message.chat.id}</code>\n"
        f"Тип: <b>{message.chat.type}</b>"
    )


@router.message(GetIdState.waiting_for_chat, F.chat_shared)
async def get_id_shared(message: Message, state: FSMContext) -> None:
    chat_id = message.chat_shared.chat_id
    await state.clear()
    
    await message.answer(
        f"<code>{chat_id}</code>\n\nID готов для копирования!",
        reply_markup=ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[[KeyboardButton(text="Назад")]])
    )


@router.message(GetIdState.waiting_for_chat)
async def get_id_cancel(message: Message, state: FSMContext) -> None:
    if (message.text or "").strip() in {"❌ Отменить", "Отменить"}:
        await state.clear()
        await message.answer(
            "Отменено.",
            reply_markup=ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[[KeyboardButton(text="Меню")]])
        )
        return
    
    await message.answer(
        "Выберите канал/чат через кнопки выше или нажмите «Отменить».",
        reply_markup=id_picker_kb(),
    )


@router.message(Command("autodelete"))
async def op_autodelete(message: Message, db: Database) -> None:
    if message.chat.type not in {"group", "supergroup"}:
        return
    if not await is_chat_admin(message.bot, message.chat.id, message.from_user.id):
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        await send_with_autodelete(
            message.bot,
            db,
            message.chat.id,
            "Использование: /autodelete 30s | /autodelete 2m | /autodelete off",
        )
        return
    value = parts[1].lower()
    if value == "off":
        await db.set_chat_autodelete(message.chat.id, None)
        await send_with_autodelete(
            message.bot,
            db,
            message.chat.id,
            "Автоудаление отключено.",
        )
        return
    seconds = parse_duration_strict(value)
    if not seconds or seconds < 15 or seconds > 300:
        await send_with_autodelete(
            message.bot,
            db,
            message.chat.id,
            "Неверное значение. Допустимо от 15s до 5m.",
        )
        return
    await db.set_chat_autodelete(message.chat.id, seconds)
    await send_with_autodelete(
        message.bot,
        db,
        message.chat.id,
        f"Автоудаление включено: {seconds} сек.",
    )


@router.message(Command("get_autodelete"))
async def op_get_autodelete(message: Message, db: Database) -> None:
    if message.chat.type not in {"group", "supergroup"}:
        return
    seconds = await db.get_chat_autodelete(message.chat.id)
    if not seconds:
        await message.answer("Автоудаление выключено.")
        return
    await message.answer(f"Автоудаление: {seconds} сек.")


@router.callback_query(F.data.startswith("op:disable:"))
async def op_disable_rule(callback: CallbackQuery, db: Database) -> None:
    rule_id = int(callback.data.split(":")[2])
    rule = await db.get_op_rule(rule_id)
    if not rule or rule.get("chat_id") != callback.message.chat.id:
        await callback.answer("Правило не найдено.", show_alert=True)
        return
    if not await is_chat_admin(callback.bot, callback.message.chat.id, callback.from_user.id):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await db.deactivate_op_rule(rule_id)
    await callback.message.edit_text("✅ Правило отключено.")
    await callback.answer()


@router.callback_query(F.data.startswith("op:check:"))
async def op_check(callback: CallbackQuery, db: Database) -> None:
    chat_id = int(callback.data.split(":")[2])
    if callback.message.chat.id != chat_id:
        await callback.answer()
        return
    missing, _ = await evaluate_rules(callback.bot, db, chat_id, callback.from_user.id)
    if missing:
        wait_left = None
        for rule in missing:
            if rule.get("type") == "bot_link":
                wait = await bot_link_wait_remaining(db, rule["id"], callback.from_user.id)
                if wait:
                    wait_left = wait if wait_left is None else min(wait_left, wait)
        if wait_left:
            await callback.answer(
                f"Перейдите по ссылке на бота и подождите {wait_left} сек.",
                show_alert=True,
            )
            return
        await callback.answer("Подпишитесь на все каналы и попробуйте снова.", show_alert=True)
        return
    await callback.message.edit_text("✅ Подписка подтверждена. Доступ открыт.")
    await callback.answer()


@router.callback_query(F.data.startswith("op:visit_bot:"))
async def op_visit_bot(callback: CallbackQuery, db: Database) -> None:
    parts = callback.data.split(":")
    rule_id = int(parts[2])
    chat_id = int(parts[3])
    if callback.message.chat.id != chat_id:
        await callback.answer()
        return
    rule = await db.fetchone("SELECT * FROM op_rules WHERE id = ?", (rule_id,))
    if not rule or rule.get("type") != "bot_link":
        await callback.answer("Правило не найдено.")
        return
    link = rule.get("invite_link")
    if not link:
        await callback.answer("Ссылка на бота не найдена.")
        return
    await db.mark_op_pass(rule_id, callback.from_user.id)
    await callback.answer("Переходите по ссылке 👇")
    await callback.message.edit_text(
        "✅ Нажимайте на кнопку ниже для перехода на бота.\n"
        f"После перехода подождите {OP_BOT_LINK_MIN_WAIT} сек и нажмите «Проверить».",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="🤖 Перейти на бота", url=link)]]
        ),
    )


@router.message(F.chat.type.in_({"group", "supergroup"}))
async def op_enforce(message: Message, db: Database) -> None:
    if message.sender_chat and message.sender_chat.type == "channel":
        return
    if not message.from_user or message.from_user.is_bot:
        return
    if await is_chat_admin(message.bot, message.chat.id, message.from_user.id):
        return
    missing, _ = await evaluate_rules(message.bot, db, message.chat.id, message.from_user.id)
    if not missing:
        return
    try:
        await message.bot.delete_message(message.chat.id, message.message_id)
    except (TelegramBadRequest, TelegramForbiddenError):
        pass
    bot_user = await message.bot.get_me()
    kb = await build_check_kb(message.bot, missing, bot_user.username, message.chat.id)
    text = (
        "🔒 Чтобы писать в этом чате, подпишитесь на обязательные каналы/чаты.\n"
        "После подписки нажмите кнопку «Проверить»."
    )
    await send_with_autodelete(message.bot, db, message.chat.id, text, reply_markup=kb)


router = Router()
routers.append(router)

TASK_TITLES = {
    "subscribe": "Подписка",
    "view": "Просмотр поста",
    "reaction": "Реакция",
    "boost": "Буст",
}


USERNAME_RE = re.compile(r"(?:https?://)?(?:t\.me|telegram\.me)/([A-Za-z0-9_]{5,32})", re.I)
BOOST_RE = re.compile(
    r"(?:https?://)?(?:t\.me|telegram\.me)/boost/([A-Za-z0-9_]{5,32})",
    re.I,
)
AT_RE = re.compile(r"@([A-Za-z0-9_]{5,32})")
PLAIN_RE = re.compile(r"^[A-Za-z0-9_]{5,32}$")
REWARD_PAGE_SIZE = 10
REWARD_TARGET_PAGE = 3
REWARD_TARGET_OFFSET = REWARD_PAGE_SIZE * REWARD_TARGET_PAGE - 1
APPEALS_PAGE_SIZE = 10

MIN_REWARDS = {
    "channel": 600,
    "group": 400,
    "reaction": 100,
}


def extract_username(text: str) -> str | None:
    text = text.strip()
    if not text:
        return None
    match = BOOST_RE.search(text)
    if match:
        return f"@{match.group(1)}"
    match = AT_RE.search(text)
    if match:
        return f"@{match.group(1)}"
    match = USERNAME_RE.search(text)
    if match:
        return f"@{match.group(1)}"
    if PLAIN_RE.match(text):
        return f"@{text}"
    return None


def normalize_chat_type(chat_type: str | None) -> str | None:
    if chat_type in {"group", "supergroup"}:
        return "group"
    if chat_type == "channel":
        return "channel"
    return chat_type


def is_bot_username(username: str) -> bool:
    return username.lower().endswith("bot")


def chat_filter_for(task_type: str | None, chat_type: str | None) -> str | None:
    if task_type != "subscribe":
        return None
    if chat_type == "channel":
        return "channel"
    return "group"


def min_reward_for(task_type: str | None, chat_type: str | None, config: Config) -> int:
    if task_type == "subscribe":
        if chat_type == "channel":
            return MIN_REWARDS["channel"]
        return MIN_REWARDS["group"]
    if task_type == "reaction":
        return MIN_REWARDS["reaction"]
    if task_type == "view":
        return config.min_reward_view
    if task_type == "boost":
        return config.min_reward_subscribe
    return 0


async def build_reward_prompt(
    db: Database, state: FSMContext, config: Config, user_id: int | None = None
) -> str:
    data = await state.get_data()
    task_type = data.get("task_type")
    chat_type = data.get("chat_type")
    if not task_type:
        return "Введите награду в BIT:"
    min_reward = min_reward_for(task_type, chat_type, config)
    lines = [
        "Введите награду в BIT:",
        f"💡 Минимальная сумма для этой категории: <b>{format_coins(min_reward)}</b>.",
        f"💸 Комиссия сервиса <b>{config.commission_percent}%</b> от суммы задания.",
        "💡 Пополнение баланса: каждые 1$ отключают комиссию на 24 часа.",
    ]
    if user_id is not None:
        user = await db.get_user(user_id)
        free_until = int(user.get("commission_free_until") or 0) if user else 0
        if free_until > now_ts():
            until_text = datetime.fromtimestamp(free_until).strftime("%d.%m.%Y %H:%M")
            lines.append(f"✅ Комиссия отключена до <b>{until_text}</b>.")
    return "\n".join(lines)
def calculate_commission_fee(total_cost: int, user: dict | None, config: Config) -> int:
    if total_cost <= 0 or not user:
        return 0
    free_until = int(user.get("commission_free_until") or 0)
    if free_until > now_ts():
        return 0
    return math.ceil(total_cost * (config.commission_percent / 100))


def calculate_total_charge(
    quantity: int, reward: int, user: dict | None, config: Config
) -> tuple[int, int]:
    total_cost = quantity * reward
    commission_fee = calculate_commission_fee(total_cost, user, config)
    return total_cost + commission_fee, commission_fee


def max_quantity_for_balance(
    balance: int, reward: int, config: Config, user: dict | None = None
) -> int:
    if reward <= 0 or balance <= 0:
        return 0
    free_until = int(user.get("commission_free_until") or 0) if user else 0
    commission_percent = 0 if free_until > now_ts() else config.commission_percent
    upper_bound = balance // reward
    if upper_bound <= 0:
        return 0

    def charge(count: int) -> int:
        cost = count * reward
        fee = 0
        if commission_percent > 0:
            fee = math.ceil(cost * (commission_percent / 100))
        return cost + fee

    low, high = 0, upper_bound
    while low < high:
        mid = (low + high + 1) // 2
        if charge(mid) <= balance:
            low = mid
        else:
            high = mid - 1
    return low


def task_type_menu() -> InlineKeyboardBuilder:
    builder = InlineKeyboardBuilder()
    builder.button(text="📢 Канал", callback_data="create:type:channel")
    builder.button(text="👥 Группа", callback_data="create:type:group")
    builder.button(text="👀 Пост", callback_data="create:type:view")
    builder.button(text="👍 Реакция", callback_data="create:type:reaction")
    builder.button(text="⚡️ Буст", callback_data="create:type:boost")
    builder.button(text="⬅️ Меню", callback_data="menu:home")
    builder.adjust(2, 2, 1, 1)
    return builder


def resolve_create_owner_id(actor_id: int, state_data: dict, config: Config) -> int:
    if actor_id not in config.admin_ids:
        return actor_id
    raw_owner_id = state_data.get("create_owner_id")
    try:
        return int(raw_owner_id) if raw_owner_id is not None else actor_id
    except (TypeError, ValueError):
        return actor_id


async def start_create_flow(
    bot: Bot,
    user_id: int,
    state: FSMContext,
    db: Database,
    owner_id: int | None = None,
) -> None:
    if not await ensure_sponsors(bot, user_id, db):
        return
    await state.clear()
    effective_owner_id = owner_id if owner_id is not None else user_id
    await state.update_data(create_owner_id=effective_owner_id)
    await state.set_state(CreateTaskState.choose_type)
    user = await db.get_user(effective_owner_id)
    balance = int(user["balance"]) if user else 0
    owner_hint = ""
    if effective_owner_id != user_id:
        owner_hint = f"\n👤 Владелец задания: <code>{effective_owner_id}</code>"
    await bot.send_message(
        user_id,
        "📢 Что вы хотите рекламировать?\n"
        f"💳 Баланс: {format_coins(balance)} BIT"
        f"{owner_hint}",
        reply_markup=task_type_menu().as_markup(),
    )


@router.message(F.text.in_(["Рекламировать", "📢 Рекламировать"]))
async def create_start(message: Message, state: FSMContext, db: Database) -> None:
    await state.clear()
    await start_create_flow(message.bot, message.from_user.id, state, db)


@router.callback_query(F.data == "menu:create")
async def create_start_cb(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    await start_create_flow(callback.bot, callback.from_user.id, state, db)
    await callback.answer()


@router.callback_query(F.data.startswith("create:type:"))
async def create_choose_type(callback: CallbackQuery, state: FSMContext) -> None:
    task_type = callback.data.split(":")[2]
    if task_type == "bot":
        await callback.answer("Тип задания недоступен.", show_alert=True)
        return
    if task_type in {"channel", "group"}:
        await state.update_data(
            task_type="subscribe",
            subscribe_kind=task_type,
            chat_type=task_type,
        )
        await state.set_state(CreateTaskState.channel)
        kind_label = "канал" if task_type == "channel" else "чат"
        prompt = (
            f"Нажмите на кнопку, чтобы выбрать {kind_label}, который хотите продвигать.\n\n"
            "⚠️ У бота должно быть право «Приглашение пользователей».\n\n"
            "Вы можете выбрать:\n"
            f"- 🏠 Свой {kind_label}, где вы администратор: бот добавится автоматически с правом «Приглашение пользователей».\n"
            f"- 🌐 Другой {kind_label}, где вы не администратор: попросите администратора добавить бота и выдать право «Приглашение пользователей»."
        )
        await callback.message.answer(
            prompt,
            reply_markup=chat_picker_kb(task_type),
        )
        await callback.answer()
        return
    await state.update_data(task_type=task_type)
    if task_type in {"subscribe", "boost"}:
        await state.set_state(CreateTaskState.channel)
        if task_type == "boost":
            prompt = "⚡️ Отправьте @username канала для буста."
        else:
            prompt = (
                "📣 Отправьте @username или ссылку на канал/группу.\n"
                "У бота должно быть право «Приглашение пользователей»."
            )
        reply = add_bot_admin_cancel_kb("group") if task_type == "subscribe" else cancel_kb()
        await callback.message.answer(prompt, reply_markup=reply)
    else:
        await state.set_state(CreateTaskState.post)
        await callback.message.answer(
            "📨 Перешлите пост из канала/группы, который нужно рекламировать.",
            reply_markup=cancel_kb(),
        )
    await callback.answer()


@router.message(CreateTaskState.channel, ~F.chat_shared)
async def create_channel(
    message: Message, state: FSMContext, bot: Bot, db: Database, config: Config
) -> None:
    data = await state.get_data()
    owner_id = resolve_create_owner_id(message.from_user.id, data, config)
    task_type = data.get("task_type")
    subscribe_kind = data.get("subscribe_kind")
    if task_type == "bot":
        await state.clear()
        await message.answer(
            "Тип задания недоступен.",
            reply_markup=main_menu_kb(message.from_user.id in config.admin_ids),
        )
        return
    if (message.text or "").strip() in {"❌ Отмена", "Отмена"}:
        await state.clear()
        await message.answer(
            "Создание задания отменено.",
            reply_markup=main_menu_kb(message.from_user.id in config.admin_ids),
        )
        return
    chat_ref = (message.text or "").strip()
    if "t.me/+" in chat_ref or "joinchat" in chat_ref:
        await message.answer(
            "Это приватная ссылка. Укажите ID чата — бот сам создаст пригласительную ссылку.\n"
            "ID можно получить через команду /id."
        )
        return
    username = extract_username(chat_ref)
    if not chat_ref:
        await message.answer("Укажите @username или ID чата.")
        return
    if username:
        chat_ref = username
    if username and task_type in {"subscribe", "boost"}:
        if is_bot_username(username.lstrip("@")):
            await message.answer("Это бот. Нужен канал или группа.")
            return
    try:
        chat = await bot.get_chat(chat_ref)
        bot_member = None
        if task_type == "subscribe":
            bot_user = await bot.get_me()
            bot_member = await bot.get_chat_member(chat.id, bot_user.id)
    except (TelegramBadRequest, TelegramForbiddenError):
        await message.answer(
            add_bot_admin_help_text(subscribe_kind, missing_access=True),
            reply_markup=add_bot_admin_cancel_kb(subscribe_kind),
        )
        return
    if task_type == "subscribe":
        if chat.type not in {"channel", "group", "supergroup"}:
            await message.answer("Нужен канал или группа, не пользователь/бот.")
            return
        if subscribe_kind == "channel" and chat.type != "channel":
            await message.answer("Нужен канал. Выберите канал из списка.")
            return
        if subscribe_kind == "group" and chat.type not in {"group", "supergroup"}:
            await message.answer("Нужен чат (группа). Выберите чат из списка.")
            return
        if bot_member.status not in {"administrator", "creator"}:
            await message.answer(
                add_bot_admin_help_text(subscribe_kind, missing_access=False),
                reply_markup=add_bot_admin_cancel_kb(subscribe_kind),
            )
            return
        if bot_member.status == "administrator" and not getattr(bot_member, "can_invite_users", False):
            await message.answer(
                "❌ У бота нет права «Приглашение пользователей».\n\n"
                "Выдайте боту это право и попробуйте снова.",
                reply_markup=add_bot_admin_cancel_kb(subscribe_kind),
            )
            return
        existing_task = await db.get_owner_subscribe_task(owner_id, chat.id)
        if existing_task:
            await message.answer(
                "Этот канал/чат вы уже рекламировали.\n"
                "Откройте «Мои задания» и используйте кнопку «Пополнить».\n"
                f"ID задания: <code>{existing_task['id']}</code>"
            )
            return
    invite_link = None
    if task_type == "subscribe" and not chat.username:
        invite_link = await create_invite_link_safe(bot, chat.id)
        if not invite_link:
            await message.answer(
                "Не удалось создать пригласительную ссылку. "
                "Проверьте права бота на приглашение пользователей."
            )
            return
    if task_type == "boost":
        # Если пользователь ввёл ссылку вида t.me/boost/имя_канала
        boost_match = re.search(r"(?:https?://)?(?:t\.me|telegram\.me)/boost/([A-Za-z0-9_]{5,32})", chat_ref, re.I)
        if boost_match:
            username = boost_match.group(1)
            chat_ref = f"@{username}"  # преобразуем в @username для дальнейшего получения chat

        # Теперь получаем chat (как обычно)
        try:
            chat = await bot.get_chat(chat_ref)
        except (TelegramBadRequest, TelegramForbiddenError):
            await message.answer("Канал не найден или бот не имеет доступа.")
            return

        # Проверки
        if chat.type != "channel":
            await message.answer("Для бустов нужен канал.")
            return
        if not chat.username:
            await message.answer("Канал должен быть публичным с @username.")
            return

    # ... остальной код (сохранение данных)



    await state.update_data(
        chat_id=chat.id,
        chat_type=normalize_chat_type(chat.type),
        chat_username=chat.username,
        chat_title=chat.title or chat.username,
        title=f"{TASK_TITLES.get(task_type, 'Задание')}: {chat.title or chat.username or 'Канал'}",
        action_link=invite_link,
    )
    await state.set_state(CreateTaskState.reward)
    await message.answer(
        await build_reward_prompt(db, state, config, owner_id),
        reply_markup=cancel_kb(),
    )


@router.message(CreateTaskState.channel, F.chat_shared)
async def create_channel_shared(
    message: Message, state: FSMContext, bot: Bot, db: Database, config: Config
) -> None:
    data = await state.get_data()
    owner_id = resolve_create_owner_id(message.from_user.id, data, config)
    task_type = data.get("task_type")
    subscribe_kind = data.get("subscribe_kind")
    if task_type != "subscribe":
        await message.answer("Отправьте ссылку или @username.")
        return
    request_id = message.chat_shared.request_id
    is_other_chat_pick = request_id in OTHER_CHAT_REQUEST_IDS
    chat_id = message.chat_shared.chat_id
    try:
        chat = await bot.get_chat(chat_id)
        bot_user = await bot.get_me()
        bot_member = await bot.get_chat_member(chat.id, bot_user.id)
    except (TelegramBadRequest, TelegramForbiddenError):
        await message.answer(
            add_bot_admin_help_text(
                subscribe_kind,
                missing_access=not is_other_chat_pick,
            ),
            reply_markup=add_bot_admin_cancel_kb(subscribe_kind),
        )
        return
    if chat.type not in {"channel", "group", "supergroup"}:
        await message.answer("Нужен канал или группа, не пользователь/бот.")
        return
    if subscribe_kind == "channel" and chat.type != "channel":
        await message.answer("Нужен канал. Выберите канал из списка.")
        return
    if subscribe_kind == "group" and chat.type not in {"group", "supergroup"}:
        await message.answer("Нужен чат (группа). Выберите чат из списка.")
        return
    if bot_member.status not in {"administrator", "creator"}:
        await message.answer(
            add_bot_admin_help_text(subscribe_kind, missing_access=False),
            reply_markup=add_bot_admin_cancel_kb(subscribe_kind),
        )
        return
    if bot_member.status == "administrator" and not getattr(bot_member, "can_invite_users", False):
        await message.answer(
            "❌ У бота нет права «Приглашение пользователей».\n\n"
            "Выдайте боту это право и попробуйте снова.",
            reply_markup=add_bot_admin_cancel_kb(subscribe_kind),
        )
        return
    invite_link = None
    if not chat.username:
        invite_link = await create_invite_link_safe(bot, chat.id)
        if not invite_link:
            await message.answer(
                "Не удалось создать пригласительную ссылку. "
                "Проверьте права бота на приглашение пользователей."
            )
            return
    await state.update_data(
        chat_id=chat.id,
        chat_type=normalize_chat_type(chat.type),
        chat_username=chat.username,
        chat_title=chat.title or chat.username,
        title=f"{TASK_TITLES.get('subscribe', 'Задание')}: {chat.title or chat.username or 'Канал'}",
        action_link=invite_link,
    )
    await state.set_state(CreateTaskState.reward)
    await message.answer(
        await build_reward_prompt(db, state, config, owner_id),
        reply_markup=cancel_kb(),
    )


@router.message(CreateTaskState.post)
async def create_post(
    message: Message, state: FSMContext, db: Database, config: Config
) -> None:
    if not message.forward_from_chat or not message.forward_from_message_id:
        await message.answer(
            "Пост должен быть переслан из канала/группы.\n"
            "Перешлите сообщение заново.",
        )
        return
    data = await state.get_data()
    task_type = data.get("task_type")
    title_prefix = TASK_TITLES.get(task_type, "Задание")
    chat_title = message.forward_from_chat.title or message.forward_from_chat.username or "Пост"
    await state.update_data(
        source_chat_id=message.forward_from_chat.id,
        source_message_id=message.forward_from_message_id,
        chat_id=message.forward_from_chat.id,
        chat_type=normalize_chat_type(message.forward_from_chat.type),
        chat_username=message.forward_from_chat.username,
        chat_title=message.forward_from_chat.title or message.forward_from_chat.username,
        title=f"{title_prefix}: {chat_title}",
    )
    data = await state.get_data()
    owner_id = resolve_create_owner_id(message.from_user.id, data, config)
    if data.get("task_type") == "reaction":
        await state.set_state(CreateTaskState.reaction)
        await message.answer("Выберите реакцию:", reply_markup=reaction_choices_kb())
        return
    await state.set_state(CreateTaskState.reward)
    await message.answer(
        await build_reward_prompt(db, state, config, owner_id),
        reply_markup=cancel_kb(),
    )


@router.callback_query(F.data.startswith("reaction:"))
async def create_reaction(
    callback: CallbackQuery, state: FSMContext, db: Database, config: Config
) -> None:
    reaction = callback.data.split(":", 1)[1]
    await state.update_data(required_reaction=reaction)
    data = await state.get_data()
    owner_id = resolve_create_owner_id(callback.from_user.id, data, config)
    await state.set_state(CreateTaskState.reward)
    await callback.message.answer(
        await build_reward_prompt(db, state, config, owner_id),
        reply_markup=cancel_kb(),
    )
    await callback.answer()


@router.message(CreateTaskState.reward)
async def create_reward(
    message: Message, state: FSMContext, db: Database, config: Config
) -> None:
    try:
        reward = int((message.text or "").strip())
    except ValueError:
        await message.answer("Введите число.")
        return
    data = await state.get_data()
    owner_id = resolve_create_owner_id(message.from_user.id, data, config)
    task_type = data.get("task_type")
    min_reward = min_reward_for(task_type, data.get("chat_type"), config)
    if reward < min_reward:
        await message.answer(f"Минимальная награда: {format_coins(min_reward)} BIT.")
        return
    await state.update_data(reward=reward)
    await state.set_state(CreateTaskState.quantity)
    user = await db.get_user(owner_id)
    balance = int(user["balance"]) if user else 0
    max_quantity = max_quantity_for_balance(balance, reward, config, user)
    await state.update_data(max_quantity=max_quantity)
    owner_hint = ""
    if owner_id != message.from_user.id:
        owner_hint = f"\nВладелец: <code>{owner_id}</code>"
    await message.answer(
        "Введите количество выполнений:\n"
        f"Стоимость одного выполнения: <b>{format_coins(reward)}</b> BIT\n"
        f"Баланс: <b>{format_coins(balance)}</b> BIT"
        f"{owner_hint}",
        reply_markup=quantity_kb(max_quantity),
    )


@router.callback_query(F.data == "create:qty:max")
async def create_quantity_max(
    callback: CallbackQuery, state: FSMContext, db: Database, config: Config
) -> None:
    data = await state.get_data()
    owner_id = resolve_create_owner_id(callback.from_user.id, data, config)
    max_quantity = int(data.get("max_quantity") or 0)
    if max_quantity <= 0:
        await callback.answer("Недостаточно данных.", show_alert=True)
        return
    user = await db.get_user(owner_id)
    await handle_quantity_input(
        max_quantity,
        owner_id,
        state,
        db,
        config,
        callback.message,
        callback.from_user.id in config.admin_ids,
        user=user,
    )
    await callback.answer()


async def handle_quantity_input(
    quantity: int,
    user_id: int,
    state: FSMContext,
    db: Database,
    config: Config,
    message: Message,
    is_admin: bool,
    user: dict | None = None,
) -> None:
    if quantity <= 0:
        await message.answer("Количество должно быть больше нуля.")
        return
    data = await state.get_data()
    reward = data.get("reward")
    if reward is None:
        await state.clear()
        await message.answer(
            "Сессия создания задания устарела. Начните заново.",
            reply_markup=main_menu_kb(is_admin),
        )
        return
    reward_value = int(reward)
    user_data = user or await db.get_user(user_id)
    total_charge, commission_fee = calculate_total_charge(
        quantity, reward_value, user_data, config
    )
    if not user_data or user_data["balance"] < total_charge:
        await message.answer(
            "Недостаточно средств на балансе для создания задания.",
            reply_markup=main_menu_kb(is_admin),
        )
        await state.clear()
        return
    await state.update_data(quantity=quantity)
    text = (
        "Проверьте данные:\n"
        f"Тип: <b>{TASK_TITLES.get(data.get('task_type'))}</b>\n"
        f"Реклама: <b>{escape(data.get('chat_title') or 'Пост')}</b>\n"
        f"Награда: <b>{format_coins(reward_value)}</b> BIT\n"
        f"Количество: <b>{quantity}</b>\n"
    )
    if commission_fee:
        text += f"Комиссия: <b>{format_coins(commission_fee)}</b> BIT\n"
    text += f"Итого к списанию: <b>{format_coins(total_charge)}</b> BIT"
    await state.set_state(CreateTaskState.confirm)
    await message.answer(text, reply_markup=create_confirm_kb())


@router.message(CreateTaskState.quantity)
async def create_quantity(
    message: Message, state: FSMContext, db: Database, config: Config
) -> None:
    try:
        quantity = int((message.text or "").strip())
    except ValueError:
        await message.answer("Введите число.")
        return
    data = await state.get_data()
    owner_id = resolve_create_owner_id(message.from_user.id, data, config)
    await handle_quantity_input(
        quantity,
        owner_id,
        state,
        db,
        config,
        message,
        message.from_user.id in config.admin_ids,
    )


@router.callback_query(F.data == "create:cancel")
async def create_cancel(callback: CallbackQuery, state: FSMContext, config: Config) -> None:
    await state.clear()
    await callback.message.answer(
        "Создание задания отменено.",
        reply_markup=main_menu_kb(callback.from_user.id in config.admin_ids),
    )
    await callback.answer()


@router.callback_query(F.data == "create:confirm")
async def create_confirm(
    callback: CallbackQuery, state: FSMContext, db: Database, config: Config
) -> None:
    data = await state.get_data()
    owner_id = resolve_create_owner_id(callback.from_user.id, data, config)
    required = ("task_type", "title", "reward", "quantity")
    if any(key not in data for key in required):
        await state.clear()
        await callback.message.answer(
            "Сессия создания задания устарела. Начните заново.",
            reply_markup=main_menu_kb(callback.from_user.id in config.admin_ids),
        )
        await callback.answer()
        return
    
    # Проверяем на дубликат задания
    duplicate = await db.check_task_duplicate(
    task_type=data.get("task_type"),
    chat_id=data.get("chat_id"),
    chat_username=data.get("chat_username"),
    source_message_id=data.get("source_message_id"),
    owner_id=owner_id,
    )
    if duplicate:
        builder = InlineKeyboardBuilder()
        if owner_id == callback.from_user.id:
            builder.button(text="✏️ Редактировать", callback_data=f"adv:task:{duplicate['id']}:1")
            builder.button(text="⬅️ Назад", callback_data="menu:ads")
        else:
            builder.button(text="✏️ Открыть в админке", callback_data=f"admin:task:edit:{duplicate['id']}")
            builder.button(text="⬅️ Админ", callback_data="menu:admin")
        builder.adjust(2)
        await callback.message.answer(
            f"⚠️ На этом канале/группе/посте уже существует задание!\n\n"
            f"ID существующего задания: <code>{duplicate['id']}</code>\n"
            f"Название: {escape(duplicate['title'])}\n\n"
            f"Вы не можете создавать несколько заданий на одном месте.",
            reply_markup=builder.as_markup(),
        )
        await state.clear()
        await callback.answer()
        return
    
    total_cost = int(data["reward"]) * int(data["quantity"])
    commission_fee = 0
    user = await db.get_user(owner_id)
    free_until = int(user.get("commission_free_until") or 0) if user else 0
    if free_until <= now_ts():
        commission_fee = math.ceil(total_cost * (config.commission_percent / 100))
    task_id = await db.create_task(
        owner_id=owner_id,
        task_type=data["task_type"],
        title=data["title"],
        description=None,
        reward=data["reward"],
        total_count=data["quantity"],
        required_reaction=data.get("required_reaction"),
        source_chat_id=data.get("source_chat_id"),
        source_message_id=data.get("source_message_id"),
        chat_id=data.get("chat_id"),
        chat_type=data.get("chat_type"),
        chat_username=data.get("chat_username"),
        chat_title=data.get("chat_title"),
        action_link=data.get("action_link"),
        commission_fee=commission_fee,
    )
    if not task_id:
        await callback.message.answer("Недостаточно средств для создания задания.")
        await state.clear()
        await callback.answer()
        return
    await notify_new_achievements(callback.bot, db, owner_id)
    await state.clear()
    owner_hint = ""
    if owner_id != callback.from_user.id:
        owner_hint = f"\nВладелец: <code>{owner_id}</code>"
    await callback.message.answer(
        f"Задание создано. ID: <code>{task_id}</code>{owner_hint}",
        reply_markup=main_menu_kb(callback.from_user.id in config.admin_ids),
    )
    await callback.answer()


router = Router()
routers.append(router)

TASKS_PER_PAGE = 10
TASK_TYPE_LABELS = {
    "subscribe": "Подписка",
    "channel": "Каналы",
    "group": "Группы",
    "view": "Просмотр",
    "reaction": "Реакция",
    "bot": "Задание",
    "boost": "Буст",
}

TASK_ACTION_TEXT = {
    "reaction": "👍 Поставьте реакцию на пост и отправьте скриншот в этот чат.",
    "boost": "⚡️ Поставьте буст каналу и отправьте скриншот.",
}

TAKE_LIMIT = 8
TAKE_WINDOW = 60
TAKE_BLOCK = 180
PROOF_LIMIT = 4
PROOF_WINDOW = 60
PROOF_BLOCK = 300


def _short_title(title: str, limit: int = 32) -> str:
    if len(title) <= limit:
        return title
    return title[: limit - 1] + "…"


def tasks_list_kb(
    tasks: list[dict], task_type: str, page: int, total_pages: int, is_admin: bool = False
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for task in tasks:
        title = _short_title(task["title"])
        if is_admin:
            text = f"#{task['id']} • {format_coins(task['reward'])} • {title}"
        else:
            text = f"{format_coins(task['reward'])} • {title}"
        builder.button(
            text=text,
            callback_data=f"task:pick:{task['id']}",
        )
    builder.adjust(1)
    nav_buttons: list[InlineKeyboardButton] = []
    if page > 1:
        nav_buttons.append(
            InlineKeyboardButton(
                text="◀️", callback_data=f"tasks:page:{task_type}:{page - 1}"
            )
        )
    if total_pages > 1:
        nav_buttons.append(
            InlineKeyboardButton(text=f"{page}/{total_pages}", callback_data="tasks:noop")
        )
    if page < total_pages:
        nav_buttons.append(
            InlineKeyboardButton(
                text="▶️", callback_data=f"tasks:page:{task_type}:{page + 1}"
            )
        )
    if nav_buttons:
        builder.row(*nav_buttons)
    builder.row(InlineKeyboardButton(text="⬅️ Типы", callback_data="menu:tasks"))
    return builder.as_markup()


ACHIEVEMENTS_PER_PAGE = 10


def achievements_kb(page: int, total_pages: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    nav: list[InlineKeyboardButton] = []
    if page > 1:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"ach:page:{page - 1}"))
    nav.append(InlineKeyboardButton(text=f"{page}/{total_pages}", callback_data="ach:noop"))
    if page < total_pages:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"ach:page:{page + 1}"))
    if nav:
        builder.row(*nav)
    builder.button(text="⬅️ Меню", callback_data="menu:home")
    return builder.as_markup()


def build_achievements_page(progress: list[dict], page: int) -> tuple[str, int, int]:
    total = len(progress)
    unlocked_total = sum(1 for item in progress if item["unlocked"])
    total_pages = max(1, (total + ACHIEVEMENTS_PER_PAGE - 1) // ACHIEVEMENTS_PER_PAGE)
    page = max(1, min(page, total_pages))
    start = (page - 1) * ACHIEVEMENTS_PER_PAGE
    items = progress[start:start + ACHIEVEMENTS_PER_PAGE]
    lines = [f"🏅 <b>Достижения</b> • <b>{unlocked_total}/{total}</b> • BIT"]
    for item in items:
        achievement = item["achievement"]
        unlocked = item["unlocked"]
        value = item["value"]
        reward_text = f"+{format_coins(achievement.reward)}" if achievement.reward > 0 else "-"
        if unlocked:
            lines.append(f"✅ {achievement.title} • {reward_text}")
            lines.append(achievement.description)
        else:
            lines.append(f"🔒 {achievement.title} • {reward_text}")
            lines.append(f"{achievement.description} • {value}/{achievement.threshold}")
        lines.append("")
    if lines and lines[-1] == "":
        lines.pop()
    return "\n".join(lines), page, total_pages


def view_next_kb() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="➡️ Дальше", callback_data="view:next")
    return builder.as_markup()


def reaction_post_kb(url: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="🔗 Открыть пост", url=url)
    return builder.as_markup()


def reaction_post_appeal_kb(url: str, claim_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="🔗 Открыть пост", url=url)
    builder.button(text="📝 Подать апелляцию", callback_data=f"appeal:task:{claim_id}")
    builder.adjust(1)
    return builder.as_markup()


def bot_intro_kb(task_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Подтвердить", callback_data=f"bot:confirm:{task_id}")
    builder.button(text="⬅️ Назад", callback_data="menu:tasks")
    builder.adjust(2)
    return builder.as_markup()


def open_link_kb(url: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="🔗 Открыть", url=url)
    return builder.as_markup()


def go_link_kb(url: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="Перейти", url=url)
    return builder.as_markup()


def open_link_appeal_kb(url: str, claim_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="🔗 Открыть", url=url)
    builder.button(text="📝 Подать апелляцию", callback_data=f"appeal:task:{claim_id}")
    builder.adjust(1)
    return builder.as_markup()


def revision_kb(url: str, claim_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="🔗 Перейти", url=url)
    builder.button(text="Отправить скриншот", callback_data=f"revision:send:{claim_id}")
    builder.adjust(1)
    return builder.as_markup()


def build_post_link(task: dict) -> str | None:
    if not task.get("source_message_id"):
        return None
    username = task.get("chat_username")
    if username:
        return f"https://t.me/{username.lstrip('@')}/{task['source_message_id']}"
    chat_id = task.get("chat_id")
    if not chat_id:
        return None
    chat_id = str(chat_id)
    if chat_id.startswith("-100"):
        internal = chat_id[4:]
    elif chat_id.startswith("-"):
        internal = chat_id[1:]
    else:
        internal = chat_id
    return f"https://t.me/c/{internal}/{task['source_message_id']}"


def build_public_link(task: dict) -> str | None:
    username = task.get("chat_username")
    if not username:
        return None
    return f"https://t.me/{username.lstrip('@')}"


def build_task_action_link(task: dict) -> str | None:
    task_type = task.get("type")
    if task_type in {"view", "reaction"}:
        return build_post_link(task)
    link = task.get("action_link")
    if link:
        return link
    return build_public_link(task)


async def check_user_blocked(bot, db: Database, user_id: int) -> bool:
    block = await db.get_user_block(user_id)
    if not block:
        return False
    remaining = int(block["blocked_until"]) - now_ts()
    if remaining <= 0:
        await db.clear_user_block(user_id)
        return False
    reason = block.get("reason") or "Подозрительная активность"
    remaining_text = _format_age_short(remaining)
    await bot.send_message(
        user_id,
        f"⛔️ Доступ временно ограничен.\nПричина: <b>{escape(reason)}</b>\n"
        f"До разбана: <b>{remaining_text}</b>.",
    )
    return True


async def show_tasks_page(
    callback: CallbackQuery,
    db: Database,
    category_type: str,
    page: int,
    base_type: str | None = None,
    chat_filter: str | None = None,
    config: Config | None = None,
) -> None:
    user_id = callback.from_user.id
    if base_type is None:
        base_type = category_type
    total = await db.count_available_tasks(user_id, base_type, chat_filter)
    if total == 0:
        try:
            await callback.message.edit_text(
                "Пока нет доступных заданий этого типа.",
                reply_markup=tasks_menu_kb(),
            )
        except TelegramBadRequest:
            await callback.message.answer(
                "Пока нет доступных заданий этого типа.",
                reply_markup=tasks_menu_kb(),
            )
        return
    total_pages = max(1, (total + TASKS_PER_PAGE - 1) // TASKS_PER_PAGE)
    page = max(1, min(page, total_pages))
    offset = (page - 1) * TASKS_PER_PAGE
    tasks = await db.list_available_tasks_paged(
        user_id, base_type, TASKS_PER_PAGE, offset, chat_filter
    )
    text = (
        "💰 Доступные задания\n"
        f"Тип: <b>{TASK_TYPE_LABELS.get(category_type, category_type)}</b>\n"
        f"Доступно: <b>{total}</b>\n"
        f"Страница: <b>{page}/{total_pages}</b>"
    )
    try:
        await callback.message.edit_text(
            text,
            reply_markup=tasks_list_kb(tasks, category_type, page, total_pages),
        )
    except TelegramBadRequest:
        await callback.message.answer(
            text,
            reply_markup=tasks_list_kb(tasks, category_type, page, total_pages),
        )


async def complete_view_after_delay(
    bot, db: Database, config: Config, claim_id: int, task: dict, user_id: int
) -> None:
    await asyncio.sleep(7)
    claim = await db.get_claim(claim_id)
    if not claim or claim["status"] not in {"view_pending", "claimed"}:
        return
    await db.update_claim(claim_id, status="completed", completed_at=now_ts())
    await grant_task_reward(db, config, user_id, task["id"], task["reward"], claim_id)
    await notify_new_achievements(bot, db, user_id)
    await bot.send_message(
        user_id,
        "✅ Задание выполнено. Награда начислена.\nНажмите «Дальше», чтобы увидеть следующий пост.",
        reply_markup=view_next_kb(),
    )


async def start_task_for_user(
    bot, db: Database, config: Config, user_id: int, task_id: int, chat_id: int
) -> None:
    if await check_user_blocked(bot, db, user_id):
        return
    task = await db.get_task(task_id)
    if not task or not task["active"]:
        await bot.send_message(chat_id, "Задание недоступно.")
        return
    existing = await db.get_claim_for_user(task_id, user_id)
    if existing:
        status = existing.get("status")
        if status in {"completed", "completed_hold", "rejected", "revoked"}:
            await bot.send_message(chat_id, "Вы уже выполняли это задание.")
            return
        if status in {"submitted", "auto_approving"}:
            await bot.send_message(
                chat_id,
                "🕒 Ваша заявка уже на проверке. Ожидайте решения.",
            )
            return
        if task["type"] == "subscribe":
            join_url = task.get("action_link")
            if not join_url and task.get("chat_username"):
                join_url = f"https://t.me/{task['chat_username'].lstrip('@')}"
            text = (
                f"📌 Подпишитесь на канал/группу: <b>{escape(task.get('chat_title') or 'Канал')}</b>\n"
                f"Награда: <b>{format_coins(task['reward'])}</b> BIT\n\n"
                "После подписки нажмите кнопку проверки."
            )
            await bot.send_message(
                chat_id, text, reply_markup=task_check_subscribe_kb(existing["id"], join_url)
            )
            return
        if task["type"] == "view":
            await bot.send_message(
                chat_id,
                "👁 Просмотр уже засчитан или выполняется. Подождите несколько секунд.",
            )
            return
        if task["type"] in {"reaction", "bot", "boost"}:
            if task["type"] == "reaction":
                link = build_post_link(task)
            else:
                link = build_task_action_link(task)
            if not link:
                await bot.send_message(chat_id, "Не удалось получить ссылку.")
                return
            text = (
                "✅ Задание уже начато, выполните задание и отправьте скриншот.\n"
                f"Награда: <b>{format_coins(task['reward'])}</b>"
            )
            if task["type"] in {"reaction", "boost"}:
                kb = open_link_appeal_kb(link, existing["id"])
                if task["type"] == "reaction":
                    kb = reaction_post_appeal_kb(link, existing["id"])
            else:
                kb = open_link_kb(link)
            await bot.send_message(chat_id, text, reply_markup=kb)
            return
    if task["type"] == "bot":
        text = (
            "🧩 <b>Задание</b>\n"
            f"Награда: <b>{format_coins(task['reward'])}</b>\n"
            "Нажмите «Подтвердить», чтобы получить ссылку."
        )
        await bot.send_message(chat_id, text, reply_markup=bot_intro_kb(task_id))
        return
    allowed, blocked_until = await db.check_rate_limit(
        user_id,
        "task_take",
        TAKE_LIMIT,
        TAKE_WINDOW,
        TAKE_BLOCK,
    )
    if not allowed:
        remaining = max(0, int(blocked_until or 0) - now_ts())
        await bot.send_message(
            chat_id,
            f"⏳ Не так быстро. Попробуйте через {remaining} сек.",
        )
        return

    if task["type"] in {"reaction", "bot", "boost"}:
        status = "waiting_proof"
    elif task["type"] == "view":
        status = "view_pending"
    else:
        status = "claimed"
    claim_id = await db.create_claim(task_id, user_id, status)
    if not claim_id:
        await bot.send_message(chat_id, "Не удалось создать заявку.")
        return

    if task["type"] == "subscribe":
        join_url = task.get("action_link")
        if not join_url and task.get("chat_username"):
            join_url = f"https://t.me/{task['chat_username'].lstrip('@')}"
        text = (
            f"📌 Подпишитесь на канал/группу: <b>{escape(task.get('chat_title') or 'Канал')}</b>\n"
            f"Награда: <b>{format_coins(task['reward'])}</b> BIT\n\n"
            "После подписки нажмите кнопку проверки."
        )
        await bot.send_message(
            chat_id, text, reply_markup=task_check_subscribe_kb(claim_id, join_url)
        )
        return

    if task["type"] == "view":
        reserved = await db.reserve_task_funds(task["id"], task["reward"])
        if not reserved:
            await db.update_claim(claim_id, status="rejected")
            await bot.send_message(chat_id, "Лимит выполнений закончился.")
            return
        try:
            await bot.forward_message(
                chat_id=user_id,
                from_chat_id=task["source_chat_id"],
                message_id=task["source_message_id"],
            )
        except (TelegramBadRequest, TelegramForbiddenError):
            await db.return_task_funds(task["id"], task["reward"])
            await db.update_claim(claim_id, status="rejected")
            # Блокируем задание с причиной
            await db.set_task_status(task["id"], "blocked", "Пост был удалён")
            try:
                await bot.send_message(
                    task["owner_id"],
                    "🚫 Задание заблокировано.\n"
                    "Причина: Пост был удалён.\n\n"
                    "Если у Вас остались вопросы напишите нам @ameropro",
                )
            except (TelegramBadRequest, TelegramForbiddenError):
                pass
            await bot.send_message(
                chat_id,
                "⚠️ Задание неактуально.\n"
                "Пост был удалён.\n\n"
                "Если у Вас остались вопросы напишите нам @ameropro",
            )
            return

        asyncio.create_task(
            complete_view_after_delay(bot, db, config, claim_id, task, user_id)
        )
        return

    if task["type"] == "reaction":
        post_link = build_post_link(task)
        if not post_link:
            await db.update_claim(claim_id, status="rejected")
            await bot.send_message(chat_id, "Не удалось получить ссылку на пост.")
            return
        await bot.send_message(
            chat_id,
            f"{TASK_ACTION_TEXT['reaction']}\n"
            f"Нужная реакция: <b>{escape(task['required_reaction'])}</b>",
            reply_markup=reaction_post_appeal_kb(post_link, claim_id),
        )
        return

    if task["type"] in {"bot", "boost"}:
        target_link = build_public_link(task)
        if not target_link:
            await db.update_claim(claim_id, status="rejected")
            await bot.send_message(chat_id, "Не удалось получить ссылку.")
            return
        text = TASK_ACTION_TEXT.get(task["type"], "Выполните задание и отправьте скриншот.")
        kb = open_link_kb(target_link)
        if task["type"] == "boost":
            kb = open_link_appeal_kb(target_link, claim_id)
        await bot.send_message(
            chat_id,
            text,
            reply_markup=kb,
        )


async def start_bot_task_confirm(
    bot, db: Database, config: Config, user_id: int, task_id: int, chat_id: int
) -> None:
    if await check_user_blocked(bot, db, user_id):
        return
    task = await db.get_task(task_id)
    if not task or not task["active"]:
        await bot.send_message(chat_id, "Задание недоступно.")
        return
    existing = await db.get_claim_for_user(task_id, user_id)
    if existing:
        await bot.send_message(chat_id, "Вы уже брали это задание.")
        return
    allowed, blocked_until = await db.check_rate_limit(
        user_id,
        "task_take",
        TAKE_LIMIT,
        TAKE_WINDOW,
        TAKE_BLOCK,
    )
    if not allowed:
        remaining = max(0, int(blocked_until or 0) - now_ts())
        await bot.send_message(chat_id, f"⏳ Не так быстро. Попробуйте через {remaining} сек.")
        return
    claim_id = await db.create_claim(task_id, user_id, "waiting_proof")
    if not claim_id:
        await bot.send_message(chat_id, "Не удалось создать заявку.")
        return
    link = task.get("action_link")
    if not link and task.get("chat_username"):
        link = f"https://t.me/{task['chat_username'].lstrip('@')}"
    if not link:
        await db.update_claim(claim_id, status="rejected")
        await bot.send_message(chat_id, "Не удалось получить ссылку.")
        return
    text = (
        "🤖 <b>Выполните задание и отправьте скриншот</b>\n"
        f"Награда: <b>{format_coins(task['reward'])}</b>\n"
        "После выполнения отправьте скриншот в этот чат."
    )
    await bot.send_message(chat_id, text, reply_markup=open_link_kb(link))


async def send_next_reaction_claim(bot, db: Database, task_id: int, owner_id: int) -> None:
    claim = await db.get_next_reaction_claim(task_id)
    if not claim:
        task = await db.get_task(task_id)
        if task and int(task["remaining_count"]) <= 0:
            await bot.send_message(owner_id, "✅ Все заявки обработаны. Задание выполнено.")
        else:
            await bot.send_message(owner_id, "✅ Все заявки обработаны.")
        return
    header = "📥 <b>Заявка на проверку</b>"
    type_label = TASK_TYPE_LABELS.get(claim.get("type"), "Задание")
    info_lines = [
        header,
        "⏳ Проверьте в течение 24 часов, иначе заявка будет одобрена автоматически.",
        f"Тип: <b>{escape(type_label)}</b>",
        f"Задание: <b>{escape(claim['task_title'])}</b>",
        f"Пользователь: <code>{claim['user_id']}</code>",
    ]
    if claim.get("required_reaction"):
        info_lines.append(f"Нужная реакция: <b>{escape(claim['required_reaction'])}</b>")
    info_text = "\n".join(info_lines)
    if claim.get("proof_message_id"):
        try:
            await bot.copy_message(
                chat_id=owner_id,
                from_chat_id=claim["user_id"],
                message_id=claim["proof_message_id"],
                caption=info_text,
                parse_mode="HTML",
                reply_markup=reaction_review_kb(claim["id"]),
            )
        except (TelegramBadRequest, TelegramForbiddenError):
            await bot.send_message(
                owner_id,
                info_text,
                reply_markup=reaction_review_kb(claim["id"]),
            )
    else:
        await bot.send_message(
            owner_id,
            info_text,
            reply_markup=reaction_review_kb(claim["id"]),
        )


@router.callback_query(F.data == "tasks:noop")
async def tasks_noop(callback: CallbackQuery) -> None:
    await callback.answer()


@router.callback_query(F.data.startswith("tasks:"))
async def list_tasks(callback: CallbackQuery, db: Database, config: Config) -> None:
    if not await ensure_sponsors(callback.bot, callback.from_user.id, db):
        await callback.answer()
        return
    parts = callback.data.split(":")
    if len(parts) >= 4 and parts[1] == "page":
        task_type = parts[2]
        page = int(parts[3])
    else:
        task_type = parts[1]
        page = 1
    if task_type not in TASK_TYPE_LABELS:
        await callback.answer("Неизвестный тип задания.", show_alert=True)
        return
    if task_type in {"channel", "group"}:
        await show_tasks_page(
            callback,
            db,
            category_type=task_type,
            page=page,
            base_type="subscribe",
            chat_filter=task_type,
            config=config,
        )
    else:
        await show_tasks_page(callback, db, task_type, page, config=config)
    await callback.answer()


@router.callback_query(F.data.startswith("task:pick:"))
async def pick_task(callback: CallbackQuery, db: Database, config: Config) -> None:
    if not await ensure_sponsors(callback.bot, callback.from_user.id, db):
        await callback.answer()
        return
    task_id = int(callback.data.split(":")[2])
    await start_task_for_user(
        callback.bot,
        db,
        config,
        callback.from_user.id,
        task_id,
        callback.from_user.id,
    )
    await callback.answer()


@router.callback_query(F.data.startswith("bot:confirm:"))
async def bot_confirm(callback: CallbackQuery, db: Database, config: Config) -> None:
    task_id = int(callback.data.split(":")[2])
    await start_bot_task_confirm(
        callback.bot,
        db,
        config,
        callback.from_user.id,
        task_id,
        callback.from_user.id,
    )
    await callback.answer()


@router.callback_query(F.data.startswith("task:take:"))
async def take_task_legacy(callback: CallbackQuery, db: Database, config: Config) -> None:
    if not await ensure_sponsors(callback.bot, callback.from_user.id, db):
        await callback.answer()
        return
    task_id = int(callback.data.split(":")[2])
    await start_task_for_user(
        callback.bot,
        db,
        config,
        callback.from_user.id,
        task_id,
        callback.from_user.id,
    )
    await callback.answer()


@router.callback_query(F.data == "view:next")
async def view_next(callback: CallbackQuery, db: Database, config: Config) -> None:
    if not await ensure_sponsors(callback.bot, callback.from_user.id, db):
        await callback.answer()
        return
    tasks = await db.list_available_tasks_paged(callback.from_user.id, "view", 1, 0)
    if not tasks:
        await callback.message.answer("Пока нет новых заданий на просмотр.")
        await callback.answer()
        return
    await start_task_for_user(
        callback.bot,
        db,
        config,
        callback.from_user.id,
        tasks[0]["id"],
        callback.from_user.id,
    )
    await callback.answer()


@router.callback_query(F.data.startswith("task:check:"))
async def check_subscribe(callback: CallbackQuery, db: Database, config: Config) -> None:
    claim_id = int(callback.data.split(":")[2])
    claim = await db.get_claim(claim_id)
    if not claim or claim["user_id"] != callback.from_user.id:
        await callback.answer("Заявка не найдена.", show_alert=True)
        return
    if claim["status"] != "claimed":
        await callback.answer("Заявка уже обработана.", show_alert=True)
        return
    task = await db.get_task(claim["task_id"])
    if not task or task["type"] != "subscribe":
        await callback.answer("Задание не найдено.", show_alert=True)
        return
    if not await is_member(callback.bot, task["chat_id"], callback.from_user.id):
        await callback.answer("Подписка не найдена. Попробуйте еще раз.", show_alert=True)
        return
    reserved = await db.reserve_task_funds(task["id"], task["reward"])
    if not reserved:
        await callback.answer("Лимит выполнений закончился.", show_alert=True)
        await db.update_claim(claim_id, status="rejected")
        return
    await db.update_claim(
        claim_id,
        status="completed_hold",
        completed_at=now_ts(),
        hold_until=hold_until_ts(config),
        penalty_deadline=None,
        warning_sent=0,
    )
    await grant_task_reward(db, config, callback.from_user.id, task["id"], task["reward"], claim_id)
    await notify_new_achievements(callback.bot, db, callback.from_user.id)
    await callback.message.answer(
        "✅ Подписка подтверждена. Награда начислена.\n"
        f"Важно: не отписывайтесь в течение {config.hold_days} дней."
    )
    await callback.answer()


@router.callback_query(F.data.startswith("appeal:"))
async def appeal_start(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    parts = callback.data.split(":")
    if len(parts) < 3:
        await callback.answer("Некорректная заявка.", show_alert=True)
        return
    try:
        claim_id = int(parts[2])
    except ValueError:
        await callback.answer("Некорректная заявка.", show_alert=True)
        return
    claim = await db.get_claim_with_task(claim_id)
    if not claim or claim["user_id"] != callback.from_user.id:
        await callback.answer("Заявка не найдена.", show_alert=True)
        return
    task = await db.get_task(claim["task_id"])
    link = build_task_action_link(task) if task else None
    link_text = link or "Ссылка недоступна"
    await state.set_state(AppealState.reason)
    await state.update_data(
        appeal_claim_id=claim_id,
        appeal_task_id=claim["task_id"],
        appeal_link=link,
    )
    await callback.message.answer(
        "Вы уверены, что хотите пожаловаться?\n"
        f"Ссылка: {link_text}\n"
        "📝 Введите причину жалобы:",
        reply_markup=cancel_kb(),
    )
    await callback.answer()


@router.message(F.photo | F.document)
async def reaction_proof(message: Message, db: Database) -> None:
    if await check_user_blocked(message.bot, db, message.from_user.id):
        return
    claim = await db.get_open_reaction_claim(message.from_user.id)
    if not claim:
        return
    allowed, blocked_until = await db.check_rate_limit(
        message.from_user.id,
        "proof_submit",
        PROOF_LIMIT,
        PROOF_WINDOW,
        PROOF_BLOCK,
    )
    if not allowed:
        remaining = max(0, int(blocked_until or 0) - now_ts())
        await message.answer(f"⏳ Не так быстро. Попробуйте через {remaining} сек.")
        return
    await db.update_claim(
        claim["id"],
        status="submitted",
        submitted_at=now_ts(),
        proof_message_id=message.message_id,
    )
    count = await db.count_pending_reaction_claims(claim["task_id"])
    button_text = f"Задание #{claim['task_id']} • {count} шт."
    builder = InlineKeyboardBuilder()
    builder.button(text=button_text, callback_data=f"review:queue:{claim['task_id']}")
    try:
        type_label = TASK_TYPE_LABELS.get(claim.get("type"), "Задание")
        info_lines = [
            "🔔 Вам пришла заявка на проверку.",
            "⏳ Проверьте в течение 24 часов, иначе заявка будет одобрена автоматически.",
            "",
            "📥 <b>Заявка на проверку</b>",
            f"Тип: <b>{escape(type_label)}</b>",
            f"Задание: <b>{escape(claim['task_title'])}</b>",
            f"Пользователь: <code>{claim['user_id']}</code>",
        ]
        if claim.get("required_reaction"):
            info_lines.append(f"Нужная реакция: <b>{escape(claim['required_reaction'])}</b>")
        info_text = "\n".join(info_lines)
        await message.bot.copy_message(
            chat_id=claim["owner_id"],
            from_chat_id=message.from_user.id,
            message_id=message.message_id,
            caption=info_text,
            parse_mode="HTML",
            reply_markup=builder.as_markup(),
        )
        await message.answer("Скриншот отправлен на проверку.")
    except (TelegramBadRequest, TelegramForbiddenError):
        await message.answer("Не удалось отправить на проверку. Попробуйте позже.")


@router.message(F.forward_from_chat)
async def handle_forwarded_message(message: Message) -> None:
    """Пересылка сообщений - из канала с указанием, от человека от имени бота (с медиа)"""
    if not message.forward_from_chat or not message.forward_from_message_id:
        return
    
    try:
        # Если из канала/группы - переслать с указанием источника
        # Если от человека - скопировать как от бота (без "Переслано от")
        if message.forward_from_chat.type in {"channel", "supergroup", "group"}:
            # Из канала/группы - переслать (медиа будет включена)
            await message.bot.forward_message(
                chat_id=message.from_user.id,
                from_chat_id=message.forward_from_chat.id,
                message_id=message.forward_from_message_id,
            )
        else:
            # От человека - копировать как от бота (медиа будет включена)
            await message.bot.copy_message(
                chat_id=message.from_user.id,
                from_chat_id=message.forward_from_chat.id,
                message_id=message.forward_from_message_id,
            )
    except (TelegramBadRequest, TelegramForbiddenError):
        # Если ошибка - копируем
        try:
            await message.bot.copy_message(
                chat_id=message.from_user.id,
                from_chat_id=message.forward_from_chat.id,
                message_id=message.forward_from_message_id,
            )
        except (TelegramBadRequest, TelegramForbiddenError):
            pass


@router.callback_query(F.data.startswith("review:queue:"))
async def review_queue(callback: CallbackQuery, db: Database, config: Config) -> None:
    task_id = int(callback.data.split(":")[2])
    task = await db.get_task(task_id)
    if not task or task["owner_id"] != callback.from_user.id:
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await send_next_reaction_claim(callback.bot, db, task_id, callback.from_user.id)
    await callback.answer()


@router.callback_query(F.data.startswith("review:approve:"))
async def review_approve(callback: CallbackQuery, db: Database, config: Config) -> None:
    claim_id = int(callback.data.split(":")[2])
    claim = await db.get_claim_with_task(claim_id)
    if not claim or claim["owner_id"] != callback.from_user.id:
        await callback.answer("Нет доступа.", show_alert=True)
        return
    if claim["status"] != "submitted":
        await callback.answer("Заявка уже обработана.", show_alert=True)
        return
    reserved = await db.reserve_task_funds(claim["task_id"], claim["reward"])
    if not reserved:
        await callback.answer("Лимит выполнений закончился.", show_alert=True)
        await db.update_claim(claim_id, status="rejected")
        await send_next_reaction_claim(callback.bot, db, claim["task_id"], callback.from_user.id)
        return
    await db.update_claim(claim_id, status="completed", completed_at=now_ts())
    await grant_task_reward(db, config, claim["user_id"], claim["task_id"], claim["reward"], claim_id)
    await notify_new_achievements(callback.bot, db, claim["user_id"])
    try:
        await callback.bot.send_message(
            claim["user_id"],
            "Ваша заявка одобрена. Награда начислена.",
        )
    except (TelegramBadRequest, TelegramForbiddenError):
        pass
    await send_next_reaction_claim(callback.bot, db, claim["task_id"], callback.from_user.id)
    await callback.answer("Заявка одобрена. Награда начислена.", show_alert=True)
    await callback.message.answer("✅ Заявка одобрена и награда начислена.")


@router.callback_query(F.data.startswith("review:reject:"))
async def review_reject(callback: CallbackQuery, db: Database) -> None:
    claim_id = int(callback.data.split(":")[2])
    claim = await db.get_claim_with_task(claim_id)
    if not claim or claim["owner_id"] != callback.from_user.id:
        await callback.answer("Нет доступа.", show_alert=True)
        return
    if claim["status"] != "submitted":
        await callback.answer("Заявка уже обработана.", show_alert=True)
        return
    await db.update_claim(claim_id, status="rejected", completed_at=now_ts())
    try:
        await callback.bot.send_message(
            claim["user_id"],
            "Ваша заявка отклонена.",
        )
    except (TelegramBadRequest, TelegramForbiddenError):
        pass
    await send_next_reaction_claim(callback.bot, db, claim["task_id"], callback.from_user.id)
    await callback.answer()


@router.callback_query(F.data.startswith("review:revise:"))
async def review_revise(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    claim_id = int(callback.data.split(":")[2])
    claim = await db.get_claim_with_task(claim_id)
    if not claim or claim["owner_id"] != callback.from_user.id:
        await callback.answer("Нет доступа.", show_alert=True)
        return
    if claim["status"] != "submitted":
        await callback.answer("Заявка уже обработана.", show_alert=True)
        return
    await state.set_state(ReviewState.note)
    await state.update_data(claim_id=claim_id, user_id=claim["user_id"], task_id=claim["task_id"])
    await callback.message.answer("Напишите, что нужно исправить.")
    await callback.answer()


@router.message(ReviewState.note)
async def review_note(message: Message, state: FSMContext, db: Database) -> None:
    data = await state.get_data()
    claim_id = data.get("claim_id")
    user_id = data.get("user_id")
    task_id = data.get("task_id")
    note = (message.text or "").strip()
    if not claim_id or not user_id or not note:
        await message.answer("Сообщение не распознано.")
        return
    await db.update_claim(claim_id, status="revision", review_note=note)
    await state.clear()
    try:
        task = await db.get_task(task_id) if task_id else None
        link = build_task_action_link(task) if task else None
        text = (
            f"Нужна доработка по заданию #{task_id}:\n\n"
            f"<blockquote>{escape(note)}</blockquote>\n\n"
            "Перейдите снова по ссылке и доработайте."
        )
        kb = revision_kb(link, claim_id) if link else None
        await message.bot.send_message(
            user_id,
            text,
            reply_markup=kb,
            parse_mode="HTML",
        )
    except (TelegramBadRequest, TelegramForbiddenError):
        pass
    if task_id:
        await send_next_reaction_claim(message.bot, db, task_id, message.from_user.id)
    await message.answer("Отправлено на доработку.")


@router.callback_query(F.data.startswith("revision:send:"))
async def revision_send(callback: CallbackQuery, db: Database) -> None:
    try:
        claim_id = int(callback.data.split(":")[2])
    except (IndexError, ValueError):
        await callback.answer("Некорректная заявка.", show_alert=True)
        return
    claim = await db.get_claim_with_task(claim_id)
    if not claim or claim["user_id"] != callback.from_user.id:
        await callback.answer("Нет доступа.", show_alert=True)
        return
    task = await db.get_task(claim["task_id"])
    link = build_task_action_link(task) if task else None
    if not link:
        await callback.message.answer("Ссылка недоступна.")
        await callback.answer()
        return
    await callback.message.answer(
        "Пришлите скриншот выполнения доработки.",
        reply_markup=go_link_kb(link),
    )
    await callback.answer()


@router.message(AppealState.reason)
async def appeal_reason(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    data = await state.get_data()
    claim_id = data.get("appeal_claim_id")
    task_id = data.get("appeal_task_id")
    reason = (message.text or "").strip()
    if not claim_id or not task_id:
        await state.clear()
        await message.answer("Сессия апелляции устарела.")
        return
    if not reason:
        await message.answer("Введите причину жалобы.")
        return
    appeal_id = await db.create_appeal(
        user_id=message.from_user.id,
        task_id=int(task_id),
        claim_id=int(claim_id),
        reason=reason,
    )
    await state.clear()

    task = await db.get_task(int(task_id))
    link = data.get("appeal_link") or (build_task_action_link(task) if task else None)
    type_label = TASK_TYPE_LABELS.get(task.get("type") if task else None, "Задание")
    admin_text_lines = [
        f"📝 Апелляция #{appeal_id}",
        f"От: <code>{message.from_user.id}</code>",
        f"Задание: <b>#{task_id}</b> ({escape(type_label)})",
    ]
    if link:
        admin_text_lines.append(f"Ссылка: {link}")
    admin_text_lines.append("")
    admin_text_lines.append(f"Причина: {escape(reason)}")
    admin_text = "\n".join(admin_text_lines)
    for admin_id in config.admin_ids:
        try:
            await message.bot.send_message(admin_id, admin_text, parse_mode="HTML")
        except (TelegramBadRequest, TelegramForbiddenError):
            pass

    await db.auto_block_task_if_needed(int(task_id))
    await message.answer(
        "✅ Апелляция отправлена. Спасибо!",
        reply_markup=main_menu_kb(message.from_user.id in config.admin_ids),
    )


router = Router()
routers.append(router)


async def send_balance(bot, chat_id: int, user_id: int, db: Database) -> None:
    user = await db.get_user(user_id)
    if not user:
        return
    await bot.send_message(
        chat_id,
        "💳 <b>Ваш баланс</b>\n"
        f"{format_coins(user['balance'])} BIT\n\n"
        "Нажмите кнопку ниже, чтобы пополнить баланс.",
        reply_markup=topup_menu_kb(),
    )


def topup_menu_kb() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ Пополнить", callback_data="pay:start")
    builder.button(text="⬅️ Меню", callback_data="menu:home")
    builder.adjust(1)
    return builder.as_markup()


def asset_kb(assets: list[str]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for asset in assets:
        builder.button(text=asset, callback_data=f"pay:asset:{asset}")
    builder.adjust(2)
    return builder.as_markup()


def invoice_kb(pay_url: str, invoice_id: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="Оплатить", url=pay_url)
    builder.button(text="Проверить оплату", callback_data=f"pay:check:{invoice_id}")
    builder.adjust(1)
    return builder.as_markup()


@router.message(F.text.in_(["Баланс", "💳 Баланс"]))
async def balance_info(message: Message, state: FSMContext, db: Database) -> None:
    await state.clear()
    if not await ensure_sponsors(message.bot, message.from_user.id, db):
        return
    await send_balance(message.bot, message.from_user.id, message.from_user.id, db)


@router.callback_query(F.data == "menu:balance")
async def balance_menu(callback: CallbackQuery, db: Database, state: FSMContext) -> None:
    await state.clear()
    if not await ensure_sponsors(callback.bot, callback.from_user.id, db):
        await callback.answer()
        return
    await send_balance(callback.bot, callback.from_user.id, callback.from_user.id, db)
    await callback.answer()


@router.callback_query(F.data == "pay:start")
async def topup_start_cb(
    callback: CallbackQuery, state: FSMContext, db: Database, config: Config
) -> None:
    await state.clear()
    if not await ensure_sponsors(callback.bot, callback.from_user.id, db):
        await callback.answer()
        return
    if not config.crypto_pay_token:
        await callback.message.answer("Оплата временно недоступна.")
        await callback.answer()
        return
    await state.set_state(TopUpState.amount)
    await callback.message.answer(
        "Введите сумму в BIT:\n💵 100 BIT = 0.005$\n\n⚠️Мин. Сумма в BIT 201",
        reply_markup=cancel_kb(),
    )
    await callback.answer()


@router.message(F.text == "Пополнить")
async def topup_start(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    await state.clear()
    if not await ensure_sponsors(message.bot, message.from_user.id, db):
        return
    if not config.crypto_pay_token:
        await message.answer("Оплата временно недоступна.")
        return
    await state.set_state(TopUpState.amount)
    await message.answer(
        "Введите сумму в BIT:\n💵 100 BIT = 0.05$",
        reply_markup=cancel_kb(),
    )


@router.message(TopUpState.amount)
async def topup_amount(message: Message, state: FSMContext, config: Config) -> None:
    try:
        coins = int((message.text or "").strip())
    except ValueError:
        await message.answer("Введите число.")
        return
    if coins <= 0:
        await message.answer("Сумма должна быть больше нуля.")
        return
    await state.update_data(coins=coins)
    await state.set_state(TopUpState.asset)
    await message.answer(
        "Выберите валюту оплаты:",
        reply_markup=asset_kb(config.payment_assets),
    )


@router.callback_query(F.data.startswith("pay:asset:"))
async def topup_create_invoice(
    callback: CallbackQuery, state: FSMContext, db: Database, config: Config
) -> None:
    if not config.crypto_pay_token:
        await callback.message.answer("Оплата временно недоступна.")
        await callback.answer()
        return
    data = await state.get_data()
    coins = int(data.get("coins", 0))
    asset = callback.data.split(":")[2]
    if asset == "USDT":
        amount = config.coins_to_usd(coins)
    elif asset == "TON":
        amount = config.coins_to_ton(coins)
    else:
        await callback.answer("Неизвестный актив.", show_alert=True)
        return
    if amount <= 0:
        await callback.answer("Неверная сумма.", show_alert=True)
        return
    client = CryptoPayClient(config.crypto_pay_token)
    try:
        invoice = await client.create_invoice(
            asset=asset,
            amount=f"{amount:.6f}",
            description=f"Пополнение {coins} BIT",
            payload=f"{callback.from_user.id}:{coins}",
        )
    except RuntimeError:
        await callback.message.answer("Не удалось создать счет. Попробуйте позже.")
        await callback.answer()
        return
    pay_url = invoice.get("pay_url", "")
    await db.create_invoice(
        callback.from_user.id,
        coins,
        asset,
        str(invoice["invoice_id"]),
        pay_url,
    )
    await state.clear()
    if pay_url:
        await callback.message.answer(
            f"Счет создан. Сумма: <b>{amount:.6f} {asset}</b>",
            reply_markup=invoice_kb(pay_url, str(invoice["invoice_id"])),
        )
    else:
        await callback.message.answer(
            f"Счет создан. Сумма: <b>{amount:.6f} {asset}</b>\n"
            f"ID счета: <code>{invoice['invoice_id']}</code>"
        )
    await callback.answer()


@router.callback_query(F.data.startswith("pay:check:"))
async def topup_check(callback: CallbackQuery, db: Database, config: Config) -> None:
    invoice_id = callback.data.split(":")[2]
    invoice = await db.get_invoice(invoice_id)
    if not invoice:
        await callback.answer("Счет не найден.", show_alert=True)
        return
    if invoice["status"] == "paid":
        await callback.answer("Счет уже оплачен.", show_alert=True)
        return
    if not config.crypto_pay_token:
        await callback.answer("Оплата недоступна.", show_alert=True)
        return
    client = CryptoPayClient(config.crypto_pay_token)
    try:
        data = await client.get_invoice(invoice_id)
    except RuntimeError:
        await callback.answer("Не удалось проверить счет.", show_alert=True)
        return
    if not data:
        await callback.answer("Счет не найден.", show_alert=True)
        return
    if data.get("status") == "paid":
        await db.mark_invoice_paid(invoice_id)
        await db.add_balance(
            invoice["user_id"],
            invoice["coins"],
            "topup",
            {"invoice_id": invoice_id},
        )
        usd_amount, free_seconds = topup_bonus_seconds(config, int(invoice["coins"]))
        if free_seconds > 0:
            await db.add_commission_free_time(invoice["user_id"], free_seconds)
        await callback.message.answer(
            build_topup_success_text(
                int(invoice["coins"]),
                usd_amount,
                free_seconds,
            )
        )
    else:
        await callback.answer("Счет пока не оплачен.", show_alert=True)


router = Router()
routers.append(router)

ADMIN_MENU_TEXT = "🛠 <b>Админ-панель</b>\nВыберите действие:"


def parse_duration(value: str) -> int | None:
    text = value.strip().lower()
    if not text:
        return None
    multiplier = 1
    if text[-1] in {"s", "m", "h", "d"}:
        unit = text[-1]
        text = text[:-1]
        if unit == "m":
            multiplier = 60
        elif unit == "h":
            multiplier = 3600
        elif unit == "d":
            multiplier = 86400
    if not text.isdigit():
        return None
    return int(text) * multiplier


async def send_admin_menu(bot: Bot, chat_id: int) -> None:
    await bot.send_message(chat_id, ADMIN_MENU_TEXT, reply_markup=admin_menu_kb())


async def send_stats(bot: Bot, chat_id: int, db: Database) -> None:
    stats = await db.stats()
    text = (
        "📊 <b>Статистика бота</b>\n"
        f"Пользователи: <b>{stats['users']}</b>\n"
        f"Задания: <b>{stats['tasks']}</b>\n"
        f"Выполнено: <b>{stats['completed']}</b>\n"
        f"Суммарный баланс: <b>{format_coins(stats['balance'])}</b> BIT"
    )
    await bot.send_message(chat_id, text, reply_markup=admin_menu_kb())


async def send_daily_stats(bot: Bot, chat_id: int, db: Database) -> None:
    stats = await db.global_stats(_day_start_ts())
    text = (
        "📅 <b>Отчет за сегодня</b>\n\n"
        f"👤 Всего пользователей: <b>{format_coins(stats['users_total'])}</b>\n"
        f"🆕 Новых сегодня: <b>{format_coins(stats['new_today'])}</b>\n\n"
        f"📣 Подписки на каналы: <b>{format_coins(stats['channel_subs'])}</b>\n"
        f"👥 Подписки на группы: <b>{format_coins(stats['group_subs'])}</b>\n\n"
        f"👁️ Просмотры: <b>{format_coins(stats['views'])}</b>\n"
        f"🔥 Реакции: <b>{format_coins(stats['reactions'])}</b>"
    )
    await bot.send_message(chat_id, text, reply_markup=admin_menu_kb())


def _format_age_short(seconds: int) -> str:
    if seconds < 60:
        return f"{seconds}с"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}м"
    hours = minutes // 60
    minutes = minutes % 60
    if hours < 24:
        return f"{hours}ч {minutes}м"
    days = hours // 24
    hours = hours % 24
    return f"{days}д {hours}ч"


async def notify_new_achievements(bot: Bot, db: Database, user_id: int) -> None:
    newly = await ensure_user_achievements(db, user_id)
    if not newly:
        return
    total_reward = sum(item.reward for item in newly if item.reward > 0)
    lines = ["🏅 <b>Новое достижение!</b>"]
    for item in newly:
        reward_text = f" +{format_coins(item.reward)} BIT" if item.reward > 0 else ""
        lines.append(f"✅ {item.title} — {item.description}{reward_text}")
    if total_reward > 0:
        lines.append(f"🎁 Бонус: <b>{format_coins(total_reward)} BIT</b>")
    try:
        await bot.send_message(user_id, "\n".join(lines))
    except (TelegramBadRequest, TelegramForbiddenError):
        pass


def _build_admin_user_text(user: dict, block: dict | None) -> tuple[str, bool]:
    now = now_ts()
    username = f"@{escape(user['username'])}" if user.get("username") else "-"
    first_name = escape(user.get("first_name") or "-")
    last_active = int(user.get("last_active") or 0)
    created_at = int(user.get("created_at") or 0)
    last_active_text = datetime.fromtimestamp(last_active).strftime("%Y-%m-%d %H:%M") if last_active else "-"
    created_text = datetime.fromtimestamp(created_at).strftime("%Y-%m-%d %H:%M") if created_at else "-"
    free_until = int(user.get("commission_free_until") or 0)
    if free_until > now:
        free_remaining = _format_age_short(free_until - now)
        free_text = f"до {datetime.fromtimestamp(free_until).strftime('%Y-%m-%d %H:%M')} (осталось {free_remaining})"
    else:
        free_text = "нет"
    is_blocked = False
    lines = [
        f"Пользователь <code>{user['id']}</code>",
        f"Username: <b>{username}</b>",
        f"Имя: <b>{first_name}</b>",
        f"Баланс: <b>{format_coins(user['balance'])}</b>",
        f"Уровень: <b>{user['level']}</b>",
        f"Выполнено: <b>{user['completed_tasks']}</b>",
        f"Реферал ID: <code>{user['referrer_id'] or '-'}</code>",
        f"Рефералы: <b>{user['referral_count']}</b>",
        f"Реф. заработок: <b>{format_coins(user['referral_earned'])}</b>",
        f"Комиссия 0%: <b>{free_text}</b>",
        f"Создан: <b>{created_text}</b>",
        f"Последняя активность: <b>{last_active_text}</b>",
    ]
    if block:
        remaining = int(block.get("blocked_until") or 0) - now
        if remaining > 0:
            is_blocked = True
            reason = block.get("reason") or "Подозрительная активность"
            lines.append("Блокировка: <b>Да</b>")
            lines.append(f"Причина: <b>{escape(reason)}</b>")
            lines.append(f"До разбана: <b>{_format_age_short(remaining)}</b>")
        else:
            lines.append("Блокировка: <b>Нет</b>")
    else:
        lines.append("Блокировка: <b>Нет</b>")
    return "\n".join(lines), is_blocked


async def send_recent_transactions(bot: Bot, chat_id: int, db: Database) -> None:
    rows = await db.list_recent_transactions(12)
    if not rows:
        await bot.send_message(chat_id, "🧾 Транзакции не найдены.", reply_markup=admin_menu_kb())
        return
    lines = ["🧾 <b>Последние транзакции</b>"]
    for row in rows:
        amount = format_coins(int(row["amount"]))
        lines.append(
            f"#{row['id']} • <code>{row['user_id']}</code> • <b>{amount}</b> • {escape(row['reason'])}"
    )
    await bot.send_message(chat_id, "\n".join(lines), reply_markup=admin_menu_kb())


async def send_pending_invoices(bot: Bot, chat_id: int, db: Database) -> None:
    invoices = await db.list_pending_invoices()
    if not invoices:
        await bot.send_message(chat_id, "🧾 Ожидающих счетов нет.", reply_markup=admin_menu_kb())
        return
    now = now_ts()
    limit = 20
    lines = ["🧾 <b>Ожидают оплаты</b>"]
    for inv in invoices[:limit]:
        age = max(0, now - int(inv.get("created_at") or 0))
        age_text = _format_age_short(age)
        coins = format_coins(int(inv.get("coins") or 0))
        asset = inv.get("asset") or "-"
        lines.append(
            f"#{inv['invoice_id']} • <code>{inv['user_id']}</code> • {coins} BIT • {asset} • {age_text}"
        )
    if len(invoices) > limit:
        lines.append(f"… еще {len(invoices) - limit}")
    await bot.send_message(chat_id, "\n".join(lines), reply_markup=admin_menu_kb())


def _format_top_users(rows: list[dict], title: str, value_key: str, suffix: str) -> str:
    if not rows:
        return "🏆 <b>Топ пока пуст</b>"
    lines = [f"🏆 <b>{title}</b>"]
    for idx, row in enumerate(rows, 1):
        name = _display_name(row)
        value = format_coins(int(row.get(value_key) or 0))
        lines.append(
            f"{idx}. {name} • <code>{row['id']}</code> • <b>{value}</b>{suffix}"
        )
    return "\n".join(lines)


async def send_blocks(bot: Bot, chat_id: int, db: Database) -> None:
    rows = await db.list_user_blocks()
    if not rows:
        await bot.send_message(chat_id, "🚫 Активных блокировок нет.", reply_markup=admin_menu_kb())
        return
    lines = ["🚫 <b>Активные блокировки</b>"]
    now = now_ts()
    for row in rows:
        remaining = max(0, int(row["blocked_until"]) - now)
        reason = row.get("reason") or "-"
        lines.append(
            f"<code>{row['user_id']}</code> • осталось {remaining} сек • {escape(reason)}"
        )
    await bot.send_message(chat_id, "\n".join(lines), reply_markup=admin_menu_kb())


async def send_sponsors_list(bot: Bot, chat_id: int, db: Database) -> None:
    sponsors = await db.list_sponsors(active_only=False)
    if sponsors:
        lines = [
            f"{s['id']}. {escape(s.get('title') or s.get('chat_username') or '')} (active={s['active']})"
            for s in sponsors
        ]
        text = "🤝 <b>Список спонсоров</b>\n" + "\n".join(lines)
    else:
        text = "🤝 Список спонсоров пуст."
    await bot.send_message(chat_id, text, reply_markup=sponsor_actions_kb().as_markup())


def sponsor_actions_kb() -> InlineKeyboardBuilder:
    builder = InlineKeyboardBuilder()
    builder.button(text="Добавить", callback_data="admin:sponsor:add")
    builder.button(text="Удалить", callback_data="admin:sponsor:remove")
    builder.adjust(2)
    return builder


@router.message(F.text == "Назад")
async def admin_back(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    if not is_admin(message.from_user.id, config):
        return
    await state.clear()
    await send_main_menu(
        message.bot,
        message.from_user.id,
        message.from_user.id in config.admin_ids,
        db,
    )


@router.callback_query(F.data == "menu:admin")
async def admin_menu_callback(callback: CallbackQuery, state: FSMContext, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await state.clear()
    await send_admin_menu(callback.bot, callback.from_user.id)
    await callback.answer()


@router.callback_query(F.data == "admin:back")
async def admin_back_callback(
    callback: CallbackQuery, state: FSMContext, db: Database, config: Config
) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await state.clear()
    await send_main_menu(
        callback.bot,
        callback.from_user.id,
        callback.from_user.id in config.admin_ids,
        db,
    )
    await callback.answer()


@router.message(F.text == "Статистика")
async def admin_stats(message: Message, db: Database, config: Config) -> None:
    if not is_admin(message.from_user.id, config):
        return
    await send_stats(message.bot, message.from_user.id, db)


@router.callback_query(F.data == "admin:stats")
async def admin_stats_cb(callback: CallbackQuery, db: Database, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await send_stats(callback.bot, callback.from_user.id, db)
    await callback.answer()


@router.callback_query(F.data == "admin:recent")
async def admin_recent_cb(callback: CallbackQuery, db: Database, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    recent = await db.fetchall(
        """SELECT id, username, first_name, registered_at, balance, completed_tasks 
        FROM users ORDER BY registered_at DESC LIMIT 30"""
    )
    if not recent:
        await callback.answer("Нет новых пользователей.", show_alert=True)
        return
    lines = ["👥 <b>Новые пользователи ({0})</b>:\n".format(len(recent))]
    for user in recent:
        name = user.get('first_name') or user.get('username') or "ID: {0}".format(user['id'])
        lines.append(
            "<code>{0}</code> | {1} | Баланс: {2} | Выполнено: {3}".format(
                user['id'], escape(name), user.get('balance', 0), user.get('completed_tasks', 0)
            )
        )
    text = "\n".join(lines[:30])
    if len(lines) > 30:
        text += "\n\n... и ещё {0} пользователей".format(len(lines) - 30)
    builder = InlineKeyboardBuilder()
    builder.button(text="⬅️ Админ", callback_data="menu:admin")
    await callback.message.answer(text, reply_markup=builder.as_markup())
    await callback.answer()


@router.callback_query(F.data == "admin:tops")
async def admin_tops_menu(callback: CallbackQuery, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    text = "🏆 <b>Топ пользователей</b>\nВыберите рейтинг:"
    try:
        await callback.message.edit_text(text, reply_markup=admin_tops_kb())
    except TelegramBadRequest:
        await callback.message.answer(text, reply_markup=admin_tops_kb())
    await callback.answer()


@router.callback_query(F.data == "admin:tops:balance")
async def admin_tops_balance(callback: CallbackQuery, db: Database, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    rows = await db.list_top_users_by_balance(10)
    text = _format_top_users(rows, "Топ по балансу", "balance", " BIT")
    try:
        await callback.message.edit_text(text, reply_markup=admin_tops_kb())
    except TelegramBadRequest:
        await callback.message.answer(text, reply_markup=admin_tops_kb())
    await callback.answer()


@router.callback_query(F.data == "admin:tops:completed")
async def admin_tops_completed(callback: CallbackQuery, db: Database, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    rows = await db.list_top_users_by_completed(10)
    text = _format_top_users(rows, "Топ по выполненным", "completed_tasks", " выполнений")
    try:
        await callback.message.edit_text(text, reply_markup=admin_tops_kb())
    except TelegramBadRequest:
        await callback.message.answer(text, reply_markup=admin_tops_kb())
    await callback.answer()


def _appeal_button_text(appeal: dict) -> str:
    title = appeal.get("task_title") or "Без названия"
    if len(title) > 24:
        title = title[:21] + "..."
    return f"#{appeal['id']} • {title}"


async def show_appeals_page(
    callback: CallbackQuery, db: Database, page: int
) -> None:
    total = await db.count_appeals("appeal")
    if total == 0:
        await callback.message.edit_text(
            "📝 Апелляций нет.",
            reply_markup=admin_menu_kb(),
        )
        return
    total_pages = max(1, (total + APPEALS_PAGE_SIZE - 1) // APPEALS_PAGE_SIZE)
    page = max(1, min(page, total_pages))
    offset = (page - 1) * APPEALS_PAGE_SIZE
    appeals = await db.list_appeals_paged("appeal", APPEALS_PAGE_SIZE, offset)
    lines = [f"📝 <b>Апелляции</b> • стр. {page}/{total_pages}"]
    for item in appeals:
        lines.append(
            f"#{item['id']} • <code>{item['user_id']}</code> • {escape(item.get('task_title') or 'Без названия')}"
        )
    builder = InlineKeyboardBuilder()
    for item in appeals:
        builder.button(
            text=_appeal_button_text(item),
            callback_data=f"admin:appeal:view:{item['id']}:{page}",
        )
    nav = []
    if page > 1:
        nav.append(
            InlineKeyboardButton(
                text="⬅️ Назад",
                callback_data=f"admin:appeals:page:{page - 1}",
            )
        )
    if page < total_pages:
        nav.append(
            InlineKeyboardButton(
                text="➡️ Далее",
                callback_data=f"admin:appeals:page:{page + 1}",
            )
        )
    if nav:
        builder.row(*nav)
    builder.row(InlineKeyboardButton(text="↩️ Админ", callback_data="menu:admin"))
    try:
        await callback.message.edit_text("\n".join(lines), reply_markup=builder.as_markup())
    except TelegramBadRequest:
        await callback.message.answer("\n".join(lines), reply_markup=builder.as_markup())


@router.callback_query(F.data.startswith("admin:appeals:page:"))
async def admin_appeals_page(callback: CallbackQuery, db: Database, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    try:
        page = int(callback.data.split(":")[-1])
    except ValueError:
        page = 1
    await show_appeals_page(callback, db, page)
    await callback.answer()


@router.callback_query(F.data.startswith("admin:appeal:view:"))
async def admin_appeal_view(callback: CallbackQuery, db: Database, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    parts = callback.data.split(":")
    if len(parts) < 5:
        await callback.answer()
        return
    appeal_id = int(parts[3])
    page = int(parts[4])
    appeal = await db.get_appeal(appeal_id)
    if not appeal:
        await callback.answer("Апелляция не найдена.", show_alert=True)
        return
    created_at = appeal.get("created_at") or 0
    created_text = datetime.fromtimestamp(int(created_at)).strftime("%Y-%m-%d %H:%M")
    link = None
    task = await db.get_task(appeal["task_id"]) if appeal.get("task_id") else None
    if task:
        link = build_task_action_link(task)
    lines = [
        f"📝 <b>Апелляция #{appeal['id']}</b>",
        f"Пользователь: <code>{appeal['user_id']}</code>",
        f"Задание: <code>{appeal['task_id']}</code>",
        f"Заявка: <code>{appeal['claim_id']}</code>",
        f"Тип: <b>{escape(appeal.get('task_type') or '-')}</b>",
        f"Название: <b>{escape(appeal.get('task_title') or '-')}</b>",
        f"Дата: <b>{created_text}</b>",
        "",
        f"Причина:\n{escape(appeal.get('reason') or '-')}",
    ]
    if link:
        lines.append(f"\nСсылка: {link}")
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Рассмотрено", callback_data=f"admin:appeal:done:{appeal_id}:{page}")
    builder.button(text="⬅️ Назад", callback_data=f"admin:appeals:page:{page}")
    builder.adjust(1)
    try:
        await callback.message.edit_text("\n".join(lines), reply_markup=builder.as_markup())
    except TelegramBadRequest:
        await callback.message.answer("\n".join(lines), reply_markup=builder.as_markup())
    await callback.answer()


@router.callback_query(F.data.startswith("admin:appeal:done:"))
async def admin_appeal_done(callback: CallbackQuery, db: Database, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    parts = callback.data.split(":")
    if len(parts) < 5:
        await callback.answer()
        return
    appeal_id = int(parts[3])
    page = int(parts[4])
    await db.update_appeal_status(appeal_id, "reviewed")
    await callback.answer("Отмечено как рассмотрено.")
    await show_appeals_page(callback, db, page)


@router.callback_query(F.data == "admin:invoices")
async def admin_invoices(callback: CallbackQuery, db: Database, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await send_pending_invoices(callback.bot, callback.from_user.id, db)
    await callback.answer()


@router.callback_query(F.data == "admin:notify")
async def admin_notify_menu(callback: CallbackQuery, db: Database, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    enabled = await is_new_user_notify_enabled(db)
    status = "включены" if enabled else "выключены"
    text = (
        "🔔 <b>Уведомления админам</b>\n"
        f"Новые пользователи: <b>{status}</b>"
    )
    try:
        await callback.message.edit_text(text, reply_markup=admin_notify_kb(enabled))
    except TelegramBadRequest:
        await callback.message.answer(text, reply_markup=admin_notify_kb(enabled))
    await callback.answer()


@router.callback_query(F.data == "admin:notify:on")
async def admin_notify_on(callback: CallbackQuery, db: Database, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await db.set_setting(NEW_USER_NOTIFY_SETTING, "1")
    enabled = True
    text = (
        "🔔 <b>Уведомления админам</b>\n"
        "Новые пользователи: <b>включены</b>"
    )
    try:
        await callback.message.edit_text(text, reply_markup=admin_notify_kb(enabled))
    except TelegramBadRequest:
        await callback.message.answer(text, reply_markup=admin_notify_kb(enabled))
    await callback.answer("Уведомления включены.")


@router.callback_query(F.data == "admin:notify:off")
async def admin_notify_off(callback: CallbackQuery, db: Database, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await db.set_setting(NEW_USER_NOTIFY_SETTING, "0")
    enabled = False
    text = (
        "🔔 <b>Уведомления админам</b>\n"
        "Новые пользователи: <b>выключены</b>"
    )
    try:
        await callback.message.edit_text(text, reply_markup=admin_notify_kb(enabled))
    except TelegramBadRequest:
        await callback.message.answer(text, reply_markup=admin_notify_kb(enabled))
    await callback.answer("Уведомления выключены.")


@router.callback_query(F.data == "admin:tx")
async def admin_tx(callback: CallbackQuery, db: Database, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await send_recent_transactions(callback.bot, callback.from_user.id, db)
    await callback.answer()


@router.callback_query(F.data == "admin:blocks")
async def admin_blocks(callback: CallbackQuery, db: Database, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await send_blocks(callback.bot, callback.from_user.id, db)
    await callback.answer()


@router.callback_query(F.data == "admin:credit")
async def admin_credit(callback: CallbackQuery, state: FSMContext, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await state.update_data(balance_mode="credit")
    await state.set_state(AdminState.balance_user)
    await callback.message.answer("Введите ID пользователя:", reply_markup=cancel_kb())
    await callback.answer()


@router.callback_query(F.data == "admin:debit")
async def admin_debit(callback: CallbackQuery, state: FSMContext, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await state.update_data(balance_mode="debit")
    await state.set_state(AdminState.balance_user)
    await callback.message.answer("Введите ID пользователя:", reply_markup=cancel_kb())
    await callback.answer()


@router.callback_query(F.data == "admin:unblock")
async def admin_unblock(callback: CallbackQuery, state: FSMContext, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await state.set_state(AdminState.unblock_user)
    await callback.message.answer("Введите ID пользователя для разбана:", reply_markup=cancel_kb())
    await callback.answer()


@router.callback_query(F.data == "admin:block")
async def admin_block(callback: CallbackQuery, state: FSMContext, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await state.set_state(AdminState.block_user)
    await callback.message.answer("Введите ID пользователя для блокировки:", reply_markup=cancel_kb())
    await callback.answer()


@router.message(F.text == "Пользователь")
async def admin_user_lookup(message: Message, state: FSMContext, config: Config) -> None:
    if not is_admin(message.from_user.id, config):
        return
    await state.set_state(AdminState.user_lookup)
    await message.answer("Введите ID пользователя:", reply_markup=cancel_kb())


@router.callback_query(F.data == "admin:user")
async def admin_user_lookup_cb(callback: CallbackQuery, state: FSMContext, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await state.set_state(AdminState.user_lookup)
    await callback.message.answer("Введите ID пользователя:", reply_markup=cancel_kb())
    await callback.answer()


@router.message(AdminState.user_lookup)
async def admin_user_show(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    if not is_admin(message.from_user.id, config):
        return
    try:
        user_id = int((message.text or "").strip())
    except ValueError:
        await message.answer("Введите корректный ID.")
        return
    user = await db.get_user(user_id)
    if not user:
        await message.answer("Пользователь не найден.")
        await state.clear()
        return
    block = await db.get_user_block(user_id)
    if block and int(block.get("blocked_until") or 0) <= now_ts():
        await db.clear_user_block(user_id)
        block = None
    text, is_blocked = _build_admin_user_text(user, block)
    await message.answer(text, reply_markup=admin_user_actions_kb(user_id, is_blocked))
    await state.clear()


@router.callback_query(F.data.startswith("admin:user:refresh:"))
async def admin_user_refresh(callback: CallbackQuery, db: Database, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    try:
        user_id = int(callback.data.split(":")[3])
    except (ValueError, IndexError):
        await callback.answer("Некорректный ID.", show_alert=True)
        return
    user = await db.get_user(user_id)
    if not user:
        await callback.answer("Пользователь не найден.", show_alert=True)
        return
    block = await db.get_user_block(user_id)
    if block and int(block.get("blocked_until") or 0) <= now_ts():
        await db.clear_user_block(user_id)
        block = None
    text, is_blocked = _build_admin_user_text(user, block)
    try:
        await callback.message.edit_text(text, reply_markup=admin_user_actions_kb(user_id, is_blocked))
    except TelegramBadRequest:
        await callback.message.answer(text, reply_markup=admin_user_actions_kb(user_id, is_blocked))
    await callback.answer()


@router.callback_query(F.data.startswith("admin:user:unblock:confirm:"))
async def admin_user_unblock_confirm(callback: CallbackQuery, db: Database, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    try:
        user_id = int(callback.data.split(":")[4])
    except (ValueError, IndexError):
        await callback.answer("Некорректный ID.", show_alert=True)
        return
    await db.clear_user_block(user_id)
    user = await db.get_user(user_id)
    if not user:
        await callback.answer("Пользователь не найден.", show_alert=True)
        return
    text, _ = _build_admin_user_text(user, None)
    try:
        await callback.message.edit_text(text, reply_markup=admin_user_actions_kb(user_id, False))
    except TelegramBadRequest:
        await callback.message.answer(text, reply_markup=admin_user_actions_kb(user_id, False))
    await callback.answer("Пользователь разблокирован.", show_alert=True)


@router.callback_query(F.data.startswith("admin:user:unblock:"))
async def admin_user_unblock(callback: CallbackQuery, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    try:
        user_id = int(callback.data.split(":")[3])
    except (ValueError, IndexError):
        await callback.answer("Некорректный ID.", show_alert=True)
        return
    text = f"Разбанить пользователя <code>{user_id}</code>?"
    try:
        await callback.message.edit_text(text, reply_markup=admin_user_unblock_confirm_kb(user_id))
    except TelegramBadRequest:
        await callback.message.answer(text, reply_markup=admin_user_unblock_confirm_kb(user_id))
    await callback.answer()


@router.callback_query(F.data.startswith("admin:user:block:"))
async def admin_user_block(callback: CallbackQuery, state: FSMContext, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    try:
        user_id = int(callback.data.split(":")[3])
    except (ValueError, IndexError):
        await callback.answer("Некорректный ID.", show_alert=True)
        return
    await state.update_data(block_user_id=user_id)
    await state.set_state(AdminState.block_duration)
    await callback.message.answer(
        f"Введите длительность блокировки для <code>{user_id}</code> (например 30m, 2h, 1d):",
        reply_markup=cancel_kb(),
    )
    await callback.answer()


@router.message(AdminState.balance_user)
async def admin_balance_user(message: Message, state: FSMContext, config: Config) -> None:
    if not is_admin(message.from_user.id, config):
        return
    try:
        user_id = int((message.text or "").strip())
    except ValueError:
        await message.answer("Введите корректный ID.")
        return
    await state.update_data(balance_user_id=user_id)
    await state.set_state(AdminState.balance_amount)
    await message.answer("Введите сумму в BIT:", reply_markup=cancel_kb())


@router.message(AdminState.balance_amount)
async def admin_balance_amount(message: Message, state: FSMContext, config: Config) -> None:
    if not is_admin(message.from_user.id, config):
        return
    try:
        amount = int((message.text or "").strip())
    except ValueError:
        await message.answer("Введите число.")
        return
    if amount <= 0:
        await message.answer("Сумма должна быть больше нуля.")
        return
    await state.update_data(balance_amount=amount)
    await state.set_state(AdminState.balance_reason)
    await message.answer("Причина (или Пропустить):", reply_markup=cancel_kb())


@router.message(AdminState.balance_reason)
async def admin_balance_reason(
    message: Message, state: FSMContext, db: Database, config: Config
) -> None:
    if not is_admin(message.from_user.id, config):
        return
    data = await state.get_data()
    user_id = data.get("balance_user_id")
    amount = int(data.get("balance_amount") or 0)
    mode = data.get("balance_mode")
    if not user_id or amount <= 0:
        await message.answer("Данные не распознаны.")
        await state.clear()
        return
    note = (message.text or "").strip()
    if note.lower() == "пропустить" or not note:
        note = "admin_credit" if mode == "credit" else "admin_debit"
    signed_amount = amount if mode == "credit" else -amount
    await db.add_balance(
        user_id,
        signed_amount,
        note,
        {"admin_id": message.from_user.id},
    )
    await message.answer(
        f"Готово. Пользователь <code>{user_id}</code> "
        f"{'получил' if signed_amount > 0 else 'потерял'} "
        f"<b>{format_coins(abs(signed_amount))}</b> BIT.",
        reply_markup=admin_menu_kb(),
    )
    await state.clear()


@router.message(AdminState.block_user)
async def admin_block_user(message: Message, state: FSMContext, config: Config) -> None:
    if not is_admin(message.from_user.id, config):
        return
    try:
        user_id = int((message.text or "").strip())
    except ValueError:
        await message.answer("Введите корректный ID.")
        return
    await state.update_data(block_user_id=user_id)
    await state.set_state(AdminState.block_duration)
    await message.answer("Введите длительность (например 30m, 2h, 1d):", reply_markup=cancel_kb())


@router.message(AdminState.block_duration)
async def admin_block_duration(
    message: Message, state: FSMContext, db: Database, config: Config
) -> None:
    if not is_admin(message.from_user.id, config):
        return
    seconds = parse_duration(message.text or "")
    if not seconds or seconds <= 0:
        await message.answer("Неверный формат. Пример: 30m, 2h, 1d.")
        return
    data = await state.get_data()
    user_id = data.get("block_user_id")
    if not user_id:
        await message.answer("ID не найден.")
        await state.clear()
        return
    await db.set_user_block(user_id, now_ts() + seconds, "Блокировка админом")
    await message.answer(
        f"Пользователь <code>{user_id}</code> заблокирован на {seconds} сек.",
        reply_markup=admin_menu_kb(),
    )
    await state.clear()


@router.message(AdminState.unblock_user)
async def admin_unblock_user(
    message: Message, state: FSMContext, db: Database, config: Config
) -> None:
    if not is_admin(message.from_user.id, config):
        return
    try:
        user_id = int((message.text or "").strip())
    except ValueError:
        await message.answer("Введите корректный ID.")
        return
    await db.clear_user_block(user_id)
    await message.answer(
        f"Пользователь <code>{user_id}</code> разблокирован.",
        reply_markup=admin_menu_kb(),
    )
    await state.clear()


@router.message(F.text.in_(["Промокод", "Промокоды"]))
async def admin_promo_start(message: Message, state: FSMContext, config: Config) -> None:
    if not is_admin(message.from_user.id, config):
        return
    await state.set_state(AdminState.promo_code)
    await message.answer("Введите код промокода:", reply_markup=cancel_kb())


@router.callback_query(F.data == "admin:promo")
async def admin_promo_start_cb(callback: CallbackQuery, state: FSMContext, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await state.set_state(AdminState.promo_code)
    await callback.message.answer("Введите код промокода:", reply_markup=cancel_kb())
    await callback.answer()


@router.message(AdminState.promo_code)
async def admin_promo_code(message: Message, state: FSMContext, config: Config) -> None:
    if not is_admin(message.from_user.id, config):
        return
    code = (message.text or "").strip()
    if not code:
        await message.answer("Код не распознан.")
        return
    await state.update_data(code=code)
    await state.set_state(AdminState.promo_amount)
    await message.answer("Введите сумму в BIT:", reply_markup=cancel_kb())


@router.message(AdminState.promo_amount)
async def admin_promo_amount(message: Message, state: FSMContext, config: Config) -> None:
    if not is_admin(message.from_user.id, config):
        return
    try:
        amount = int((message.text or "").strip())
    except ValueError:
        await message.answer("Введите число.")
        return
    if amount <= 0:
        await message.answer("Сумма должна быть больше нуля.")
        return
    await state.update_data(amount=amount)
    await state.set_state(AdminState.promo_max_uses)
    await message.answer("Введите максимальное число использований:", reply_markup=cancel_kb())


@router.message(AdminState.promo_max_uses)
async def admin_promo_max(message: Message, state: FSMContext, config: Config) -> None:
    if not is_admin(message.from_user.id, config):
        return
    try:
        max_uses = int((message.text or "").strip())
    except ValueError:
        await message.answer("Введите число.")
        return
    if max_uses <= 0:
        await message.answer("Количество должно быть больше нуля.")
        return
    await state.update_data(max_uses=max_uses)
    await state.set_state(AdminState.promo_expires)
    await message.answer(
        "Введите срок действия в днях (или 0 без ограничения):",
        reply_markup=cancel_kb(),
    )


@router.message(AdminState.promo_expires)
async def admin_promo_finish(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    if not is_admin(message.from_user.id, config):
        return
    text = (message.text or "").strip()
    if text.lower() in {"пропустить", "-"}:
        days = 0
    else:
        try:
            days = int(text)
        except ValueError:
            await message.answer("Введите число.")
            return
    data = await state.get_data()
    expires_at = None if days <= 0 else now_ts() + days * 24 * 60 * 60
    await db.create_promo(data["code"], data["amount"], data["max_uses"], expires_at)
    await message.answer("Промокод создан.", reply_markup=admin_menu_kb())
    await state.clear()


@router.callback_query(F.data == "admin:promo:list")
async def admin_promo_list(callback: CallbackQuery, db: Database, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    promos = await db.list_promos(active_only=False)
    if not promos:
        await callback.message.answer("Промокоды не найдены.", reply_markup=admin_menu_kb())
        await callback.answer()
        return
    lines = ["🎁 <b>Промокоды</b>\n"]
    now = now_ts()
    for promo in promos:
        active = "✅" if int(promo.get("active") or 0) else "⛔️"
        expires_at = promo.get("expires_at")
        if expires_at:
            expires_text = datetime.fromtimestamp(int(expires_at)).strftime("%Y-%m-%d")
        else:
            expires_text = "без срока"
        lines.append(
            f"{active} <code>{promo['code']}</code> • {format_coins(promo['amount'])} "
            f"• {promo['used_count']}/{promo['max_uses']} • {expires_text}"
        )
    await callback.message.answer("\n".join(lines), reply_markup=admin_menu_kb())
    await callback.answer()


@router.callback_query(F.data == "admin:promo:delete")
async def admin_promo_delete_start(callback: CallbackQuery, state: FSMContext, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await state.set_state(AdminState.promo_delete_code)
    await callback.message.answer("Введите код промокода для удаления:", reply_markup=cancel_kb())
    await callback.answer()


@router.message(AdminState.promo_delete_code)
async def admin_promo_delete(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    if not is_admin(message.from_user.id, config):
        return
    code = (message.text or "").strip()
    if not code:
        await message.answer("Код не распознан.")
        return
    if not await db.deactivate_promo(code):
        await message.answer("Промокод не найден.")
        return
    await message.answer("Промокод удален.", reply_markup=admin_menu_kb())
    await state.clear()


@router.callback_query(F.data == "admin:tasks:manage")
async def admin_tasks_manage_start(callback: CallbackQuery, state: FSMContext, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await state.set_state(AdminState.admin_user_id)
    await callback.message.answer("Введите ID пользователя для управления его заданиями:", reply_markup=cancel_kb())
    await callback.answer()


@router.callback_query(F.data == "admin:create:as_user")
async def admin_create_as_user_start(
    callback: CallbackQuery, state: FSMContext, config: Config
) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await state.set_state(AdminState.admin_create_task_owner)
    await callback.message.answer(
        "Введите ID пользователя, от имени которого нужно создать задание:",
        reply_markup=cancel_kb(),
    )
    await callback.answer()


@router.message(AdminState.admin_create_task_owner)
async def admin_create_as_user_owner(
    message: Message, state: FSMContext, db: Database, config: Config
) -> None:
    if not is_admin(message.from_user.id, config):
        return
    try:
        owner_id = int((message.text or "").strip())
    except ValueError:
        await message.answer("Введите числовой ID пользователя.")
        return
    user = await db.get_user(owner_id)
    if not user:
        await message.answer("Пользователь не найден в базе. Сначала он должен запустить бота.")
        return
    await start_create_flow(
        message.bot,
        message.from_user.id,
        state,
        db,
        owner_id=owner_id,
    )


@router.callback_query(F.data.startswith("admin:task:block:"))
async def admin_task_block_start(callback: CallbackQuery, state: FSMContext, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    task_id = int(callback.data.split(":")[3])
    await state.update_data(block_task_id=task_id)
    await state.set_state(AdminState.admin_task_block_reason)
    await callback.message.answer(
        "🚫 Введите причину блокировки задания (она будет показана пользователю):",
        reply_markup=cancel_kb()
    )
    await callback.answer()

@router.message(AdminState.admin_task_block_reason)
async def admin_task_block_reason(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    if not is_admin(message.from_user.id, config):
        return
    data = await state.get_data()
    task_id = data.get("block_task_id")
    reason = (message.text or "").strip()
    if not reason:
        await message.answer("Причина не может быть пустой.")
        return

    # Получаем задание
    task = await db.get_task(task_id)
    if not task:
        await message.answer("Задание не найдено.")
        await state.clear()
        return

    # Устанавливаем статус blocked и сохраняем причину
    await db.set_task_status(task_id, "blocked", reason)

    # Отправляем уведомление владельцу задания
    try:
        await message.bot.send_message(
            task["owner_id"],
            f"🚫 <b>Ваше задание #{task_id} заблокировано администратором.</b>\n\n"
            f"Причина: {escape(reason)}\n\n"
            f"Если у Вас остались вопросы, напишите нам @ameropro",
            parse_mode="HTML"
        )
    except (TelegramBadRequest, TelegramForbiddenError):
        pass

    await message.answer(
        f"✅ Задание #{task_id} заблокировано. Причина сохранена.",
        reply_markup=admin_menu_kb()
    )
    await state.clear()

@router.message(AdminState.admin_user_id)
async def admin_tasks_list(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    if not is_admin(message.from_user.id, config):
        return
    try:
        user_id = int((message.text or "").strip())
    except ValueError:
        await message.answer("Введите число.")
        return
    
    # Получаем все задания пользователя
    tasks = await db.fetchall(
        "SELECT id, title, type, reward, active, remaining_count, total_count, created_at FROM tasks WHERE owner_id = ? AND deleted = 0 ORDER BY created_at DESC",
        (user_id,)
    )
    
    if not tasks:
        await message.answer(f"У пользователя {user_id} нет активных заданий.")
        await state.clear()
        return
    
    lines = [f"📋 <b>Задания пользователя {user_id}</b> ({len(tasks)} шт.):\n"]
    for task in tasks:
        status = "🟢" if task['active'] else "⏸"
        type_label = ADV_TASK_TYPE_LABELS.get(task['type'], task['type'])
        remaining = task['remaining_count']
        lines.append(
            f"{status} <b>#{task['id']}</b> | {escape(task['title'][:30])} | "
            f"{type_label} | Награда: {format_coins(task['reward'])} BIT | "
            f"Осталось: {remaining}"
        )
    
    text = "\n".join(lines)
    builder = InlineKeyboardBuilder()
    builder.button(text="📝 Редактировать задание", callback_data="admin:task:edit:id")
    builder.button(text="⏸ Приостановить задание", callback_data="admin:task:pause:id")
    builder.button(text="▶️ Возобновить задание", callback_data="admin:task:resume:id")
    builder.button(text="🗑 Удалить задание", callback_data="admin:task:del:id")
    builder.button(text="⬅️ Админ", callback_data="menu:admin")
    
    await message.answer(text, reply_markup=builder.as_markup(), parse_mode="HTML")
    await state.update_data(admin_user_id=user_id)


@router.callback_query(F.data == "admin:task:edit:id")
async def admin_task_edit_start(callback: CallbackQuery, state: FSMContext, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await state.set_state(AdminState.admin_task_edit)
    await callback.message.answer("Введите ID задания для редактирования:", reply_markup=cancel_kb())
    await callback.answer()


@router.message(AdminState.admin_task_edit)
async def admin_task_edit_select(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    if not is_admin(message.from_user.id, config):
        return
    try:
        task_id = int((message.text or "").strip())
    except ValueError:
        await message.answer("Введите число.")
        return
    
    task = await db.get_task(task_id)
    if not task:
        await message.answer("Задание не найдено.")
        await state.clear()
        return
    
    # Показываем текущие параметры и кнопки редактирования
    type_label = ADV_TASK_TYPE_LABELS.get(task['type'], task['type'])
    text = (
        f"📝 <b>Редактирование задания #{task_id}</b>\n\n"
        f"<b>Текущие данные:</b>\n"
        f"Название: <b>{escape(task['title'])}</b>\n"
        f"Тип: <b>{type_label}</b>\n"
        f"Награда: <b>{format_coins(task['reward'])}</b> BIT\n"
        f"Описание: <b>{escape(task.get('description', 'Нет'))}</b>\n\n"
        f"Выберите что редактировать:"
    )
    
    builder = InlineKeyboardBuilder()
    builder.button(text="✏️ Название", callback_data=f"admin:edit:title:{task_id}")
    builder.button(text="💰 Награда", callback_data=f"admin:edit:reward:{task_id}")
    builder.button(text="📝 Описание", callback_data=f"admin:edit:desc:{task_id}")
    builder.button(text="⬅️ Назад", callback_data="menu:admin")
    
    await message.answer(text, reply_markup=builder.as_markup(), parse_mode="HTML")
    await state.clear()


@router.callback_query(F.data.startswith("admin:edit:title:"))
async def admin_edit_title_start(callback: CallbackQuery, state: FSMContext, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    task_id = int(callback.data.split(":")[-1])
    await state.set_state(AdminState.admin_task_edit_title)
    await state.update_data(edit_task_id=task_id)
    await callback.message.answer("Введите новое название для задания:", reply_markup=cancel_kb())
    await callback.answer()


@router.message(AdminState.admin_task_edit_title)
async def admin_edit_title(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    if not is_admin(message.from_user.id, config):
        return
    
    data = await state.get_data()
    task_id = data.get("edit_task_id")
    new_title = (message.text or "").strip()
    
    if not new_title:
        await message.answer("Название не может быть пусто.")
        return
    
    if len(new_title) > 100:
        await message.answer("Название не может быть длиннее 100 символов.")
        return
    
    await db.execute(
        "UPDATE tasks SET title = ? WHERE id = ?",
        (new_title, task_id)
    )
    
    await message.answer(f"✅ Название задания #{task_id} изменено на: <b>{escape(new_title)}</b>", parse_mode="HTML")
    await state.clear()


@router.callback_query(F.data.startswith("admin:edit:reward:"))
async def admin_edit_reward_start(callback: CallbackQuery, state: FSMContext, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    task_id = int(callback.data.split(":")[-1])
    await state.set_state(AdminState.admin_task_edit_reward)
    await state.update_data(edit_task_id=task_id)
    await callback.message.answer("Введите новую награду в BIT:", reply_markup=cancel_kb())
    await callback.answer()


@router.message(AdminState.admin_task_edit_reward)
async def admin_edit_reward(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    if not is_admin(message.from_user.id, config):
        return
    
    data = await state.get_data()
    task_id = data.get("edit_task_id")
    
    try:
        new_reward = int((message.text or "").strip())
    except ValueError:
        await message.answer("Введите число.")
        return
    
    if new_reward <= 0:
        await message.answer("Награда должна быть больше 0.")
        return
    
    await db.execute(
        "UPDATE tasks SET reward = ? WHERE id = ?",
        (new_reward, task_id)
    )
    
    await message.answer(f"✅ Награда задания #{task_id} изменена на: <b>{format_coins(new_reward)}</b> BIT", parse_mode="HTML")
    await state.clear()


@router.callback_query(F.data.startswith("admin:edit:desc:"))
async def admin_edit_desc_start(callback: CallbackQuery, state: FSMContext, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    task_id = int(callback.data.split(":")[-1])
    await state.set_state(AdminState.admin_task_edit_desc)
    await state.update_data(edit_task_id=task_id)
    await callback.message.answer("Введите новое описание для задания:", reply_markup=cancel_kb())
    await callback.answer()


@router.message(AdminState.admin_task_edit_desc)
async def admin_edit_desc(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    if not is_admin(message.from_user.id, config):
        return
    
    data = await state.get_data()
    task_id = data.get("edit_task_id")
    new_desc = (message.text or "").strip()
    
    await db.execute(
        "UPDATE tasks SET description = ? WHERE id = ?",
        (new_desc or None, task_id)
    )
    
    await message.answer(f"✅ Описание задания #{task_id} изменено.", parse_mode="HTML")
    await state.clear()


@router.callback_query(F.data == "admin:task:pause:id")
async def admin_task_pause_start(callback: CallbackQuery, state: FSMContext, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await state.set_state(AdminState.admin_task_pause)
    await callback.message.answer("Введите ID задания для приостановки:", reply_markup=cancel_kb())
    await callback.answer()


@router.message(AdminState.admin_task_pause)
async def admin_task_pause(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    if not is_admin(message.from_user.id, config):
        return
    try:
        task_id = int((message.text or "").strip())
    except ValueError:
        await message.answer("Введите число.")
        return
    
    task = await db.get_task(task_id)
    if not task:
        await message.answer("Задание не найдено.")
        await state.clear()
        return
    
    await db.pause_task(task_id, "Приостановлено администратором")
    await message.answer(f"✅ Задание #{task_id} приостановлено.")
    await state.clear()


@router.callback_query(F.data == "admin:task:resume:id")
async def admin_task_resume_start(callback: CallbackQuery, state: FSMContext, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await state.set_state(AdminState.admin_task_resume)
    await callback.message.answer("Введите ID задания для возобновления:", reply_markup=cancel_kb())
    await callback.answer()


@router.message(AdminState.admin_task_resume)
async def admin_task_resume(message: Message, state: FSMContext, db: Database, config: Config, bot: Bot) -> None:
    if not is_admin(message.from_user.id, config):
        return
    try:
        task_id = int((message.text or "").strip())
    except ValueError:
        await message.answer("Введите число.")
        return
    
    task = await db.get_task(task_id)
    if not task:
        await message.answer("Задание не найдено.")
        await state.clear()
        return
    
    # Проверяем, были ли решены проблемы, вызвавшие предупреждение
    verification = await verify_task_for_resume(bot, task)
    
    if not verification["can_resume"]:
        # Проблемы всё ещё существуют - не возобновляем
        message_text = f"⚠️ <b>Не удалось возобновить задание #{task_id}</b>\n\n{verification['human_message']}"
        if verification["human_action"]:
            message_text += f"\n\n<b>Рекомендуемое действие:</b> {verification['human_action']}"
        await message.answer(message_text, parse_mode="HTML")
        await state.clear()
        return
    
    # Проблемы решены или задание не имело проблем - восстанавливаем
    await db.set_task_status(task_id, "active")
    
    success_text = f"✅ Задание #{task_id} возобновлено."
    if verification["problems_fixed"]:
        success_text += "\n\n" + verification["human_message"]
    
    await message.answer(success_text, parse_mode="HTML")
    await state.clear()


@router.callback_query(F.data == "admin:task:del:id")
async def admin_task_delete_start(callback: CallbackQuery, state: FSMContext, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await state.set_state(AdminState.admin_task_delete)
    await callback.message.answer("Введите ID задания для удаления:", reply_markup=cancel_kb())
    await callback.answer()


@router.message(AdminState.admin_task_delete)
async def admin_task_delete(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    if not is_admin(message.from_user.id, config):
        return
    try:
        task_id = int((message.text or "").strip())
    except ValueError:
        await message.answer("Введите число.")
        return
    
    task = await db.get_task(task_id)
    if not task:
        await message.answer("Задание не найдено.")
        await state.clear()
        return
    
    owner_id = task['owner_id']
    refund = await db.delete_task(owner_id, task_id)
    if refund is None:
        await message.answer("Не удалось удалить задание.")
        await state.clear()
        return
    
    await message.answer(f"✅ Задание #{task_id} удалено. Возвращено {format_coins(refund)} BIT владельцу.")
    await state.clear()


@router.callback_query(F.data == "admin:task:info")
async def admin_task_info_start(callback: CallbackQuery, state: FSMContext, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await state.set_state(AdminState.task_info_id)
    await callback.message.answer("Введите ID задания:", reply_markup=cancel_kb())
    await callback.answer()


@router.message(AdminState.task_info_id)
async def admin_task_info(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    if not is_admin(message.from_user.id, config):
        return
    try:
        task_id = int((message.text or "").strip())
    except ValueError:
        await message.answer("Введите число.")
        return
    task = await db.get_task(task_id)
    if not task:
        await message.answer("Задание не найдено.")
        await state.clear()
        return
    stats = await db.get_task_claim_stats(task_id)
    remaining = int(task.get("remaining_count") or 0)
    total = int(task.get("total_count") or 0)
    completed = max(0, total - remaining)
    active = int(task.get("active") or 0)
    deleted = int(task.get("deleted") or 0)
    task_status = task.get("task_status", "active")
    if deleted:
        status = "🗑 Удалено"
    elif remaining <= 0:
        status = "✅ Завершено"
    elif task_status == "blocked":
        status = "🚫 Заблокировано"
    elif task_status in ("bot_removed", "no_permissions"):
        status = "🚫 Проблема"
    elif active:
        status = "🟢 Активно"
    else:
        status = "⏸ Пауза"
    type_label = ADV_TASK_TYPE_LABELS.get(task["type"], task["type"])
    if task["type"] == "subscribe":
        if task.get("chat_type") == "channel":
            type_label = f"{type_label} (канал)"
        elif task.get("chat_type"):
            type_label = f"{type_label} (группа)"
    created_at = int(task.get("created_at") or 0)
    created_text = datetime.fromtimestamp(created_at).strftime("%Y-%m-%d %H:%M")
    lines = [
        f"🧾 <b>Задание #{task_id}</b>",
        f"Тип: <b>{escape(type_label)}</b>",
        f"Владелец: <code>{task['owner_id']}</code>",
        f"Название: <b>{escape(task['title'])}</b>",
        f"Награда: <b>{format_coins(task['reward'])}</b> BIT",
        f"Статус: <b>{status}</b>",
        f"Создано: <b>{created_text}</b>",
    ]
    chat_title = task.get("chat_title") or task.get("chat_username")
    if chat_title:
        lines.append(f"Чат: <b>{escape(chat_title)}</b>")
    lines.extend(
        [
            "",
            f"Выполнено: <b>{completed}</b> из <b>{total}</b>",
            f"Осталось: <b>{remaining}</b>",
            f"Эскроу: <b>{format_coins(task['escrow_balance'])}</b> BIT",
            "",
            "Заявки:",
            f"🕒 На проверке: <b>{stats['submitted']}</b>",
            f"✅ Завершено: <b>{stats['completed'] + stats['completed_hold']}</b>",
            f"📝 На доработке: <b>{stats['revision']}</b>",
            f"❌ Отклонено: <b>{stats['rejected']}</b>",
            f"🚫 Отозвано: <b>{stats['revoked']}</b>",
        ]
    )
    builder = InlineKeyboardBuilder()
    builder.button(text="✏️ Редактировать", callback_data=f"admin:task:edit:{task_id}")
    builder.button(text="🚫 Заблокировать", callback_data=f"admin:task:block:{task_id}")
    builder.button(text="⬅️ Назад", callback_data="admin:back")
    builder.adjust(1)
    await message.answer("\n".join(lines), reply_markup=builder.as_markup())
    await state.clear()


@router.callback_query(F.data == "admin:task:delete")
async def admin_task_delete_start(callback: CallbackQuery, state: FSMContext, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await state.set_state(AdminState.task_delete_id)
    await callback.message.answer("Введите ID задания для удаления:", reply_markup=cancel_kb())
    await callback.answer()


@router.message(AdminState.task_delete_id)
async def admin_task_delete(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    if not is_admin(message.from_user.id, config):
        return
    try:
        task_id = int((message.text or "").strip())
    except ValueError:
        await message.answer("Введите число.")
        return
    refund = await db.delete_task_by_admin(task_id)
    if refund is None:
        await message.answer("Задание не найдено.")
        return
    text = (
        f"✅ Задание удалено. Возврат владельцу: {format_coins(refund)} BIT."
        if refund > 0
        else "✅ Задание удалено."
    )
    await message.answer(text, reply_markup=admin_menu_kb())
    await state.clear()


@router.message(F.text == "Спонсоры")
async def admin_sponsors(message: Message, db: Database, config: Config) -> None:
    if not is_admin(message.from_user.id, config):
        return
    await send_sponsors_list(message.bot, message.from_user.id, db)


@router.callback_query(F.data == "admin:sponsors")
async def admin_sponsors_cb(callback: CallbackQuery, db: Database, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await send_sponsors_list(callback.bot, callback.from_user.id, db)
    await callback.answer()


@router.callback_query(F.data == "admin:sponsor:add")
async def admin_sponsor_add(callback: CallbackQuery, state: FSMContext, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await state.update_data(sponsor_mode="add")
    await state.set_state(AdminState.sponsor_chat)
    await callback.message.answer("Введите @username или ID канала/группы:")
    await callback.answer()


@router.callback_query(F.data == "admin:sponsor:remove")
async def admin_sponsor_remove(callback: CallbackQuery, state: FSMContext, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await state.update_data(sponsor_mode="remove")
    await state.set_state(AdminState.sponsor_action)
    await callback.message.answer("Введите ID спонсора для удаления:")
    await callback.answer()


@router.message(AdminState.sponsor_action)
async def admin_sponsor_remove_confirm(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    if not is_admin(message.from_user.id, config):
        return
    try:
        sponsor_id = int((message.text or "").strip())
    except ValueError:
        await message.answer("Введите корректный ID.")
        return
    await db.deactivate_sponsor(sponsor_id)
    await message.answer("Спонсор деактивирован.", reply_markup=admin_menu_kb())
    await state.clear()


@router.message(AdminState.sponsor_chat)
async def admin_sponsor_chat(message: Message, state: FSMContext, bot: Bot, config: Config) -> None:
    if not is_admin(message.from_user.id, config):
        return
    chat_ref = (message.text or "").strip()
    if "t.me/" in chat_ref:
        chat_ref = chat_ref.split("t.me/")[-1].strip("/")
        if not chat_ref.startswith("@"):
            chat_ref = f"@{chat_ref}"
    if not chat_ref:
        await message.answer("Введите @username или ID.")
        return
    try:
        chat = await bot.get_chat(chat_ref)
    except Exception:
        await message.answer("Чат не найден.")
        return
    await state.update_data(
        chat_id=chat.id,
        chat_username=chat.username,
        title=chat.title or chat.username,
    )
    await state.set_state(AdminState.sponsor_link)
    await message.answer("Введите invite ссылку (или Пропустить):", reply_markup=cancel_kb())


@router.message(AdminState.sponsor_link)
async def admin_sponsor_link(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    if not is_admin(message.from_user.id, config):
        return
    link = (message.text or "").strip()
    if link.lower() == "пропустить":
        link = None
    data = await state.get_data()
    await db.add_sponsor(
        data["chat_id"],
        data.get("chat_username"),
        data.get("title"),
        link,
    )
    await message.answer("Спонсор добавлен.", reply_markup=admin_menu_kb())
    await state.clear()


@router.message(F.text == "Рассылка")
async def admin_broadcast_start(message: Message, state: FSMContext, config: Config) -> None:
    if not is_admin(message.from_user.id, config):
        return
    await state.set_state(AdminState.broadcast_text)
    await message.answer("Введите текст рассылки (HTML поддерживается):", reply_markup=cancel_kb())


@router.callback_query(F.data == "admin:broadcast")
async def admin_broadcast_start_cb(callback: CallbackQuery, state: FSMContext, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    await state.set_state(AdminState.broadcast_text)
    await callback.message.answer("Введите текст рассылки (HTML поддерживается):", reply_markup=cancel_kb())
    await callback.answer()


@router.callback_query(F.data == "admin:dump")
async def admin_dump(callback: CallbackQuery, db: Database, config: Config) -> None:
    if not is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа.", show_alert=True)
        return
    user_ids = await db.list_user_ids()
    dump_path = Path("users.txt")
    content = "\n".join(str(user_id) for user_id in user_ids)
    if content:
        content += "\n"
    dump_path.write_text(content, encoding="utf-8")
    await callback.message.answer_document(
        FSInputFile(str(dump_path)),
        caption=f"📦 Дамп базы: {len(user_ids)} пользователей",
    )
    await callback.answer()


@router.message(AdminState.broadcast_text)
async def admin_broadcast_text(message: Message, state: FSMContext, config: Config) -> None:
    if not is_admin(message.from_user.id, config):
        return
    text = message.html_text or message.text
    if not text:
        await message.answer("Текст не распознан.")
        return
    await state.update_data(broadcast_text=text)
    await state.set_state(AdminState.broadcast_button)
    await message.answer("Текст кнопки (или Пропустить):", reply_markup=cancel_kb())


async def _send_broadcast(
    bot: Bot,
    db: Database,
    text: str,
    button_text: str | None,
    button_url: str | None,
) -> tuple[int, int, int, float]:
    keyboard = None
    if button_text and button_url:
        builder = InlineKeyboardBuilder()
        builder.button(text=button_text, url=button_url)
        keyboard = builder.as_markup()
    users = await db.list_user_ids()
    sent = 0
    failed = 0
    started = time.monotonic()
    for user_id in users:
        try:
            await bot.send_message(user_id, text, reply_markup=keyboard)
            sent += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)
    duration = time.monotonic() - started
    return sent, failed, len(users), duration


def format_broadcast_summary(sent: int, failed: int, total: int, duration: float) -> str:
    total_attempts = max(total, sent + failed)
    success_pct = (sent / total_attempts * 100.0) if total_attempts else 0.0
    speed = (total_attempts / duration) if duration > 0 else 0.0
    return (
        "✅ <b>Рассылка завершена!</b>\n\n"
        f"📨 Успешно отправлено: <b>{sent}</b> ({success_pct:.1f}%)\n"
        f"Неудачные: <b>{failed}</b>\n\n"
        f"⏳ Время выполнения: <b>{duration:.1f} сек</b>\n"
        f"🚀 Средняя скорость: <b>{speed:.1f} сообщ/сек</b> "
        f"({speed * 60:.1f} сообщ/мин)"
    )


@router.message(AdminState.broadcast_button)
async def admin_broadcast_button(
    message: Message, state: FSMContext, db: Database, config: Config
) -> None:
    if not is_admin(message.from_user.id, config):
        return
    text = (message.text or "").strip()
    if text.lower() == "пропустить":
        data = await state.get_data()
        sent, failed, total, duration = await _send_broadcast(
            message.bot,
            db,
            data.get("broadcast_text"),
            None,
            None,
        )
        await message.answer(
            format_broadcast_summary(sent, failed, total, duration),
            reply_markup=admin_menu_kb(),
        )
        await state.clear()
        return
    await state.update_data(button_text=text)
    await state.set_state(AdminState.broadcast_button_url)
    await message.answer("Введите ссылку для кнопки:")


@router.message(AdminState.broadcast_button_url)
async def admin_broadcast_send(
    message: Message, state: FSMContext, db: Database, config: Config
) -> None:
    if not is_admin(message.from_user.id, config):
        return
    data = await state.get_data()
    text = data.get("broadcast_text")
    button_text = data.get("button_text")
    button_url = None
    if button_text:
        button_url = (message.text or "").strip()
        if not button_url:
            await message.answer("Введите ссылку.")
            return
    sent, failed, total, duration = await _send_broadcast(
        message.bot, db, text, button_text, button_url
    )
    await message.answer(
        format_broadcast_summary(sent, failed, total, duration),
        reply_markup=admin_menu_kb(),
    )
    await state.clear()


router = Router()
routers.append(router)


def menu_back_kb():
    builder = InlineKeyboardBuilder()
    builder.button(text="⬅️ Меню", callback_data="menu:home")
    return builder.as_markup()


def _display_name(row: dict) -> str:
    if row.get("username"):
        return f"@{escape(row['username'])}"
    if row.get("first_name"):
        return escape(row["first_name"])
    return f"ID {row['id']}"


@router.callback_query(F.data == "menu:leaderboard")
async def public_stats_callback(callback: CallbackQuery, db: Database) -> None:
    if not await ensure_sponsors(callback.bot, callback.from_user.id, db):
        await callback.answer()
        return
    stats = await db.global_stats(_day_start_ts())
    text = (
        "📊 <b>Статистика AD GRAM</b>\n\n"
        f"👤 Общее количество пользователей: <b>{format_coins(stats['users_total'])}</b>\n"
        f"🆕 Новых сегодня: <b>{format_coins(stats['new_today'])}</b>\n\n"
        f"📣 Подписались на каналы: <b>{format_coins(stats['channel_subs'])}</b>\n"
        f"👥 Подписались на группы: <b>{format_coins(stats['group_subs'])}</b>\n\n"
        f"👁️ Общее количество просмотров: <b>{format_coins(stats['views'])}</b>\n"
        f"🔥 Общее количество реакций: <b>{format_coins(stats['reactions'])}</b>"
    )
    await callback.message.answer(text, reply_markup=menu_back_kb())
    await callback.answer()


@router.callback_query(F.data == "menu:achievements")
async def achievements(callback: CallbackQuery, db: Database) -> None:
    if not await ensure_sponsors(callback.bot, callback.from_user.id, db):
        await callback.answer()
        return
    await notify_new_achievements(callback.bot, db, callback.from_user.id)
    progress = await get_achievement_progress(db, callback.from_user.id)
    text, page, total_pages = build_achievements_page(progress, 1)
    await callback.message.answer(text, reply_markup=achievements_kb(page, total_pages))
    await callback.answer()


router = Router()
routers.append(router)

ADV_TASKS_PER_PAGE = 10
ADV_TASK_TYPE_LABELS = {
    "subscribe": "Подписка",
    "view": "Просмотр",
    "reaction": "Реакция",
    "bot": "Задание",
    "boost": "Буст",
}


def _adv_short_title(title: str, limit: int = 28) -> str:
    if len(title) <= limit:
        return title
    return title[: limit - 1] + "…"


def blocked_task_text(reason: str | None) -> str:
    reason_text = reason or "Заблокировано администратором"
    return (
        "🚫 <b>Задание заблокировано</b>\n"
        f"Причина: {escape(reason_text)}\n\n"
        "Если у Вас остались вопросы напишите нам @ameropro"
    )


def advertiser_tasks_kb(
    tasks: list[dict], page: int, total_pages: int
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for task in tasks:
        title = _adv_short_title(task["title"])
        remaining = int(task.get("remaining_count") or 0)
        active = int(task.get("active") or 0)
        task_status = task.get("task_status", "active")
        if remaining <= 0:
            status = "✅"
        elif task_status == "blocked":
            status = "🚫"
        elif task_status in ("bot_removed", "no_permissions"):
            status = "⚠️"
        elif active:
            status = "🟢"
        else:
            status = "⏸"
        builder.button(
            text=f"{status} #{task['id']} • {format_coins(task['reward'])} • {title}",
            callback_data=f"adv:task:{task['id']}:{page}",
        )
    builder.adjust(1)
    nav_buttons: list[InlineKeyboardButton] = []
    if page > 1:
        nav_buttons.append(
            InlineKeyboardButton(text="◀️", callback_data=f"adv:page:{page - 1}")
        )
    if total_pages > 1:
        nav_buttons.append(
            InlineKeyboardButton(text=f"{page}/{total_pages}", callback_data="adv:noop")
        )
    if page < total_pages:
        nav_buttons.append(
            InlineKeyboardButton(text="▶️", callback_data=f"adv:page:{page + 1}")
        )
    if nav_buttons:
        builder.row(*nav_buttons)
    builder.row(InlineKeyboardButton(text="⬅️ Меню", callback_data="menu:home"))
    return builder.as_markup()


def advertiser_back_kb(page: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="⬅️ Назад", callback_data=f"adv:page:{page}")
    builder.button(text="🏠 Меню", callback_data="menu:home")
    builder.adjust(2)
    return builder.as_markup()


def advertiser_task_kb(task: dict, page: int, has_permission_error: bool = False) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if task.get("type") in {"reaction", "bot", "boost"}:
        builder.button(text="📥 Заявки", callback_data=f"review:queue:{task['id']}")
    remaining = int(task.get("remaining_count") or 0)
    active = int(task.get("active") or 0)
    task_status = task.get("task_status", "active")
    is_blocked = task_status == "blocked"
    is_permission_error = has_permission_error or (
        task.get("type") == "subscribe" and task_status in ("bot_removed", "no_permissions")
    )
    
    # Если есть ошибка прав доступа, показываем кнопку добавления бота
    if is_blocked:
        # Заблокировано админом/системой — только навигация
        pass
    elif is_permission_error and task.get("type") == "subscribe":
        chat_type = task.get("chat_type", "").lower()
        if chat_type == "channel":
            add_bot_link = "https://t.me/adgramo_bot?startchannel&admin=post_messages+edit_messages+delete_messages+invite_users"
        else:
            add_bot_link = "https://t.me/adgramo_bot?startgroup&admin=post_messages+edit_messages+delete_messages+invite_users"
        builder.button(text="➕ Добавить бота", url=add_bot_link)
        builder.button(text="🔄 Проверить права", callback_data=f"adv:check_rights:{task['id']}:{page}")
    else:
        if remaining > 0 and active:
            builder.button(text="⏸ Остановить", callback_data=f"adv:stop:{task['id']}:{page}")
        elif remaining > 0:
            builder.button(text="▶️ Возобновить", callback_data=f"adv:resume:{task['id']}:{page}")
        builder.button(text="➕ Пополнить", callback_data=f"adv:add:{task['id']}:{page}")
        builder.button(text="➖ Уменьшить", callback_data=f"adv:reduce:{task['id']}:{page}")
        builder.button(text="🗑 Удалить", callback_data=f"adv:delete:{task['id']}:{page}")
    
    builder.button(text="⬅️ Назад", callback_data=f"adv:page:{page}")
    builder.button(text="🏠 Меню", callback_data="menu:home")
    builder.adjust(1, 2, 2, 2)
    return builder.as_markup()
 

def _parse_adv_manage(data: str) -> tuple[int, int]:
    parts = data.split(":")
    task_id = int(parts[2])
    page = int(parts[3]) if len(parts) > 3 else 1
    return task_id, page


@router.callback_query(F.data.startswith("adv:stop:"))
async def adv_stop(callback: CallbackQuery, db: Database, config: Config) -> None:
    task_id, page = _parse_adv_manage(callback.data)
    if callback.from_user.id in config.admin_ids:
        task = await db.get_task(task_id)
    else:
        task = await db.get_owner_task(callback.from_user.id, task_id)
    if not task:
        await callback.answer("Задание не найдено.", show_alert=True)
        return

    task_status = task.get("task_status", "active")
    if task_status == "blocked" and callback.from_user.id not in config.admin_ids:
        await callback.message.answer(blocked_task_text(task.get("status_reason")), parse_mode="HTML")
        await callback.answer()
        return
    
    # Проверяем статус задания на подписку
    has_permission_error = task.get("type") == "subscribe" and task_status in ("bot_removed", "no_permissions")
    
    # Всегда останавливаем задание
    await db.stop_task(callback.from_user.id, task_id)
    
    # Если есть ошибка прав, показываем предупреждение
    if has_permission_error and callback.from_user.id not in config.admin_ids:
        chat_type = task.get("chat_type", "").lower()
        if chat_type == "channel":
            add_bot_link = "https://t.me/adgramo_bot?startchannel&admin=post_messages+edit_messages+delete_messages+invite_users"
        else:
            add_bot_link = "https://t.me/adgramo_bot?startgroup&admin=post_messages+edit_messages+delete_messages+invite_users"
        
        status_reason = task.get("status_reason")
        human_reason = format_permission_error(status_reason)
        
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➕ Добавить бота", url=add_bot_link)]])
        await callback.message.answer(
            f"⏸ <b>Задание остановлено</b>\n\n"
            f"⚠️ <b>Проблема: Нет права «Приглашение пользователей»</b>\n"
            f"{human_reason}\n\n"
            f"<b>Монеты заморожены</b> в этом задании.",
            reply_markup=kb
        )
    else:
        await callback.answer("Задание остановлено.", show_alert=True)
    
    await show_advertiser_tasks_page(callback, db, page)


@router.callback_query(F.data.startswith("adv:resume:"))
async def adv_resume(callback: CallbackQuery, db: Database, config: Config, bot: Bot) -> None:
    task_id, page = _parse_adv_manage(callback.data)
    if callback.from_user.id in config.admin_ids:
        task = await db.get_task(task_id)
    else:
        task = await db.get_owner_task(callback.from_user.id, task_id)
    if not task:
        await callback.answer("Задание не найдено.", show_alert=True)
        return
    if task.get("task_status") == "blocked" and callback.from_user.id not in config.admin_ids:
        await callback.message.answer(blocked_task_text(task.get("status_reason")), parse_mode="HTML")
        await callback.answer()
        return
    if int(task.get("remaining_count") or 0) <= 0:
        await callback.answer("Нет оставшихся выполнений.", show_alert=True)
        return
    
    # Проверяем, были ли решены проблемы, вызвавшие предупреждение
    verification = await verify_task_for_resume(bot, task)
    
    if not verification["can_resume"]:
        # Проблемы всё ещё существуют - не возобновляем
        kb = None
        if verification["human_action"]:
            # Если в action_message есть ссылка на добавление бота
            if "startchannel" in verification["human_action"] or "startgroup" in verification["human_action"]:
                # Извлекаем URL из сообщения (очень простой парсинг)
                import re
                url_match = re.search(r'href="(https://[^"]+)"', verification["human_action"])
                if url_match:
                    url = url_match.group(1)
                    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➕ Добавить бота", url=url)]])
        
        message_text = f"🚫 <b>Не удалось возобновить задание</b>\n\n{verification['human_message']}"
        if not kb and verification["human_action"]:
            message_text += f"\n\n{verification['human_action']}"
        
        if kb:
            await callback.message.answer(message_text, reply_markup=kb)
        else:
            await callback.message.answer(message_text)
        await callback.answer()
        return
    
    # Проблемы решены или задание не имело проблем - восстанавливаем
    await db.resume_task(callback.from_user.id, task_id)
    await callback.answer("Задание возобновлено.", show_alert=True)
    await show_advertiser_tasks_page(callback, db, page)


@router.callback_query(F.data.startswith("adv:check_rights:"))
async def adv_check_rights(callback: CallbackQuery, db: Database, config: Config, bot: Bot) -> None:
    task_id, page = _parse_adv_manage(callback.data)
    if callback.from_user.id in config.admin_ids:
        task = await db.get_task(task_id)
    else:
        task = await db.get_owner_task(callback.from_user.id, task_id)
    if not task:
        await callback.answer("Задание не найдено.", show_alert=True)
        return
    if task.get("type") != "subscribe":
        await callback.answer("Проверка прав доступна только для подписки.", show_alert=True)
        return
    chat_id = task.get("chat_id")
    if not chat_id:
        await callback.answer("Не удалось проверить права.", show_alert=True)
        return
    chat_type = (task.get("chat_type") or "").lower()
    perm_check = await check_bot_permissions(
        bot, chat_id, task.get("chat_username"), chat_type
    )
    if perm_check["has_permissions"]:
        await db.set_task_status(task_id, "active")
        await callback.message.answer("✅ Права бота восстановлены. Задание активировано.")
        await show_advertiser_tasks_page(callback, db, page)
        await callback.answer()
        return
    if perm_check["bot_not_found"]:
        status = "bot_removed"
        reason = "bot_removed"
        problem_text = "Бот был удалён из канала/чата."
    else:
        status = "no_permissions"
        reason = ", ".join(perm_check["missing_permissions"]) or "неизвестно"
        problem_text = "У бота нет нужного права:\n• " + reason
    await db.set_task_status(task_id, status, reason)
    await callback.message.answer(
        f"🚫 Проблема с доступом.\n{problem_text}\n\n"
        "После исправления нажмите «Возобновить» или «Проверить права» ещё раз."
    )
    await show_advertiser_tasks_page(callback, db, page)
    await callback.answer()


@router.callback_query(F.data.startswith("adv:delete:"))
async def adv_delete(callback: CallbackQuery, db: Database, config: Config) -> None:
    task_id, page = _parse_adv_manage(callback.data)
    if callback.from_user.id in config.admin_ids:
        task = await db.get_task(task_id)
    else:
        task = await db.get_owner_task(callback.from_user.id, task_id)
    if not task:
        await callback.answer("Задание не найдено.", show_alert=True)
        return
    if task.get("task_status") == "blocked" and callback.from_user.id not in config.admin_ids:
        await callback.message.answer(blocked_task_text(task.get("status_reason")), parse_mode="HTML")
        await callback.answer()
        return
    
    # Проверяем статус задания на подписку - если потеряны права, блокируем удаление (кроме админов)
    task_status = task.get("task_status", "active")
    if task.get("type") == "subscribe" and task_status in ("bot_removed", "no_permissions") and callback.from_user.id not in config.admin_ids:
        chat_type = task.get("chat_type", "").lower()
        if chat_type == "channel":
            add_bot_link = "https://t.me/adgramo_bot?startchannel&admin=post_messages+edit_messages+delete_messages+invite_users"
        else:
            add_bot_link = "https://t.me/adgramo_bot?startgroup&admin=post_messages+edit_messages+delete_messages+invite_users"
        
        status_reason = task.get("status_reason")
        human_reason = format_permission_error(status_reason)
        
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➕ Добавить бота", url=add_bot_link)]])
        await callback.message.answer(
            f"🚫 <b>Проблема: Нет права «Приглашение пользователей»</b>\n\n"
            f"{human_reason}\n\n"
            f"<b>Монеты заморожены</b> в этом задании.",
            reply_markup=kb
        )
        await callback.answer()
        return
    
    refund = await db.delete_task(callback.from_user.id, task_id)
    if refund is None:
        await callback.answer("Не удалось удалить.", show_alert=True)
        return
    text = (
        f"✅ Задание удалено. Возврат: {format_coins(refund)} BIT."
        if refund > 0
        else "✅ Задание удалено."
    )
    await callback.answer(text, show_alert=True)
    await show_advertiser_tasks_page(callback, db, page)


@router.callback_query(F.data.startswith("adv:add:"))
async def adv_add(callback: CallbackQuery, state: FSMContext, db: Database, config: Config) -> None:
    task_id, page = _parse_adv_manage(callback.data)
    if callback.from_user.id in config.admin_ids:
        task = await db.get_task(task_id)
    else:
        task = await db.get_owner_task(callback.from_user.id, task_id)
    if not task:
        await callback.answer("Задание не найдено.", show_alert=True)
        return
    if task.get("task_status") == "blocked" and callback.from_user.id not in config.admin_ids:
        await callback.message.answer(blocked_task_text(task.get("status_reason")), parse_mode="HTML")
        await callback.answer()
        return
    
    # Проверяем статус задания на подписку - если потеряны права, блокируем пополнение (кроме админов)
    task_status = task.get("task_status", "active")
    if task.get("type") == "subscribe" and task_status in ("bot_removed", "no_permissions") and callback.from_user.id not in config.admin_ids:
        chat_type = task.get("chat_type", "").lower()
        if chat_type == "channel":
            add_bot_link = "https://t.me/adgramo_bot?startchannel&admin=post_messages+edit_messages+delete_messages+invite_users"
        else:
            add_bot_link = "https://t.me/adgramo_bot?startgroup&admin=post_messages+edit_messages+delete_messages+invite_users"
        
        status_reason = task.get("status_reason")
        human_reason = format_permission_error(status_reason)
        
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➕ Добавить бота", url=add_bot_link)]])
        await callback.message.answer(
            f"❌ <b>Пополнение невозможно!</b>\n\n"
            f"{human_reason}\n\n"
            f"<b>Монеты заморожены</b> в этом задании.",
            reply_markup=kb
        )
        await callback.answer()
        return
    
    await state.update_data(manage_action="add", manage_task_id=task_id, manage_page=page)
    await state.set_state(ManageTaskState.amount)
    await callback.message.answer("Введите количество для пополнения:", reply_markup=cancel_kb())
    await callback.answer()


@router.callback_query(F.data.startswith("adv:reduce:"))
async def adv_reduce(callback: CallbackQuery, state: FSMContext, db: Database, config: Config) -> None:
    task_id, page = _parse_adv_manage(callback.data)
    if callback.from_user.id in config.admin_ids:
        task = await db.get_task(task_id)
    else:
        task = await db.get_owner_task(callback.from_user.id, task_id)
    if not task:
        await callback.answer("Задание не найдено.", show_alert=True)
        return
    if task.get("task_status") == "blocked" and callback.from_user.id not in config.admin_ids:
        await callback.message.answer(blocked_task_text(task.get("status_reason")), parse_mode="HTML")
        await callback.answer()
        return
    
    # Проверяем статус задания на подписку - если потеряны права, блокируем уменьшение (кроме админов)
    task_status = task.get("task_status", "active")
    if task.get("type") == "subscribe" and task_status in ("bot_removed", "no_permissions") and callback.from_user.id not in config.admin_ids:
        chat_type = task.get("chat_type", "").lower()
        if chat_type == "channel":
            add_bot_link = "https://t.me/adgramo_bot?startchannel&admin=post_messages+edit_messages+delete_messages+invite_users"
        else:
            add_bot_link = "https://t.me/adgramo_bot?startgroup&admin=post_messages+edit_messages+delete_messages+invite_users"
        
        status_reason = task.get("status_reason")
        human_reason = format_permission_error(status_reason)
        
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➕ Добавить бота", url=add_bot_link)]])
        await callback.message.answer(
            f"❌ <b>Операция невозможна!</b>\n\n"
            f"{human_reason}\n\n"
            f"<b>Монеты заморожены</b> в этом задании.",
            reply_markup=kb
        )
        await callback.answer()
        return
    
    await state.update_data(manage_action="reduce", manage_task_id=task_id, manage_page=page)
    await state.set_state(ManageTaskState.amount)
    await callback.message.answer("Введите количество для уменьшения:", reply_markup=cancel_kb())
    await callback.answer()


@router.message(ManageTaskState.amount)
async def adv_manage_amount(
    message: Message, state: FSMContext, db: Database, config: Config
) -> None:
    data = await state.get_data()
    action = data.get("manage_action")
    task_id = data.get("manage_task_id")
    page = data.get("manage_page", 1)
    try:
        amount = int((message.text or "").strip())
    except ValueError:
        await message.answer("Введите число.")
        return
    if amount <= 0:
        await message.answer("Количество должно быть больше нуля.")
        return
    if not task_id or action not in {"add", "reduce"}:
        await state.clear()
        await message.answer(
            "Сессия управления заданием устарела.",
            reply_markup=main_menu_kb(message.from_user.id in config.admin_ids),
        )
        return
    task = await db.get_owner_task(message.from_user.id, int(task_id))
    if not task:
        await state.clear()
        await message.answer("Задание не найдено.")
        return
    if action == "add":
        user = await db.get_user(message.from_user.id)
        total_cost = amount * int(task.get("reward") or 0)
        commission_fee = calculate_commission_fee(total_cost, user, config)
        cost = await db.add_task_quantity(
            message.from_user.id, int(task_id), amount, commission_fee=commission_fee
        )
        if cost is None:
            await message.answer("Недостаточно BIT для пополнения.")
            return
        await state.clear()
        if commission_fee:
            text = (
                f"✅ Пополнено на {amount}. Списано {format_coins(cost)} BIT "
                f"(комиссия {format_coins(commission_fee)} BIT)."
            )
        else:
            text = f"✅ Пополнено на {amount}. Списано {format_coins(cost)} BIT."
        await message.answer(text)
    else:
        remaining = int(task.get("remaining_count") or 0)
        if amount > remaining:
            await message.answer("Нельзя уменьшить больше, чем осталось.")
            return
        refund = await db.reduce_task_quantity(message.from_user.id, int(task_id), amount)
        if refund is None:
            await message.answer("Не удалось уменьшить задание.")
            return
        await state.clear()
        await message.answer(
            f"✅ Уменьшено на {amount}. Возврат {format_coins(refund)} BIT."
        )
    await send_advertiser_tasks_page(message, db, page)


async def send_advertiser_tasks_page(message: Message, db: Database, page: int) -> None:
    total = await db.count_owner_tasks(message.from_user.id)
    if total == 0:
        await message.answer(
            "📈 У вас пока нет созданных заданий.",
            reply_markup=advertiser_back_kb(1),
        )
        return
    total_pages = max(1, (total + ADV_TASKS_PER_PAGE - 1) // ADV_TASKS_PER_PAGE)
    page = max(1, min(page, total_pages))
    offset = (page - 1) * ADV_TASKS_PER_PAGE
    tasks = await db.list_owner_tasks(message.from_user.id, ADV_TASKS_PER_PAGE, offset)
    stats = await db.owner_stats(message.from_user.id)
    text = (
        "📈 <b>Мои задания</b>\n"
        f"Всего: <b>{stats['total']}</b> • Активных: <b>{stats['active']}</b>\n"
        f"Выполнено: <b>{stats['completed']}</b> • Потрачено: "
        f"<b>{format_coins(stats['spent'])}</b> BIT\n\n"
        "Выберите задание для деталей:"
    )
    await message.answer(
        text,
        reply_markup=advertiser_tasks_kb(tasks, page, total_pages),
    )


async def show_advertiser_tasks_page(callback: CallbackQuery, db: Database, page: int) -> None:
    total = await db.count_owner_tasks(callback.from_user.id)
    if total == 0:
        await callback.message.edit_text(
            "📈 У вас пока нет созданных заданий.",
            reply_markup=advertiser_back_kb(1),
        )
        return
    total_pages = max(1, (total + ADV_TASKS_PER_PAGE - 1) // ADV_TASKS_PER_PAGE)
    page = max(1, min(page, total_pages))
    offset = (page - 1) * ADV_TASKS_PER_PAGE
    tasks = await db.list_owner_tasks(callback.from_user.id, ADV_TASKS_PER_PAGE, offset)
    stats = await db.owner_stats(callback.from_user.id)
    text = (
        "📈 <b>Мои задания</b>\n"
        f"Всего: <b>{stats['total']}</b> • Активных: <b>{stats['active']}</b>\n"
        f"Выполнено: <b>{stats['completed']}</b> • Потрачено: "
        f"<b>{format_coins(stats['spent'])}</b> BIT\n\n"
        "Выберите задание для деталей:"
    )
    await callback.message.edit_text(
        text,
        reply_markup=advertiser_tasks_kb(tasks, page, total_pages),
    )


@router.callback_query(F.data == "menu:ads")
async def menu_ads(callback: CallbackQuery, db: Database) -> None:
    if not await ensure_sponsors(callback.bot, callback.from_user.id, db):
        await callback.answer()
        return
    await show_advertiser_tasks_page(callback, db, page=1)
    await callback.answer()


@router.callback_query(F.data == "adv:noop")
async def adv_noop(callback: CallbackQuery) -> None:
    await callback.answer()


@router.callback_query(F.data.startswith("adv:page:"))
async def adv_page(callback: CallbackQuery, db: Database) -> None:
    if not await ensure_sponsors(callback.bot, callback.from_user.id, db):
        await callback.answer()
        return
    page = int(callback.data.split(":")[2])
    await show_advertiser_tasks_page(callback, db, page=page)
    await callback.answer()


@router.callback_query(F.data.startswith("adv:task:"))
async def adv_task(callback: CallbackQuery, db: Database) -> None:
    if not await ensure_sponsors(callback.bot, callback.from_user.id, db):
        await callback.answer()
        return
    parts = callback.data.split(":")
    task_id = int(parts[2])
    page = int(parts[3]) if len(parts) > 3 else 1
    task = await db.get_owner_task(callback.from_user.id, task_id)
    if not task:
        await callback.answer("Задание не найдено.", show_alert=True)
        return
    stats = await db.get_task_claim_stats(task_id)
    completed = int(task["total_count"]) - int(task["remaining_count"])
    spent = int(task["total_count"]) * int(task["reward"]) - int(task["escrow_balance"])
    remaining = int(task.get("remaining_count") or 0)
    active = int(task.get("active") or 0)
    task_status = task.get("task_status", "active")
    if remaining <= 0:
        status = "✅ Завершено"
    elif task_status == "blocked":
        status = "🚫 Заблокировано"
    elif task_status in ("bot_removed", "no_permissions"):
        status = "🚫 Проблема"
    elif active:
        status = "🟢 Активно"
    else:
        status = "⏸ Пауза"
    fill_rate = 0.0
    if task["total_count"] > 0:
        fill_rate = (completed / task["total_count"]) * 100.0
    text = (
        f"🧾 <b>Задание #{task_id}</b>\n"
        f"Тип: <b>{ADV_TASK_TYPE_LABELS.get(task['type'], task['type'])}</b>\n"
        f"Название: <b>{escape(task['title'])}</b>\n"
        f"Награда: <b>{format_coins(task['reward'])}</b> BIT\n"
        f"Статус: <b>{status}</b>\n\n"
        f"Выполнено: <b>{completed}</b> из <b>{task['total_count']}</b>\n"
        f"Прогресс: <b>{fill_rate:.1f}%</b>\n"
        f"Потрачено: <b>{format_coins(spent)}</b> BIT\n\n"
        f"Ожидают проверки: <b>{stats['submitted']}</b>\n"
        f"На доработке: <b>{stats['revision']}</b>\n"
        f"Удержание: <b>{stats['completed_hold']}</b>\n"
        f"Отклонено: <b>{stats['rejected']}</b>\n"
        f"Отозвано: <b>{stats['revoked']}</b>\n"
        f"Зачтено: <b>{stats['completed']}</b>"
    )
    
    # Проверяем на ошибки прав доступа / блокировку
    has_permission_error = (
        task.get("type") == "subscribe" and task_status in ("bot_removed", "no_permissions")
    )
    is_blocked = task_status == "blocked"
    
    if is_blocked:
        text += "\n\n" + blocked_task_text(task.get("status_reason"))
    elif has_permission_error:
        human_reason = format_permission_error(task.get("status_reason"))
        text += (
            "\n\n🚫 <b>Проблема: Нет права «Приглашение пользователей»</b>\n"
            f"{human_reason}\n\nПосле добавления бота, сообщите в тех. поддержку @ameropro."
        )
    
    await callback.message.edit_text(text, reply_markup=advertiser_task_kb(task, page, has_permission_error))
    await callback.answer()
