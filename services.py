import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

import aiohttp
from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from config import Config
from utils import escape, format_coins, now_ts


# Маппинг технических ошибок на понятные сообщения
PERMISSION_ERROR_MESSAGES = {
    "not_member": "Бот был удалён из канала/чата",
    "bot_removed": "Бот был удалён из канала/чата",
    "no_access": "Нет прав на доступ к информации о подписчиках",
    "no_admin": "Бот не является администратором в канале/чате",
    "channel_private": "Канал приватный и бот не имеет доступа",
    "user_not_member": "Бот не видит членов канала",
    "timeout": "Ошибка соединения с Telegram (тайм-аут)",
    "api_error": "Ошибка при работе с Telegram API",
}


def format_permission_error(error_code: str | None) -> str:
    """Переводит технический код ошибки в понятное сообщение для пользователя"""
    if not error_code:
        return "Бот потерял доступ к каналу/чату"
    return PERMISSION_ERROR_MESSAGES.get(error_code, f"Проблема с доступом: {error_code}")


@dataclass(frozen=True)
class Achievement:
    code: str
    title: str
    description: str
    field: str
    threshold: int
    reward: int = 0


ACHIEVEMENTS: list[Achievement] = [
    Achievement(
        code="tasks_1",
        title="Первый шаг",
        description="Выполнить 1 задание",
        field="completed_tasks",
        threshold=1,
        reward=250,
    ),
    Achievement(
        code="tasks_10",
        title="Усердный исполнитель",
        description="Выполнить 10 заданий",
        field="completed_tasks",
        threshold=10,
        reward=1500,
    ),
    Achievement(
        code="tasks_50",
        title="Профессионал",
        description="Выполнить 50 заданий",
        field="completed_tasks",
        threshold=50,
        reward=10000,
    ),
    Achievement(
        code="tasks_200",
        title="Легенда",
        description="Выполнить 200 заданий",
        field="completed_tasks",
        threshold=200,
        reward=50000,
    ),
    Achievement(
        code="tasks_500",
        title="Мастер",
        description="Выполнить 500 заданий",
        field="completed_tasks",
        threshold=500,
        reward=100000,
    ),
    Achievement(
        code="tasks_1000",
        title="Абсолют",
        description="Выполнить 1000 заданий",
        field="completed_tasks",
        threshold=1000,
        reward=250000,
    ),
    Achievement(
        code="balance_1000",
        title="Копилка",
        description="Держать на балансе 1000 BIT",
        field="balance",
        threshold=1000,
        reward=250,
    ),
    Achievement(
        code="balance_5000",
        title="Уверенный запас",
        description="Держать на балансе 5000 BIT",
        field="balance",
        threshold=5000,
        reward=500,
    ),
    Achievement(
        code="balance_20000",
        title="Финансовая подушка",
        description="Держать на балансе 20000 BIT",
        field="balance",
        threshold=20000,
        reward=2500,
    ),
    Achievement(
        code="ref_1",
        title="Первый реферал",
        description="Пригласить 1 реферала",
        field="referral_count",
        threshold=1,
        reward=150,
    ),
    Achievement(
        code="ref_5",
        title="Реферальный лидер",
        description="Пригласить 5 рефералов",
        field="referral_count",
        threshold=5,
        reward=750,
    ),
    Achievement(
        code="ref_20",
        title="Реферальный мастер",
        description="Пригласить 20 рефералов",
        field="referral_count",
        threshold=20,
        reward=2500,
    ),
    Achievement(
        code="ref_50",
        title="Реферальная сеть",
        description="Пригласить 50 рефералов",
        field="referral_count",
        threshold=50,
        reward=10000,
    ),
    Achievement(
        code="ref_earned_100",
        title="Первые выплаты",
        description="Заработать 100 BIT с рефералов",
        field="referral_earned",
        threshold=100,
        reward=40,
    ),
    Achievement(
        code="ref_earned_500",
        title="Стабильный доход",
        description="Заработать 500 BIT с рефералов",
        field="referral_earned",
        threshold=500,
        reward=200,
    ),
    Achievement(
        code="ref_earned_2000",
        title="Реферальный эксперт",
        description="Заработать 2000 BIT с рефералов",
        field="referral_earned",
        threshold=2000,
        reward=600,
    ),
    Achievement(
        code="creator_1",
        title="Рекламодатель",
        description="Создать 1 задание",
        field="tasks_created",
        threshold=1,
        reward=250,
    ),
    Achievement(
        code="creator_10",
        title="Продюсер",
        description="Создать 10 заданий",
        field="tasks_created",
        threshold=10,
        reward=750,
    ),
    Achievement(
        code="creator_50",
        title="Медиамагнат",
        description="Создать 50 заданий",
        field="tasks_created",
        threshold=50,
        reward=5000,
    ),
]


def _value_for(field: str, user: dict, tasks_created: int) -> int:
    if field == "tasks_created":
        return tasks_created
    return int(user.get(field, 0))


async def ensure_user_achievements(db, user_id: int) -> list[Achievement]:
    user = await db.get_user(user_id)
    if not user:
        return []
    tasks_created = await db.count_tasks_created(user_id)
    unlocked = {row["code"] for row in await db.list_user_achievements(user_id)}
    newly_unlocked: list[Achievement] = []
    for achievement in ACHIEVEMENTS:
        value = _value_for(achievement.field, user, tasks_created)
        if value >= achievement.threshold and achievement.code not in unlocked:
            created = await db.add_user_achievement(user_id, achievement.code, now_ts())
            if created:
                if achievement.reward > 0:
                    await db.add_balance(
                        user_id,
                        achievement.reward,
                        "achievement_reward",
                        {"code": achievement.code},
                    )
                newly_unlocked.append(achievement)
    return newly_unlocked


async def get_achievement_progress(db, user_id: int) -> list[dict]:
    user = await db.get_user(user_id)
    if not user:
        return []
    tasks_created = await db.count_tasks_created(user_id)
    unlocked = {row["code"] for row in await db.list_user_achievements(user_id)}
    progress: list[dict] = []
    for achievement in ACHIEVEMENTS:
        value = _value_for(achievement.field, user, tasks_created)
        progress.append(
            {
                "achievement": achievement,
                "unlocked": achievement.code in unlocked or value >= achievement.threshold,
                "value": value,
            }
        )
    return progress


def format_achievements(progress: Iterable[dict]) -> str:
    progress_list = list(progress)
    total = len(progress_list)
    unlocked_total = sum(1 for item in progress_list if item["unlocked"])
    lines = [f"🏅 <b>Достижения</b> • <b>{unlocked_total}/{total}</b> • BIT"]
    for item in progress_list:
        achievement: Achievement = item["achievement"]
        unlocked = item["unlocked"]
        value = item["value"]
        reward_text = f"+{format_coins(achievement.reward)}" if achievement.reward > 0 else "-"
        if unlocked:
            status = "✅"
            lines.append(f"{status} {achievement.title} • {reward_text}")
            lines.append(f"{achievement.description}")
        else:
            status = "🔒"
            progress_text = f"{value}/{achievement.threshold}"
            lines.append(f"{status} {achievement.title} • {reward_text}")
            lines.append(f"{achievement.description} • {progress_text}")
        lines.append("")
    if lines and lines[-1] == "":
        lines.pop()
    return "\n".join(lines)


LEVEL_THRESHOLDS = [0, 10, 50, 200, 500, 1000]
AUTO_APPROVE_SECONDS = 24 * 60 * 60
INVOICE_CHECK_INTERVAL = 60
INVOICE_AUTO_CHECK_MAX_AGE = 15 * 60
TOPUP_FREE_SECONDS_PER_USD = 24 * 60 * 60


def level_for(completed_tasks: int) -> int:
    level = 1
    for idx, threshold in enumerate(LEVEL_THRESHOLDS[1:], start=2):
        if completed_tasks >= threshold:
            level = idx
    return level


def next_level_target(completed_tasks: int) -> int | None:
    for threshold in LEVEL_THRESHOLDS[1:]:
        if completed_tasks < threshold:
            return threshold
    return None


def _format_usd_amount(amount: float) -> str:
    if amount >= 1:
        return f"{amount:.2f}"
    text = f"{amount:.4f}"
    return text.rstrip("0").rstrip(".")


def topup_bonus_seconds(config: Config, coins: int) -> tuple[float, int]:
    usd_amount = config.coins_to_usd(coins)
    free_seconds = int(usd_amount) * TOPUP_FREE_SECONDS_PER_USD
    return usd_amount, free_seconds


def build_topup_success_text(coins: int, usd_amount: float, free_seconds: int) -> str:
    lines = [
        "✅ <b>Оплата подтверждена!</b>",
        f"💳 Зачислено: <b>{format_coins(coins)} BIT</b>",
    ]
    if usd_amount > 0:
        lines.append(f"💵 Эквивалент: <b>${_format_usd_amount(usd_amount)}</b>")
    if free_seconds > 0:
        free_hours = free_seconds // 3600
        lines.append(f"🎉 Комиссия отключена на <b>{free_hours} ч.</b>")
    lines.append("Спасибо за пополнение!")
    return "\n".join(lines)


async def grant_task_reward(
    db, config: Config, user_id: int, task_id: int, reward: int, claim_id: int
) -> int:
    await db.add_balance(user_id, reward, "task_reward", {"task_id": task_id, "claim_id": claim_id})
    completed = await db.increment_completed_tasks(user_id)
    await db.set_level(user_id, level_for(completed))
    await ensure_user_achievements(db, user_id)
    bonus = await apply_referral_bonus(db, config, user_id, reward, task_id, claim_id)
    return bonus


async def apply_referral_bonus(
    db,
    config: Config,
    performer_id: int,
    reward: int,
    task_id: int,
    claim_id: int,
) -> int:
    referrer_id = await db.get_referrer_id(performer_id)
    if not referrer_id:
        return 0
    bonus = reward * config.referral_percent // 100
    if bonus <= 0:
        return 0
    await db.add_balance(
        referrer_id,
        bonus,
        "referral_bonus",
        {"task_id": task_id, "claim_id": claim_id, "from_user": performer_id},
    )
    await db.add_referral_earned(referrer_id, bonus)
    return bonus


async def revoke_task_reward(
    db,
    config: Config,
    user_id: int,
    task_id: int,
    reward: int,
    claim_id: int,
) -> None:
    await db.add_balance(user_id, -reward, "task_revoke", {"task_id": task_id, "claim_id": claim_id})
    completed = await db.decrement_completed_tasks(user_id)
    await db.set_level(user_id, level_for(completed))
    referrer_id = await db.get_referrer_id(user_id)
    if not referrer_id:
        return
    bonus = reward * config.referral_percent // 100
    if bonus <= 0:
        return
    await db.add_balance(
        referrer_id,
        -bonus,
        "referral_revoke",
        {"task_id": task_id, "claim_id": claim_id, "from_user": user_id},
    )
    await db.add_referral_earned(referrer_id, -bonus)


def hold_until_ts(config: Config) -> int:
    return now_ts() + config.hold_days * 24 * 60 * 60


def penalty_deadline_ts(config: Config) -> int:
    return now_ts() + config.unsub_grace_seconds


async def is_member(bot: Bot, chat_id: int, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        return member.status in {"member", "administrator", "creator"}
    except TelegramForbiddenError:
        # Bot doesn't have permission to check membership
        # This usually means bot is not in the channel or doesn't have rights
        return True  # Assume member (benefit of doubt)
    except TelegramBadRequest as e:
        # User not found in chat or other issue
        error_str = str(e).lower()
        # Check if it's a "user not found" error
        if "not a member" in error_str or "not found" in error_str:
            return False
        # If member list is inaccessible or other permission issues, assume member (benefit of doubt)
        if "inaccessible" in error_str or "member list" in error_str:
            return True
        # For other errors, assume member (network issues, etc)
        return True
    except Exception:
        # Unexpected error - assume member
        return True
        return True


async def check_sponsors(bot: Bot, user_id: int, sponsors: list[dict]) -> bool:
    for sponsor in sponsors:
        chat_id = sponsor.get("chat_id")
        if not chat_id:
            continue
        if not await is_member(bot, chat_id, user_id):
            return False
    return True


class CryptoPayClient:
    def __init__(self, token: str) -> None:
        self._token = token
        self._base_url = "https://pay.crypt.bot/api/"

    async def _request(self, method: str, path: str, json_data: dict) -> dict:
        headers = {"Crypto-Pay-API-Token": self._token}
        async with aiohttp.ClientSession() as session:
            async with session.request(
                method, self._base_url + path, json=json_data, headers=headers
            ) as resp:
                data = await resp.json()
        if not data.get("ok"):
            raise RuntimeError(data.get("error", "CryptoPay error"))
        return data["result"]

    async def create_invoice(
        self,
        asset: str,
        amount: str,
        description: str,
        payload: str,
        expires_in: int = 3600,
    ) -> dict:
        return await self._request(
            "POST",
            "createInvoice",
            {
                "asset": asset,
                "amount": amount,
                "description": description,
                "payload": payload,
                "expires_in": expires_in,
            },
        )

    async def get_invoice(self, invoice_id: str) -> dict | None:
        result = await self._request(
            "POST",
            "getInvoices",
            {"invoice_ids": invoice_id},
        )
        items = result.get("items", [])
        return items[0] if items else None


def build_chat_link(chat_id: int | None, chat_username: str | None) -> str | None:
    if chat_username:
        return f"https://t.me/{chat_username.lstrip('@')}"
    if not chat_id:
        return None
    chat_id_str = str(chat_id)
    if chat_id_str.startswith("-100"):
        internal = chat_id_str[4:]
    elif chat_id_str.startswith("-"):
        internal = chat_id_str[1:]
    else:
        internal = chat_id_str
    return f"https://t.me/c/{internal}/1"


async def subscription_watchdog(bot: Bot, db, config: Config) -> None:
    while True:
        claims = await db.list_subscribe_holds()
        now = now_ts()
        for claim in claims:
            hold_until = claim.get("hold_until") or 0
            if hold_until and now >= hold_until:
                await db.finalize_hold(claim["id"])
                continue
            if not await is_member(bot, claim["chat_id"], claim["user_id"]):
                if not claim.get("penalty_deadline"):
                    deadline = now + config.unsub_grace_seconds
                    await db.update_claim(
                        claim["id"],
                        penalty_deadline=deadline,
                        warning_sent=1,
                    )
                    task = await db.get_task(claim["task_id"])
                    chat_link = None
                    if task:
                        chat_link = task.get("action_link") or build_chat_link(
                            task.get("chat_id"), task.get("chat_username")
                        )
                    if not chat_link:
                        chat_link = build_chat_link(
                            claim.get("chat_id"), claim.get("chat_username")
                        )
                    keyboard = None
                    buttons = []
                    if chat_link:
                        buttons.append([
                            InlineKeyboardButton(
                                text="🔗 Подписаться обратно", url=chat_link
                            )
                        ])
                    buttons.append([
                        InlineKeyboardButton(
                            text="📝 Подать апелляцию", callback_data=f"appeal:unsubscribe:{claim['id']}"
                        )
                    ])
                    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
                    try:
                        await bot.send_message(
                            claim["user_id"],
                            "⚠️ Вы отписались раньше срока.\n"
                            "Подпишитесь обратно, иначе награда будет снята.\n"
                            "Если средств не хватает, баланс может уйти в минус.\n"
                            "У вас есть 1 час.",
                            reply_markup=keyboard,
                        )
                    except (TelegramBadRequest, TelegramForbiddenError):
                        pass
                elif now >= int(claim["penalty_deadline"]):
                    await db.update_claim(
                        claim["id"],
                        status="revoked",
                        penalty_deadline=None,
                        completed_at=now,
                    )
                    await db.return_task_funds(claim["task_id"], claim["reward"])
                    task = await db.get_task(claim["task_id"])
                    if task and task.get("task_status") not in {"blocked", "paused"}:
                        await db.resume_task(task["id"])
                    await revoke_task_reward(
                        db,
                        config,
                        claim["user_id"],
                        claim["task_id"],
                        claim["reward"],
                        claim["id"],
                    )
                    try:
                        await bot.send_message(
                            claim["user_id"],
                            f"Награда снята: {format_coins(claim['reward'])} монет.",
                        )
                    except (TelegramBadRequest, TelegramForbiddenError):
                        pass
            else:
                if claim.get("penalty_deadline"):
                    await db.update_claim(
                        claim["id"],
                        penalty_deadline=None,
                        warning_sent=0,
                    )
        await asyncio.sleep(300)


async def invoice_watchdog(bot: Bot, db, config: Config) -> None:
    if not config.crypto_pay_token:
        return
    client = CryptoPayClient(config.crypto_pay_token)
    while True:
        invoices = await db.list_pending_invoices(max_age_seconds=INVOICE_AUTO_CHECK_MAX_AGE)
        for inv in invoices:
            try:
                data = await client.get_invoice(inv["invoice_id"])
            except RuntimeError:
                continue
            if not data:
                continue
            if data.get("status") == "paid":
                await db.mark_invoice_paid(inv["invoice_id"])
                await db.add_balance(
                    inv["user_id"],
                    inv["coins"],
                    "topup",
                    {"invoice_id": inv["invoice_id"]},
                )
                usd_amount, free_seconds = topup_bonus_seconds(config, int(inv["coins"]))
                if free_seconds > 0:
                    await db.add_commission_free_time(inv["user_id"], free_seconds)
                try:
                    await bot.send_message(
                        inv["user_id"],
                        build_topup_success_text(
                            int(inv["coins"]),
                            usd_amount,
                            free_seconds,
                        ),
                    )
                except (TelegramBadRequest, TelegramForbiddenError):
                    pass
            await asyncio.sleep(0.05)
        await asyncio.sleep(INVOICE_CHECK_INTERVAL)


async def check_bot_permissions(
    bot: Bot,
    chat_id: int,
    chat_username: str | None,
    chat_type: str | None = None,
) -> dict:
    """
    Проверяет конкретные права бота в чате/канале.
    Возвращает dict с результатом проверки.
    """
    result = {
        "has_permissions": True,
        "missing_permissions": [],
        "bot_not_found": False,
        "total_required_permissions": 0,
    }
    
    # Требуемые права
    required_permissions = {
        "can_post_messages": "Постить сообщения",
        "can_edit_messages": "Редактировать сообщения",
        "can_delete_messages": "Удалять сообщения",
        "can_invite_users": "Приглашать пользователей",
    }
    
    try:
        # Извлекаем ID бота из токена (формат: bot_id:token_part)
        bot_id = int(bot.token.split(':')[0])
        member = await bot.get_chat_member(chat_id, bot_id)

        # Если бот создатель – у него есть все права
        if member.status == "creator":
            return result

        # Если бот не администратор – проблема прав
        if member.status != "administrator":
            result["missing_permissions"].append("Бот не является администратором")
            result["has_permissions"] = False
            result["total_required_permissions"] = 1
            return result
        
        # Проверяем каждое требуемое право
        checked_permissions = 0
        for permission_attr, permission_name in required_permissions.items():
            value = getattr(member, permission_attr, None)
            if value is None:
                continue
            checked_permissions += 1
            if not value:
                result["missing_permissions"].append(permission_name)
        
        result["total_required_permissions"] = checked_permissions or len(required_permissions)
        if result["missing_permissions"]:
            result["has_permissions"] = False
            
    except TelegramForbiddenError:
        # Бот не имеет доступа к чату или был исключён
        result["bot_not_found"] = True
        result["has_permissions"] = False
    except TelegramBadRequest as e:
        error_str = str(e).lower()
        if "not a member" in error_str or "chat not found" in error_str or "user not found" in error_str:
            # Бот не в чате
            result["bot_not_found"] = True
            result["has_permissions"] = False
        else:
            # Не можем проверить – считаем проблемой прав
            result["missing_permissions"].append("Нет доступа для проверки прав")
            result["has_permissions"] = False
    except Exception as e:
        # Неожиданная ошибка – не считаем потерей прав
        print(f"[WARNING] check_bot_permissions: {e}")
        result["has_permissions"] = True
    
    return result


async def verify_task_for_resume(bot: Bot, task: dict) -> dict:
    """
    Проверяет, были ли решены проблемы, из-за которых задание было приостановлено.
    Возвращает dict с результатом:
    {
        "can_resume": bool,  # Разрешить ли восстановление
        "problems_fixed": bool,  # Все ли проблемы решены
        "human_message": str,  # Сообщение для пользователя
        "human_action": str,  # Рекомендуемое действие
    }
    """
    task_status = task.get("task_status", "active")
    
    # Если задание не имеет предупреждений, можно восстановить
    if task_status == "active":
        return {
            "can_resume": True,
            "problems_fixed": True,
            "human_message": "Задание активно",
            "human_action": None,
        }
    
    # Проверяем только для subscribe заданий
    if task.get("type") != "subscribe":
        return {
            "can_resume": True,
            "problems_fixed": True,
            "human_message": "Задание готово к восстановлению",
            "human_action": None,
        }
    
    chat_id = task.get("chat_id")
    chat_username = task.get("chat_username")
    chat_type = task.get("chat_type", "").lower()
    
    if not chat_id:
        return {
            "can_resume": True,
            "problems_fixed": False,
            "human_message": "Не удалось проверить статус чата",
            "human_action": None,
        }
    
    # Проверяем текущие права бота
    perm_check = await check_bot_permissions(bot, chat_id, chat_username, task.get("chat_type"))
    
    if perm_check["has_permissions"]:
        # Все права восстановлены - можно пересумировать
        return {
            "can_resume": True,
            "problems_fixed": True,
            "human_message": "✅ Проблемы решены! Права бота восстановлены.",
            "human_action": None,
        }
    else:
        # Проблемы всё ещё существуют
        if perm_check["bot_not_found"]:
            if chat_type == "channel":
                add_bot_link = "https://t.me/adgramo_bot?startchannel&admin=post_messages+edit_messages+delete_messages+invite_users"
            else:
                add_bot_link = "https://t.me/adgramo_bot?startgroup&admin=post_messages+edit_messages+delete_messages+invite_users"
            
            return {
                "can_resume": False,
                "problems_fixed": False,
                "human_message": "❌ Бот всё ещё удалён из чата",
                "human_action": f"Добавьте бота заново: <a href=\"{add_bot_link}\">Добавить бота</a>",
            }
        else:
            missing = ", ".join(perm_check["missing_permissions"]) or "неизвестно"
            if chat_type == "channel":
                add_bot_link = "https://t.me/adgramo_bot?startchannel&admin=post_messages+edit_messages+delete_messages+invite_users"
            else:
                add_bot_link = "https://t.me/adgramo_bot?startgroup&admin=post_messages+edit_messages+delete_messages+invite_users"
            
            return {
                "can_resume": False,
                "problems_fixed": False,
                "human_message": f"⚠️ Бот всё ещё потерял права:\n• {missing}",
                "human_action": f"Выдайте боту права или добавьте его заново: <a href=\"{add_bot_link}\">Добавить бота</a>",
            }


async def subscribe_task_watchdog(bot: Bot, db, config: Config) -> None:
    """
    Проверяет права бота в каналах/группах для заданий на подписку каждые 5 минут.
    Если права потеряны, паузирует задание и уведомляет владельца.
    """
    while True:
        try:
            tasks = await db.list_active_subscribe_tasks()
            now = now_ts()
            
            for task in tasks:
                chat_id = task.get("chat_id")
                chat_username = task.get("chat_username")
                owner_id = task.get("owner_id")
                task_id = task.get("id")
                current_status = task.get("task_status", "active")
                status_changed_at = int(task.get("status_changed_at") or 0)
                chat_type = task.get("chat_type", "").lower()
                
                if not chat_id:
                    continue
                
                should_check = False
                if current_status == "active":
                    should_check = True
                elif current_status in ("no_permissions", "bot_removed"):
                    if status_changed_at <= 0:
                        await db.set_task_status(task_id, current_status, task.get("status_reason"))
                        status_changed_at = now
                    if now - status_changed_at <= 3600:
                        should_check = True
                else:
                    continue
                
                if not should_check:
                    continue
                
                perm_check = await check_bot_permissions(bot, chat_id, chat_username, chat_type)
                
                if not perm_check["has_permissions"]:
                    if current_status != "active":
                        continue
                    # Определяем ссылку для добавления бота заново в зависимости от типа чата
                    if chat_type == "channel":
                        add_bot_link = "https://t.me/adgramo_bot?startchannel&admin=post_messages+edit_messages+delete_messages+invite_users+manage_chat"
                    else:
                        add_bot_link = "https://t.me/adgramo_bot?startgroup&admin=post_messages+edit_messages+delete_messages+invite_users+manage_chat"
                    
                    # Проверяем сколько прав пропало
                    missing_count = len(perm_check["missing_permissions"])
                    total_required = perm_check.get("total_required_permissions", 5)
                    
                    if perm_check["bot_not_found"] or missing_count == total_required:
                        # Все права пропали или бот удален - объединяем оба случая
                        await db.set_task_status(
                            task_id, 
                            "bot_removed",
                            "bot_removed"
                        )
                        message = (
                            f"❌ <b>Задание приостановлено!</b>\n\n"
                            f"Задание: <b>{task.get('chat_title', 'Неизвестный чат')}</b>\n\n"
                            f"<b>Проблема:</b> Бот был удалён или у него пропали все права\n\n"
                            f"<b>Что делать:</b> Добавьте бота заново по ссылке:\n"
                            f"<a href=\"{add_bot_link}\">Добавить бота</a>\n\n"
                            f"После добавления нажмите «Возобновить»"
                        )
                    else:
                        # Только часть прав пропала
                        missing = ", ".join(perm_check["missing_permissions"]) or "неизвестно"
                        await db.set_task_status(
                            task_id,
                            "no_permissions",
                            missing
                        )
                        message = (
                            f"⚠️ <b>Задание приостановлено!</b>\n\n"
                            f"Задание: <b>{task.get('chat_title', 'Неизвестный чат')}</b>\n\n"
                            f"<b>Проблема:</b> Бот потерял некоторые права:\n"
                            f"• {missing}\n\n"
                            f"<b>Что делать:</b> Вариант 1 - выдайте боту необходимые права в администраторских настройках\n\n"
                            f"Вариант 2 - удалите и добавьте бота заново с правами:\n"
                            f"<a href=\"{add_bot_link}\">Добавить бота заново</a>\n\n"
                            f"После этого нажмите «Возобновить»"
                        )
                    
                    # Отправляем уведомление владельцу
                    try:
                        await bot.send_message(
                            owner_id,
                            message,
                            parse_mode="HTML"
                        )
                    except (TelegramBadRequest, TelegramForbiddenError):
                        pass
                else:
                    if current_status in ("no_permissions", "bot_removed"):
                        await db.set_task_status(task_id, "active")
                        try:
                            await bot.send_message(
                                owner_id,
                                f"✅ <b>Задание возобновлено!</b>\n\n"
                                f"Права бота восстановлены. Задание вернулось в активные.",
                                parse_mode="HTML"
                            )
                        except (TelegramBadRequest, TelegramForbiddenError):
                            pass
            
            await asyncio.sleep(300)  # Проверяем каждые 5 минут
        except Exception as e:
            print(f"[ERROR] subscribe_task_watchdog: {e}")
            await asyncio.sleep(300)


async def auto_approve_watchdog(bot: Bot, db, config: Config) -> None:
    while True:
        cutoff = now_ts() - AUTO_APPROVE_SECONDS
        claims = await db.list_overdue_submitted_claims(cutoff)
        for claim in claims:
            locked = await db.mark_claim_auto_approving(claim["id"])
            if not locked:
                continue
            reserved = await db.reserve_task_funds(claim["task_id"], claim["reward"])
            if not reserved:
                await db.update_claim(claim["id"], status="rejected", completed_at=now_ts())
                try:
                    await bot.send_message(
                        claim["user_id"],
                        "⚠️ Заявка отклонена автоматически: лимит выполнений закончился.",
                    )
                except (TelegramBadRequest, TelegramForbiddenError):
                    pass
                try:
                    title = escape(claim.get("task_title") or "задание")
                    await bot.send_message(
                        claim["owner_id"],
                        "⚠️ Авто-проверка не смогла одобрить заявку.\n"
                        f"Задание: <b>{title}</b>\n"
                        f"Пользователь: <code>{claim['user_id']}</code>\n"
                        "Причина: лимит выполнений закончился.",
                    )
                except (TelegramBadRequest, TelegramForbiddenError):
                    pass
                continue
            await db.update_claim(claim["id"], status="completed", completed_at=now_ts())
            await grant_task_reward(
                db,
                config,
                claim["user_id"],
                claim["task_id"],
                claim["reward"],
                claim["id"],
            )
            try:
                title = escape(claim.get("task_title") or "задание")
                await bot.send_message(
                    claim["user_id"],
                    "✅ Заявка одобрена автоматически (не была проверена за 24 часа).\n"
                    f"Задание: <b>{title}</b>",
                )
            except (TelegramBadRequest, TelegramForbiddenError):
                pass
            try:
                title = escape(claim.get("task_title") or "задание")
                await bot.send_message(
                    claim["owner_id"],
                    "⌛️ Заявка одобрена автоматически (24 часа без проверки).\n"
                    f"Задание: <b>{title}</b>\n"
                    f"Пользователь: <code>{claim['user_id']}</code>",
                )
            except (TelegramBadRequest, TelegramForbiddenError):
                pass
        await asyncio.sleep(60)


async def start_background_tasks(bot: Bot, db, config: Config) -> list[asyncio.Task]:
    tasks: list[asyncio.Task] = []
    tasks.append(asyncio.create_task(subscription_watchdog(bot, db, config)))
    tasks.append(asyncio.create_task(invoice_watchdog(bot, db, config)))
    tasks.append(asyncio.create_task(auto_approve_watchdog(bot, db, config)))
    tasks.append(asyncio.create_task(subscribe_task_watchdog(bot, db, config)))
    from promo_link_watchdog import promo_link_watchdog
    tasks.append(asyncio.create_task(promo_link_watchdog(bot, db, config)))
    return tasks
