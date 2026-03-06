# -*- coding: utf-8 -*-
"""
Расширенная версия с сохранением состояния потерянных прав в БД
Рекомендуется использовать эту версию для production
"""

import asyncio
from typing import Dict, Optional
from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from db import Database
from config import Config
from utils import now_ts, escape


class BotPermissionsWatchdogAdvanced:
    """
    Продвинутая версия watchdog с сохранением состояния в БД.
    Данные о потерянных правах сохраняются в базу и переживают перезагрузку бота.
    """

    CHECK_INTERVAL = 5 * 60  # 5 минут
    RECOVERY_TIMEOUT = 60 * 60  # 1 час

    def __init__(self, bot: Bot, db: Database, config: Config):
        self.bot = bot
        self.db = db
        self.config = config
        self._initialized = False

    async def start(self):
        """Инициализация и запуск watchdog"""
        await self._ensure_db_columns()
        self._initialized = True
        asyncio.create_task(self._watchdog_loop())

    async def _ensure_db_columns(self):
        """Убеждаемся, что есть необходимые колонки в БД"""
        try:
            # Проверяем и добавляем колонку для времени потери прав
            await self.db.execute(
                """
                ALTER TABLE tasks 
                ADD COLUMN lost_permissions_at INTEGER DEFAULT NULL
                """
            )
        except Exception as e:
            # Колонка уже существует или другая ошибка – игнорируем
            if "duplicate column" not in str(e).lower():
                pass

    async def _watchdog_loop(self):
        """Основной цикл проверки"""
        while True:
            try:
                await self._check_all_active_tasks()
                await self._cleanup_expired_losses()
            except Exception:
                pass
            await asyncio.sleep(self.CHECK_INTERVAL)

    async def _check_all_active_tasks(self):
        """Проверяет все активные задания"""
        try:
            tasks = await self.db.fetchall(
                """
                SELECT 
                    id, owner_id, chat_id, chat_type, chat_title, 
                    lost_permissions_at
                FROM tasks
                WHERE active = 1 AND deleted = 0 AND chat_id IS NOT NULL
                """
            )

            for task in tasks:
                task_id = task["id"]
                owner_id = task["owner_id"]
                chat_id = task["chat_id"]
                chat_type = task["chat_type"]
                chat_title = escape(task["chat_title"])
                lost_at = task["lost_permissions_at"]

                has_permissions = await self._check_bot_permissions(
                    chat_id, chat_type
                )

                if not has_permissions:
                    # Права потеряны
                    if lost_at is None:
                        # Первое обнаружение потери
                        await self._handle_permission_loss_first_time(
                            task_id, owner_id, chat_id, chat_title
                        )
                    # Иначе продолжаем ждать восстановления
                else:
                    # Права восстановлены
                    if lost_at is not None:
                        # Были потеряны, но восстановились
                        await self._handle_permissions_restored(
                            task_id, owner_id, chat_id, chat_title
                        )
        except Exception:
            pass

    async def _cleanup_expired_losses(self):
        """Удаляет задания, у которых закончилось время восстановления"""
        try:
            now = now_ts()
            expired_cutoff = now - self.RECOVERY_TIMEOUT

            # Получаем задания с истёкшим временем
            expired_tasks = await self.db.fetchall(
                """
                SELECT id, owner_id, chat_title
                FROM tasks
                WHERE 
                    lost_permissions_at IS NOT NULL 
                    AND lost_permissions_at < ?
                    AND deleted = 0
                """,
                (expired_cutoff,)
            )

            for task in expired_tasks:
                task_id = task["id"]
                owner_id = task["owner_id"]
                chat_title = escape(task["chat_title"])

                # Удаляем задание
                await self.db.execute(
                    """
                    UPDATE tasks 
                    SET deleted = 1, lost_permissions_at = NULL 
                    WHERE id = ?
                    """,
                    (task_id,)
                )

                # Уведомляем пользователя
                await self._notify_task_deleted(owner_id, task_id, chat_title)

        except Exception:
            pass

    async def _check_bot_permissions(
        self, chat_id: int, chat_type: str
    ) -> bool:
        try:
            bot_info = await self.bot.get_me()
            bot_id = bot_info.id
            bot_member = await self.bot.get_chat_member(chat_id, bot_id)
        except (TelegramBadRequest, TelegramForbiddenError):
            return False

        # Создатель имеет все права
        if bot_member.status == "creator":
            return True

        # Проверяем статус администратора
        if bot_member.status != "administrator":
            return False

        # Проверяем права
        required_perms = [
            ("can_invite_users", "приглашение пользователей"),
        ]
        for attr, _ in required_perms:
            if not getattr(bot_member, attr, False):
                return False

        return True

    async def _handle_permission_loss_first_time(
        self, task_id: int, owner_id: int, chat_id: int, chat_title: str
    ):
        """Обрабатывает первый раз обнаруженную потерю прав"""
        now = now_ts()

        # Сохраняем время потери в БД
        await self.db.execute(
            "UPDATE tasks SET active = 0, lost_permissions_at = ? WHERE id = ?",
            (now, task_id)
        )

        # Уведомляем пользователя
        await self._notify_permission_lost(owner_id, task_id, chat_title)

    async def _handle_permissions_restored(
        self, task_id: int, owner_id: int, chat_id: int, chat_title: str
    ):
        """Обрабатывает восстановление прав"""
        # Очищаем флаг потери прав
        await self.db.execute(
            "UPDATE tasks SET active = 1, lost_permissions_at = NULL WHERE id = ?",
            (task_id,)
        )

        # Уведомляем пользователя
        await self._notify_permissions_restored(owner_id, task_id, chat_title)

    async def _notify_permission_lost(
        self, owner_id: int, task_id: int, chat_title: str
    ):
        """Уведомляет о потере прав"""
        text = (
            f"⚠️ <b>Проблема с заданием #{task_id}</b>\n\n"
            f"Бот потерял доступ к нужному праву в:\n"
            f"<b>{chat_title}</b>\n\n"
            f"🔐 <b>Требуемые права:</b>\n"
            f"• Приглашение пользователей\n\n"
            f"<b>Действие:</b>\n"
            f"1️⃣ Убедитесь, что бот администратор\n"
            f"2️⃣ Включите право «Приглашение пользователей»\n"
            f"3️⃣ Проверьте права (кнопка ниже)\n"
            f"4️⃣ Или отмените задание\n\n"
            f"⏰ <b>Время на восстановление: 1 час</b>\n"
            f"После истечения задание удалится автоматически."
        )

        keyboard = self._get_recovery_keyboard(task_id)

        try:
            await self.bot.send_message(
                chat_id=owner_id,
                text=text,
                reply_markup=keyboard,
                parse_mode="HTML"
            )
        except Exception:
            pass

    async def _notify_permissions_restored(
        self, owner_id: int, task_id: int, chat_title: str
    ):
        """Уведомляет о восстановлении прав"""
        text = (
            f"✅ <b>Права восстановлены!</b>\n\n"
            f"Бот вернул права в:\n"
            f"<b>{chat_title}</b>\n\n"
            f"Задание #{task_id} вернулось в активный статус ✨"
        )

        try:
            await self.bot.send_message(
                chat_id=owner_id,
                text=text,
                parse_mode="HTML"
            )
        except Exception:
            pass

    async def _notify_task_deleted(
        self, owner_id: int, task_id: int, chat_title: str
    ):
        """Уведомляет об удалении задания"""
        text = (
            f"❌ <b>Задание удалено</b>\n\n"
            f"Время восстановления прав истекло.\n\n"
            f"<b>{chat_title}</b> (Задание #{task_id})\n\n"
            f"Для создания нового задания добавьте бота администратором и включите право «Приглашение пользователей»."
        )

        try:
            await self.bot.send_message(
                chat_id=owner_id,
                text=text,
                parse_mode="HTML"
            )
        except Exception:
            pass

    def _get_recovery_keyboard(self, task_id: int) -> InlineKeyboardMarkup:
        """Создаёт клавиатуру восстановления"""
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="🔄 Проверить права",
                        callback_data=f"check_perms:{task_id}"
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="❌ Отменить задание",
                        callback_data=f"cancel_task:{task_id}"
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="📖 Инструкция",
                        callback_data="help_bot_admin"
                    )
                ]
            ]
        )

    async def get_lost_permissions_stats(self) -> Dict:
        """Получает статистику по потерянным правам"""
        try:
            total = await self.db.fetchone(
                "SELECT COUNT(*) as count FROM tasks WHERE lost_permissions_at IS NOT NULL"
            )

            by_owner = await self.db.fetchall(
                """
                SELECT owner_id, COUNT(*) as count 
                FROM tasks 
                WHERE lost_permissions_at IS NOT NULL
                GROUP BY owner_id
                """
            )

            return {
                "total_with_lost_perms": total["count"],
                "by_owner": {row["owner_id"]: row["count"] for row in by_owner}
            }
        except Exception:
            return {"total_with_lost_perms": 0, "by_owner": {}}


async def bot_permissions_watchdog_advanced(
    bot: Bot, db: Database, config: Config
):
    """Запускает продвинутый watchdog"""
    watchdog = BotPermissionsWatchdogAdvanced(bot, db, config)
    await watchdog.start()


__all__ = ["BotPermissionsWatchdogAdvanced", "bot_permissions_watchdog_advanced"]
