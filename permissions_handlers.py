# -*- coding: utf-8 -*-
"""
Обработчики для восстановления прав бота в каналах/группах
"""

from aiogram import Router, F, Bot
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError

from db import Database
from config import Config
from utils import escape

router = Router()


@router.callback_query(F.data.startswith("check_perms:"))
async def check_permissions_callback(
    callback: CallbackQuery,
    bot: Bot,
    db: Database,
    config: Config
):
    """Проверяет и пытается восстановить права бота"""
    await callback.answer("🔄 Проверяю права...", show_alert=False)
    
    try:
        task_id = int(callback.data.split(":")[1])
    except (ValueError, IndexError):
        await callback.answer("❌ Ошибка: неверный ID задания", show_alert=True)
        return
    
    # Получаем информацию о задании
    task = await db.fetchone(
        "SELECT chat_id, chat_type, chat_title, owner_id FROM tasks WHERE id = ?",
        (task_id,)
    )
    
    if not task:
        await callback.answer("❌ Задание не найдено", show_alert=True)
        return
    
    chat_id = task["chat_id"]
    chat_title = escape(task["chat_title"])
    
    # Проверяем права
    has_permissions = await _check_bot_permissions(bot, chat_id, task["chat_type"])
    
    if has_permissions:
        # Права восстановлены, активируем задание
        await db.execute(
            "UPDATE tasks SET active = 1 WHERE id = ?",
            (task_id,)
        )
        
        text = (
            f"✅ <b>Права восстановлены успешно!</b>\n\n"
            f"Канал/группа: <b>{chat_title}</b>\n"
            f"Задание #{task_id} вернулось в активный статус.\n\n"
            f"Работа будет возобновлена на следующей проверке."
        )
        
        try:
            await callback.message.edit_text(text, parse_mode="HTML")
        except TelegramBadRequest:
            await callback.message.answer(text, parse_mode="HTML")
    
    else:
        # Права еще не восстановлены
        text = (
            f"⚠️ <b>Права по-прежнему отсутствуют</b>\n\n"
            f"Канал/группа: <b>{chat_title}</b>\n\n"
            f"<b>Проверьте:</b>\n"
            f"1. Бот добавлен как администратор?\n"
            f"2. Есть ли у бота право «Приглашение пользователей»?\n\n"
            f"⏱️ Попробуйте позже или нажмите кнопку снова."
        )
        
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="🔄 Попробовать снова",
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
        
        try:
            await callback.message.edit_text(
                text,
                reply_markup=keyboard,
                parse_mode="HTML"
            )
        except TelegramBadRequest:
            await callback.message.answer(text, reply_markup=keyboard, parse_mode="HTML")


@router.callback_query(F.data.startswith("cancel_task:"))
async def cancel_task_callback(
    callback: CallbackQuery,
    db: Database
):
    """Отменяет задание"""
    try:
        task_id = int(callback.data.split(":")[1])
    except (ValueError, IndexError):
        await callback.answer("❌ Ошибка: неверный ID задания", show_alert=True)
        return
    
    # Получаем информацию о задании
    task = await db.fetchone(
        "SELECT id, owner_id, chat_title FROM tasks WHERE id = ?",
        (task_id,)
    )
    
    if not task:
        await callback.answer("❌ Задание не найдено", show_alert=True)
        return
    
    # Удаляем задание
    await db.execute(
        "UPDATE tasks SET deleted = 1 WHERE id = ?",
        (task_id,)
    )
    
    chat_title = escape(task["chat_title"])
    
    text = (
        f"✅ <b>Задание отменено</b>\n\n"
        f"Канал/группа: <b>{chat_title}</b>\n"
        f"Задание #{task_id} было удалено.\n\n"
        f"Вы можете создать новое задание в любой момент."
    )
    
    try:
        await callback.message.edit_text(text, parse_mode="HTML")
    except TelegramBadRequest:
        await callback.message.answer(text, parse_mode="HTML")


@router.callback_query(F.data == "help_bot_admin")
async def help_bot_admin_callback(callback: CallbackQuery):
    """Показывает инструкцию по добавлению бота админом"""
    text = (
        "📖 <b>Инструкция: Как добавить бота администратором</b>\n\n"
        
        "<b>Для группы/супергруппы:</b>\n"
        "1. Откройте группу\n"
        "2. Тапните на название группы (вверху экрана)\n"
        "3. Перейдите в <b>Администраторы</b>\n"
        "4. Нажмите <b>+ Добавить администратора</b>\n"
        "5. Найдите и выберите бота\n"
        "6. Убедитесь, что включено право:\n"
        "   ✓ Приглашение пользователей\n"
        "7. Нажмите <b>Сохранить</b>\n\n"
        
        "<b>Для канала:</b>\n"
        "1. Откройте канал\n"
        "2. Тапните на название канала (вверху)\n"
        "3. Перейдите в <b>Администраторы</b>\n"
        "4. Нажмите <b>+ Администратор</b>\n"
        "5. Выберите бота\n"
        "6. Включите право «Приглашение пользователей»\n"
        "7. Сохраните\n\n"
        
        "<b>Минимально необходимые права:</b>\n"
        "• 👥 Приглашение пользователей\n\n"
        
        "❓ Если остались вопросы, обратитесь в поддержку."
    )
    
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="◀️ Вернуться",
                    callback_data="back_to_task"
                )
            ]
        ]
    )
    
    try:
        await callback.message.edit_text(
            text,
            reply_markup=keyboard,
            parse_mode="HTML"
        )
    except TelegramBadRequest:
        await callback.message.answer(text, reply_markup=keyboard, parse_mode="HTML")


async def _check_bot_permissions(
    bot: Bot,
    chat_id: int,
    chat_type: str
) -> bool:
    try:
        bot_info = await bot.get_me()
        bot_id = bot_info.id
        bot_member = await bot.get_chat_member(chat_id, bot_id)
    except (TelegramBadRequest, TelegramForbiddenError):
        # Бот не состоит в чате или доступ запрещён
        return False
    except Exception as e:
        # Непредвиденная ошибка – логируем и считаем, что прав нет
        print(f"⚠️ Неожиданная ошибка при проверке прав: {e}")
        return False

    # Если бот – создатель, у него есть все права
    if bot_member.status == "creator":
        return True

    # Для администратора проверяем конкретные права
    if bot_member.status != "administrator":
        return False

    required = ["can_invite_users"]
    for attr in required:
        if not getattr(bot_member, attr, False):
            return False

    return True


__all__ = ["router"]
