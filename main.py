import asyncio

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.storage.memory import MemoryStorage
from dotenv import load_dotenv

from config import Config
from db import Database
from handlers import BlockedUserMiddleware, CallbackAntiFloodMiddleware, MessageAntiFloodMiddleware, routers
from services import start_background_tasks


async def main() -> None:
    load_dotenv()
    config = Config.load()
    if not config.bot_token:
        raise RuntimeError("BOT_TOKEN is required")
    db = Database(config.db_path)
    await db.init()
    bot = Bot(
        config.bot_token,
        default=DefaultBotProperties(parse_mode="HTML", link_preview_is_disabled=True),
    )
    dp = Dispatcher(storage=MemoryStorage())
    dp["db"] = db
    dp["config"] = config
    dp.message.middleware(BlockedUserMiddleware())
    dp.callback_query.middleware(BlockedUserMiddleware())
    dp.message.middleware(MessageAntiFloodMiddleware(limit=0.7))
    dp.callback_query.middleware(CallbackAntiFloodMiddleware(limit=0.7))
    for router in routers:
        dp.include_router(router)
    tasks = await start_background_tasks(bot, db, config)
    try:
        await dp.start_polling(bot)
    finally:
        for task in tasks:
            task.cancel()
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
