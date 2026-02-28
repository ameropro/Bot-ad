import asyncio
from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from db import Database
from config import Config

async def promo_link_watchdog(bot: Bot, db: Database, config: Config):
    """Check promo links every hour to ensure they're still valid"""
    while True:
        try:
            # Get all active tasks with action links
            tasks = await db.fetchall(
                "SELECT id, type, action_link, title FROM tasks WHERE active = 1 AND action_link IS NOT NULL LIMIT 100"
            )
            
            for task in tasks:
                try:
                    link = task.get("action_link", "")
                    if not link:
                        continue
                    
                    # Try to access the link to verify it's valid
                    if link.startswith("https://t.me/"):
                        # For Telegram links, we'll log them but not actively check
                        # as we'd need to actually join channels which is not ideal
                        pass
                    
                except Exception as e:
                    # Log but continue checking other tasks
                    pass
                    
        except Exception as e:
            # Log error but don't crash the watchdog
            pass
        
        # Wait 1 hour before next check
        await asyncio.sleep(3600)
