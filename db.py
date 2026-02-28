import json
from typing import Any, Optional

import aiosqlite

from utils import now_ts


class Database:
    def __init__(self, path: str) -> None:
        self._path = path
        self._conn: Optional[aiosqlite.Connection] = None

    async def init(self) -> None:
        self._conn = await aiosqlite.connect(self._path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA foreign_keys = ON")
        await self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                balance INTEGER NOT NULL DEFAULT 0,
                level INTEGER NOT NULL DEFAULT 1,
                referrer_id INTEGER,
                referral_count INTEGER NOT NULL DEFAULT 0,
                referral_earned INTEGER NOT NULL DEFAULT 0,
                completed_tasks INTEGER NOT NULL DEFAULT 0,
                commission_free_until INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL,
                last_active INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER NOT NULL,
                type TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                reward INTEGER NOT NULL,
                required_reaction TEXT,
                source_chat_id INTEGER,
                source_message_id INTEGER,
                chat_id INTEGER,
                chat_type TEXT,
                chat_username TEXT,
                chat_title TEXT,
                action_link TEXT,
                created_at INTEGER NOT NULL,
                active INTEGER NOT NULL DEFAULT 1,
                deleted INTEGER NOT NULL DEFAULT 0,
                total_count INTEGER NOT NULL,
                remaining_count INTEGER NOT NULL,
                escrow_balance INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS task_claims (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                submitted_at INTEGER,
                completed_at INTEGER,
                hold_until INTEGER,
                penalty_deadline INTEGER,
                warning_sent INTEGER NOT NULL DEFAULT 0,
                proof_message_id INTEGER,
                review_message_id INTEGER,
                review_note TEXT,
                UNIQUE(task_id, user_id)
            );

            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                amount INTEGER NOT NULL,
                reason TEXT NOT NULL,
                meta TEXT,
                created_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS promo_codes (
                code TEXT PRIMARY KEY,
                amount INTEGER NOT NULL,
                max_uses INTEGER NOT NULL,
                used_count INTEGER NOT NULL DEFAULT 0,
                expires_at INTEGER,
                active INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS promo_redemptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                used_at INTEGER NOT NULL,
                UNIQUE(code, user_id)
            );

            CREATE TABLE IF NOT EXISTS sponsors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                chat_username TEXT,
                title TEXT,
                invite_link TEXT,
                active INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS invoices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                coins INTEGER NOT NULL,
                asset TEXT NOT NULL,
                invoice_id TEXT NOT NULL,
                pay_url TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at INTEGER NOT NULL,
                paid_at INTEGER
            );

            CREATE TABLE IF NOT EXISTS user_achievements (
                user_id INTEGER NOT NULL,
                code TEXT NOT NULL,
                achieved_at INTEGER NOT NULL,
                PRIMARY KEY (user_id, code)
            );

            CREATE TABLE IF NOT EXISTS rate_limits (
                user_id INTEGER NOT NULL,
                action TEXT NOT NULL,
                window_start INTEGER NOT NULL,
                count INTEGER NOT NULL DEFAULT 0,
                blocked_until INTEGER,
                strike_count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (user_id, action)
            );

            CREATE TABLE IF NOT EXISTS user_blocks (
                user_id INTEGER PRIMARY KEY,
                blocked_until INTEGER NOT NULL,
                reason TEXT
            );

            CREATE TABLE IF NOT EXISTS op_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                type TEXT NOT NULL,
                target_chat_id INTEGER,
                target_username TEXT,
                invite_link TEXT,
                referrer_id INTEGER,
                created_at INTEGER NOT NULL,
                expires_at INTEGER,
                target_count INTEGER,
                completed_count INTEGER NOT NULL DEFAULT 0,
                active INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS op_passes (
                rule_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                passed_at INTEGER NOT NULL,
                PRIMARY KEY (rule_id, user_id)
            );

            CREATE TABLE IF NOT EXISTS chat_settings (
                chat_id INTEGER PRIMARY KEY,
                auto_delete_seconds INTEGER
            );

            CREATE TABLE IF NOT EXISTS app_settings (
                setting_key TEXT PRIMARY KEY,
                setting_value TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_task_claims_status ON task_claims(status);
            CREATE INDEX IF NOT EXISTS idx_tasks_active ON tasks(active, type);
            CREATE INDEX IF NOT EXISTS idx_op_rules_chat ON op_rules(chat_id, active);
            """
        )
        await self._ensure_columns()
        await self._conn.commit()

    async def _ensure_columns(self) -> None:
        if not self._conn:
            return
        cursor = await self._conn.execute("PRAGMA table_info(tasks)")
        rows = await cursor.fetchall()
        columns = {row["name"] for row in rows}
        if "chat_type" not in columns:
            await self._conn.execute("ALTER TABLE tasks ADD COLUMN chat_type TEXT")
        if "action_link" not in columns:
            await self._conn.execute("ALTER TABLE tasks ADD COLUMN action_link TEXT")
        if "deleted" not in columns:
            await self._conn.execute(
                "ALTER TABLE tasks ADD COLUMN deleted INTEGER NOT NULL DEFAULT 0"
            )
        if "task_status" not in columns:
            await self._conn.execute(
                "ALTER TABLE tasks ADD COLUMN task_status TEXT DEFAULT 'active'"
            )
        if "status_reason" not in columns:
            await self._conn.execute(
                "ALTER TABLE tasks ADD COLUMN status_reason TEXT"
            )
        cursor = await self._conn.execute("PRAGMA table_info(users)")
        rows = await cursor.fetchall()
        user_columns = {row["name"] for row in rows}
        if "commission_free_until" not in user_columns:
            await self._conn.execute(
                "ALTER TABLE users ADD COLUMN commission_free_until INTEGER NOT NULL DEFAULT 0"
            )

    async def close(self) -> None:
        if self._conn:
            await self._conn.close()

    async def execute(self, query: str, params: tuple = ()) -> aiosqlite.Cursor:
        if not self._conn:
            raise RuntimeError("Database is not initialized")
        cursor = await self._conn.execute(query, params)
        await self._conn.commit()
        return cursor

    async def fetchone(self, query: str, params: tuple = ()) -> Optional[dict]:
        if not self._conn:
            raise RuntimeError("Database is not initialized")
        cursor = await self._conn.execute(query, params)
        row = await cursor.fetchone()
        return dict(row) if row else None

    async def fetchall(self, query: str, params: tuple = ()) -> list[dict]:
        if not self._conn:
            raise RuntimeError("Database is not initialized")
        cursor = await self._conn.execute(query, params)
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    async def upsert_user(self, user_id: int, username: str | None, first_name: str | None) -> None:
        now = now_ts()
        existing = await self.fetchone("SELECT id FROM users WHERE id = ?", (user_id,))
        if existing:
            await self.execute(
                "UPDATE users SET username = ?, first_name = ?, last_active = ? WHERE id = ?",
                (username, first_name, now, user_id),
            )
        else:
            await self.execute(
                """
                INSERT INTO users (id, username, first_name, created_at, last_active)
                VALUES (?, ?, ?, ?, ?)
                """,
                (user_id, username, first_name, now, now),
            )

    async def get_user(self, user_id: int) -> Optional[dict]:
        return await self.fetchone("SELECT * FROM users WHERE id = ?", (user_id,))

    async def get_referrer_id(self, user_id: int) -> Optional[int]:
        row = await self.fetchone(
            "SELECT referrer_id FROM users WHERE id = ?",
            (user_id,),
        )
        if not row or row["referrer_id"] is None:
            return None
        return int(row["referrer_id"])

    async def list_user_achievements(self, user_id: int) -> list[dict]:
        return await self.fetchall(
            "SELECT code, achieved_at FROM user_achievements WHERE user_id = ?",
            (user_id,),
        )

    async def add_user_achievement(self, user_id: int, code: str, achieved_at: int) -> bool:
        cursor = await self.execute(
            """
            INSERT OR IGNORE INTO user_achievements (user_id, code, achieved_at)
            VALUES (?, ?, ?)
            """,
            (user_id, code, achieved_at),
        )
        return cursor.rowcount > 0

    async def count_user_achievements(self, user_id: int) -> int:
        row = await self.fetchone(
            "SELECT COUNT(*) as cnt FROM user_achievements WHERE user_id = ?",
            (user_id,),
        )
        return int(row["cnt"]) if row else 0

    async def update_last_active(self, user_id: int) -> None:
        await self.execute("UPDATE users SET last_active = ? WHERE id = ?", (now_ts(), user_id))

    async def add_balance(self, user_id: int, amount: int, reason: str, meta: Optional[dict] = None) -> None:
        meta_json = json.dumps(meta or {}, ensure_ascii=False)
        await self.execute("UPDATE users SET balance = balance + ? WHERE id = ?", (amount, user_id))
        await self.execute(
            """
            INSERT INTO transactions (user_id, amount, reason, meta, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (user_id, amount, reason, meta_json, now_ts()),
        )

    async def increment_completed_tasks(self, user_id: int) -> int:
        await self.execute(
            "UPDATE users SET completed_tasks = completed_tasks + 1 WHERE id = ?",
            (user_id,),
        )
        row = await self.fetchone("SELECT completed_tasks FROM users WHERE id = ?", (user_id,))
        return int(row["completed_tasks"]) if row else 0

    async def decrement_completed_tasks(self, user_id: int) -> int:
        await self.execute(
            """
            UPDATE users
            SET completed_tasks = CASE
                WHEN completed_tasks > 0 THEN completed_tasks - 1
                ELSE 0
            END
            WHERE id = ?
            """,
            (user_id,),
        )
        row = await self.fetchone("SELECT completed_tasks FROM users WHERE id = ?", (user_id,))
        return int(row["completed_tasks"]) if row else 0

    async def set_level(self, user_id: int, level: int) -> None:
        await self.execute("UPDATE users SET level = ? WHERE id = ?", (level, user_id))

    async def set_referrer(self, user_id: int, referrer_id: int) -> None:
        await self.execute(
            "UPDATE users SET referrer_id = ? WHERE id = ? AND referrer_id IS NULL",
            (referrer_id, user_id),
        )

    async def update_referrer(self, user_id: int, referrer_id: int) -> None:
        await self.execute(
            "UPDATE users SET referrer_id = ? WHERE id = ?",
            (referrer_id, user_id),
        )
        row = await self.fetchone("SELECT referrer_id FROM users WHERE id = ?", (user_id,))
        if row and row["referrer_id"]:
            return int(row["referrer_id"])
        return None

    async def add_referral_count(self, referrer_id: int) -> None:
        await self.execute(
            "UPDATE users SET referral_count = referral_count + 1 WHERE id = ?",
            (referrer_id,),
        )

    async def add_referral_earned(self, referrer_id: int, amount: int) -> None:
        await self.execute(
            "UPDATE users SET referral_earned = referral_earned + ? WHERE id = ?",
            (amount, referrer_id),
        )

    async def add_commission_free_time(self, user_id: int, seconds: int) -> None:
        if seconds <= 0:
            return
        row = await self.fetchone(
            "SELECT commission_free_until FROM users WHERE id = ?",
            (user_id,),
        )
        current = int(row["commission_free_until"] or 0) if row else 0
        base = max(now_ts(), current)
        await self.execute(
            "UPDATE users SET commission_free_until = ? WHERE id = ?",
            (base + seconds, user_id),
        )

    async def create_task(
        self,
        owner_id: int,
        task_type: str,
        title: str,
        description: str | None,
        reward: int,
        total_count: int,
        required_reaction: str | None,
        source_chat_id: int | None,
        source_message_id: int | None,
        chat_id: int | None,
        chat_type: str | None,
        chat_username: str | None,
        chat_title: str | None,
        action_link: str | None,
        commission_fee: int = 0,
    ) -> Optional[int]:
        if not self._conn:
            raise RuntimeError("Database is not initialized")
        total_cost = reward * total_count
        total_charge = total_cost + max(0, commission_fee)
        await self._conn.execute("BEGIN")
        row = await self._conn.execute(
            "SELECT balance FROM users WHERE id = ?",
            (owner_id,),
        )
        data = await row.fetchone()
        balance = int(data["balance"]) if data else 0
        if balance < total_charge:
            await self._conn.execute("ROLLBACK")
            return None
        await self._conn.execute(
            "UPDATE users SET balance = balance - ? WHERE id = ?",
            (total_charge, owner_id),
        )
        await self._conn.execute(
            """
            INSERT INTO transactions (user_id, amount, reason, meta, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                owner_id,
                -total_cost,
                "task_create",
                json.dumps({"total_cost": total_cost}, ensure_ascii=False),
                now_ts(),
            ),
        )
        if commission_fee and commission_fee > 0:
            await self._conn.execute(
                """
                INSERT INTO transactions (user_id, amount, reason, meta, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    owner_id,
                    -commission_fee,
                    "task_fee",
                    json.dumps({"fee": commission_fee}, ensure_ascii=False),
                    now_ts(),
                ),
            )
        cursor = await self._conn.execute(
            """
            INSERT INTO tasks (
                owner_id, type, title, description, reward, required_reaction,
                source_chat_id, source_message_id, chat_id, chat_type, chat_username, chat_title, action_link,
                created_at, deleted, total_count, remaining_count, escrow_balance
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                owner_id,
                task_type,
                title,
                description,
                reward,
                required_reaction,
                source_chat_id,
                source_message_id,
                chat_id,
                chat_type,
                chat_username,
                chat_title,
                action_link,
                now_ts(),
                0,
                total_count,
                total_count,
                total_cost,
            ),
        )
        await self._conn.commit()
        return int(cursor.lastrowid)

    async def get_task(self, task_id: int) -> Optional[dict]:
        return await self.fetchone("SELECT * FROM tasks WHERE id = ?", (task_id,))

    async def list_available_tasks(
        self, user_id: int, task_type: str, limit: int = 10, chat_filter: str | None = None
    ) -> list[dict]:
        extra = ""
        params: list = [task_type, user_id, user_id, user_id]
        if chat_filter == "channel":
            extra = " AND chat_type = ?"
            params.append("channel")
        elif chat_filter == "group":
            extra = " AND (chat_type IS NULL OR chat_type != ?)"
            params.append("channel")
        params.append(limit)
        return await self.fetchall(
            f"""
            SELECT * FROM tasks
            WHERE active = 1
                AND remaining_count > 0
                AND type = ?
                AND owner_id != ?
                AND (
                    id NOT IN (
                        SELECT task_id FROM task_claims
                        WHERE user_id = ?
                            AND status IN ('completed', 'completed_hold', 'rejected', 'revoked')
                    )
                    OR id IN (
                        SELECT task_id FROM task_claims
                        WHERE user_id = ?
                            AND status NOT IN ('completed', 'completed_hold', 'rejected', 'revoked')
                    )
                ){extra}
            ORDER BY id DESC
            LIMIT ?
            """,
            tuple(params),
        )

    async def get_reward_threshold(
        self, task_type: str, offset: int, chat_filter: str | None = None
    ) -> Optional[int]:
        extra = ""
        params: list = [task_type]
        if chat_filter == "channel":
            extra = " AND chat_type = ?"
            params.append("channel")
        elif chat_filter == "group":
            extra = " AND (chat_type IS NULL OR chat_type != ?)"
            params.append("channel")
        params.append(offset)
        row = await self.fetchone(
            f"""
            SELECT reward
            FROM tasks
            WHERE active = 1
              AND remaining_count > 0
              AND type = ?{extra}
            ORDER BY reward DESC, id DESC
            LIMIT 1 OFFSET ?
            """,
            tuple(params),
        )
        return int(row["reward"]) if row else None

    async def list_owner_tasks(self, owner_id: int, limit: int, offset: int) -> list[dict]:
        return await self.fetchall(
            """
            SELECT * FROM tasks
            WHERE owner_id = ? AND deleted = 0
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
            """,
            (owner_id, limit, offset),
        )

    async def count_owner_tasks(self, owner_id: int) -> int:
        row = await self.fetchone(
            "SELECT COUNT(*) as cnt FROM tasks WHERE owner_id = ? AND deleted = 0",
            (owner_id,),
        )
        return int(row["cnt"]) if row else 0

    async def owner_stats(self, owner_id: int) -> dict:
        row = await self.fetchone(
            """
            SELECT
              COUNT(*) as total,
              SUM(CASE WHEN active = 1 AND remaining_count > 0 THEN 1 ELSE 0 END) as active,
              SUM(total_count - remaining_count) as completed,
              SUM((total_count * reward) - escrow_balance) as spent
            FROM tasks
            WHERE owner_id = ? AND deleted = 0
            """,
            (owner_id,),
        )
        return {
            "total": int(row["total"] or 0) if row else 0,
            "active": int(row["active"] or 0) if row else 0,
            "completed": int(row["completed"] or 0) if row else 0,
            "spent": int(row["spent"] or 0) if row else 0,
        }

    async def get_owner_task(self, owner_id: int, task_id: int) -> Optional[dict]:
        return await self.fetchone(
            "SELECT * FROM tasks WHERE id = ? AND owner_id = ? AND deleted = 0",
            (task_id, owner_id),
        )

    async def stop_task(self, owner_id: int, task_id: int) -> bool:
        cursor = await self.execute(
            "UPDATE tasks SET active = 0 WHERE id = ? AND owner_id = ? AND deleted = 0",
            (task_id, owner_id),
        )
        return cursor.rowcount > 0

    async def add_task_quantity(self, owner_id: int, task_id: int, amount: int) -> Optional[int]:
        if not self._conn:
            raise RuntimeError("Database is not initialized")
        await self._conn.execute("BEGIN")
        task_row = await self._conn.execute(
            "SELECT reward FROM tasks WHERE id = ? AND owner_id = ?",
            (task_id, owner_id),
        )
        task = await task_row.fetchone()
        if not task:
            await self._conn.execute("ROLLBACK")
            return None
        reward = int(task["reward"])
        total_cost = reward * amount
        balance_row = await self._conn.execute("SELECT balance FROM users WHERE id = ?", (owner_id,))
        balance_data = await balance_row.fetchone()
        balance = int(balance_data["balance"]) if balance_data else 0
        if balance < total_cost:
            await self._conn.execute("ROLLBACK")
            return None
        await self._conn.execute(
            "UPDATE users SET balance = balance - ? WHERE id = ?",
            (total_cost, owner_id),
        )
        await self._conn.execute(
            """
            INSERT INTO transactions (user_id, amount, reason, meta, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                owner_id,
                -total_cost,
                "task_extend",
                json.dumps({"task_id": task_id, "count": amount}, ensure_ascii=False),
                now_ts(),
            ),
        )
        await self._conn.execute(
            """
            UPDATE tasks
            SET total_count = total_count + ?,
                remaining_count = remaining_count + ?,
                escrow_balance = escrow_balance + ?,
                active = 1
            WHERE id = ? AND owner_id = ?
            """,
            (amount, amount, total_cost, task_id, owner_id),
        )
        await self._conn.commit()
        return total_cost

    async def reduce_task_quantity(self, owner_id: int, task_id: int, amount: int) -> Optional[int]:
        if not self._conn:
            raise RuntimeError("Database is not initialized")
        await self._conn.execute("BEGIN")
        task_row = await self._conn.execute(
            "SELECT reward, remaining_count, total_count FROM tasks WHERE id = ? AND owner_id = ?",
            (task_id, owner_id),
        )
        task = await task_row.fetchone()
        if not task:
            await self._conn.execute("ROLLBACK")
            return None
        remaining = int(task["remaining_count"])
        total = int(task["total_count"])
        if amount > remaining:
            await self._conn.execute("ROLLBACK")
            return None
        refund = int(task["reward"]) * amount
        new_remaining = remaining - amount
        new_total = total - amount
        await self._conn.execute(
            """
            UPDATE tasks
            SET total_count = ?,
                remaining_count = ?,
                escrow_balance = escrow_balance - ?,
                active = CASE WHEN ? > 0 THEN active ELSE 0 END
            WHERE id = ? AND owner_id = ?
            """,
            (new_total, new_remaining, refund, new_remaining, task_id, owner_id),
        )
        await self._conn.execute(
            "UPDATE users SET balance = balance + ? WHERE id = ?",
            (refund, owner_id),
        )
        await self._conn.execute(
            """
            INSERT INTO transactions (user_id, amount, reason, meta, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                owner_id,
                refund,
                "task_reduce",
                json.dumps({"task_id": task_id, "count": amount}, ensure_ascii=False),
                now_ts(),
            ),
        )
        await self._conn.commit()
        return refund

    async def delete_task(self, owner_id: int, task_id: int) -> Optional[int]:
        if not self._conn:
            raise RuntimeError("Database is not initialized")
        await self._conn.execute("BEGIN")
        task_row = await self._conn.execute(
            "SELECT reward, remaining_count, total_count, owner_id FROM tasks WHERE id = ? AND deleted = 0",
            (task_id, owner_id),
        )
        task = await task_row.fetchone()
        if not task:
            await self._conn.execute("ROLLBACK")
            return None
        remaining = int(task["remaining_count"])
        total = int(task["total_count"])
        refund = int(task["reward"]) * remaining
        new_total = total - remaining
        await self._conn.execute(
            """
            UPDATE tasks
            SET total_count = ?, remaining_count = 0, escrow_balance = 0, active = 0, deleted = 1
            WHERE id = ? AND owner_id = ?
            """,
            (new_total, task_id, owner_id),
        )
        if refund > 0:
            await self._conn.execute(
                "UPDATE users SET balance = balance + ? WHERE id = ?",
                (refund, owner_id),
            )
            await self._conn.execute(
                """
                INSERT INTO transactions (user_id, amount, reason, meta, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    owner_id,
                    refund,
                    "task_delete_refund",
                    json.dumps({"task_id": task_id, "count": remaining}, ensure_ascii=False),
                    now_ts(),
                ),
            )
        await self._conn.commit()
        return refund

    async def resume_task(self, task_id: int, owner_id: int | None = None) -> bool:
        if owner_id:
            cursor = await self.execute(
                """
                UPDATE tasks
                SET active = 1, task_status = 'active', status_reason = NULL
                WHERE id = ? AND owner_id = ? AND remaining_count > 0 AND deleted = 0
                """,
                (task_id, owner_id),
            )
        else:
            cursor = await self.execute(
                """
                UPDATE tasks
                SET active = 1, task_status = 'active', status_reason = NULL
                WHERE id = ? AND remaining_count > 0 AND deleted = 0
                """,
                (task_id,),
            )
        return cursor.rowcount > 0

    async def pause_task(self, task_id: int, reason: str) -> None:
        await self.execute(
            """
            UPDATE tasks
            SET active = 0, task_status = 'paused', status_reason = ?
            WHERE id = ?
            """,
            (reason, task_id),
        )

    async def set_task_status(self, task_id: int, status: str, reason: str | None = None) -> None:
        active = 1 if status == "active" else 0
        await self.execute(
            """
            UPDATE tasks
            SET active = ?, task_status = ?, status_reason = ?
            WHERE id = ?
            """,
            (active, status, reason, task_id),
        )

    async def get_owner_subscribe_task(self, owner_id: int, chat_id: int) -> Optional[dict]:
        return await self.fetchone(
            """
            SELECT * FROM tasks
            WHERE owner_id = ? AND type = 'subscribe' AND chat_id = ? AND deleted = 0
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (owner_id, chat_id),
        )

    async def delete_task_by_admin(self, task_id: int) -> Optional[int]:
        if not self._conn:
            raise RuntimeError("Database is not initialized")
        await self._conn.execute("BEGIN")
        task_row = await self._conn.execute(
            "SELECT reward, remaining_count, total_count, owner_id FROM tasks WHERE id = ? AND deleted = 0",
            (task_id,),
        )
        task = await task_row.fetchone()
        if not task:
            await self._conn.execute("ROLLBACK")
            return None
        remaining = int(task["remaining_count"])
        total = int(task["total_count"])
        refund = int(task["reward"]) * remaining
        new_total = total - remaining
        owner_id = int(task["owner_id"])
        await self._conn.execute(
            """
            UPDATE tasks
            SET total_count = ?, remaining_count = 0, escrow_balance = 0, active = 0, deleted = 1
            WHERE id = ?
            """,
            (new_total, task_id),
        )
        if refund > 0:
            await self._conn.execute(
                "UPDATE users SET balance = balance + ? WHERE id = ?",
                (refund, owner_id),
            )
            await self._conn.execute(
                """
                INSERT INTO transactions (user_id, amount, reason, meta, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    owner_id,
                    refund,
                    "task_delete_refund",
                    json.dumps({"task_id": task_id, "count": remaining}, ensure_ascii=False),
                    now_ts(),
                ),
            )
        await self._conn.commit()
        return refund

    async def list_promos(self, active_only: bool = True) -> list[dict]:
        if active_only:
            return await self.fetchall(
                "SELECT * FROM promo_codes WHERE active = 1 ORDER BY code ASC",
                (),
            )
        return await self.fetchall(
            "SELECT * FROM promo_codes ORDER BY code ASC",
            (),
        )

    async def deactivate_promo(self, code: str) -> bool:
        cursor = await self.execute(
            "UPDATE promo_codes SET active = 0 WHERE code = ?",
            (code,),
        )
        return cursor.rowcount > 0

    async def get_task_claim_stats(self, task_id: int) -> dict:
        row = await self.fetchone(
            """
            SELECT
              SUM(CASE WHEN status = 'submitted' THEN 1 ELSE 0 END) as submitted,
              SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
              SUM(CASE WHEN status = 'completed_hold' THEN 1 ELSE 0 END) as completed_hold,
              SUM(CASE WHEN status = 'rejected' THEN 1 ELSE 0 END) as rejected,
              SUM(CASE WHEN status = 'revision' THEN 1 ELSE 0 END) as revision,
              SUM(CASE WHEN status = 'revoked' THEN 1 ELSE 0 END) as revoked
            FROM task_claims
            WHERE task_id = ?
            """,
            (task_id,),
        )
        return {
            "submitted": int(row["submitted"] or 0) if row else 0,
            "completed": int(row["completed"] or 0) if row else 0,
            "completed_hold": int(row["completed_hold"] or 0) if row else 0,
            "rejected": int(row["rejected"] or 0) if row else 0,
            "revision": int(row["revision"] or 0) if row else 0,
            "revoked": int(row["revoked"] or 0) if row else 0,
        }

    async def count_tasks_created(self, owner_id: int) -> int:
        row = await self.fetchone(
            "SELECT COUNT(*) as cnt FROM tasks WHERE owner_id = ?",
            (owner_id,),
        )
        return int(row["cnt"]) if row else 0

    async def list_top_users_by_completed(self, limit: int = 10) -> list[dict]:
        return await self.fetchall(
            """
            SELECT id, username, first_name, completed_tasks
            FROM users
            ORDER BY completed_tasks DESC, id ASC
            LIMIT ?
            """,
            (limit,),
        )

    async def list_top_users_by_balance(self, limit: int = 10) -> list[dict]:
        return await self.fetchall(
            """
            SELECT id, username, first_name, balance
            FROM users
            ORDER BY balance DESC, id ASC
            LIMIT ?
            """,
            (limit,),
        )

    async def get_user_rank_by_completed(self, user_id: int) -> Optional[int]:
        user = await self.fetchone(
            "SELECT completed_tasks FROM users WHERE id = ?",
            (user_id,),
        )
        if not user:
            return None
        row = await self.fetchone(
            "SELECT COUNT(*) as cnt FROM users WHERE completed_tasks > ?",
            (user["completed_tasks"],),
        )
        return int(row["cnt"]) + 1 if row else 1

    async def get_user_rank_by_balance(self, user_id: int) -> Optional[int]:
        user = await self.fetchone(
            "SELECT balance FROM users WHERE id = ?",
            (user_id,),
        )
        if not user:
            return None
        row = await self.fetchone(
            "SELECT COUNT(*) as cnt FROM users WHERE balance > ?",
            (user["balance"],),
        )
        return int(row["cnt"]) + 1 if row else 1

    async def list_available_tasks_paged(
        self, user_id: int, task_type: str, limit: int, offset: int, chat_filter: str | None = None
    ) -> list[dict]:
        extra = ""
        params: list = [task_type, user_id, user_id, user_id]
        if chat_filter == "channel":
            extra = " AND chat_type = ?"
            params.append("channel")
        elif chat_filter == "group":
            extra = " AND (chat_type IS NULL OR chat_type != ?)"
            params.append("channel")
        params.extend([limit, offset])
        return await self.fetchall(
            f"""
            SELECT * FROM tasks
            WHERE active = 1
                AND remaining_count > 0
                AND type = ?
                AND owner_id != ?
                AND (
                    id NOT IN (
                        SELECT task_id FROM task_claims
                        WHERE user_id = ?
                            AND status IN ('completed', 'completed_hold', 'rejected', 'revoked')
                    )
                    OR id IN (
                        SELECT task_id FROM task_claims
                        WHERE user_id = ?
                            AND status NOT IN ('completed', 'completed_hold', 'rejected', 'revoked')
                    )
                ){extra}
            ORDER BY reward DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            tuple(params),
        )

    async def count_available_tasks(
        self, user_id: int, task_type: str, chat_filter: str | None = None
    ) -> int:
        extra = ""
        params: list = [task_type, user_id, user_id, user_id]
        if chat_filter == "channel":
            extra = " AND chat_type = ?"
            params.append("channel")
        elif chat_filter == "group":
            extra = " AND (chat_type IS NULL OR chat_type != ?)"
            params.append("channel")
        row = await self.fetchone(
            f"""
            SELECT COUNT(*) as cnt FROM tasks
            WHERE active = 1
              AND remaining_count > 0
              AND type = ?
              AND owner_id != ?
              AND (
                id NOT IN (
                  SELECT task_id FROM task_claims WHERE user_id = ? AND status IN ('completed', 'completed_hold', 'rejected', 'revoked')
                )
                OR id IN (
                  SELECT task_id FROM task_claims WHERE user_id = ? AND status NOT IN ('completed', 'completed_hold', 'rejected', 'revoked')
                )
              ){extra}
            """,
            tuple(params),
        )
        return int(row["cnt"]) if row else 0

    async def sum_available_rewards(self, user_id: int) -> int:
        row = await self.fetchone(
            """
            SELECT COALESCE(SUM(reward), 0) as total FROM tasks
            WHERE active = 1
              AND remaining_count > 0
              AND owner_id != ?
              AND id NOT IN (
                SELECT task_id FROM task_claims WHERE user_id = ?
              )
            """,
            (user_id, user_id),
        )
        return int(row["total"] or 0) if row else 0

    async def available_task_summary(self, user_id: int) -> dict:
        row = await self.fetchone(
            """
            SELECT
              COALESCE(SUM(reward), 0) as total_reward,
              SUM(CASE WHEN type = 'subscribe' AND chat_type = 'channel' THEN 1 ELSE 0 END) as channels,
              SUM(CASE WHEN type = 'subscribe' AND (chat_type IS NULL OR chat_type != 'channel') THEN 1 ELSE 0 END) as groups,
              SUM(CASE WHEN type = 'view' THEN 1 ELSE 0 END) as views,
              SUM(CASE WHEN type = 'reaction' THEN 1 ELSE 0 END) as reactions,
              SUM(CASE WHEN type = 'bot' THEN 1 ELSE 0 END) as bots,
              SUM(CASE WHEN type = 'boost' THEN 1 ELSE 0 END) as boosts
            FROM tasks
            WHERE active = 1
              AND remaining_count > 0
              AND owner_id != ?
              AND (
                id NOT IN (
                  SELECT task_id FROM task_claims WHERE user_id = ? AND status IN ('completed', 'completed_hold', 'rejected', 'revoked')
                )
                OR id IN (
                  SELECT task_id FROM task_claims WHERE user_id = ? AND status NOT IN ('completed', 'completed_hold', 'rejected', 'revoked')
                )
              )
            """,
            (user_id, user_id, user_id),
        )
        return {
            "total_reward": int(row["total_reward"] or 0) if row else 0,
            "channels": int(row["channels"] or 0) if row else 0,
            "groups": int(row["groups"] or 0) if row else 0,
            "views": int(row["views"] or 0) if row else 0,
            "reactions": int(row["reactions"] or 0) if row else 0,
            "bots": int(row["bots"] or 0) if row else 0,
            "boosts": int(row["boosts"] or 0) if row else 0,
        }

    async def create_claim(self, task_id: int, user_id: int, status: str) -> Optional[int]:
        try:
            cursor = await self.execute(
                "INSERT INTO task_claims (task_id, user_id, status) VALUES (?, ?, ?)",
                (task_id, user_id, status),
            )
            return int(cursor.lastrowid)
        except aiosqlite.IntegrityError:
            return None

    async def get_claim(self, claim_id: int) -> Optional[dict]:
        return await self.fetchone("SELECT * FROM task_claims WHERE id = ?", (claim_id,))

    async def get_claim_for_user(self, task_id: int, user_id: int) -> Optional[dict]:
        return await self.fetchone(
            "SELECT * FROM task_claims WHERE task_id = ? AND user_id = ?",
            (task_id, user_id),
        )

    async def update_claim(self, claim_id: int, **fields: Any) -> None:
        if not fields:
            return
        keys = ", ".join([f"{k} = ?" for k in fields.keys()])
        values = list(fields.values()) + [claim_id]
        await self.execute(f"UPDATE task_claims SET {keys} WHERE id = ?", tuple(values))

    async def mark_claim_auto_approving(self, claim_id: int) -> bool:
        cursor = await self.execute(
            """
            UPDATE task_claims
            SET status = 'auto_approving'
            WHERE id = ? AND status = 'submitted'
            """,
            (claim_id,),
        )
        return cursor.rowcount > 0

    async def reserve_task_funds(self, task_id: int, reward: int) -> bool:
        cursor = await self.execute(
            """
            UPDATE tasks
            SET escrow_balance = escrow_balance - ?, remaining_count = remaining_count - 1
            WHERE id = ? AND remaining_count > 0 AND escrow_balance >= ?
            """,
            (reward, task_id, reward),
        )
        if cursor.rowcount == 0:
            return False
        await self.execute(
            "UPDATE tasks SET active = 0 WHERE id = ? AND remaining_count <= 0",
            (task_id,),
        )
        return True

    async def return_task_funds(self, task_id: int, reward: int) -> None:
        await self.execute(
            """
            UPDATE tasks
            SET escrow_balance = escrow_balance + ?, remaining_count = remaining_count + 1, active = 1
            WHERE id = ?
            """,
            (reward, task_id),
        )

    async def list_pending_reviews(self, owner_id: int) -> list[dict]:
        return await self.fetchall(
            """
            SELECT c.*, t.reward, t.owner_id
            FROM task_claims c
            JOIN tasks t ON t.id = c.task_id
            WHERE t.owner_id = ? AND c.status = 'submitted'
            ORDER BY c.submitted_at ASC
            """,
            (owner_id,),
        )

    async def list_overdue_submitted_claims(self, before_ts: int) -> list[dict]:
        return await self.fetchall(
            """
            SELECT c.*, t.owner_id, t.reward, t.required_reaction, t.type, t.title as task_title
            FROM task_claims c
            JOIN tasks t ON t.id = c.task_id
            WHERE c.status = 'submitted'
              AND c.submitted_at IS NOT NULL
              AND c.submitted_at <= ?
              AND t.type IN ('reaction', 'bot', 'boost')
            ORDER BY c.submitted_at ASC
            """,
            (before_ts,),
        )

    async def get_open_reaction_claim(self, user_id: int) -> Optional[dict]:
        return await self.fetchone(
            """
            SELECT c.*, t.owner_id, t.reward, t.required_reaction, t.type, t.title as task_title,
                   t.chat_id, t.chat_username, t.chat_title, t.description, t.action_link
            FROM task_claims c
            JOIN tasks t ON t.id = c.task_id
            WHERE c.user_id = ?
              AND t.type IN ('reaction', 'bot', 'boost')
              AND c.status IN ('waiting_proof', 'revision')
            ORDER BY c.id DESC
            LIMIT 1
            """,
            (user_id,),
        )

    async def get_open_subscribe_claim(self, user_id: int) -> Optional[dict]:
        return await self.fetchone(
            """
            SELECT c.*, t.owner_id, t.reward, t.type, t.title as task_title,
                   t.chat_id, t.chat_username, t.chat_title, t.action_link
            FROM task_claims c
            JOIN tasks t ON t.id = c.task_id
            WHERE c.user_id = ?
              AND t.type = 'subscribe'
              AND c.status IN ('claimed', 'waiting_proof')
            ORDER BY c.id DESC
            LIMIT 1
            """,
            (user_id,),
        )

    async def get_claim_with_task(self, claim_id: int) -> Optional[dict]:
        return await self.fetchone(
            """
            SELECT c.*, t.owner_id, t.reward, t.required_reaction, t.type, t.title as task_title,
                   t.chat_id, t.chat_username, t.chat_title, t.remaining_count
            FROM task_claims c
            JOIN tasks t ON t.id = c.task_id
            WHERE c.id = ?
            """,
            (claim_id,),
        )

    async def get_next_reaction_claim(self, task_id: int) -> Optional[dict]:
        return await self.fetchone(
            """
            SELECT c.*, t.owner_id, t.reward, t.required_reaction, t.title as task_title,
                   t.chat_id, t.chat_username, t.chat_title, t.remaining_count, t.type,
                   t.description, t.action_link
            FROM task_claims c
            JOIN tasks t ON t.id = c.task_id
            WHERE c.task_id = ? AND c.status = 'submitted'
              AND t.type IN ('reaction', 'bot', 'boost')
            ORDER BY c.submitted_at ASC
            LIMIT 1
            """,
            (task_id,),
        )

    async def count_pending_reaction_claims(self, task_id: int) -> int:
        row = await self.fetchone(
            """
            SELECT COUNT(*) as cnt
            FROM task_claims c
            JOIN tasks t ON t.id = c.task_id
            WHERE c.task_id = ? AND c.status = 'submitted'
              AND t.type IN ('reaction', 'bot', 'boost')
            """,
            (task_id,),
        )
        return int(row["cnt"]) if row else 0

    async def list_subscribe_holds(self) -> list[dict]:
        return await self.fetchall(
            """
            SELECT c.*, t.chat_id, t.chat_username, t.chat_title, t.reward, t.owner_id
            FROM task_claims c
            JOIN tasks t ON t.id = c.task_id
            WHERE t.type = 'subscribe' AND c.status = 'completed_hold'
            """,
            (),
        )

    async def list_active_subscribe_tasks(self) -> list[dict]:
        return await self.fetchall(
            """
            SELECT id, owner_id, chat_id, chat_username, chat_title, task_status, 
                   status_reason, active
            FROM tasks
            WHERE type = 'subscribe' AND deleted = 0
            """,
        )

    async def finalize_hold(self, claim_id: int) -> None:
        await self.update_claim(claim_id, status="completed", penalty_deadline=None)

    async def create_promo(
        self, code: str, amount: int, max_uses: int, expires_at: Optional[int]
    ) -> None:
        await self.execute(
            """
            INSERT OR REPLACE INTO promo_codes (code, amount, max_uses, expires_at, active)
            VALUES (?, ?, ?, ?, 1)
            """,
            (code, amount, max_uses, expires_at),
        )

    async def get_promo(self, code: str) -> Optional[dict]:
        return await self.fetchone("SELECT * FROM promo_codes WHERE code = ?", (code,))

    async def redeem_promo(self, code: str, user_id: int) -> Optional[int]:
        if not self._conn:
            raise RuntimeError("Database is not initialized")
        await self._conn.execute("BEGIN")
        promo_row = await self._conn.execute(
            "SELECT * FROM promo_codes WHERE code = ? AND active = 1",
            (code,),
        )
        promo = await promo_row.fetchone()
        if not promo:
            await self._conn.execute("ROLLBACK")
            return None
        if promo["expires_at"] and now_ts() > int(promo["expires_at"]):
            await self._conn.execute("ROLLBACK")
            return None
        if int(promo["used_count"]) >= int(promo["max_uses"]):
            await self._conn.execute("ROLLBACK")
            return None
        existing = await self._conn.execute(
            "SELECT id FROM promo_redemptions WHERE code = ? AND user_id = ?",
            (code, user_id),
        )
        if await existing.fetchone():
            await self._conn.execute("ROLLBACK")
            return None
        await self._conn.execute(
            "UPDATE promo_codes SET used_count = used_count + 1 WHERE code = ?",
            (code,),
        )
        await self._conn.execute(
            "INSERT INTO promo_redemptions (code, user_id, used_at) VALUES (?, ?, ?)",
            (code, user_id, now_ts()),
        )
        await self._conn.commit()
        return int(promo["amount"])

    async def list_sponsors(self, active_only: bool = True) -> list[dict]:
        if active_only:
            return await self.fetchall("SELECT * FROM sponsors WHERE active = 1", ())
        return await self.fetchall("SELECT * FROM sponsors", ())

    async def add_sponsor(
        self, chat_id: int, chat_username: Optional[str], title: Optional[str], invite_link: Optional[str]
    ) -> None:
        await self.execute(
            """
            INSERT INTO sponsors (chat_id, chat_username, title, invite_link, active)
            VALUES (?, ?, ?, ?, 1)
            """,
            (chat_id, chat_username, title, invite_link),
        )

    async def deactivate_sponsor(self, sponsor_id: int) -> None:
        await self.execute("UPDATE sponsors SET active = 0 WHERE id = ?", (sponsor_id,))

    async def create_invoice(
        self, user_id: int, coins: int, asset: str, invoice_id: str, pay_url: str
    ) -> None:
        await self.execute(
            """
            INSERT INTO invoices (user_id, coins, asset, invoice_id, pay_url, status, created_at)
            VALUES (?, ?, ?, ?, ?, 'pending', ?)
            """,
            (user_id, coins, asset, invoice_id, pay_url, now_ts()),
        )

    async def get_invoice(self, invoice_id: str) -> Optional[dict]:
        return await self.fetchone("SELECT * FROM invoices WHERE invoice_id = ?", (invoice_id,))

    async def list_pending_invoices(self, max_age_seconds: int | None = None) -> list[dict]:
        if max_age_seconds is None:
            return await self.fetchall("SELECT * FROM invoices WHERE status = 'pending'", ())
        cutoff = now_ts() - max_age_seconds
        return await self.fetchall(
            "SELECT * FROM invoices WHERE status = 'pending' AND created_at >= ?",
            (cutoff,),
        )

    async def mark_invoice_paid(self, invoice_id: str) -> None:
        await self.execute(
            "UPDATE invoices SET status = 'paid', paid_at = ? WHERE invoice_id = ?",
            (now_ts(), invoice_id),
        )

    async def stats(self) -> dict:
        users = await self.fetchone("SELECT COUNT(*) as cnt FROM users", ())
        tasks = await self.fetchone("SELECT COUNT(*) as cnt FROM tasks", ())
        completed = await self.fetchone(
            "SELECT COUNT(*) as cnt FROM task_claims WHERE status IN ('completed','completed_hold')", ()
        )
        balance = await self.fetchone("SELECT SUM(balance) as total FROM users", ())
        return {
            "users": int(users["cnt"]) if users else 0,
            "tasks": int(tasks["cnt"]) if tasks else 0,
            "completed": int(completed["cnt"]) if completed else 0,
            "balance": int(balance["total"] or 0) if balance else 0,
        }

    async def global_stats(self, day_start_ts: int) -> dict:
        users = await self.fetchone(
            """
            SELECT
              COUNT(*) as total,
              SUM(CASE WHEN created_at >= ? THEN 1 ELSE 0 END) as new_today
            FROM users
            """,
            (day_start_ts,),
        )
        channel_subs = await self.fetchone(
            """
            SELECT COUNT(*) as cnt
            FROM task_claims c
            JOIN tasks t ON t.id = c.task_id
            WHERE t.type = 'subscribe'
              AND t.chat_type = 'channel'
              AND c.status IN ('completed', 'completed_hold')
            """,
            (),
        )
        group_subs = await self.fetchone(
            """
            SELECT COUNT(*) as cnt
            FROM task_claims c
            JOIN tasks t ON t.id = c.task_id
            WHERE t.type = 'subscribe'
              AND (t.chat_type IS NULL OR t.chat_type != 'channel')
              AND c.status IN ('completed', 'completed_hold')
            """,
            (),
        )
        views = await self.fetchone(
            """
            SELECT COUNT(*) as cnt
            FROM task_claims c
            JOIN tasks t ON t.id = c.task_id
            WHERE t.type = 'view'
              AND c.status IN ('completed', 'completed_hold')
            """,
            (),
        )
        reactions = await self.fetchone(
            """
            SELECT COUNT(*) as cnt
            FROM task_claims c
            JOIN tasks t ON t.id = c.task_id
            WHERE t.type = 'reaction'
              AND c.status IN ('completed', 'completed_hold')
            """,
            (),
        )
        return {
            "users_total": int(users["total"] or 0) if users else 0,
            "new_today": int(users["new_today"] or 0) if users else 0,
            "channel_subs": int(channel_subs["cnt"] or 0) if channel_subs else 0,
            "group_subs": int(group_subs["cnt"] or 0) if group_subs else 0,
            "views": int(views["cnt"] or 0) if views else 0,
            "reactions": int(reactions["cnt"] or 0) if reactions else 0,
        }

    async def list_user_ids(self) -> list[int]:
        rows = await self.fetchall("SELECT id FROM users ORDER BY id ASC", ())
        return [int(row["id"]) for row in rows]

    async def get_chat_autodelete(self, chat_id: int) -> Optional[int]:
        row = await self.fetchone(
            "SELECT auto_delete_seconds FROM chat_settings WHERE chat_id = ?",
            (chat_id,),
        )
        if not row or row["auto_delete_seconds"] is None:
            return None
        return int(row["auto_delete_seconds"])

    async def set_chat_autodelete(self, chat_id: int, seconds: Optional[int]) -> None:
        if seconds is None:
            await self.execute("DELETE FROM chat_settings WHERE chat_id = ?", (chat_id,))
            return
        await self.execute(
            """
            INSERT INTO chat_settings (chat_id, auto_delete_seconds)
            VALUES (?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET auto_delete_seconds = excluded.auto_delete_seconds
            """,
            (chat_id, seconds),
        )

    async def get_setting(self, key: str) -> Optional[str]:
        row = await self.fetchone(
            "SELECT setting_value FROM app_settings WHERE setting_key = ?",
            (key,),
        )
        return row["setting_value"] if row else None

    async def set_setting(self, key: str, value: Optional[str]) -> None:
        if value is None:
            await self.execute("DELETE FROM app_settings WHERE setting_key = ?", (key,))
            return
        await self.execute(
            """
            INSERT INTO app_settings (setting_key, setting_value)
            VALUES (?, ?)
            ON CONFLICT(setting_key) DO UPDATE SET setting_value = excluded.setting_value
            """,
            (key, value),
        )

    async def count_active_op_rules(self, chat_id: int) -> int:
        row = await self.fetchone(
            "SELECT COUNT(*) as cnt FROM op_rules WHERE chat_id = ? AND active = 1",
            (chat_id,),
        )
        return int(row["cnt"] or 0) if row else 0

    async def list_op_rules(self, chat_id: int, active_only: bool = True) -> list[dict]:
        if active_only:
            return await self.fetchall(
                "SELECT * FROM op_rules WHERE chat_id = ? AND active = 1 ORDER BY id ASC",
                (chat_id,),
            )
        return await self.fetchall(
            "SELECT * FROM op_rules WHERE chat_id = ? ORDER BY id ASC",
            (chat_id,),
        )

    async def authorize_bot_ref_user(self, referrer_id: int, user_id: int) -> None:
        rules = await self.fetchall(
            "SELECT id FROM op_rules WHERE type = 'bot_ref' AND referrer_id = ? AND active = 1",
            (referrer_id,),
        )
        for rule in rules:
            try:
                await self.execute(
                    "INSERT INTO op_passes (rule_id, user_id, passed_at) VALUES (?, ?, ?)",
                    (rule["id"], user_id, now_ts()),
                )
            except aiosqlite.IntegrityError:
                pass

    async def create_op_rule(
        self,
        chat_id: int,
        rule_type: str,
        target_chat_id: Optional[int],
        target_username: Optional[str],
        invite_link: Optional[str],
        referrer_id: Optional[int],
        expires_at: Optional[int],
        target_count: Optional[int],
    ) -> int:
        cursor = await self.execute(
            """
            INSERT INTO op_rules (
                chat_id, type, target_chat_id, target_username, invite_link, referrer_id,
                created_at, expires_at, target_count, completed_count, active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 1)
            """,
            (
                chat_id,
                rule_type,
                target_chat_id,
                target_username,
                invite_link,
                referrer_id,
                now_ts(),
                expires_at,
                target_count,
            ),
        )
        return int(cursor.lastrowid)

    async def deactivate_op_rule(self, rule_id: int) -> None:
        await self.execute(
            "UPDATE op_rules SET active = 0 WHERE id = ?",
            (rule_id,),
        )

    async def deactivate_op_rules_by_target(
        self, chat_id: int, target_chat_id: Optional[int], target_username: Optional[str], rule_type: Optional[str]
    ) -> int:
        if target_chat_id is not None:
            cursor = await self.execute(
                "UPDATE op_rules SET active = 0 WHERE chat_id = ? AND target_chat_id = ? AND active = 1",
                (chat_id, target_chat_id),
            )
            return int(cursor.rowcount)
        if target_username:
            cursor = await self.execute(
                "UPDATE op_rules SET active = 0 WHERE chat_id = ? AND target_username = ? AND active = 1",
                (chat_id, target_username),
            )
            return int(cursor.rowcount)
        if rule_type:
            cursor = await self.execute(
                "UPDATE op_rules SET active = 0 WHERE chat_id = ? AND type = ? AND active = 1",
                (chat_id, rule_type),
            )
            return int(cursor.rowcount)
        return 0

    async def deactivate_all_op_rules(self, chat_id: int) -> int:
        cursor = await self.execute(
            "UPDATE op_rules SET active = 0 WHERE chat_id = ? AND active = 1",
            (chat_id,),
        )
        return int(cursor.rowcount)

    async def mark_op_pass(self, rule_id: int, user_id: int) -> bool:
        try:
            await self.execute(
                "INSERT INTO op_passes (rule_id, user_id, passed_at) VALUES (?, ?, ?)",
                (rule_id, user_id, now_ts()),
            )
        except aiosqlite.IntegrityError:
            return False
        await self.execute(
            "UPDATE op_rules SET completed_count = completed_count + 1 WHERE id = ?",
            (rule_id,),
        )
        return True

    async def get_op_rule(self, rule_id: int) -> Optional[dict]:
        return await self.fetchone("SELECT * FROM op_rules WHERE id = ?", (rule_id,))

    async def list_recent_transactions(self, limit: int = 10) -> list[dict]:
        return await self.fetchall(
            "SELECT * FROM transactions ORDER BY created_at DESC LIMIT ?",
            (limit,),
        )

    async def get_user_block(self, user_id: int) -> Optional[dict]:
        return await self.fetchone(
            "SELECT * FROM user_blocks WHERE user_id = ?",
            (user_id,),
        )

    async def list_user_blocks(self) -> list[dict]:
        return await self.fetchall(
            "SELECT * FROM user_blocks WHERE blocked_until > ? ORDER BY blocked_until DESC",
            (now_ts(),),
        )

    async def set_user_block(self, user_id: int, blocked_until: int, reason: str | None) -> None:
        await self.execute(
            """
            INSERT INTO user_blocks (user_id, blocked_until, reason)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET blocked_until = excluded.blocked_until, reason = excluded.reason
            """,
            (user_id, blocked_until, reason),
        )

    async def clear_user_block(self, user_id: int) -> None:
        await self.execute("DELETE FROM user_blocks WHERE user_id = ?", (user_id,))

    async def check_rate_limit(
        self,
        user_id: int,
        action: str,
        limit: int,
        window_seconds: int,
        block_seconds: int,
        auto_block_strikes: int = 5,
    ) -> tuple[bool, Optional[int]]:
        now = now_ts()
        row = await self.fetchone(
            "SELECT * FROM rate_limits WHERE user_id = ? AND action = ?",
            (user_id, action),
        )
        if row and row.get("blocked_until") and now < int(row["blocked_until"]):
            return False, int(row["blocked_until"])

        if not row or now - int(row["window_start"]) >= window_seconds:
            strike_count = int(row["strike_count"]) if row else 0
            await self.execute(
                """
                INSERT INTO rate_limits (user_id, action, window_start, count, blocked_until, strike_count)
                VALUES (?, ?, ?, ?, NULL, ?)
                ON CONFLICT(user_id, action) DO UPDATE SET window_start = excluded.window_start,
                    count = excluded.count, blocked_until = NULL, strike_count = excluded.strike_count
                """,
                (user_id, action, now, 1, strike_count),
            )
            return True, None

        count = int(row["count"] or 0) + 1
        if count > limit:
            strike_count = int(row["strike_count"] or 0) + 1
            block_for = min(block_seconds * strike_count, 24 * 60 * 60)
            blocked_until = now + block_for
            await self.execute(
                """
                UPDATE rate_limits
                SET blocked_until = ?, strike_count = ?, count = ?
                WHERE user_id = ? AND action = ?
                """,
                (blocked_until, strike_count, count, user_id, action),
            )
            if strike_count >= auto_block_strikes:
                await self.set_user_block(
                    user_id,
                    now + 24 * 60 * 60,
                    "Подозрительная активность",
                )
            return False, blocked_until

        await self.execute(
            "UPDATE rate_limits SET count = ? WHERE user_id = ? AND action = ?",
            (count, user_id, action),
        )
        return True, None

    async def check_task_duplicate(
        self, chat_id: Optional[int], chat_username: Optional[str], source_message_id: Optional[int], owner_id: int
    ) -> Optional[dict]:
        """
        Проверяет, существует ли уже задание на этом чате/посте.
        Возвращает существующее задание или None.
        """
        if not self._conn:
            raise RuntimeError("Database is not initialized")
        
        # Для subscribe задач проверяем по chat_id и owner_id
        if chat_id:
            row = await self.fetchone(
                """
                SELECT * FROM tasks
                WHERE chat_id = ? AND owner_id = ? AND deleted = 0 AND type = 'subscribe'
                LIMIT 1
                """,
                (chat_id, owner_id),
            )
            if row:
                return row
        
        # Для channel/group задач проверяем по source_message_id и owner_id
        if source_message_id:
            row = await self.fetchone(
                """
                SELECT * FROM tasks
                WHERE source_message_id = ? AND owner_id = ? AND deleted = 0
                LIMIT 1
                """,
                (source_message_id, owner_id),
            )
            if row:
                return row
        
        return None
