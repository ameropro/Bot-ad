import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    bot_token: str
    admin_ids: set[int]
    db_path: str
    crypto_pay_token: str
    coin_rate_usd: float
    ton_usd_rate: float
    hold_days: int
    unsub_grace_seconds: int
    referral_percent: int
    min_reward_subscribe: int
    min_reward_view: int
    min_reward_reaction: int
    payment_assets: list[str]
    commission_percent: int

    @staticmethod
    def load() -> "Config":
        admin_ids_raw = os.getenv("ADMIN_IDS", "")
        admin_ids = {
            int(x.strip())
            for x in admin_ids_raw.split(",")
            if x.strip().isdigit()
        }
        payment_assets = [
            x.strip().upper()
            for x in os.getenv("PAYMENT_ASSETS", "USDT,TON").split(",")
            if x.strip()
        ]
        return Config(
            bot_token=os.getenv("BOT_TOKEN", "").strip(),
            admin_ids=admin_ids,
            db_path=os.getenv("DB_PATH", "bot.db").strip(),
            crypto_pay_token=os.getenv("CRYPTO_PAY_TOKEN", "").strip(),
            coin_rate_usd=float(os.getenv("COIN_RATE_USD", "0.00005")),
            ton_usd_rate=float(os.getenv("TON_USD_RATE", "2.0")),
            hold_days=int(os.getenv("HOLD_DAYS", "7")),
            unsub_grace_seconds=int(os.getenv("UNSUB_GRACE_SECONDS", "3600")),
            referral_percent=int(os.getenv("REFERRAL_PERCENT", "15")),
            min_reward_subscribe=int(os.getenv("MIN_REWARD_SUBSCRIBE", "1000")),
            min_reward_view=int(os.getenv("MIN_REWARD_VIEW", "300")),
            min_reward_reaction=int(os.getenv("MIN_REWARD_REACTION", "500")),
            payment_assets=payment_assets,
            commission_percent=int(os.getenv("COMMISSION_PERCENT", "15")),
        )

    def coins_to_usd(self, coins: int) -> float:
        return round(coins * self.coin_rate_usd, 6)

    def coins_to_ton(self, coins: int) -> float:
        if self.ton_usd_rate <= 0:
            return 0.0
        usd = self.coins_to_usd(coins)
        return round(usd / self.ton_usd_rate, 6)
