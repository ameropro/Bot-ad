import html
import time
from typing import Optional


def now_ts() -> int:
    return int(time.time())


def escape(text: Optional[str]) -> str:
    if text is None:
        return ""
    return html.escape(text)


def format_coins(amount: int) -> str:
    return f"{amount:,}".replace(",", " ")


def parse_referral_arg(arg: str) -> Optional[int]:
    if not arg:
        return None
    if arg.startswith("ref_"):
        arg = arg[4:]
    if arg.isdigit():
        return int(arg)
    return None
