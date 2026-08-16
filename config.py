import os
from dotenv import load_dotenv

load_dotenv()


def _env_int(name: str, default: int, minimum=None, maximum=None) -> int:
    try:
        value = int(os.getenv(name, str(default)).strip())
    except Exception:
        value = int(default)
    if minimum is not None:
        value = max(value, int(minimum))
    if maximum is not None:
        value = min(value, int(maximum))
    return value


def _env_float(name: str, default: float, minimum=None, maximum=None) -> float:
    try:
        value = float(os.getenv(name, str(default)).strip())
    except Exception:
        value = float(default)
    if minimum is not None:
        value = max(value, float(minimum))
    if maximum is not None:
        value = min(value, float(maximum))
    return value


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return raw.strip().lower() in {"1", "true", "yes", "on"}


BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

def _ordered_numeric_ids(raw: str) -> tuple[int, ...]:
    result = []
    seen = set()
    for item in str(raw or "").split(","):
        item = item.strip()
        if not item.isdigit():
            continue
        value = int(item)
        if value <= 0 or value > 9_223_372_036_854_775_807 or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return tuple(result)


# Bootstrap authorization: order matters. The first valid numeric ID from
# ADMIN_IDS is the sole root Admin; in v3.6 later IDs are one-time migration
# seeds for the SQLite-backed reseller list.
ENV_ADMIN_IDS = _ordered_numeric_ids(os.getenv("ADMIN_IDS", ""))
ROOT_ADMIN_ID = ENV_ADMIN_IDS[0] if ENV_ADMIN_IDS else 0

# Service timeouts stay in ENV; connection credentials are migrated to SQLite.
MIKROTIK_API_TIMEOUT_SECONDS = _env_float("MIKROTIK_API_TIMEOUT_SECONDS", 5.0, 2.0, 15.0)

UM_CONNECT_TIMEOUT_SECONDS = _env_float("UM_CONNECT_TIMEOUT_SECONDS", 3.0, 1.0, 15.0)
UM_READ_TIMEOUT_SECONDS = _env_float("UM_READ_TIMEOUT_SECONDS", 5.0, 2.0, 20.0)

ZARINPAL_CONNECT_TIMEOUT_SECONDS = _env_float("ZARINPAL_CONNECT_TIMEOUT_SECONDS", 3.0, 1.0, 15.0)
ZARINPAL_READ_TIMEOUT_SECONDS = _env_float("ZARINPAL_READ_TIMEOUT_SECONDS", 6.0, 2.0, 30.0)
ZARINPAL_TOTAL_TIMEOUT_SECONDS = _env_float("ZARINPAL_TOTAL_TIMEOUT_SECONDS", 9.0, 4.0, 45.0)

XUI_TIMEOUT = _env_int("XUI_TIMEOUT", 12, 2, 60)
XUI_CONNECT_TIMEOUT_SECONDS = _env_float("XUI_CONNECT_TIMEOUT_SECONDS", 3.0, 1.0, 15.0)
XUI_SUB_FALLBACK_PATH = os.getenv("XUI_SUB_FALLBACK_PATH", "/sub/").strip() or "/sub/"
DATA_DIR = os.getenv("DATA_DIR", "/var/lib/account-sales-bot").strip() or "/var/lib/account-sales-bot"

REFERRAL_CODE_PREFIX = (os.getenv("REFERRAL_CODE_PREFIX", "ASB").strip().upper() or "ASB")[:32]
REFERRAL_DISCOUNT_PERCENT = _env_int("REFERRAL_DISCOUNT_PERCENT", 50, 0, 100)
REFERRAL_REWARD_PERCENT = _env_int("REFERRAL_REWARD_PERCENT", 100, 0, 10_000)

# Optional display mapping for V2Ray locations. Format: FLAG=LABEL|FLAG=LABEL
XUI_LOCATION_LABELS_RAW = os.getenv("XUI_LOCATION_LABELS", "").strip()
XUI_LOCATION_LABELS = {}
for _item in XUI_LOCATION_LABELS_RAW.split("|"):
    if "=" not in _item:
        continue
    _key, _value = _item.split("=", 1)
    _key, _value = _key.strip(), _value.strip()
    if _key and _value:
        XUI_LOCATION_LABELS[_key] = _value

# v2 performance / concurrency / resilience
# Number of real user handlers allowed to execute at the same time.
# The update processor has a much larger waiting room, so queued updates from
# one user can never consume all execution capacity and starve other users.
BOT_CONCURRENT_UPDATES = _env_int("BOT_CONCURRENT_UPDATES", 8, 2, 32)
BOT_UPDATE_QUEUE_CAP = _env_int("BOT_UPDATE_QUEUE_CAP", 512, 64, 4096)
# Every external service has an isolated executor. A slow RouterOS
# call therefore cannot consume the workers used by ZarinPal, 3x-ui or SQLite.
# BOT_IO_WORKERS now covers only miscellaneous local/blocking helpers.
BOT_IO_WORKERS = _env_int("BOT_IO_WORKERS", 4, 2, 16)
BOT_DB_WORKERS = _env_int("BOT_DB_WORKERS", 4, 2, 16)
BOT_MIKROTIK_WORKERS = _env_int("BOT_MIKROTIK_WORKERS", 3, 1, 8)
BOT_XUI_WORKERS = _env_int("BOT_XUI_WORKERS", 4, 1, 12)
BOT_ZARINPAL_WORKERS = _env_int("BOT_ZARINPAL_WORKERS", 2, 1, 6)
STATUS_CACHE_TTL_SECONDS = _env_float("STATUS_CACHE_TTL_SECONDS", 30.0, 1.0, 300.0)
# User-facing reads already have transport timeouts. Retrying every timeout
# doubles perceived hangs, so v2.0.2 defaults to fail-fast; it can still be
# raised through ENV if desired.
SERVICE_READ_RETRIES = _env_int("SERVICE_READ_RETRIES", 0, 0, 2)
SERVICE_RETRY_DELAY_SECONDS = _env_float("SERVICE_RETRY_DELAY_SECONDS", 0.5, 0.1, 5.0)
SERVICE_READ_TOTAL_TIMEOUT_SECONDS = _env_float("SERVICE_READ_TOTAL_TIMEOUT_SECONDS", 15.0, 5.0, 45.0)
PERF_SLOW_SECONDS = _env_float("PERF_SLOW_SECONDS", 0.8, 0.1, 30.0)

# Anti-spam. Same user remains sequential regardless; these limits stop button floods.
BOT_RATE_LIMIT_WINDOW_SECONDS = _env_float("BOT_RATE_LIMIT_WINDOW_SECONDS", 5.0, 1.0, 60.0)
BOT_RATE_LIMIT_MAX_ACTIONS = _env_int("BOT_RATE_LIMIT_MAX_ACTIONS", 12, 3, 100)
BOT_DUPLICATE_CALLBACK_COOLDOWN = _env_float("BOT_DUPLICATE_CALLBACK_COOLDOWN", 0.6, 0.0, 5.0)

# Health, watchdog and backups
HEARTBEAT_INTERVAL_SECONDS = _env_float("HEARTBEAT_INTERVAL_SECONDS", 15.0, 5.0, 120.0)
HEALTH_PROBE_INTERVAL_SECONDS = _env_float("HEALTH_PROBE_INTERVAL_SECONDS", 60.0, 15.0, 3600.0)
WATCHDOG_ENABLED = _env_bool("WATCHDOG_ENABLED", True)
WATCHDOG_STALE_SECONDS = _env_float("WATCHDOG_STALE_SECONDS", 180.0, 60.0, 1800.0)
AUTO_BACKUP_ENABLED = _env_bool("AUTO_BACKUP_ENABLED", True)
# The DB-backed admin switch overrides AUTO_BACKUP_ENABLED after first toggle.
BACKUP_KEEP = _env_int("BACKUP_KEEP", 14, 2, 90)

_DEFAULT_MAINTENANCE_MESSAGE = (
    "🛠 ربات در حال بروزرسانی/تعمیرات است. مشاهده اکانت‌ها و پرداخت‌های در انتظار فعال است، "
    "اما خرید یا تمدید جدید موقتاً متوقف شده است."
)
MAINTENANCE_MESSAGE = (
    os.getenv("MAINTENANCE_MESSAGE", _DEFAULT_MAINTENANCE_MESSAGE).strip()
    or _DEFAULT_MAINTENANCE_MESSAGE
)[:1000]

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing")
