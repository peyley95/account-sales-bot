import html
import asyncio
import hashlib
import shutil
import logging
import math
import time
import secrets
import functools
import threading
from types import MappingProxyType
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import unquote, urlsplit

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, CopyTextButton, InputFile
from telegram.error import BadRequest
from telegram.ext import ApplicationBuilder, BaseUpdateProcessor, CallbackQueryHandler, CommandHandler, ContextTypes, MessageHandler, filters

from config import (
    BOT_TOKEN, REFERRAL_DISCOUNT_PERCENT,
    REFERRAL_REWARD_PERCENT, XUI_LOCATION_LABELS, BOT_CONCURRENT_UPDATES,
    BOT_UPDATE_QUEUE_CAP, BOT_IO_WORKERS, BOT_DB_WORKERS,
    BOT_MIKROTIK_WORKERS, BOT_XUI_WORKERS, BOT_ZARINPAL_WORKERS,
    STATUS_CACHE_TTL_SECONDS, SERVICE_READ_RETRIES, SERVICE_RETRY_DELAY_SECONDS,
    SERVICE_READ_TOTAL_TIMEOUT_SECONDS, ZARINPAL_TOTAL_TIMEOUT_SECONDS,
    PERF_SLOW_SECONDS, BOT_RATE_LIMIT_WINDOW_SECONDS, BOT_RATE_LIMIT_MAX_ACTIONS,
    BOT_DUPLICATE_CALLBACK_COOLDOWN, HEARTBEAT_INTERVAL_SECONDS,
    HEALTH_PROBE_INTERVAL_SECONDS, WATCHDOG_ENABLED, WATCHDOG_STALE_SECONDS,
    BACKUP_KEEP, MAINTENANCE_MESSAGE,
)
from app_settings import (
    APP_SETTINGS, APP_TIMEZONE, APP_BACKUP_HOUR,
    add_inbound, add_reseller, change_reseller_debt,
    charge_reseller_order, delete_inbound, edit_reseller,
    effective_admin_ids, get_setting as get_app_setting,
    inbound_records, is_admin as runtime_is_admin, normalize_setting,
    is_reseller, remove_reseller, rename_inbound, reseller_record,
    reseller_records, root_admin_id, update_setting,
    enabled_sales_services, service_sales_enabled, set_service_sales_enabled,
    enabled_payment_gateways, payment_gateway_enabled, set_payment_gateway_enabled,
)
from plans import (
    TEST_PLAN, price_rial, gb_to_bytes, plan_snapshot, plans_for, refresh_plans,
    refresh_test_plan, test_plan_enabled, pending_plan_is_stale, snapshot_for_delivery,
)
from storage import (
    list_accounts, upsert_account, has_test, mark_test,
    add_pending, latest_pending_for_user, list_pending_for_user,
    create_reseller_pending,
    pending_first_purchase_for_user, get_pending, pop_pending, update_pending,
    authorize_pending_payment,
    has_completed_purchase, get_or_create_referral_code, find_referrer_by_code,
    referral_already_used, mark_referral_used, record_purchase,
    wallet_balance, wallet_available, reserved_wallet_for_user, debit_wallet, refund_wallet,
    credit_referral_reward, wallet_order_debited,
    update_user_profile, get_user_profile, record_transaction, list_transactions,
    list_known_users, search_known_users, admin_adjust_wallet,
    get_user_admin_summary, list_user_transactions,
    record_admin_audit, list_admin_audit, maintenance_mode, set_maintenance_mode,
    auto_backup_enabled, set_auto_backup_enabled, auto_backup_status,
    admin_dashboard_stats, admin_referral_stats,
    get_referral_settings, set_referral_percent,
    list_service_sale_plans, create_sale_plan,
    update_sale_plan, delete_sale_plan,
    update_trial_plan, set_trial_plan_enabled,
    list_admin_pending_payments, get_admin_pending_payment_by_id,
    create_card_transfer_request, get_card_transfer_request,
    get_card_transfer_request_by_authority, active_card_transfer_request_for_user,
    submit_card_transfer_receipt, list_card_transfer_requests,
    claim_card_transfer_request, reject_card_transfer_request,
    cancel_card_transfer_request, complete_card_transfer_request,
    backup_database, export_database_snapshot, database_stats,
    get_fulfillment, prepare_fulfillment, mark_fulfillment_executing,
    mark_fulfillment_prepared, mark_fulfillment_remote_done,
    mark_fulfillment_provisioned, mark_fulfillment_completed,
    list_incomplete_fulfillments,
)
from services import mikrotik
from services.xui import XUIClient
from services.zarinpal import (
    create_payment, verify_payment,
    verify_payment_for_cancel, test_connection as test_zarinpal_connection,
)
from runtime import STATUS_CACHE, RUNTIME, CallbackRateLimiter, TTLCache

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("account-sales-bot")

SERVICE_LABEL = {"openvpn": "OpenVPN", "v2ray": "V2Ray"}


def welcome_text() -> str:
    brand = str(get_app_setting("bot_brand_name", "Account Sales Bot"))
    return f"👋 به ربات {brand} خوش آمدید.\n\nلطفاً یکی از گزینه‌های زیر را انتخاب کنید:"
MAX_ADMIN_WALLET_ADJUST_TOMAN = 1_000_000_000_000
MAX_ACCOUNT_IDENTIFIER_CHARS = 256
MAX_ADMIN_SEARCH_CHARS = 128
MAX_ADMIN_PLAN_GB = 1_000_000
MAX_ADMIN_PLAN_MONTHS = 1_200
MAX_ADMIN_PLAN_PRICE_TOMAN = 1_000_000_000_000
MAX_ADMIN_PLAN_PROFILE_CHARS = 128
MAX_ADMIN_TRIAL_DAYS = 36_500
MAX_RESELLER_MONEY_TOMAN = 9_000_000_000_000_000

# v3 financial settings are loaded once from SQLite into an in-memory cache.
# User purchase paths therefore stay O(1) and do not add a database read per click.
_REFERRAL_SETTINGS_LOCK = threading.RLock()
_REFERRAL_SETTINGS = MappingProxyType(get_referral_settings(
    default_discount_percent=REFERRAL_DISCOUNT_PERCENT,
    default_reward_percent=REFERRAL_REWARD_PERCENT,
))

# Maintenance is checked for almost every callback. Reading SQLite here made a
# routine button press depend on storage latency and allowed a temporary DB lock
# to escape before callback_router's error boundary. Keep one immutable runtime
# value and replace it only after the Admin write commits successfully.
_RUNTIME_FLAGS_LOCK = threading.RLock()
_RUNTIME_FLAGS = MappingProxyType({"maintenance_mode": bool(maintenance_mode())})


def current_maintenance_mode() -> bool:
    with _RUNTIME_FLAGS_LOCK:
        current = _RUNTIME_FLAGS
    return bool(current.get("maintenance_mode", False))


def _apply_maintenance_mode(enabled: bool) -> bool:
    global _RUNTIME_FLAGS
    fresh = MappingProxyType({"maintenance_mode": bool(enabled)})
    with _RUNTIME_FLAGS_LOCK:
        _RUNTIME_FLAGS = fresh
    return bool(enabled)


_PURCHASE_STATUS_CACHE = TTLCache(max_entries=16_384)
_PURCHASE_CACHE_MISS = object()


async def completed_purchase_for_menu(tg_id: int) -> bool:
    """Short cache for a display-only menu decision.

    Purchase authorization still uses the authoritative SQLite functions. The
    cache only avoids reopening SQLite whenever /start or Home is rendered.
    """
    uid = int(tg_id)
    cached = _PURCHASE_STATUS_CACHE.get(uid, _PURCHASE_CACHE_MISS)
    if cached is not _PURCHASE_CACHE_MISS:
        return bool(cached)
    value = bool(await run_blocking(has_completed_purchase, uid))
    _PURCHASE_STATUS_CACHE.set(uid, value, 30.0)
    return value

def current_referral_discount_percent() -> int:
    with _REFERRAL_SETTINGS_LOCK:
        current = _REFERRAL_SETTINGS
    return int(current.get("discount_percent", REFERRAL_DISCOUNT_PERCENT))

def current_referral_reward_percent() -> int:
    with _REFERRAL_SETTINGS_LOCK:
        current = _REFERRAL_SETTINGS
    return int(current.get("reward_percent", REFERRAL_REWARD_PERCENT))

def _apply_referral_settings(settings: dict) -> dict:
    global _REFERRAL_SETTINGS
    fresh = MappingProxyType(dict(settings or {}))
    with _REFERRAL_SETTINGS_LOCK:
        _REFERRAL_SETTINGS = fresh
    return dict(fresh)

FLAG_LOCATION_FA = dict(XUI_LOCATION_LABELS)

# If no custom mapping is provided, labels are derived from the inbound/VLESS remark.
DEFAULT_VLESS_LOCATION_BY_INDEX = []


class ServiceBusyError(RuntimeError):
    pass


class BlockingLane:
    """A bounded, isolated executor whose capacity follows the real OS job.

    asyncio deadlines cannot stop a sync function that is already running. The
    lane token is therefore released by the concurrent Future only when that
    function actually exits, not when the awaiting Telegram handler is
    cancelled. This prevents timed-out jobs from silently overfilling a pool.
    """
    def __init__(self, name: str, workers: int, *, queue_factor: int = 2, min_capacity: int = 0):
        self.name = str(name)
        self.workers = max(int(workers), 1)
        self.capacity = max(
            self.workers * max(int(queue_factor), 1),
            self.workers,
            max(int(min_capacity), 0),
        )
        self.executor = ThreadPoolExecutor(max_workers=self.workers, thread_name_prefix=f"vpn-{self.name}")
        self._slots = threading.BoundedSemaphore(self.capacity)
        self._lock = threading.RLock()
        self.active = 0
        self.rejected = 0
        self.completed = 0

    def submit(self, call, loop):
        if not self._slots.acquire(blocking=False):
            with self._lock:
                self.rejected += 1
            raise ServiceBusyError(
                "این بخش موقتاً درگیر پاسخ قبلی است؛ چند لحظه بعد دوباره تلاش کنید."
            )
        with self._lock:
            self.active += 1
        try:
            future = self.executor.submit(call)
        except Exception:
            self._finish()
            raise
        future.add_done_callback(lambda _future: self._finish())
        return asyncio.wrap_future(future, loop=loop)

    def _finish(self):
        with self._lock:
            self.active = max(self.active - 1, 0)
            self.completed += 1
        self._slots.release()

    def snapshot(self):
        with self._lock:
            return {
                "workers": self.workers,
                "capacity": self.capacity,
                "active": self.active,
                "rejected": self.rejected,
                "completed": self.completed,
            }

    def shutdown(self):
        self.executor.shutdown(wait=False, cancel_futures=True)


class KeyedAsyncLocks:
    """Short-lived keyed locks with ref-counted eviction."""

    def __init__(self):
        self._locks = {}
        self._refs = {}
        self._guard = asyncio.Lock()

    @asynccontextmanager
    async def hold(self, key: str):
        key = str(key or "")
        async with self._guard:
            lock = self._locks.get(key)
            if lock is None:
                lock = asyncio.Lock()
                self._locks[key] = lock
            self._refs[key] = int(self._refs.get(key, 0)) + 1
        try:
            async with lock:
                yield
        finally:
            async with self._guard:
                refs = max(int(self._refs.get(key, 1)) - 1, 0)
                if refs:
                    self._refs[key] = refs
                else:
                    self._refs.pop(key, None)
                    if self._locks.get(key) is lock:
                        self._locks.pop(key, None)


BLOCKING_LANES = {
    "misc": BlockingLane("misc", BOT_IO_WORKERS, queue_factor=4),
    "db": BlockingLane("db", BOT_DB_WORKERS, queue_factor=8),
    # The normal update processor may run BOT_CONCURRENT_UPDATES different
    # users at once, plus a health probe. Capacity below that number caused
    # valid requests to be rejected before they could enter the executor.
    "mikrotik": BlockingLane(
        "mikrotik", BOT_MIKROTIK_WORKERS, queue_factor=4,
        min_capacity=BOT_CONCURRENT_UPDATES + 2,
    ),
    "xui": BlockingLane(
        "xui", BOT_XUI_WORKERS, queue_factor=3,
        min_capacity=BOT_CONCURRENT_UPDATES + 2,
    ),
    "zarinpal": BlockingLane(
        "zarinpal", BOT_ZARINPAL_WORKERS, queue_factor=6,
        min_capacity=BOT_CONCURRENT_UPDATES + 2,
    ),
    # Backups can be I/O heavy on a large SQLite file. Keeping them out of the
    # DB lane guarantees routine user reads/writes never queue behind a backup.
    "backup": BlockingLane("backup", 1, queue_factor=1),
}


def _blocking_lane_for(func, explicit: str | None = None) -> BlockingLane:
    if explicit:
        return BLOCKING_LANES.get(str(explicit), BLOCKING_LANES["misc"])
    module = str(getattr(func, "__module__", "") or "")
    owner = getattr(func, "__self__", None)
    if owner is not None:
        module = str(getattr(owner.__class__, "__module__", module) or module)
    if module == "storage":
        return BLOCKING_LANES["db"]
    if module.endswith("services.mikrotik"):
        return BLOCKING_LANES["mikrotik"]
    if module.endswith("services.xui"):
        return BLOCKING_LANES["xui"]
    if module.endswith("services.zarinpal"):
        return BLOCKING_LANES["zarinpal"]
    return BLOCKING_LANES["misc"]


async def run_blocking(func, /, *args, _lane: str | None = None, **kwargs):
    """Run blocking work off-loop on an isolated, bounded executor lane."""
    label = getattr(func, "__qualname__", None) or getattr(func, "__name__", None) or str(func)
    started = time.perf_counter()
    ok = False
    call = functools.partial(func, *args, **kwargs)
    try:
        loop = asyncio.get_running_loop()
        lane = _blocking_lane_for(func, _lane)
        result = await lane.submit(call, loop)
        ok = True
        return result
    finally:
        elapsed = time.perf_counter() - started
        RUNTIME.operation(label, elapsed, ok)
        if elapsed >= PERF_SLOW_SECONDS:
            logger.warning("slow blocking operation %.2fs name=%s ok=%s", elapsed, label, ok)


async def run_blocking_retry(func, /, *args, retries: int | None = None, _lane: str | None = None, **kwargs):
    """Run a safe/read operation with a hard async deadline.

    The underlying sync libraries already have transport timeouts, but some
    OpenVPN status paths chain RouterOS + User-Manager calls. Without an outer
    deadline one user could hold their per-user queue for close to a minute.
    A timed-out thread may finish later, but it cannot hold the Telegram update
    lock. Timeouts are never retried automatically.
    """
    attempts = SERVICE_READ_RETRIES if retries is None else max(int(retries), 0)
    last = None
    for attempt in range(attempts + 1):
        try:
            return await asyncio.wait_for(
                run_blocking(func, *args, _lane=_lane, **kwargs),
                timeout=SERVICE_READ_TOTAL_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError as exc:
            label = getattr(func, "__qualname__", None) or getattr(func, "__name__", None) or str(func)
            logger.warning("read operation deadline exceeded %.1fs name=%s", SERVICE_READ_TOTAL_TIMEOUT_SECONDS, label)
            raise RuntimeError("سرویس مقصد در زمان مناسب پاسخ نداد؛ دوباره تلاش کنید.") from exc
        except Exception as exc:
            last = exc
            if attempt >= attempts:
                raise
            await asyncio.sleep(SERVICE_RETRY_DELAY_SECONDS * (attempt + 1))
    raise last


async def run_zarinpal(func, /, *args, **kwargs):
    """Run one gateway call with an isolated lane and a user-facing deadline."""
    try:
        return await asyncio.wait_for(
            run_blocking(func, *args, _lane="zarinpal", **kwargs),
            timeout=ZARINPAL_TOTAL_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError as exc:
        logger.warning(
            "ZarinPal deadline exceeded %.1fs name=%s",
            ZARINPAL_TOTAL_TIMEOUT_SECONDS,
            getattr(func, "__qualname__", getattr(func, "__name__", str(func))),
        )
        raise RuntimeError("زرین‌پال در زمان مناسب پاسخ نداد؛ لطفاً دوباره تلاش کنید.") from exc


async def safe_callback_answer(query, text: str | None = None, *, show_alert: bool = False) -> bool:
    """Answer a callback without abandoning the actual action if it is already old."""
    if text is not None:
        text = str(text)
        if len(text) > 200:
            text = text[:197] + "…"
    try:
        await query.answer(text=text, show_alert=show_alert)
        return True
    except BadRequest as exc:
        lowered = str(exc).lower()
        if "query is too old" in lowered or "query id is invalid" in lowered:
            logger.warning("callback answer expired data=%s", str(getattr(query, "data", ""))[:120])
        else:
            logger.warning(
                "callback answer rejected data=%s error=%s",
                str(getattr(query, "data", ""))[:120], exc,
            )
        return False
    except Exception as exc:
        # A transient Telegram timeout while clearing the spinner must not abort
        # the real button action and leave the user thinking the bot is frozen.
        logger.warning(
            "callback answer failed data=%s error=%s",
            str(getattr(query, "data", ""))[:120], exc,
        )
        return False


async def safe_edit_text(message, text: str, **kwargs) -> bool:
    """Edit one Telegram message; repeated identical clicks stay silent."""
    try:
        await message.edit_text(text, **kwargs)
        return True
    except BadRequest as exc:
        if "message is not modified" in str(exc).lower():
            return False
        raise


async def callback_failure_reply(query, user_id: int):
    """Show a generic failure even when the original callback message cannot be edited."""
    text = "❌ انجام این عملیات با خطا روبه‌رو شد. لطفاً دوباره تلاش کنید."
    try:
        markup = await main_menu_keyboard(user_id)
    except Exception:
        # Failure reporting must never trigger the global error handler merely
        # because the optional menu lookup also failed.
        logger.warning("callback failure menu build failed", exc_info=True)
        markup = None
    try:
        await query.message.edit_text(text, reply_markup=markup)
        return
    except Exception:
        logger.warning("callback failure edit failed; trying a new message", exc_info=True)
    try:
        await query.message.reply_text(text, reply_markup=markup)
    except Exception:
        logger.warning("callback fallback message failed", exc_info=True)


CALLBACK_LIMITER = CallbackRateLimiter(
    BOT_RATE_LIMIT_WINDOW_SECONDS, BOT_RATE_LIMIT_MAX_ACTIONS, BOT_DUPLICATE_CALLBACK_COOLDOWN
)
ORDER_LOCKS = KeyedAsyncLocks()


class PerUserUpdateProcessor(BaseUpdateProcessor):
    """Fair per-user serialization without global starvation.

    BaseUpdateProcessor owns an outer semaphore. In v2.0-v2.0.1 that semaphore
    had only BOT_CONCURRENT_UPDATES slots. Four queued updates from one user
    could therefore consume all four slots *before* three of them waited on the
    same user's lock. Other users (even /start) never reached do_process_update.

    The outer semaphore is now only a generous bounded waiting room. Actual
    execution capacity is enforced by _work_slots *after* the per-user lock is
    acquired. Queued updates from one user no longer consume execution slots.
    /start has a separate execution lane for cross-user responsiveness, but it
    still respects its own user lock so it cannot race with an in-flight flow.
    """
    def __init__(self, max_concurrent_updates: int):
        self.worker_limit = max(int(max_concurrent_updates), 1)
        outer_cap = max(int(BOT_UPDATE_QUEUE_CAP), self.worker_limit * 32)
        super().__init__(max_concurrent_updates=outer_cap)
        self._locks = {}
        self._lock_refs = {}
        self._locks_guard = asyncio.Lock()
        self._work_slots = asyncio.Semaphore(self.worker_limit)
        self._fast_slots = asyncio.Semaphore(4)

    async def initialize(self):
        return None

    async def shutdown(self):
        return None

    @staticmethod
    def _is_start(update) -> bool:
        message = getattr(update, "message", None)
        text = str(getattr(message, "text", "") or "").strip()
        if not text:
            return False
        first = text.split(None, 1)[0].lower()
        return first == "/start" or first.startswith("/start@")

    async def _run_and_measure(self, update, coroutine, key: int, queued_at: float):
        exec_started = time.perf_counter()
        RUNTIME.update_started()
        try:
            await coroutine
        finally:
            finished = time.perf_counter()
            elapsed = finished - exec_started
            queue_wait = exec_started - queued_at
            callback = getattr(getattr(update, "callback_query", None), "data", "") or ""
            RUNTIME.update_finished(elapsed=elapsed, tg_id=key, callback=callback)
            if queue_wait >= PERF_SLOW_SECONDS:
                logger.warning(
                    "slow update queue %.2fs tg_id=%s callback=%s",
                    queue_wait, key, callback[:120],
                )
            if elapsed >= PERF_SLOW_SECONDS:
                logger.warning(
                    "slow update execution %.2fs tg_id=%s callback=%s",
                    elapsed, key, callback[:120],
                )

    async def do_process_update(self, update, coroutine):
        user = getattr(update, "effective_user", None)
        key = int(user.id) if user is not None else 0
        queued_at = time.perf_counter()

        async with self._locks_guard:
            lock = self._locks.get(key)
            if lock is None:
                lock = asyncio.Lock()
                self._locks[key] = lock
            self._lock_refs[key] = int(self._lock_refs.get(key, 0)) + 1

        # Keep one user's context.user_data strictly serialized, including /start.
        # /start gets a fast execution lane only AFTER its own per-user queue, so
        # it cannot race with a purchase/renew handler that is already running.
        try:
            async with lock:
                if self._is_start(update):
                    async with self._fast_slots:
                        await self._run_and_measure(update, coroutine, key, queued_at)
                    return
                # Crucially, acquire the global execution slot only *after* the
                # per-user queue. Waiting updates from one user consume no work slot.
                async with self._work_slots:
                    await self._run_and_measure(update, coroutine, key, queued_at)
        finally:
            # A permanent dict entry per Telegram ID is an unbounded memory leak
            # on a long-running public bot. refs includes both the holder and all
            # waiters, so zero is safe to evict.
            async with self._locks_guard:
                refs = max(int(self._lock_refs.get(key, 1)) - 1, 0)
                if refs:
                    self._lock_refs[key] = refs
                else:
                    self._lock_refs.pop(key, None)
                    if self._locks.get(key) is lock:
                        self._locks.pop(key, None)


def is_admin(uid: int) -> bool:
    return runtime_is_admin(uid)


def _maintenance_blocks_callback(data: str) -> bool:
    parts = str(data or "").split("|")
    if not parts:
        return False
    if parts[0] == "act" and len(parts) >= 2 and parts[1] in {"buy", "renew", "test"}:
        return True
    if parts[0] == "plan":
        return True
    if parts[0] == "ref":
        return True
    if parts[0] in {"acct", "myact"} and len(parts) >= 2 and parts[1] == "renew":
        return True
    if parts[0] == "manual" and len(parts) >= 2 and parts[1] == "renew":
        return True
    return False


def generate_username() -> str:
    prefix = str(get_app_setting("account_username_prefix", "accountbot"))
    return f"{prefix}{secrets.randbelow(1_000_000):06d}"


def generate_password_numeric() -> str:
    return str(100000 + secrets.randbelow(900000))


def human_bytes(n: int | float) -> str:
    n = max(float(n or 0), 0)
    gb = n / (1024 ** 3)
    if gb >= 1:
        return f"{gb:.2f} GB"
    mb = n / (1024 ** 2)
    return f"{mb:.0f} MB"


def account_ref(identifier: str) -> str:
    """Short deterministic callback reference (Telegram limits callback data to 64 bytes)."""
    return hashlib.blake2s(str(identifier or "").encode("utf-8"), digest_size=8).hexdigest()


def _resolve_account_ref(tg_id: int, service: str, ref: str) -> str:
    matches = [
        str(item.get("identifier") or "")
        for item in list_accounts(tg_id, service)
        if account_ref(str(item.get("identifier") or "")) == str(ref or "")
    ]
    matches = [item for item in matches if item]
    if len(matches) != 1:
        raise RuntimeError("مرجع اکانت معتبر نیست؛ لطفاً فهرست اکانت‌ها را دوباره باز کنید.")
    return matches[0]


def admin_plan_ref(plan_key: str, service: str = "openvpn") -> str:
    raw = f"{str(service or '').lower()}:{str(plan_key or '')}"
    return hashlib.blake2s(raw.encode("utf-8"), digest_size=6).hexdigest()


def _resolve_admin_plan_ref(service: str, ref: str) -> str:
    registry = plans_for(service)
    matches = [
        str(key) for key in registry
        if admin_plan_ref(str(key), service) == str(ref or "")
    ]
    if len(matches) != 1:
        raise RuntimeError("مرجع بسته معتبر نیست؛ لطفاً فهرست بسته‌ها را دوباره باز کنید.")
    return matches[0]


def _plan_duration_text(plan: dict) -> str:
    months = int(plan.get("months") or 0)
    if months > 0:
        return f"{months} ماهه"
    return f"{int(plan.get('days') or 0)} روزه"


def plan_text(key: str, service: str = "openvpn") -> str:
    p = plans_for(service)[key]
    return f"{p['gb']} گیگ - {_plan_duration_text(p)} - {p['price_toman']:,} تومان"


async def main_menu_keyboard(tg_id: int):
    enabled = enabled_sales_services()
    reseller = is_reseller(tg_id)
    if len(enabled) == 1:
        # With one sale service there is no redundant service-selection step.
        rows = list(service_menu_keyboard(enabled[0], tg_id).inline_keyboard[:-1])
        rows.append([InlineKeyboardButton(
            "📒 مشاهده بدهی" if reseller else "💰 کیف پول",
            callback_data="menu|reseller_debt" if reseller else "menu|wallet",
        )])
    else:
        rows = [
            [InlineKeyboardButton("🛍 انتخاب سرویس", callback_data="menu|services")],
            [InlineKeyboardButton(
                "📒 مشاهده بدهی" if reseller else "💰 کیف پول",
                callback_data="menu|reseller_debt" if reseller else "menu|wallet",
            )],
        ]
    if not reseller and await completed_purchase_for_menu(tg_id):
        rows.append([InlineKeyboardButton("🎁 دعوت دوستان و دریافت اکانت رایگان", callback_data="menu|referral")])
    if is_admin(tg_id):
        rows.append([InlineKeyboardButton("⚙️ پنل مدیریت", callback_data="admin_tools")])
    return InlineKeyboardMarkup(rows)


def service_choose_keyboard():
    rows = []
    if service_sales_enabled("openvpn"):
        rows.append([InlineKeyboardButton("🔵 OpenVPN", callback_data="svc|openvpn")])
    if service_sales_enabled("v2ray"):
        rows.append([InlineKeyboardButton("🟣 V2Ray", callback_data="svc|v2ray")])
    rows.append([InlineKeyboardButton("🔙 بازگشت", callback_data="home")])
    return InlineKeyboardMarkup(rows)


def wallet_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 بازگشت", callback_data="home")]])


def referral_keyboard(code: str):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 کپی کد معرف", copy_text=CopyTextButton(str(code or "-")[:256]))],
        [InlineKeyboardButton("🔙 بازگشت", callback_data="home")],
    ])


def first_purchase_referral_keyboard(service: str):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎟 کد معرف دارم", callback_data="ref|have")],
        [InlineKeyboardButton("➡️ کد معرف ندارم", callback_data="ref|none")],
        [InlineKeyboardButton("🔙 بازگشت", callback_data=f"svc|{service}")],
    ])


def _trial_visible_for_user(tg_id: int) -> bool:
    reseller = reseller_record(tg_id)
    return not reseller or bool(reseller.get("trial_enabled", True))


def service_menu_keyboard(service: str, tg_id: int = 0):
    rows = [
        [InlineKeyboardButton("🛒 خرید اکانت جدید", callback_data=f"act|buy|{service}")],
        [InlineKeyboardButton("♻️ تمدید اکانت‌های قبلی", callback_data=f"act|renew|{service}")],
        [InlineKeyboardButton("👤 مشاهده اکانت‌های من", callback_data=f"act|accounts|{service}")],
    ]
    if service == "openvpn":
        if test_plan_enabled() and _trial_visible_for_user(tg_id):
            rows.append([InlineKeyboardButton("🎁 دریافت اکانت OpenVPN تست رایگان", callback_data="act|test|openvpn")])
        current_url = str(get_app_setting("openvpn_connections_url", "0") or "").strip()
        connection_url = urlsplit(current_url if current_url != "0" else "")
        if connection_url.scheme in {"http", "https"} and connection_url.netloc:
            rows.append([InlineKeyboardButton("⬇️ دریافت کانکشن‌های OpenVPN", url=current_url)])
    else:
        if test_plan_enabled() and _trial_visible_for_user(tg_id):
            rows.append([InlineKeyboardButton("🎁 دریافت اکانت V2Ray تست رایگان", callback_data="act|test|v2ray")])
    rows.append([InlineKeyboardButton("🔙 بازگشت", callback_data="home")])
    return InlineKeyboardMarkup(rows)


def plans_keyboard(action: str, service: str, back_callback: str | None = None):
    registry = plans_for(service)
    rows = [[InlineKeyboardButton(plan_text(k, service), callback_data=f"plan|{action}|{service}|{k}")] for k in registry]
    rows.append([InlineKeyboardButton("🔙 بازگشت", callback_data=back_callback or f"svc|{service}")])
    return InlineKeyboardMarkup(rows)


def pay_keyboard(payment_url: str = ""):
    rows = []
    parsed = urlsplit(str(payment_url or "").strip())
    if parsed.scheme in {"http", "https"} and parsed.netloc:
        rows.append([InlineKeyboardButton("💳 ورود مستقیم به درگاه زرین‌پال", url=payment_url)])
    rows.extend([
        [InlineKeyboardButton("✅ پرداخت کردم", callback_data="payment|check")],
        [InlineKeyboardButton("❌ لغو سفارش", callback_data="payment|cancel")],
        [InlineKeyboardButton("🔙 منوی اصلی", callback_data="home")],
    ])
    return InlineKeyboardMarkup(rows)


def back_service(service: str):
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 بازگشت", callback_data=f"svc|{service}")]])


def openvpn_credentials_keyboard(username: str, password: str):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 کپی یوزرنیم", copy_text=CopyTextButton(str(username or "-")[:256]))],
        [InlineKeyboardButton("📋 کپی پسورد", copy_text=CopyTextButton(str(password or "-")[:256]))],
        [InlineKeyboardButton("🔙 بازگشت", callback_data="svc|openvpn")],
    ])


def text_code_block(value: str) -> str:
    """Compact Telegram HTML code block (equivalent to ```text ... ```)."""
    return f'<pre><code class="language-text">{html.escape(str(value or ""))}</code></pre>\n'


def openvpn_plan_days(local: dict, profile: str) -> int:
    plan_key = str(local.get("plan_key") or "")
    if plan_key in plans_for("openvpn"):
        return int(plans_for("openvpn")[plan_key].get("days") or 0)
    if local.get("is_test") or profile == TEST_PLAN.get("openvpn_profile"):
        return int(TEST_PLAN.get("days") or 0)

    # Fallback for accounts imported manually or created by an older bot version.
    m = __import__("re").search(r"(\d+)D", profile or "", __import__("re").IGNORECASE)
    if m:
        return int(m.group(1))
    m = __import__("re").search(r"(\d+)M", profile or "", __import__("re").IGNORECASE)
    if m:
        return int(m.group(1)) * 30
    return 30


def _optional_int_code(value, valid_codes):
    if isinstance(value, bool) or value is None:
        return None
    try:
        code = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    return code if code in valid_codes else None


def render_openvpn_status(info: dict, identifier: str, local: dict) -> str:
    """Render OpenVPN status from authoritative User Manager state codes."""
    profile = str(info.get("profile") or local.get("profile") or "")
    m = __import__("re").search(r"(\d+)G", profile, __import__("re").IGNORECASE)
    quota_gb = int(m.group(1)) if m else 0
    usage_available = bool(info.get("usage_available", True))
    used = int(info.get("total_download") or 0) + int(info.get("total_upload") or 0)
    remaining = max(gb_to_bytes(quota_gb) - used, 0) if quota_gb and usage_available else 0
    package_days = openvpn_plan_days(local, profile)
    profile_state = str(info.get("profile_state") or "").strip().lower().replace("_", "-").replace(" ", "-")

    state_code = _optional_int_code(
        info.get("um_profile_state"), mikrotik.UM_PROFILE_STATE_LABELS
    )
    if state_code is None:
        state_code = _optional_int_code(profile_state, mikrotik.UM_PROFILE_STATE_LABELS)
    if state_code is None:
        # Exact User Manager textual enum mapping. Do not infer semantics from
        # English wording: Waiting=0, Running=1, Running active=2, Used=3.
        if profile_state == "waiting":
            state_code = 0
        elif profile_state == "running":
            state_code = 1
        elif profile_state == "running-active":
            state_code = 2
        elif profile_state == "used":
            state_code = 3

    starts_at_code = _optional_int_code(
        info.get("um_profile_starts_at"), mikrotik.UM_PROFILE_STARTS_AT_LABELS
    )

    exp = info.get("expiry")
    expiry_days = None
    if isinstance(exp, datetime):
        if exp.tzinfo is None:
            exp = exp.replace(tzinfo=timezone.utc)
        seconds = max((exp - datetime.now(timezone.utc)).total_seconds(), 0)
        expiry_days = math.ceil(seconds / 86400) if seconds > 0 else 0

    if state_code == 0:
        # "از اولین استفاده" keeps the full package duration before the first
        # connection. An immediately-started profile must use its live expiry.
        days = (
            expiry_days
            if starts_at_code == 1 and expiry_days is not None
            else package_days
        )
        if quota_gb:
            remaining = gb_to_bytes(quota_gb)
    elif state_code == 1:
        days = expiry_days if expiry_days is not None else 0
        remaining = 0
    elif state_code == 2:
        days = expiry_days if expiry_days is not None else package_days
    elif state_code == 3:
        days = 0
    else:
        # Never turn an incomplete/failed status response into "expired". Older
        # versions did that when User Manager returned only part of the profile.
        days = expiry_days

    state_icons = {0: "🟡", 1: "⛔", 2: "✅", 3: "❌"}
    state_label = mikrotik.UM_PROFILE_STATE_LABELS.get(state_code, "نامشخص؛ دوباره بروزرسانی کنید")
    starts_line = ""
    if starts_at_code is not None:
        starts_label = mikrotik.UM_PROFILE_STARTS_AT_LABELS[starts_at_code]
        starts_line = f"\n⏱ شروع اعتبار: <b>{starts_label}</b>"

    return (
        f"📊 <b>وضعیت OpenVPN</b>\n\n"
        f"👤 <code>{html.escape(info.get('matched_name') or identifier)}</code>\n"
        f"📦 حجم باقی‌مانده: <b>{human_bytes(remaining) if usage_available else 'نامشخص'}</b>\n"
        f"📅 روز باقی‌مانده: <b>{str(days) + ' روز' if days is not None else 'نامشخص'}</b>\n"
        f"📌 وضعیت بسته: <b>{state_icons.get(state_code, '⚠️')} {state_label}</b>"
        f"{starts_line}"
    )


def _sync_user_accounts(tg_id: int, service: str, *, refresh_remote: bool = False) -> list[dict]:
    """Return locally known accounts; use 3x-ui only for recovery/cache misses.

    Bot-created V2Ray accounts are persisted locally at creation/renewal time, so
    calling get_by_tg_id on every account-list page only adds latency. For old
    installations with no local V2Ray records we still perform a one-time remote
    discovery, and callers can explicitly request a refresh when needed.
    """
    accounts = list_accounts(tg_id, service)
    if service == "v2ray" and (refresh_remote or not accounts):
        try:
            remote = XUIClient().get_by_tg_id(tg_id)
            known = {str(a.get("identifier") or "") for a in accounts}
            changed = False
            for row in remote:
                c = row.get("client") if isinstance(row, dict) else None
                email = str(c.get("email") or "") if isinstance(c, dict) else ""
                if email and email not in known:
                    upsert_account(tg_id, "v2ray", email, sub_id=c.get("subId", ""))
                    known.add(email)
                    changed = True
            if changed:
                accounts = list_accounts(tg_id, service)
        except Exception as e:
            logger.warning("V2Ray account discovery failed for tg_id=%s: %s", tg_id, e)
    return accounts


def _account_record(tg_id: int, service: str, identifier: str) -> dict:
    # Fast path is always local. Only recover from 3x-ui when a V2Ray account
    # truly is not present in the local store.
    accounts = list_accounts(tg_id, service)
    found = next((a for a in accounts if str(a.get("identifier")) == identifier), {})
    if found or service != "v2ray":
        return found
    accounts = _sync_user_accounts(tg_id, service, refresh_remote=True)
    return next((a for a in accounts if str(a.get("identifier")) == identifier), {})


def my_account_keyboard(service: str, identifier: str, *, username: str = "", password: str = ""):
    ref = account_ref(identifier)
    rows = []
    if service == "openvpn":
        copy_username = str(username or identifier or "-")[:256]
        rows.append([InlineKeyboardButton("📋 کپی یوزرنیم", copy_text=CopyTextButton(copy_username))])
        if password:
            rows.append([InlineKeyboardButton("📋 کپی پسورد", copy_text=CopyTextButton(str(password)[:256]))])
    rows.append([InlineKeyboardButton("📊 مشاهده حجم و روز باقیمانده", callback_data=f"myactref|status|{service}|{ref}")])
    if service_sales_enabled(service):
        rows.append([InlineKeyboardButton("♻️ تمدید اکانت", callback_data=f"myactref|renew|{service}|{ref}")])
    rows.append([InlineKeyboardButton("🔙 بازگشت به اکانت‌های من", callback_data=f"act|accounts|{service}")])
    return InlineKeyboardMarkup(rows)


def my_account_status_keyboard(service: str, identifier: str):
    ref = account_ref(identifier)
    rows = [
        [InlineKeyboardButton("🔄 بروزرسانی وضعیت", callback_data=f"myactref|refresh|{service}|{ref}")],
        [InlineKeyboardButton("🔐 مشاهده اطلاعات کامل اکانت", callback_data=f"myacctref|{service}|{ref}")],
    ]
    if service_sales_enabled(service):
        rows.append([InlineKeyboardButton("♻️ تمدید اکانت", callback_data=f"myactref|renew|{service}|{ref}")])
    rows.append([InlineKeyboardButton("🔙 بازگشت به اکانت‌های من", callback_data=f"act|accounts|{service}")])
    return InlineKeyboardMarkup(rows)


def _vless_location_label(link: str, index: int) -> str:
    remark = ""
    try:
        remark = unquote(urlsplit(link).fragment or "").strip()
    except Exception:
        pass

    # Prefer the flag embedded in the VLESS remark. If the panel returns a
    # generic fragment, fall back to the configured inbound remark by index.
    current_inbounds = tuple(APP_SETTINGS.get("xui_inbound_remarks", ()))
    candidates = [remark]
    if index < len(current_inbounds):
        candidates.append(current_inbounds[index])
    for candidate in candidates:
        for flag, location in FLAG_LOCATION_FA.items():
            if flag in candidate:
                return f"{flag} {location}"
    if index < len(DEFAULT_VLESS_LOCATION_BY_INDEX):
        return DEFAULT_VLESS_LOCATION_BY_INDEX[index]
    return remark or (current_inbounds[index] if index < len(current_inbounds) else f"🌐 لوکیشن {index + 1}")


def format_vless_configs(links: list[str], max_chars: int = 3200) -> str:
    blocks = []
    used_chars = 0
    omitted = 0
    for i, link in enumerate(links):
        label = _vless_location_label(link, i)
        block = f"<b>{html.escape(label)}</b>\n{text_code_block(link)}"
        if len(block) > max(int(max_chars), 0) - used_chars:
            omitted = len(links) - i
            break
        blocks.append(block)
        used_chars += len(block) + 1
    if omitted:
        blocks.append(
            f"<i>برای جلوگیری از عبور پیام از محدودیت تلگرام، {omitted} کانفیگ دیگر نمایش داده نشد؛ "
            "همه لوکیشن‌ها داخل لینک Subscription موجود است.</i>"
        )
    return "\n".join(blocks)


def v2ray_delivery_text(title: str, email: str, gb: int, days: int, sub_url: str, links: list[str]) -> str:
    text = (
        f"{title}\n\n"
        f"👤 <b>نام اکانت</b>\n{text_code_block(email)}"
        f"📦 {gb} گیگ - {days} روز\n\n"
        f"🔗 <b>Subscription</b>\n{text_code_block(sub_url)}"
        "با کپی کردن لینک سابسکریپشن داخل نرم‌افزارهای وی تو ری، می‌توانید همه لوکیشن‌ها را همزمان داشته باشید و "
        "حجم و روز باقی‌مانده اکانت خود را مستقیماً داخل نرم‌افزار مشاهده کنید."
    )
    configs = format_vless_configs(links, max_chars=max(3800 - len(text), 0))
    if configs:
        text += f"\n\n⚙️ <b>لینک‌های VLESS</b>\n{configs}"
    return text


PROFILE_SAVE_CACHE = TTLCache()


async def save_telegram_profile(user, signature=None):
    if not user:
        return
    uid = int(user.id)
    try:
        await run_blocking(
            update_user_profile,
            uid,
            first_name=user.first_name or "",
            last_name=user.last_name or "",
            username=user.username or "",
            language_code=getattr(user, "language_code", "") or "",
        )
    except Exception as e:
        # Allow a later update to retry persistence if this background save failed.
        PROFILE_SAVE_CACHE.invalidate(uid)
        logger.warning("save telegram profile %s failed: %s", uid, e)


def schedule_telegram_profile(context, user):
    """Persist Telegram profile out-of-band; never delay a user interaction."""
    if not user:
        return
    uid = int(user.id)
    signature = (
        str(user.first_name or ""), str(user.last_name or ""),
        str(user.username or ""), str(getattr(user, "language_code", "") or ""),
    )
    if PROFILE_SAVE_CACHE.get(uid) == signature:
        return
    PROFILE_SAVE_CACHE.set(uid, signature, 300.0)
    try:
        context.application.create_task(
            save_telegram_profile(user, signature),
            name=f"profile-save-{uid}",
        )
    except Exception:
        # Fallback for unusual test/startup contexts. Still do not block the handler.
        asyncio.create_task(save_telegram_profile(user, signature))


def buyer_info_text(tg_id: int, profile: dict | None = None) -> str:
    profile = dict(profile or {})
    rows = ["👤 <b>اطلاعات خریدار</b>"]
    first_name = str(profile.get("first_name") or "").strip()
    last_name = str(profile.get("last_name") or "").strip()
    username = str(profile.get("username") or "").strip()
    language = str(profile.get("language_code") or "").strip()
    phone = str(profile.get("phone_number") or "").strip()
    email = str(profile.get("email") or "").strip()
    if first_name:
        rows.append(f"نام: <b>{html.escape(first_name)}</b>")
    if last_name:
        rows.append(f"نام خانوادگی: <b>{html.escape(last_name)}</b>")
    if username:
        rows.append(f"یوزرنیم: @{html.escape(username)}")
    rows.append(f"Telegram ID: <code>{tg_id}</code>")
    if phone:
        rows.append(f"شماره: <code>{html.escape(phone)}</code>")
    if email:
        rows.append(f"ایمیل: <code>{html.escape(email)}</code>")
    if language:
        rows.append(f"زبان تلگرام: <code>{html.escape(language)}</code>")
    return "\n".join(rows)


def _admin_notification_account_keyboard(service: str, tg_id: int, ref: str):
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(
            "🔐 مشاهده اطلاعات کامل اکانت",
            callback_data=f"admnot_ref|{service}|{tg_id}|{ref}",
        )
    ]])


def admin_account_keyboard(service: str, tg_id: int, identifier: str):
    return _admin_notification_account_keyboard(
        service, tg_id, account_ref(identifier)
    )


async def notify_admins(context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup=None):
    async def send_one(admin_id: int):
        try:
            await context.bot.send_message(
                admin_id, text, parse_mode="HTML", disable_web_page_preview=True, reply_markup=reply_markup
            )
        except Exception as e:
            logger.warning("notify admin %s failed: %s", admin_id, e)

    admins = effective_admin_ids()
    if admins:
        await asyncio.gather(*(send_one(admin_id) for admin_id in admins))


def _format_tx_time(value: str) -> str:
    try:
        dt = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(_backup_timezone()).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(value or "-")[:16]


def admin_transactions_keyboard(page: int, total: int, page_size: int = 5):
    rows = []
    nav = []
    offset = page * page_size
    if offset + page_size < total:
        nav.append(InlineKeyboardButton("⬅️ ۵ تراکنش قبلی", callback_data=f"admin_tx|{page + 1}"))
    if page > 0:
        nav.append(InlineKeyboardButton("۵ تراکنش بعدی ➡️", callback_data=f"admin_tx|{page - 1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("🔙 پرداخت و سفارش‌ها", callback_data="admin_payments_menu")])
    return InlineKeyboardMarkup(rows)


async def show_admin_transactions(message, admin_tg_id: int, page: int = 0):
    if not is_admin(admin_tg_id):
        return
    page = max(int(page or 0), 0)
    page_size = 5
    rows, total = await run_blocking(list_transactions, offset=page * page_size, limit=page_size)
    max_page = max((total - 1) // page_size, 0) if total else 0
    if page > max_page:
        page = max_page
        rows, total = await run_blocking(list_transactions, offset=page * page_size, limit=page_size)

    text = f"🧾 <b>تراکنش‌ها</b> — صفحه {page + 1} از {max_page + 1 if total else 1}\n"
    text += f"تعداد کل: <b>{total}</b>\n\n"
    if not rows:
        text += "هنوز تراکنش موفقی ثبت نشده است."
    else:
        for i, tx in enumerate(rows, start=page * page_size + 1):
            tg_id = int(tx.get("tg_id") or 0)
            profile = (await run_blocking(get_user_profile, tg_id)) if tg_id else {}
            name = " ".join(
                x for x in [
                    str(profile.get("first_name") or "").strip(),
                    str(profile.get("last_name") or "").strip(),
                ] if x
            ).strip()
            username = str(profile.get("username") or "").strip()
            who = name or (f"@{username}" if username else str(tg_id))
            if username and name:
                who += f" (@{username})"
            action = "خرید" if str(tx.get("action")) == "buy" else "تمدید"
            service = SERVICE_LABEL.get(str(tx.get("service") or ""), str(tx.get("service") or "-"))
            plan_key = str(tx.get("plan_key") or "-")
            payment_kind = str(tx.get("payment_kind") or "").strip().lower()
            gateway = int(tx.get("gateway_toman") or 0)
            wallet = int(tx.get("wallet_used_toman") or 0)
            if payment_kind == "owner":
                paid_text = "خرید مدیر — رایگان"
            elif payment_kind == "reseller_debt":
                paid_text = f"{int(tx.get('reseller_charge_toman') or 0):,} تومان بدهی ریسلر"
            else:
                components = []
                if gateway:
                    method = str(tx.get("payment_method") or "")
                    components.append(
                        f"{gateway:,} تومان {'کارت به کارت' if method == 'card_transfer' else 'زرین‌پال'}"
                    )
                if wallet:
                    components.append(f"{wallet:,} تومان کیف پول")
                paid_text = " + ".join(components) or "0 تومان"
            text += (
                f"<b>{i}.</b> {action} {html.escape(service)} — <code>{html.escape(plan_key)}</code>\n"
                f"👤 {html.escape(who)} | <code>{tg_id}</code>\n"
                f"💵 <b>{html.escape(paid_text)}</b> | 🕒 {_format_tx_time(str(tx.get('created_at') or ''))}\n"
            )
            if tx.get("legacy"):
                text += "<i>ثبت قدیمی؛ جزئیات روش پرداخت در نسخه قبلی ذخیره نشده.</i>\n"
            text += "\n"
    await message.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=admin_transactions_keyboard(page, total),
        disable_web_page_preview=True,
    )


def admin_user_label(tg_id: int, profile: dict | None = None) -> str:
    profile = dict(profile or {})
    first = str(profile.get("first_name") or "").strip()
    last = str(profile.get("last_name") or "").strip()
    username = str(profile.get("username") or "").strip().lstrip("@")
    name = " ".join(x for x in (first, last) if x).strip()
    if name and username:
        return f"{name} @{username}"
    if name:
        return name
    if username:
        return f"@{username}"
    return str(int(tg_id))


def admin_wallet_users_keyboard(users: list[dict], *, page: int, total: int, positive_only: bool, page_size: int = 10):
    rows = []
    for user in users:
        tg_id = int(user.get("tg_id") or 0)
        label = str(user.get("label") or tg_id)
        balance = int(user.get("balance_toman") or 0)
        button_text = f"{label} — {balance:,} تومان" if positive_only else label
        target = f"admin_wallet_user|{tg_id}" if positive_only else f"admin_user|{tg_id}"
        rows.append([InlineKeyboardButton(button_text[:64], callback_data=target)])
    nav = []
    offset = page * page_size
    prefix = "admin_wallet_pos" if positive_only else "admin_users"
    if offset + page_size < total:
        nav.append(InlineKeyboardButton("⬅️ قبلی", callback_data=f"{prefix}|{page + 1}"))
    if page > 0:
        nav.append(InlineKeyboardButton("بعدی ➡️", callback_data=f"{prefix}|{page - 1}"))
    if nav:
        rows.append(nav)
    back_cb = "admin_users_menu"
    rows.append([InlineKeyboardButton("🔙 بازگشت", callback_data=back_cb)])
    return InlineKeyboardMarkup(rows)


async def show_admin_positive_wallets(message, admin_tg_id: int, page: int = 0):
    if not is_admin(admin_tg_id):
        return
    page_size = 10
    page = max(int(page or 0), 0)
    users, total = await run_blocking(list_known_users, positive_wallet_only=True, offset=page * page_size, limit=page_size)
    max_page = max((total - 1) // page_size, 0) if total else 0
    if page > max_page:
        page = max_page
        users, total = await run_blocking(list_known_users, positive_wallet_only=True, offset=page * page_size, limit=page_size)
    text = f"💰 <b>موجودی کیف پول کاربران</b>\n\nکاربران با موجودی بیشتر از صفر: <b>{total}</b>\n"
    if not users:
        text += "\nدر حال حاضر هیچ کاربری موجودی مثبت ندارد."
    else:
        text += "\nکاربر موردنظر را انتخاب کنید:"
    await message.edit_text(
        text, parse_mode="HTML",
        reply_markup=admin_wallet_users_keyboard(users, page=page, total=total, positive_only=True, page_size=page_size),
    )


def admin_wallet_manage_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔎 جستجوی کاربر", callback_data="admin_wallet_search")],
        [InlineKeyboardButton("📋 مشاهده کاربران", callback_data="admin_users|0")],
        [InlineKeyboardButton("🔙 کاربران", callback_data="admin_users_menu")],
    ])


async def show_admin_wallet_manage(message, admin_tg_id: int):
    if not is_admin(admin_tg_id):
        return
    await message.edit_text(
        "👤 <b>مدیریت کیف پول کاربر</b>\n\nکاربر را جستجو کنید یا از لیست همه کاربران انتخاب کنید.",
        parse_mode="HTML", reply_markup=admin_wallet_manage_keyboard(),
    )


async def show_admin_all_users(message, admin_tg_id: int, page: int = 0):
    if not is_admin(admin_tg_id):
        return
    page_size = 10
    page = max(int(page or 0), 0)
    users, total = await run_blocking(list_known_users, positive_wallet_only=False, offset=page * page_size, limit=page_size)
    max_page = max((total - 1) // page_size, 0) if total else 0
    if page > max_page:
        page = max_page
        users, total = await run_blocking(list_known_users, positive_wallet_only=False, offset=page * page_size, limit=page_size)
    text = f"📋 <b>کاربران ربات</b> — صفحه {page + 1} از {max_page + 1 if total else 1}\nتعداد کل: <b>{total}</b>\n\n"
    text += "کاربر موردنظر را انتخاب کنید:" if users else "هنوز کاربری در دیتابیس ثبت نشده است."
    await message.edit_text(
        text, parse_mode="HTML",
        reply_markup=admin_wallet_users_keyboard(users, page=page, total=total, positive_only=False, page_size=page_size),
    )


async def show_admin_wallet_user(message, admin_tg_id: int, user_tg_id: int):
    if not is_admin(admin_tg_id):
        return
    profile, balance, reserved = await asyncio.gather(
        run_blocking(get_user_profile, user_tg_id),
        run_blocking(wallet_balance, user_tg_id),
        run_blocking(reserved_wallet_for_user, user_tg_id),
    )
    label = admin_user_label(user_tg_id, profile)
    available = max(int(balance) - int(reserved), 0)
    text = (
        f"👤 <b>{html.escape(label)}</b>\n"
        f"💰 موجودی کیف پول: <b>{balance:,} تومان</b>\n"
    )
    if reserved:
        text += f"🔒 رزرو سفارش در انتظار: <b>{reserved:,} تومان</b>\nقابل کاهش: <b>{available:,} تومان</b>\n"
    markup = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("➕ افزایش موجودی", callback_data=f"admin_wallet_inc|{user_tg_id}"),
            InlineKeyboardButton("➖ کاهش موجودی", callback_data=f"admin_wallet_dec|{user_tg_id}"),
        ],
        [InlineKeyboardButton("🔙 مدیریت کیف پول", callback_data="admin_wallet_manage")],
    ])
    await message.edit_text(text, parse_mode="HTML", reply_markup=markup)


def admin_wallet_confirm_keyboard(action: str, user_tg_id: int, amount: int, operation_id: str):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "✅ تایید",
                callback_data=f"aw|{action}|{user_tg_id}|{amount}|{operation_id}",
            ),
            InlineKeyboardButton("❌ لغو", callback_data=f"admin_wallet_user|{user_tg_id}"),
        ]
    ])


def admin_tools_keyboard(_maintenance: bool | None = None):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👥 کاربران", callback_data="admin_users_menu")],
        [InlineKeyboardButton("💳 پرداخت و سفارش‌ها", callback_data="admin_payments_menu")],
        [InlineKeyboardButton("🖥 سیستم و سرورها", callback_data="admin_system_menu")],
        [InlineKeyboardButton("⚙️ تنظیمات", callback_data="admin_settings_menu")],
        [InlineKeyboardButton("🔙 منوی اصلی", callback_data="home")],
    ])


async def show_admin_tools(message, admin_tg_id: int):
    if not is_admin(admin_tg_id):
        return
    stats, maintenance, backup = await asyncio.gather(
        run_blocking(database_stats, check_integrity=False),
        run_blocking(maintenance_mode),
        run_blocking(auto_backup_status),
    )
    counts = stats.get("counts") or {}
    text = (
        "⚙️ <b>پنل مدیریت</b>\n\n"
        f"👥 کاربران: <b>{int(counts.get('users', 0)):,}</b>\n"
        f"⏳ سفارش Pending: <b>{int(counts.get('pending_payments', 0)):,}</b>\n"
        f"🛠 تعمیرات: <b>{'فعال 🔴' if maintenance else 'غیرفعال 🟢'}</b>\n"
        f"💾 بکاپ خودکار: <b>{'روشن ✅' if backup.get('enabled') else 'خاموش ❌'}</b>"
    )
    await message.edit_text(text, parse_mode="HTML", reply_markup=admin_tools_keyboard())


def admin_users_menu_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔎 جستجوی کاربر", callback_data="admin_global_search")],
        [InlineKeyboardButton("📋 لیست کاربران", callback_data="admin_users|0")],
        [InlineKeyboardButton("💰 کیف پول‌های دارای موجودی", callback_data="admin_wallet_pos|0")],
        [InlineKeyboardButton("🔙 پنل مدیریت", callback_data="admin_tools")],
    ])


async def show_admin_users_menu(message, admin_tg_id: int):
    if not is_admin(admin_tg_id):
        return
    stats = await run_blocking(database_stats, check_integrity=False)
    counts = stats.get("counts") or {}
    await message.edit_text(
        "👥 <b>کاربران</b>\n\n"
        f"کاربران ثبت‌شده: <b>{int(counts.get('users', 0)):,}</b>\n"
        f"اکانت‌های ثبت‌شده: <b>{int(counts.get('accounts', 0)):,}</b>\n\n"
        "جستجو، مشاهده کاربران و مدیریت کیف پول از این بخش انجام می‌شود.",
        parse_mode="HTML",
        reply_markup=admin_users_menu_keyboard(),
    )


def admin_payments_menu_keyboard(
    pending_count: int = 0, incomplete_count: int = 0, card_waiting: int = 0
):
    rows = [
        [InlineKeyboardButton("✅ پرداخت‌های موفق", callback_data="admin_tx|0")],
        [InlineKeyboardButton(f"⏳ سفارش‌های در انتظار ({pending_count})", callback_data="admin_pending|0")],
        [InlineKeyboardButton(
            f"🧾 رسیدهای کارت به کارت ({card_waiting})", callback_data="admin_card_requests|0"
        )],
    ]
    if incomplete_count:
        rows.append([InlineKeyboardButton(f"⚠️ تحویل‌های ناقص ({incomplete_count})", callback_data="admin_fulfillments")])
    rows.extend([
        [InlineKeyboardButton("📊 داشبورد فروش", callback_data="admin_dashboard")],
        [InlineKeyboardButton("🎁 گزارش معرفی و کیف پول", callback_data="admin_referral_report")],
        [InlineKeyboardButton("🔙 پنل مدیریت", callback_data="admin_tools")],
    ])
    return InlineKeyboardMarkup(rows)


async def show_admin_payments_menu(message, admin_tg_id: int):
    if not is_admin(admin_tg_id):
        return
    stats = await run_blocking(database_stats, check_integrity=False)
    counts = stats.get("counts") or {}
    pending = int(counts.get("pending_payments", 0))
    incomplete = int(stats.get("incomplete_fulfillments") or 0)
    card_waiting = int(stats.get("card_transfer_waiting") or 0)
    await message.edit_text(
        "💳 <b>پرداخت و سفارش‌ها</b>\n\n"
        f"✅ تراکنش‌های موفق: <b>{int(counts.get('transactions', 0)):,}</b>\n"
        f"⏳ Pending: <b>{pending:,}</b>\n"
        f"⚠️ تحویل ناقص: <b>{incomplete:,}</b>\n\n"
        "بررسی پرداخت‌ها فقط برای همان سفارش و با درخواست مستقیم انجام می‌شود.",
        parse_mode="HTML",
        reply_markup=admin_payments_menu_keyboard(pending, incomplete, card_waiting),
    )


async def show_admin_reports_menu(message, admin_tg_id: int):
    # Safe compatibility for old Telegram callback buttons from v3.4.
    await show_admin_payments_menu(message, admin_tg_id)


def _admin_report_boundaries():
    tz = _backup_timezone()
    now_local = datetime.now(tz)
    day_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    month_local = day_local.replace(day=1)
    return (
        day_local.astimezone(timezone.utc).isoformat(),
        month_local.astimezone(timezone.utc).isoformat(),
        now_local,
    )


async def show_admin_dashboard(message, admin_tg_id: int):
    if not is_admin(admin_tg_id):
        return
    day_start, month_start, now_local = _admin_report_boundaries()
    data = await run_blocking(
        admin_dashboard_stats, day_start_utc=day_start, month_start_utc=month_start
    )
    counts = data.get("counts") or {}
    today = data.get("today") or {}
    month = data.get("month") or {}
    text = (
        "📊 <b>داشبورد مدیریت</b>\n"
        f"<i>{now_local.strftime('%Y-%m-%d %H:%M')}</i>\n\n"
        f"👥 کل کاربران: <b>{int(counts.get('users', 0)):,}</b>\n"
        f"🆕 کاربران جدید امروز: <b>{int(counts.get('new_users_today', 0)):,}</b>\n"
        f"🔐 اکانت‌ها: <b>{int(counts.get('accounts', 0)):,}</b>\n\n"
        f"🛒 خرید امروز: <b>{int(today.get('buys', 0)):,}</b>\n"
        f"🔄 تمدید امروز: <b>{int(today.get('renews', 0)):,}</b>\n"
        f"🔵 OpenVPN: <b>{int(today.get('openvpn', 0)):,}</b> | 🟣 V2Ray: <b>{int(today.get('v2ray', 0)):,}</b>\n"
        f"💰 فروش امروز: <b>{int(today.get('revenue_toman', 0)):,} تومان</b>\n"
        f"💰 فروش این ماه: <b>{int(month.get('revenue_toman', 0)):,} تومان</b>\n\n"
        f"⏳ Pending: <b>{int(counts.get('pending', 0)):,}</b>\n"
        f"⚠️ تحویل ناقص: <b>{int(counts.get('incomplete', 0)):,}</b>"
    )
    await message.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🧾 تراکنش‌ها", callback_data="admin_tx|0")],
            [InlineKeyboardButton("🔙 پرداخت و سفارش‌ها", callback_data="admin_payments_menu")],
        ]),
    )


async def show_admin_referral_report(message, admin_tg_id: int):
    if not is_admin(admin_tg_id):
        return
    data = await run_blocking(admin_referral_stats)
    await message.edit_text(
        "🎁 <b>گزارش معرفی و کیف پول</b>\n\n"
        f"کدهای معرف ساخته‌شده: <b>{int(data.get('codes', 0)):,}</b>\n"
        f"معرف‌های استفاده‌شده: <b>{int(data.get('used', 0)):,}</b>\n"
        f"کل پاداش پرداخت‌شده: <b>{int(data.get('reward_toman', 0)):,} تومان</b>\n"
        f"کاربران دارای موجودی: <b>{int(data.get('wallet_users', 0)):,}</b>\n"
        f"مجموع موجودی کیف پول‌ها: <b>{int(data.get('wallet_total_toman', 0)):,} تومان</b>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("💰 کیف پول‌های دارای موجودی", callback_data="admin_wallet_pos|0")],
            [InlineKeyboardButton("🔙 پرداخت و سفارش‌ها", callback_data="admin_payments_menu")],
        ]),
    )


def admin_system_menu_keyboard(incomplete: int = 0):
    rows = [
        [InlineKeyboardButton("🩺 وضعیت ربات و سرورها", callback_data="admin_health")],
        [InlineKeyboardButton("🗄 وضعیت دیتابیس", callback_data="admin_database")],
    ]
    if incomplete:
        rows.append([InlineKeyboardButton(f"⚠️ تحویل‌های ناقص ({incomplete})", callback_data="admin_fulfillments")])
    rows.extend([
        [InlineKeyboardButton("💾 بکاپ فوری", callback_data="admin_backup")],
        [InlineKeyboardButton("📜 Audit Log", callback_data="admin_audit|0")],
        [InlineKeyboardButton("🔙 پنل مدیریت", callback_data="admin_tools")],
    ])
    return InlineKeyboardMarkup(rows)


async def show_admin_system_menu(message, admin_tg_id: int):
    if not is_admin(admin_tg_id):
        return
    db, backup = await asyncio.gather(
        run_blocking(database_stats, check_integrity=False),
        run_blocking(auto_backup_status),
    )
    snap = RUNTIME.snapshot()
    services = snap.get("service_health") or {}
    def cached(name: str) -> str:
        row = services.get(name) or {}
        if not row:
            return "⚪"
        return "✅" if row.get("ok") else "❌"
    await message.edit_text(
        "🖥 <b>سیستم و سرورها</b>\n\n"
        f"RouterOS: {cached('mikrotik')} | 3x-ui: {cached('xui')}\n"
        f"SQLite: <b>آماده</b> | {human_bytes(int(db.get('size_bytes') or 0))}\n"
        f"Uptime: <b>{_human_duration(snap.get('uptime_seconds', 0))}</b>\n"
        f"💾 بکاپ خودکار: <b>{'روشن ✅' if backup.get('enabled') else 'خاموش ❌'}</b>\n\n"
        "نمایش این صفحه از Cache و SQLite است؛ تست شبکه فقط با دکمه تست زنده انجام می‌شود.",
        parse_mode="HTML",
        reply_markup=admin_system_menu_keyboard(int(db.get("incomplete_fulfillments") or 0)),
    )


async def show_admin_database(message, admin_tg_id: int):
    if not is_admin(admin_tg_id):
        return
    db = await run_blocking(database_stats, check_integrity=True)
    counts = db.get("counts") or {}
    await message.edit_text(
        "🗄 <b>وضعیت دیتابیس</b>\n\n"
        f"Integrity: <b>{html.escape(str(db.get('quick_check') or '-'))}</b>\n"
        f"حجم: <b>{human_bytes(int(db.get('size_bytes') or 0))}</b>\n"
        f"Users: <b>{int(counts.get('users', 0)):,}</b>\n"
        f"Accounts: <b>{int(counts.get('accounts', 0)):,}</b>\n"
        f"Transactions: <b>{int(counts.get('transactions', 0)):,}</b>\n"
        f"Pending: <b>{int(counts.get('pending_payments', 0)):,}</b>\n"
        f"Wallet TX: <b>{int(counts.get('wallet_transactions', 0)):,}</b>\n"
        f"Fulfillments: <b>{int(counts.get('fulfillments', 0)):,}</b>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 سیستم و سرورها", callback_data="admin_system_menu")]]),
    )


def admin_settings_menu_keyboard(maintenance: bool, backup_enabled: bool):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🤖 تنظیمات ربات", callback_data="admin_cfg|bot")],
        [InlineKeyboardButton("🔵 تنظیمات میکروتیک", callback_data="admin_cfg|mikrotik")],
        [InlineKeyboardButton("🟣 تنظیمات ثنایی", callback_data="admin_cfg|xui")],
        [InlineKeyboardButton("💳 درگاه‌های پرداخت", callback_data="admin_gateways")],
        [InlineKeyboardButton("🎁 تنظیمات Referral", callback_data="admin_referral_settings")],
        [InlineKeyboardButton(
            "💾 تنظیمات بکاپ" + (" ✅" if backup_enabled else " ❌"),
            callback_data="admin_backup_settings",
        )],
        [InlineKeyboardButton("📦 مدیریت بسته‌ها", callback_data="admin_plans|0")],
        [InlineKeyboardButton(
            "🟢 خاموش کردن تعمیرات" if maintenance else "🔴 فعال کردن تعمیرات",
            callback_data=f"admin_maintenance_set|{0 if maintenance else 1}",
        )],
        [InlineKeyboardButton("🔙 پنل مدیریت", callback_data="admin_tools")],
    ])


async def show_admin_settings_menu(message, admin_tg_id: int):
    if not is_admin(admin_tg_id):
        return
    maintenance = current_maintenance_mode()
    backup = await run_blocking(auto_backup_status)
    await message.edit_text(
        "⚙️ <b>تنظیمات</b>\n\n"
        f"حالت تعمیرات: <b>{'فعال 🔴' if maintenance else 'غیرفعال 🟢'}</b>\n"
        f"بکاپ خودکار: <b>{'روشن ✅' if backup.get('enabled') else 'خاموش ❌'}</b>\n"
        f"زمان بکاپ: <b>{APP_BACKUP_HOUR:02d}:00</b>",
        parse_mode="HTML",
        reply_markup=admin_settings_menu_keyboard(maintenance, bool(backup.get("enabled"))),
    )


_ADMIN_CONFIG_FIELDS = {
    "brand": ("bot_brand_name", "نام برند", "bot"),
    "userpre": ("account_username_prefix", "پیشوند نام اکانت", "bot"),
    "refpre": ("referral_code_prefix", "پیشوند کد Referral", "bot"),
    "ovpnurl": ("openvpn_connections_url", "لینک راهنمای OpenVPN", "bot"),
    "mtip": ("api_ip", "Mikrotik IP", "mikrotik"),
    "mtport": ("api_port", "Mikrotik API Port", "mikrotik"),
    "mtuser": ("api_user", "Mikrotik Username", "mikrotik"),
    "mtpass": ("api_pass", "Mikrotik Password", "mikrotik"),
    "umscheme": ("um_scheme", "User Manager Connection Type", "mikrotik"),
    "umpath": ("um_path", "User Manager Path", "mikrotik"),
    "xutoken": ("xui_api_token", "XUI API Token", "xui"),
    "xuscheme": ("xui_scheme", "XUI Connection Type", "xui"),
    "xuhost": ("xui_host", "XUI IP", "xui"),
    "xuport": ("xui_port", "XUI Panel Port", "xui"),
    "xupath": ("xui_base_path", "XUI Panel Path", "xui"),
    "xutls": ("xui_verify_tls", "XUI TLS Verify", "xui"),
    "xusub": ("xui_sub_public_base", "Change Subscription URL", "xui"),
    "zpsandbox": ("zarinpal_sandbox", "حالت Sandbox", "zarinpal"),
    "zpmerchant": ("zarinpal_merchant_id", "Merchant ID", "zarinpal"),
    "cardnum": ("card_transfer_card_number", "شماره کارت", "card"),
    "cardholder": ("card_transfer_card_holder", "نام صاحب کارت", "card"),
}


def _masked_secret(value: str) -> str:
    value = str(value or "")
    if not value:
        return "تنظیم نشده"
    return "••••" + value[-4:] if len(value) > 4 else "••••"


def _admin_config_back(group: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("🔙 بازگشت", callback_data=f"admin_cfg|{group}")
    ]])


async def show_admin_bot_settings(message, admin_tg_id: int):
    if not is_admin(admin_tg_id):
        return
    snap = APP_SETTINGS.snapshot()
    openvpn_url = str(snap.get("openvpn_connections_url") or "0")
    await message.edit_text(
        "🤖 <b>تنظیمات ربات</b>\n\n"
        f"نام برند: <code>{html.escape(str(snap.get('bot_brand_name') or ''))}</code>\n"
        f"پیشوند نام اکانت: <code>{html.escape(str(snap.get('account_username_prefix') or ''))}</code>\n"
        f"پیشوند Referral: <code>{html.escape(str(snap.get('referral_code_prefix') or ''))}</code>\n"
        f"لینک OpenVPN: <code>{html.escape('Disabled' if openvpn_url == '0' else openvpn_url)}</code>\n"
        f"مدیر اصلی: <code>{root_admin_id()}</code>\n"
        f"ریسلرهای فعال: <b>{len(reseller_records())}</b>",
        parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✏️ نام برند", callback_data="admin_cfg_edit|brand")],
            [InlineKeyboardButton("👥 ریسلرها", callback_data="admin_resellers")],
            [InlineKeyboardButton("✏️ پیشوند نام اکانت", callback_data="admin_cfg_edit|userpre")],
            [InlineKeyboardButton("✏️ پیشوند کد Referral", callback_data="admin_cfg_edit|refpre")],
            [InlineKeyboardButton("✏️ لینک راهنمای OpenVPN", callback_data="admin_cfg_edit|ovpnurl")],
            [InlineKeyboardButton("🔙 تنظیمات", callback_data="admin_settings_menu")],
        ]),
    )


def _runtime_reseller_by_id(reseller_id: int) -> dict:
    return next(
        (row for row in reseller_records() if int(row.get("id") or 0) == int(reseller_id)),
        {},
    )


async def show_admin_resellers(message, admin_tg_id: int):
    if not is_admin(admin_tg_id):
        return
    rows = []
    records = reseller_records()
    for reseller in records:
        reseller_id = int(reseller.get("id") or 0)
        label = f"• {str(reseller.get('name') or '')} — {int(reseller.get('tg_id') or 0)}"
        rows.append([InlineKeyboardButton(label[:64], callback_data=f"rs|{reseller_id}")])
    rows.extend([
        [InlineKeyboardButton("➕ افزودن ریسلر", callback_data="rsadd")],
        [InlineKeyboardButton("🔙 تنظیمات ربات", callback_data="admin_cfg|bot")],
    ])
    text = "👥 <b>ریسلرها</b>\n\n"
    if records:
        for reseller in records:
            rate = int(reseller.get("price_per_gb_toman") or 0)
            warning = " ⚠️ نیازمند تعیین نرخ" if rate <= 0 else ""
            text += (
                f"• <b>{html.escape(str(reseller.get('name') or ''))}</b> — "
                f"<code>{int(reseller.get('tg_id') or 0)}</code>{warning}\n"
            )
    else:
        text += "هنوز ریسلری ثبت نشده است.\n"
    await message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(rows))


async def show_admin_reseller_detail(message, admin_tg_id: int, reseller_id: int):
    if not is_admin(admin_tg_id):
        return
    reseller = _runtime_reseller_by_id(reseller_id)
    if not reseller:
        await show_admin_resellers(message, admin_tg_id)
        return
    rate = int(reseller.get("price_per_gb_toman") or 0)
    debt = int(reseller.get("debt_toman") or 0)
    trial_enabled = bool(reseller.get("trial_enabled", True))
    text = (
        "👤 <b>جزئیات ریسلر</b>\n\n"
        f"نام: <b>{html.escape(str(reseller.get('name') or ''))}</b>\n"
        f"Telegram ID: <code>{int(reseller.get('tg_id') or 0)}</code>\n"
        f"هزینه هر گیگ: <b>{rate:,} تومان</b>\n"
        f"بدهی فعلی: <b>{debt:,} تومان</b>\n"
        f"اکانت تست: <b>{'فعال ✅' if trial_enabled else 'غیرفعال ⛔'}</b>"
    )
    if rate <= 0:
        text += "\n\n⚠️ خرید این ریسلر تا تعیین هزینه هر گیگ مسدود است."
    await message.edit_text(
        text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✏️ نام", callback_data=f"rsedit|{reseller_id}|n"),
                InlineKeyboardButton("✏️ Telegram ID", callback_data=f"rsedit|{reseller_id}|i"),
            ],
            [InlineKeyboardButton("✏️ هزینه هر گیگ", callback_data=f"rsedit|{reseller_id}|p")],
            [InlineKeyboardButton(
                "⛔ غیرفعال‌کردن اکانت تست" if trial_enabled else "✅ فعال‌کردن اکانت تست",
                callback_data=f"rstrial|{reseller_id}|{0 if trial_enabled else 1}",
            )],
            [InlineKeyboardButton("📒 مدیریت بدهی", callback_data=f"rsdebt|{reseller_id}")],
            [InlineKeyboardButton("🗑 حذف ریسلر", callback_data=f"rsdel|{reseller_id}")],
            [InlineKeyboardButton("🔙 ریسلرها", callback_data="admin_resellers")],
        ]),
    )


async def show_admin_reseller_debt(message, admin_tg_id: int, reseller_id: int):
    if not is_admin(admin_tg_id):
        return
    reseller = _runtime_reseller_by_id(reseller_id)
    if not reseller:
        await show_admin_resellers(message, admin_tg_id)
        return
    await message.edit_text(
        "📒 <b>مدیریت بدهی ریسلر</b>\n\n"
        f"ریسلر: <b>{html.escape(str(reseller.get('name') or ''))}</b>\n"
        f"بدهی فعلی: <b>{int(reseller.get('debt_toman') or 0):,} تومان</b>",
        parse_mode="HTML", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✏️ تغییر بدهی", callback_data=f"rsset|{reseller_id}")],
            [InlineKeyboardButton("0️⃣ صفر کردن بدهی", callback_data=f"rszero|{reseller_id}")],
            [InlineKeyboardButton("🔙 جزئیات ریسلر", callback_data=f"rs|{reseller_id}")],
        ]),
    )


async def show_admin_mikrotik_settings(message, admin_tg_id: int):
    if not is_admin(admin_tg_id):
        return
    snap = APP_SETTINGS.snapshot()
    sales_enabled = bool(snap.get("openvpn_sales_enabled", True))
    await message.edit_text(
        "🔵 <b>تنظیمات میکروتیک</b>\n\n"
        f"فروش OpenVPN: <b>{'فعال ✅' if sales_enabled else 'غیرفعال ⛔'}</b>\n"
        f"Mikrotik IP: <code>{html.escape(str(snap.get('api_ip') or ''))}</code>\n"
        f"Mikrotik API Port: <code>{int(snap.get('api_port') or 0)}</code>\n"
        f"Mikrotik Username: <code>{html.escape(str(snap.get('api_user') or ''))}</code>\n"
        f"Mikrotik Password: <code>{html.escape(str(snap.get('api_pass') or ''))}</code>\n"
        f"User Manager Connection Type: <b>{html.escape(str(snap.get('um_scheme') or '').upper())}</b>\n"
        f"User Manager Path: <code>{html.escape(str(snap.get('um_path') or ''))}</code>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(
                f"فروش OpenVPN: {'فعال ✅' if sales_enabled else 'غیرفعال ⛔'}",
                callback_data="admin_service_sales|openvpn",
            )],
            [InlineKeyboardButton("Mikrotik IP", callback_data="admin_cfg_edit|mtip")],
            [InlineKeyboardButton("Mikrotik API Port", callback_data="admin_cfg_edit|mtport")],
            [InlineKeyboardButton("Mikrotik Username", callback_data="admin_cfg_edit|mtuser")],
            [InlineKeyboardButton("Mikrotik Password", callback_data="admin_cfg_edit|mtpass")],
            [InlineKeyboardButton("User Manager Connection Type", callback_data="admin_cfg_edit|umscheme")],
            [InlineKeyboardButton("User Manager Path", callback_data="admin_cfg_edit|umpath")],
            [InlineKeyboardButton("🧪 Test Connection", callback_data="admin_cfg_test|mikrotik")],
            [InlineKeyboardButton("🔙 تنظیمات", callback_data="admin_settings_menu")],
        ]),
    )


async def show_admin_xui_settings(message, admin_tg_id: int):
    if not is_admin(admin_tg_id):
        return
    snap = APP_SETTINGS.snapshot()
    sales_enabled = bool(snap.get("v2ray_sales_enabled", True))
    await message.edit_text(
        "🟣 <b>تنظیمات ثنایی</b>\n\n"
        f"فروش V2ray: <b>{'فعال ✅' if sales_enabled else 'غیرفعال ⛔'}</b>\n"
        f"XUI API Token: <code>{html.escape(_masked_secret(str(snap.get('xui_api_token') or '')))}</code>\n"
        f"XUI Connection Type: <b>{html.escape(str(snap.get('xui_scheme') or '').upper())}</b>\n"
        f"XUI IP: <code>{html.escape(str(snap.get('xui_host') or ''))}</code>\n"
        f"XUI Panel Port: <code>{int(snap.get('xui_port') or 0)}</code>\n"
        f"XUI Panel Path: <code>{html.escape(str(snap.get('xui_base_path') or ''))}</code>\n"
        f"XUI TLS Verify: <b>{'Enabled' if snap.get('xui_verify_tls') else 'Disabled'}</b>\n"
        f"Client Inbounds: <b>{len(inbound_records())}</b>\n"
        f"Change Subscription URL: <code>{html.escape(str(snap.get('xui_sub_public_base') or '0'))}</code>",
        parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(
                f"فروش V2ray: {'فعال ✅' if sales_enabled else 'غیرفعال ⛔'}",
                callback_data="admin_service_sales|v2ray",
            )],
            [InlineKeyboardButton("XUI API Token", callback_data="admin_cfg_edit|xutoken")],
            [InlineKeyboardButton("XUI Connection Type", callback_data="admin_cfg_edit|xuscheme")],
            [InlineKeyboardButton("XUI IP", callback_data="admin_cfg_edit|xuhost")],
            [InlineKeyboardButton("XUI Panel Port", callback_data="admin_cfg_edit|xuport")],
            [InlineKeyboardButton("XUI Panel Path", callback_data="admin_cfg_edit|xupath")],
            [InlineKeyboardButton("XUI TLS Verify", callback_data="admin_cfg_edit|xutls")],
            [InlineKeyboardButton("Client Inbounds", callback_data="admin_inbounds")],
            [InlineKeyboardButton("Change Subscription URL", callback_data="admin_cfg_edit|xusub")],
            [InlineKeyboardButton("🧪 Test Connection", callback_data="admin_cfg_test|xui")],
            [InlineKeyboardButton("🔙 تنظیمات", callback_data="admin_settings_menu")],
        ]),
    )


async def show_admin_inbounds(message, admin_tg_id: int):
    if not is_admin(admin_tg_id):
        return
    records = inbound_records()
    rows = [
        [InlineKeyboardButton(f"• {remark}"[:64], callback_data=f"admin_inbound|{inbound_id}")]
        for inbound_id, remark in records
    ]
    rows.extend([
        [InlineKeyboardButton("➕ Add Inbound", callback_data="admin_inbound_add")],
        [InlineKeyboardButton("🔙 تنظیمات ثنایی", callback_data="admin_cfg|xui")],
    ])
    text = "<b>Client Inbounds</b>\n\n"
    text += "".join(f"• {html.escape(remark)}\n" for _, remark in records) or "هیچ Inbound ثبت نشده است."
    await message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(rows))


async def show_admin_inbound_detail(message, admin_tg_id: int, inbound_id: int):
    if not is_admin(admin_tg_id):
        return
    found = next((item for item in inbound_records() if item[0] == int(inbound_id)), None)
    if not found:
        await show_admin_inbounds(message, admin_tg_id)
        return
    await message.edit_text(
        f"<b>Client Inbound</b>\n\n• {html.escape(found[1])}",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✏️ Rename", callback_data=f"admin_inbound_rename|{found[0]}")],
            [InlineKeyboardButton("🗑 Delete", callback_data=f"admin_inbound_delete|{found[0]}")],
            [InlineKeyboardButton("🔙 Client Inbounds", callback_data="admin_inbounds")],
        ]),
    )


async def show_admin_zarinpal_settings(message, admin_tg_id: int):
    if not is_admin(admin_tg_id):
        return
    snap = APP_SETTINGS.snapshot()
    enabled = bool(snap.get("zarinpal_enabled", True))
    await message.edit_text(
        "💳 <b>تنظیمات زرین‌پال</b>\n\n"
        f"وضعیت: <b>{'فعال ✅' if enabled else 'غیرفعال ⛔'}</b>\n"
        f"حالت Sandbox: <b>{'Enabled' if snap.get('zarinpal_sandbox') else 'Disabled'}</b>\n"
        f"Merchant ID: <code>{html.escape(str(snap.get('zarinpal_merchant_id') or ''))}</code>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(
                f"زرین‌پال: {'فعال ✅' if enabled else 'غیرفعال ⛔'}",
                callback_data="admin_gateway_toggle|zarinpal",
            )],
            [InlineKeyboardButton("حالت Sandbox", callback_data="admin_cfg_edit|zpsandbox")],
            [InlineKeyboardButton("Merchant ID", callback_data="admin_cfg_edit|zpmerchant")],
            [InlineKeyboardButton("🧪 Test Connection", callback_data="admin_cfg_test|zarinpal")],
            [InlineKeyboardButton("🔙 درگاه‌های پرداخت", callback_data="admin_gateways")],
        ]),
    )


def _format_card_number(value: str) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits


async def show_admin_payment_gateways(message, admin_tg_id: int):
    if not is_admin(admin_tg_id):
        return
    snap = APP_SETTINGS.snapshot()
    zp_enabled = bool(snap.get("zarinpal_enabled", True))
    card_enabled = bool(snap.get("card_transfer_enabled", False))
    await message.edit_text(
        "💳 <b>درگاه‌های پرداخت</b>\n\n"
        f"زرین‌پال: <b>{'فعال ✅' if zp_enabled else 'غیرفعال ⛔'}</b>\n"
        f"کارت به کارت: <b>{'فعال ✅' if card_enabled else 'غیرفعال ⛔'}</b>\n\n"
        "حداقل یکی از درگاه‌ها باید فعال بماند.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("💳 زرین‌پال", callback_data="admin_cfg|zarinpal")],
            [InlineKeyboardButton("🏦 کارت به کارت", callback_data="admin_cfg|card")],
            [InlineKeyboardButton("🔙 تنظیمات", callback_data="admin_settings_menu")],
        ]),
    )


async def show_admin_card_transfer_settings(message, admin_tg_id: int):
    if not is_admin(admin_tg_id):
        return
    snap = APP_SETTINGS.snapshot()
    enabled = bool(snap.get("card_transfer_enabled", False))
    card_number = str(snap.get("card_transfer_card_number") or "")
    card_holder = str(snap.get("card_transfer_card_holder") or "")
    await message.edit_text(
        "🏦 <b>تنظیمات کارت به کارت</b>\n\n"
        f"وضعیت: <b>{'فعال ✅' if enabled else 'غیرفعال ⛔'}</b>\n"
        f"شماره کارت: <code>{html.escape(_format_card_number(card_number) or 'تنظیم نشده')}</code>\n"
        f"نام صاحب کارت: <b>{html.escape(card_holder or 'تنظیم نشده')}</b>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(
                f"کارت به کارت: {'فعال ✅' if enabled else 'غیرفعال ⛔'}",
                callback_data="admin_gateway_toggle|card_transfer",
            )],
            [InlineKeyboardButton("✏️ شماره کارت", callback_data="admin_cfg_edit|cardnum")],
            [InlineKeyboardButton("✏️ نام صاحب کارت", callback_data="admin_cfg_edit|cardholder")],
            [InlineKeyboardButton("🔙 درگاه‌های پرداخت", callback_data="admin_gateways")],
        ]),
    )


async def show_admin_config_group(message, admin_tg_id: int, group: str):
    if group == "bot":
        await show_admin_bot_settings(message, admin_tg_id)
    elif group == "mikrotik":
        await show_admin_mikrotik_settings(message, admin_tg_id)
    elif group == "xui":
        await show_admin_xui_settings(message, admin_tg_id)
    elif group == "zarinpal":
        await show_admin_zarinpal_settings(message, admin_tg_id)
    elif group == "card":
        await show_admin_card_transfer_settings(message, admin_tg_id)


def _config_choice_markup(short_key: str, group: str) -> InlineKeyboardMarkup:
    if short_key in {"umscheme", "xuscheme"}:
        choices = (("HTTP", "http"), ("HTTPS", "https"))
    else:
        choices = (("Enabled", "1"), ("Disabled", "0"))
    rows = [[InlineKeyboardButton(label, callback_data=f"admin_cfg_set|{short_key}|{value}")] for label, value in choices]
    rows.append([InlineKeyboardButton("🔙 بازگشت", callback_data=f"admin_cfg|{group}")])
    return InlineKeyboardMarkup(rows)


def _format_backup_time(value: str) -> str:
    if not value:
        return "هنوز ایجاد نشده"
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(_backup_timezone()).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(value)[:19]


async def show_admin_backup_settings(message, admin_tg_id: int):
    if not is_admin(admin_tg_id):
        return
    status = await run_blocking(auto_backup_status)
    enabled = bool(status.get("enabled"))
    last = status.get("last_backup") or {}
    next_local = datetime.now(_backup_timezone()) + timedelta(seconds=_seconds_until_next_backup())
    await message.edit_text(
        "💾 <b>بکاپ خودکار دیتابیس</b>\n\n"
        f"وضعیت: <b>{'روشن ✅' if enabled else 'خاموش ❌'}</b>\n"
        f"زمان اجرا: <b>هر روز ساعت {APP_BACKUP_HOUR:02d}:00</b>\n"
        f"آخرین بکاپ خودکار: <b>{html.escape(_format_backup_time(str(last.get('created_at') or '')))}</b>\n"
        f"بکاپ بعدی زمان‌بندی‌شده: <b>{next_local.strftime('%Y-%m-%d %H:%M')}</b>\n"
        f"تعداد فایل‌های نگهداری‌شده: <b>{BACKUP_KEEP}</b>\n\n"
        "بکاپ در صف مستقل اجرا می‌شود و پردازش کاربران را اشغال نمی‌کند.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(
                "❌ خاموش کردن بکاپ خودکار" if enabled else "✅ روشن کردن بکاپ خودکار",
                callback_data=f"admin_auto_backup_set|{0 if enabled else 1}",
            )],
            [InlineKeyboardButton("💾 بکاپ فوری", callback_data="admin_backup")],
            [InlineKeyboardButton("🔙 تنظیمات", callback_data="admin_settings_menu")],
        ]),
    )


def _admin_plan_list_keyboard(service: str | int, page: int | None = None, page_size: int = 8):
    # Compatibility for extensions that called the v3.3 private helper with
    # only a page number. The live v3.4 UI always passes an explicit service.
    if page is None:
        page = int(service)
        service = "openvpn"
        legacy_trial_row = True
    else:
        service = str(service)
        legacy_trial_row = False
    registry = plans_for(service)
    keys = list(registry.keys())
    page = max(int(page), 0)
    start = page * page_size
    rows = []
    if legacy_trial_row and page == 0:
        trial_state = "✅" if test_plan_enabled() else "⛔"
        rows.append([InlineKeyboardButton(
            f"🎁 تست | {int(TEST_PLAN.get('gb') or 0)}GB | {int(TEST_PLAN.get('days') or 0)} روز | {trial_state}"[:64],
            callback_data="admin_trial_view",
        )])
    for key in keys[start:start + page_size]:
        plan = registry[key]
        rows.append([InlineKeyboardButton(
            f"📦 {int(plan['gb'])}GB | {_plan_duration_text(plan)} | {int(plan['price_toman']):,}"[:64],
            callback_data=f"admin_plan_view|{service}|{admin_plan_ref(key, service)}",
        )])
    nav = []
    if start + page_size < len(keys):
        nav.append(InlineKeyboardButton("⬅️ بعدی", callback_data=f"admin_plans_service|{service}|{page + 1}"))
    if page > 0:
        nav.append(InlineKeyboardButton("قبلی ➡️", callback_data=f"admin_plans_service|{service}|{page - 1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("➕ افزودن بسته", callback_data=f"admin_plan_add|{service}")])
    rows.append([InlineKeyboardButton("🔙 مدیریت بسته‌ها", callback_data="admin_plans|0")])
    return InlineKeyboardMarkup(rows)


async def show_admin_plans(message, admin_tg_id: int, page: int = 0):
    if not is_admin(admin_tg_id):
        return
    rows = []
    if service_sales_enabled("openvpn"):
        rows.append([InlineKeyboardButton(
            f"🔵 بسته‌های OpenVPN ({len(plans_for('openvpn'))})",
            callback_data="admin_plans_service|openvpn|0",
        )])
    if service_sales_enabled("v2ray"):
        rows.append([InlineKeyboardButton(
            f"🟣 بسته‌های V2Ray ({len(plans_for('v2ray'))})",
            callback_data="admin_plans_service|v2ray|0",
        )])
    trial_state = "✅" if test_plan_enabled() else "⛔"
    rows.extend([
        [InlineKeyboardButton(
            f"🎁 بسته تست | {int(TEST_PLAN.get('gb') or 0)}GB | {trial_state}"[:64],
            callback_data="admin_trial_view",
        )],
        [InlineKeyboardButton("🔙 تنظیمات", callback_data="admin_settings_menu")],
    ])
    await message.edit_text(
        "📦 <b>مدیریت بسته‌ها</b>\n\n"
        "بسته‌های فروش OpenVPN و V2Ray مستقل هستند.\n"
        "بسته‌های سرویس غیرفعال در این بخش نمایش داده نمی‌شوند؛ اطلاعات آن‌ها در دیتابیس محفوظ می‌ماند.\n\n"
        f"اکانت تست: <b>{'فعال' if test_plan_enabled() else 'غیرفعال'}</b>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(rows),
    )


async def show_admin_service_plans(message, admin_tg_id: int, service: str, page: int = 0):
    if not is_admin(admin_tg_id):
        return
    if not service_sales_enabled(service):
        await show_admin_plans(message, admin_tg_id)
        return
    registry = plans_for(service)
    label = SERVICE_LABEL[service]
    text = (
        f"📦 <b>بسته‌های {label}</b>\n\n"
        f"تعداد بسته‌ها: <b>{len(registry)}</b>\n"
        + (
            "برای هر بسته OpenVPN نام دقیق پکیج MikroTik نیز نگهداری می‌شود."
            if service == "openvpn"
            else "هر بسته V2Ray حجم، مدت و قیمت مستقل خود را دارد."
        )
    )
    if not registry:
        text += "\n\n⚠️ هنوز بسته‌ای برای این سرویس تعریف نشده است."
    await message.edit_text(
        text, parse_mode="HTML",
        reply_markup=_admin_plan_list_keyboard(service, page),
    )


async def show_admin_trial_detail(message, admin_tg_id: int):
    if not is_admin(admin_tg_id):
        return
    enabled = test_plan_enabled()
    await message.edit_text(
        "🎁 <b>بسته تست</b>\n\n"
        f"وضعیت: <b>{'✅ فعال' if enabled else '⛔ غیرفعال'}</b>\n"
        f"حجم: <b>{int(TEST_PLAN.get('gb') or 0):,} GB</b>\n"
        f"مدت: <b>{int(TEST_PLAN.get('days') or 0):,} روز</b>\n"
        f"پکیج MikroTik: <code>{html.escape(str(TEST_PLAN.get('openvpn_profile') or ''))}</code>\n\n"
        "🔵 OpenVPN: از همین Profile استفاده می‌کند\n"
        "🟣 V2Ray: از همین حجم و تعداد روز استفاده می‌کند\n\n"
        "اکانت‌های تستی که قبلاً ساخته شده‌اند با غیرفعال‌کردن این گزینه حذف نمی‌شوند.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✏️ حجم", callback_data="admin_trial_edit|gb"),
                InlineKeyboardButton("✏️ روز", callback_data="admin_trial_edit|days"),
            ],
            [InlineKeyboardButton("✏️ پکیج MikroTik", callback_data="admin_trial_edit|openvpn_profile")],
            [InlineKeyboardButton(
                "⛔ غیرفعال‌کردن تست" if enabled else "✅ فعال‌کردن تست",
                callback_data=f"admin_trial_toggle|{0 if enabled else 1}",
            )],
            [InlineKeyboardButton("🔙 بسته‌ها", callback_data="admin_plans|0")],
        ]),
    )


async def show_admin_plan_detail(
    message, admin_tg_id: int, service: str, plan_key: str | None = None
):
    if not is_admin(admin_tg_id):
        return
    if plan_key is None:
        plan_key = service
        service = "openvpn"
    registry = plans_for(service)
    plan = registry.get(str(plan_key))
    plan_ref = admin_plan_ref(plan_key, service)
    if not plan:
        await message.edit_text(
            "⚠️ این بسته دیگر وجود ندارد.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 بسته‌ها", callback_data=f"admin_plans_service|{service}|0")]]),
        )
        return
    await message.edit_text(
        f"📦 <b>مشخصات بسته {SERVICE_LABEL[service]}</b>\n\n"
        f"حجم: <b>{int(plan['gb']):,} GB</b>\n"
        f"مدت: <b>{html.escape(_plan_duration_text(plan))}</b>"
        + (f" — {int(plan['days'])} روز" if int(plan.get("months") or 0) > 0 else "") + "\n"
        f"قیمت: <b>{int(plan['price_toman']):,} تومان</b>"
        + (f"\nپکیج MikroTik: <code>{html.escape(str(plan['openvpn_profile']))}</code>" if service == "openvpn" else ""),
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✏️ حجم", callback_data=f"admin_plan_edit|{service}|{plan_ref}|gb"),
                InlineKeyboardButton("✏️ ماه", callback_data=f"admin_plan_edit|{service}|{plan_ref}|months"),
            ],
            [InlineKeyboardButton("✏️ قیمت", callback_data=f"admin_plan_edit|{service}|{plan_ref}|price_toman")],
            *([[InlineKeyboardButton("✏️ پکیج MikroTik", callback_data=f"admin_plan_edit|{service}|{plan_ref}|openvpn_profile")]] if service == "openvpn" else []),
            [InlineKeyboardButton("🗑 حذف بسته", callback_data=f"admin_plan_delete|{service}|{plan_ref}")],
            [InlineKeyboardButton("🔙 بسته‌ها", callback_data=f"admin_plans_service|{service}|0")],
        ]),
    )


def _plan_draft_summary(draft: dict) -> str:
    service = str(draft.get("service") or "openvpn")
    text = (
        f"📦 <b>بسته جدید {SERVICE_LABEL.get(service, service)}</b>\n\n"
        f"حجم: <b>{int(draft.get('gb') or 0):,} GB</b>\n"
        f"مدت: <b>{int(draft.get('months') or 0)} ماه</b>\n"
        f"قیمت: <b>{int(draft.get('price_toman') or 0):,} تومان</b>"
    )
    if service == "openvpn":
        text += f"\nپکیج MikroTik: <code>{html.escape(str(draft.get('openvpn_profile') or ''))}</code>"
        if service_sales_enabled("v2ray"):
            text += "\n\nآیا همین حجم، مدت و قیمت برای OpenVPN و V2Ray ثبت شود؟"
    return text


async def show_admin_referral_settings(message, admin_tg_id: int):
    if not is_admin(admin_tg_id):
        return
    discount = current_referral_discount_percent()
    reward = current_referral_reward_percent()
    await message.edit_text(
        "🎁 <b>تنظیمات Referral</b>\n\n"
        f"پاداش خریدار / تخفیف خرید اول: <b>{discount}%</b>\n"
        f"پاداش معرف: <b>{reward}%</b>\n\n"
        "تغییرات از این بخش در دیتابیس ذخیره می‌شوند و بعد از Restart کانتینر نیز باقی می‌مانند.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✏️ درصد پاداش خریدار", callback_data="admin_referral_edit|discount")],
            [InlineKeyboardButton("✏️ درصد پاداش معرف", callback_data="admin_referral_edit|reward")],
            [InlineKeyboardButton("📊 گزارش Referral", callback_data="admin_referral_report")],
            [InlineKeyboardButton("🔙 تنظیمات", callback_data="admin_settings_menu")],
        ]),
    )


def admin_pending_keyboard(rows: list[dict], *, page: int, total: int, page_size: int = 6):
    keyboard = []
    for item in rows:
        pending_id = int(item.get("pending_id") or 0)
        payload = item.get("payload") or {}
        service = str(payload.get("service") or "-")
        plan = str(payload.get("plan_key") or "-")
        label = str(item.get("label") or item.get("tg_id") or "کاربر")
        icon = "🔵" if service == "openvpn" else ("🟣" if service == "v2ray" else "⏳")
        keyboard.append([InlineKeyboardButton(
            f"{icon} {label} | {plan}"[:64], callback_data=f"admin_pending_view|{pending_id}"
        )])
    nav = []
    if (page + 1) * page_size < total:
        nav.append(InlineKeyboardButton("⬅️ قدیمی‌تر", callback_data=f"admin_pending|{page+1}"))
    if page > 0:
        nav.append(InlineKeyboardButton("جدیدتر ➡️", callback_data=f"admin_pending|{page-1}"))
    if nav:
        keyboard.append(nav)
    keyboard.append([InlineKeyboardButton("🔙 پرداخت و سفارش‌ها", callback_data="admin_payments_menu")])
    return InlineKeyboardMarkup(keyboard)


async def show_admin_pending_payments(message, admin_tg_id: int, page: int = 0):
    if not is_admin(admin_tg_id):
        return
    page = max(int(page or 0), 0)
    size = 6
    rows, total = await run_blocking(list_admin_pending_payments, offset=page*size, limit=size)
    max_page = max((total - 1) // size, 0) if total else 0
    if page > max_page:
        page = max_page
        rows, total = await run_blocking(list_admin_pending_payments, offset=page*size, limit=size)
    text = f"⏳ <b>سفارش‌های در انتظار</b> — صفحه {page+1} از {max_page+1 if total else 1}\nتعداد کل: <b>{total}</b>\n\n"
    if not rows:
        text += "سفارش Pending وجود ندارد."
    else:
        now_ts = int(time.time())
        for item in rows:
            payload = item.get("payload") or {}
            age = max(now_ts - int(item.get("ts") or payload.get("ts") or now_ts), 0)
            text += (
                f"• 👤 {html.escape(str(item.get('label') or item.get('tg_id')))} | <code>{int(item.get('tg_id') or 0)}</code>\n"
                f"  {html.escape(SERVICE_LABEL.get(str(payload.get('service') or ''), str(payload.get('service') or '-')))} — "
                f"<code>{html.escape(str(payload.get('plan_key') or '-'))}</code> | {_human_duration(age)}\n"
            )
    await message.edit_text(
        text, parse_mode="HTML", reply_markup=admin_pending_keyboard(rows, page=page, total=total, page_size=size)
    )


async def show_admin_pending_detail(message, admin_tg_id: int, pending_id: int):
    if not is_admin(admin_tg_id):
        return
    item = await run_blocking(get_admin_pending_payment_by_id, pending_id)
    if not item:
        await message.edit_text(
            "✅ این سفارش دیگر Pending نیست.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Pendingها", callback_data="admin_pending|0")]]),
        )
        return
    payload = item.get("payload") or {}
    authority = str(item.get("authority") or "")
    payment_kind = str(payload.get("payment_kind") or "gateway")
    amount_rial = int(payload.get("amount_rial") or 0)
    amount_toman = int(payload.get("gateway_toman") or (amount_rial // 10 if amount_rial else 0))
    age = max(int(time.time()) - int(item.get("ts") or payload.get("ts") or int(time.time())), 0)
    text = (
        "⏳ <b>جزئیات سفارش Pending</b>\n\n"
        f"👤 {html.escape(str(item.get('label') or item.get('tg_id')))}\n"
        f"Telegram ID: <code>{int(item.get('tg_id') or 0)}</code>\n"
        f"سرویس: <b>{html.escape(SERVICE_LABEL.get(str(payload.get('service') or ''), str(payload.get('service') or '-')))}</b>\n"
        f"عملیات: <b>{'تمدید' if payload.get('action') == 'renew' else 'خرید'}</b>\n"
        f"پلن: <code>{html.escape(str(payload.get('plan_key') or '-'))}</code>\n"
        f"مبلغ درگاه: <b>{amount_toman:,} تومان</b>\n"
        f"نوع پرداخت: <code>{html.escape(payment_kind)}</code>\n"
        f"سن سفارش: <b>{_human_duration(age)}</b>\n"
        f"Authority: <code>{html.escape(authority)}</code>"
    )
    rows = [[InlineKeyboardButton("👤 مشاهده کاربر", callback_data=f"admin_user|{int(item.get('tg_id') or 0)}")]]
    if payment_kind == "card_transfer":
        request_id = int(payload.get("card_request_id") or 0)
        if request_id:
            rows.append([InlineKeyboardButton(
                "🧾 مشاهده درخواست کارت به کارت", callback_data=f"cardview|{request_id}"
            )])
    elif payment_kind not in {"wallet", "admin", "owner", "reseller_debt", "preflight"} and amount_rial > 0 and authority:
        rows.append([InlineKeyboardButton("🔄 بررسی وضعیت زرین‌پال", callback_data=f"admin_pending_verify|{pending_id}")])
        rows.append([InlineKeyboardButton("🗑 آزادسازی در صورت پرداخت‌نشده", callback_data=f"admin_pending_cancel|{pending_id}")])
    rows.append([InlineKeyboardButton("🔙 Pendingها", callback_data="admin_pending|0")])
    await message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(rows))


_CARD_TRANSFER_STATUS_LABELS = {
    "awaiting_receipt": "در انتظار رسید",
    "submitted": "در انتظار بررسی",
    "processing": "در حال تحویل",
    "approved": "تأییدشده",
    "rejected": "ردشده",
    "cancelled": "لغوشده",
}


def _card_transfer_admin_text(request: dict, *, include_receipt: bool = True) -> str:
    payload = dict(request.get("payload") or {})
    status = str(request.get("status") or "")
    amount = int(payload.get("gateway_toman") or 0)
    action = "تمدید" if payload.get("action") == "renew" else "خرید"
    text = (
        "🏦 <b>درخواست کارت به کارت</b>\n\n"
        f"شناسه: <code>{int(request.get('id') or 0)}</code>\n"
        f"وضعیت: <b>{html.escape(_CARD_TRANSFER_STATUS_LABELS.get(status, status or '-'))}</b>\n"
        f"کاربر: {html.escape(str(request.get('label') or request.get('tg_id') or '-'))}\n"
        f"Telegram ID: <code>{int(request.get('tg_id') or 0)}</code>\n"
        f"سرویس: <b>{html.escape(SERVICE_LABEL.get(str(payload.get('service') or ''), '-'))}</b>\n"
        f"عملیات: <b>{action}</b>\n"
        f"بسته: <code>{html.escape(str(payload.get('plan_key') or '-'))}</code>\n"
        f"مبلغ کارت به کارت: <b>{amount:,} تومان</b>"
    )
    if include_receipt and str(request.get("receipt_kind") or "") == "text":
        receipt = str(request.get("receipt_text") or "")
        text += f"\n\n🧾 <b>متن رسید:</b>\n{html.escape(receipt[:2000])}"
    return text


def _card_transfer_admin_markup(request: dict, *, back: str = "admin_card_requests|0"):
    request_id = int(request.get("id") or 0)
    status = str(request.get("status") or "")
    rows = []
    if status == "submitted":
        rows.append([
            InlineKeyboardButton("✅ تأیید و ساخت اکانت", callback_data=f"cardadm|approve|{request_id}"),
            InlineKeyboardButton("❌ رد", callback_data=f"cardadm|reject|{request_id}"),
        ])
    elif status == "processing":
        rows.append([InlineKeyboardButton(
            "🔄 ادامه/تلاش مجدد تحویل", callback_data=f"cardadm|approve|{request_id}"
        )])
    if str(request.get("receipt_kind") or "") in {"photo", "document"}:
        rows.append([InlineKeyboardButton("🖼 نمایش رسید", callback_data=f"cardadm|receipt|{request_id}")])
    rows.extend([
        [InlineKeyboardButton("👤 مشاهده کاربر", callback_data=f"admin_user|{int(request.get('tg_id') or 0)}")],
        [InlineKeyboardButton("🔙 رسیدهای کارت به کارت", callback_data=back)],
    ])
    return InlineKeyboardMarkup(rows)


async def show_admin_card_requests(message, admin_tg_id: int, page: int = 0):
    if not is_admin(admin_tg_id):
        return
    page = max(int(page or 0), 0)
    size = 6
    rows, total = await run_blocking(
        list_card_transfer_requests, statuses=("submitted", "processing"),
        offset=page * size, limit=size,
    )
    max_page = max((total - 1) // size, 0) if total else 0
    if page > max_page:
        page = max_page
        rows, total = await run_blocking(
            list_card_transfer_requests, statuses=("submitted", "processing"),
            offset=page * size, limit=size,
        )
    keyboard = []
    for item in rows:
        payload = dict(item.get("payload") or {})
        keyboard.append([InlineKeyboardButton(
            f"🏦 {item.get('label') or item.get('tg_id')} | {int(payload.get('gateway_toman') or 0):,}"[:64],
            callback_data=f"cardview|{int(item.get('id') or 0)}",
        )])
    nav = []
    if (page + 1) * size < total:
        nav.append(InlineKeyboardButton("⬅️ قدیمی‌تر", callback_data=f"admin_card_requests|{page + 1}"))
    if page > 0:
        nav.append(InlineKeyboardButton("جدیدتر ➡️", callback_data=f"admin_card_requests|{page - 1}"))
    if nav:
        keyboard.append(nav)
    keyboard.append([InlineKeyboardButton("🔙 پرداخت و سفارش‌ها", callback_data="admin_payments_menu")])
    text = (
        "🧾 <b>رسیدهای کارت به کارت</b>\n\n"
        f"در انتظار بررسی/تحویل: <b>{total}</b>"
        + ("" if rows else "\n\nدرخواستی برای بررسی وجود ندارد.")
    )
    if getattr(message, "text", None) is None:
        await message.reply_text(
            text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard)
        )
    else:
        await message.edit_text(
            text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard)
        )


async def show_admin_card_request_detail(message, admin_tg_id: int, request_id: int):
    if not is_admin(admin_tg_id):
        return
    request = await run_blocking(get_card_transfer_request, request_id)
    if not request:
        await message.edit_text(
            "⚠️ درخواست پیدا نشد.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(
                "🔙 رسیدها", callback_data="admin_card_requests|0"
            )]]),
        )
        return
    if getattr(message, "text", None) is None:
        await message.reply_text(
            _card_transfer_admin_text(request), parse_mode="HTML",
            reply_markup=_card_transfer_admin_markup(request),
        )
    else:
        await message.edit_text(
            _card_transfer_admin_text(request), parse_mode="HTML",
            reply_markup=_card_transfer_admin_markup(request),
        )


async def notify_card_transfer_admins(context, request: dict):
    text = _card_transfer_admin_text(request)
    markup = _card_transfer_admin_markup(request)
    kind = str(request.get("receipt_kind") or "")
    file_id = str(request.get("receipt_file_id") or "")
    for admin_tg_id in effective_admin_ids():
        try:
            if kind == "photo" and file_id:
                await context.bot.send_photo(
                    chat_id=admin_tg_id, photo=file_id, caption=text[:1024],
                    parse_mode="HTML", reply_markup=markup,
                )
            elif kind == "document" and file_id:
                await context.bot.send_document(
                    chat_id=admin_tg_id, document=file_id, caption=text[:1024],
                    parse_mode="HTML", reply_markup=markup,
                )
            else:
                await context.bot.send_message(
                    chat_id=admin_tg_id, text=text, parse_mode="HTML", reply_markup=markup,
                )
        except Exception as exc:
            logger.warning(
                "card transfer admin notification failed request=%s admin=%s: %s",
                request.get("id"), admin_tg_id, exc,
            )


async def _reject_card_transfer_and_notify(
    context, request_id: int, *, admin_tg_id: int, reason: str = ""
) -> dict:
    request = await run_blocking(
        reject_card_transfer_request, request_id,
        admin_tg_id=admin_tg_id, reason=reason,
    )
    if not request:
        raise ValueError("درخواست کارت به کارت پیدا نشد")
    user_text = "❌ درخواست کارت به کارت شما رد شد."
    if str(reason or "").strip():
        user_text += f"\n\nعلت: {html.escape(str(reason).strip())}"
    try:
        await context.bot.send_message(
            chat_id=int(request.get("tg_id") or 0), text=user_text, parse_mode="HTML",
            reply_markup=await main_menu_keyboard(int(request.get("tg_id") or 0)),
        )
    except Exception as exc:
        logger.warning(
            "card transfer rejection notification failed request=%s: %s", request_id, exc
        )
    return request


async def _approve_card_transfer_request(
    context, request_id: int, *, admin_tg_id: int, admin_message
) -> dict:
    claimed = await run_blocking(
        claim_card_transfer_request, request_id, admin_tg_id=admin_tg_id
    )
    if not claimed:
        raise ValueError("درخواست کارت به کارت پیدا نشد")
    status = str(claimed.get("status") or "")
    if status == "approved":
        return claimed
    if status != "processing":
        raise ValueError("این درخواست دیگر قابل تأیید نیست")
    authority = str(claimed.get("authority") or "")
    payload = dict(claimed.get("payload") or {})
    order_id = str(payload.get("order_id") or "")
    if not authority or not order_id:
        raise RuntimeError("اطلاعات پایدار سفارش کارت به کارت ناقص است")
    async with ORDER_LOCKS.hold(order_id):
        fresh_request = await run_blocking(get_card_transfer_request, request_id)
        if str(fresh_request.get("status") or "") == "approved":
            return fresh_request
        if str(fresh_request.get("status") or "") != "processing":
            raise ValueError("وضعیت درخواست هنگام تأیید تغییر کرده است")
        pending = await run_blocking(get_pending, authority)
        if not pending:
            raise RuntimeError("سفارش کارت به کارت برای تحویل پیدا نشد")
        pending = await run_blocking(
            authorize_pending_payment, authority,
            method="card_transfer", admin_tg_id=admin_tg_id,
        )
        delivered = await _deliver_verified_pending_unlocked(
            authority, pending, admin_message, context,
            delivery_chat_id=int(pending.get("tg_id") or 0),
            remove_pending=False,
            delivery_prefix="✅ <b>رسید کارت به کارت شما تأیید شد.</b>",
        )
        if not delivered:
            raise RuntimeError("تحویل انجام شد اما ثبت نهایی مالی کامل نشد؛ درخواست برای بازیابی محفوظ است")
        completed = await run_blocking(
            complete_card_transfer_request, request_id, admin_tg_id=admin_tg_id
        )
        if str(completed.get("status") or "") != "approved":
            raise RuntimeError("ثبت نهایی تأیید کارت به کارت کامل نشد")
        return completed



def admin_user_detail_keyboard(summary: dict):
    tg_id = int(summary.get("tg_id") or 0)
    rows = [
        [InlineKeyboardButton("💰 مدیریت کیف پول", callback_data=f"admin_wallet_user|{tg_id}")],
        [InlineKeyboardButton("🧾 تراکنش‌های این کاربر", callback_data=f"admin_user_tx|{tg_id}|0")],
    ]
    for account in (summary.get("accounts") or [])[:90]:
        service = str(account.get("service") or "")
        identifier = str(account.get("identifier") or "")
        if service in SERVICE_LABEL and identifier:
            icon = "🔵" if service == "openvpn" else "🟣"
            rows.append([InlineKeyboardButton(
                f"{icon} {identifier}"[:64],
                callback_data=f"admacc_ref|{service}|{tg_id}|{account_ref(identifier)}",
            )])
    rows.append([InlineKeyboardButton("🔙 کاربران", callback_data="admin_users_menu")])
    return InlineKeyboardMarkup(rows)


async def show_admin_user_detail(message, admin_tg_id: int, user_tg_id: int):
    if not is_admin(admin_tg_id):
        return
    summary = await run_blocking(get_user_admin_summary, user_tg_id)
    if not summary:
        await message.edit_text(
            "❌ کاربر پیدا نشد.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 کاربران", callback_data="admin_users_menu")]]),
        )
        return
    profile = summary.get("profile") or {}
    text = (
        f"👤 <b>{html.escape(str(summary.get('label') or user_tg_id))}</b>\n"
        f"Telegram ID: <code>{user_tg_id}</code>\n"
    )
    if profile.get("phone_number"):
        text += f"📞 <code>{html.escape(str(profile['phone_number']))}</code>\n"
    if profile.get("email"):
        text += f"✉️ <code>{html.escape(str(profile['email']))}</code>\n"
    balance = int(summary.get("balance_toman") or 0)
    reserved = int(summary.get("reserved_toman") or 0)
    text += (
        f"\n💰 کیف پول: <b>{balance:,} تومان</b>"
        + (f" (رزرو: {reserved:,})" if reserved else "") + "\n"
        f"🛒 خریدهای موفق: <b>{int(summary.get('purchase_count') or 0)}</b>\n"
        f"🧾 تراکنش‌ها: <b>{int(summary.get('transaction_count') or 0)}</b>\n"
        f"🔐 اکانت‌ها: <b>{len(summary.get('accounts') or [])}</b>\n"
    )
    ref = summary.get("referral") or {}
    if ref.get("code"):
        text += f"🎁 کد معرف: <code>{html.escape(str(ref.get('code')))}</code>\n"
    if ref.get("used_code"):
        text += f"🎟 کد استفاده‌شده: <code>{html.escape(str(ref.get('used_code')))}</code>\n"
    if summary.get("accounts"):
        text += "\n<b>اکانت‌ها:</b>\n"
        shown = 0
        for a in summary["accounts"]:
            identifier = str(a.get("identifier") or "")
            block = (
                f"• {html.escape(SERVICE_LABEL.get(str(a.get('service')), str(a.get('service'))))}: "
                f"<code>{html.escape(identifier[:256])}</code>"
                f"{' (تست)' if a.get('is_test') else ''}\n"
            )
            if len(text) + len(block) > 3600:
                break
            text += block
            shown += 1
        omitted = len(summary["accounts"]) - shown
        if omitted:
            text += f"<i>{omitted} اکانت دیگر برای حفظ محدودیت پیام تلگرام نمایش داده نشد.</i>"
    await message.edit_text(text, parse_mode="HTML", reply_markup=admin_user_detail_keyboard(summary), disable_web_page_preview=True)


def admin_user_tx_keyboard(tg_id: int, page: int, total: int, page_size: int = 5):
    rows = []
    nav = []
    if (page + 1) * page_size < total:
        nav.append(InlineKeyboardButton("⬅️ قدیمی‌تر", callback_data=f"admin_user_tx|{tg_id}|{page+1}"))
    if page > 0:
        nav.append(InlineKeyboardButton("جدیدتر ➡️", callback_data=f"admin_user_tx|{tg_id}|{page-1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("🔙 بازگشت به کاربر", callback_data=f"admin_user|{tg_id}")])
    return InlineKeyboardMarkup(rows)


async def show_admin_user_transactions(message, admin_tg_id: int, user_tg_id: int, page: int = 0):
    if not is_admin(admin_tg_id):
        return
    page = max(int(page or 0), 0)
    page_size = 5
    rows, total = await run_blocking(list_user_transactions, user_tg_id, offset=page*page_size, limit=page_size)
    max_page = max((total-1)//page_size, 0) if total else 0
    page = min(page, max_page)
    if page > 0 and not rows:
        rows, total = await run_blocking(list_user_transactions, user_tg_id, offset=page*page_size, limit=page_size)
    text = f"🧾 <b>تراکنش‌های کاربر</b> <code>{user_tg_id}</code>\nصفحه {page+1} از {max_page+1 if total else 1}\n\n"
    if not rows:
        text += "تراکنشی ثبت نشده است."
    for tx in rows:
        action = "خرید" if tx.get("action") == "buy" else "تمدید"
        amount = max(int(tx.get("base_price_toman") or 0)-int(tx.get("referral_discount_toman") or 0), 0)
        text += f"• {action} {html.escape(SERVICE_LABEL.get(str(tx.get('service')), str(tx.get('service'))))} — <b>{amount:,} تومان</b> — {_format_tx_time(str(tx.get('created_at') or ''))}\n"
    await message.edit_text(text, parse_mode="HTML", reply_markup=admin_user_tx_keyboard(user_tg_id, page, total, page_size))


def admin_audit_keyboard(page: int, total: int, page_size: int = 10):
    rows = []
    nav = []
    if (page+1)*page_size < total:
        nav.append(InlineKeyboardButton("⬅️ قدیمی‌تر", callback_data=f"admin_audit|{page+1}"))
    if page > 0:
        nav.append(InlineKeyboardButton("جدیدتر ➡️", callback_data=f"admin_audit|{page-1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("🔙 سیستم و سرورها", callback_data="admin_system_menu")])
    return InlineKeyboardMarkup(rows)


async def show_admin_audit(message, admin_tg_id: int, page: int = 0):
    if not is_admin(admin_tg_id):
        return
    page = max(int(page or 0), 0)
    size = 10
    rows, total = await run_blocking(list_admin_audit, offset=page*size, limit=size)
    max_page = max((total-1)//size, 0) if total else 0
    if page > max_page:
        page = max_page
        rows, total = await run_blocking(list_admin_audit, offset=page*size, limit=size)
    text = f"📜 <b>Audit Log</b> — صفحه {page+1} از {max_page+1 if total else 1}\n\n"
    if not rows:
        text += "هنوز عملیات مدیریتی ثبت نشده است."
    for row in rows:
        text += (
            f"• <b>{html.escape(str(row.get('action') or '-'))}</b>\n"
            f"Admin: <code>{int(row.get('admin_tg_id') or 0)}</code>"
            + (f" | User: <code>{int(row.get('target_tg_id') or 0)}</code>" if int(row.get('target_tg_id') or 0) else "")
            + f"\n🕒 {_format_tx_time(str(row.get('created_at') or ''))}\n\n"
        )
    await message.edit_text(text, parse_mode="HTML", reply_markup=admin_audit_keyboard(page, total, size))


def _human_duration(seconds: float) -> str:
    seconds = max(int(seconds or 0), 0)
    if seconds >= 86400:
        return f"{seconds//86400} روز"
    if seconds >= 3600:
        return f"{seconds//3600} ساعت"
    if seconds >= 60:
        return f"{seconds//60} دقیقه"
    return f"{seconds} ثانیه"


async def live_health_snapshot():
    async def mt():
        started = time.perf_counter()
        try:
            await run_blocking_retry(mikrotik.healthcheck)
            RUNTIME.set_service_health("mikrotik", True, "RouterOS API", time.perf_counter()-started)
        except Exception as exc:
            RUNTIME.set_service_health("mikrotik", False, str(exc), time.perf_counter()-started)
    async def xui_probe():
        started = time.perf_counter()
        try:
            obj = (await run_blocking_retry(XUIClient().healthcheck)).get("obj") or []
            RUNTIME.set_service_health("xui", True, f"{len(obj)} inbounds", time.perf_counter()-started)
        except Exception as exc:
            RUNTIME.set_service_health("xui", False, str(exc), time.perf_counter()-started)
    await asyncio.gather(mt(), xui_probe())
    snap = RUNTIME.snapshot()
    snap["executor_lanes"] = {name: lane.snapshot() for name, lane in BLOCKING_LANES.items()}
    return snap, await run_blocking(database_stats)


async def show_incomplete_fulfillments(message, admin_tg_id: int):
    if not is_admin(admin_tg_id):
        return
    rows = await run_blocking(list_incomplete_fulfillments, None, 20)
    text = "⚠️ <b>تحویل‌های تکمیل‌نشده</b>\n\n"
    keyboard = []
    if not rows:
        text += "موردی وجود ندارد."
    shown = 0
    for item in rows:
        order_id = str(item.get("order_id") or "")
        tg_id = int(item.get("tg_id") or 0)
        service = str(item.get("service") or "")
        action = "تمدید" if item.get("action") == "renew" else "خرید"
        state = str(item.get("state") or "")
        ident = str(item.get("delivery_identifier") or item.get("requested_identifier") or "")
        block = (
            f"• <code>{html.escape(order_id)}</code>\n"
            f"User: <code>{tg_id}</code> | {html.escape(SERVICE_LABEL.get(service, service))} | {action}\n"
            f"State: <b>{html.escape(state)}</b>"
            + (f" | <code>{html.escape(ident)}</code>" if ident else "") + "\n\n"
        )
        if len(text) + len(block) > 3600:
            break
        text += block
        shown += 1
        if tg_id:
            keyboard.append([InlineKeyboardButton(f"👤 کاربر {tg_id}", callback_data=f"admin_user|{tg_id}")])
    if shown < len(rows):
        text += f"<i>{len(rows) - shown} مورد دیگر برای حفظ محدودیت پیام تلگرام نمایش داده نشد.</i>"
    keyboard.append([InlineKeyboardButton("🔙 سیستم و سرورها", callback_data="admin_system_menu")])
    await message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard))


async def show_admin_health(message, admin_tg_id: int, *, live: bool = False):
    if not is_admin(admin_tg_id):
        return
    if live:
        snap, db = await live_health_snapshot()
    else:
        snap, db = RUNTIME.snapshot(), await run_blocking(database_stats, check_integrity=False)
    services = snap.get("service_health") or {}
    def svc(name):
        row = services.get(name) or {}
        if not row:
            return "⚪ هنوز تست نشده"
        return ("✅ " if row.get("ok") else "❌ ") + html.escape(str(row.get("detail") or "")) + f" ({float(row.get('seconds') or 0):.2f}s)"
    text = (
        "🩺 <b>وضعیت ربات</b>\n\n"
        f"Uptime: <b>{_human_duration(snap.get('uptime_seconds',0))}</b>\n"
        f"Heartbeat age: <b>{float(snap.get('heartbeat_age_seconds',0)):.1f}s</b>\n"
        f"آخرین Update: <b>{float(snap.get('last_update_age_seconds',0)):.1f}s</b>\n"
        f"در حال پردازش: <b>{int(snap.get('in_flight',0))}</b>\n"
        f"کل Updateها: <b>{int(snap.get('total_updates',0)):,}</b>\n\n"
        f"RouterOS: {svc('mikrotik')}\n"
        f"3x-ui: {svc('xui')}\n"
        f"SQLite: {'✅ آماده' if db.get('quick_check') == 'not_checked' else ('✅ ok' if db.get('quick_check') == 'ok' else '❌ ' + html.escape(str(db.get('quick_check'))))}\n"
        f"DB size: <b>{human_bytes(int(db.get('size_bytes') or 0))}</b>\n"
        f"تحویل‌های تکمیل‌نشده: <b>{int(db.get('incomplete_fulfillments') or 0)}</b>"
        + (f" | ⚠️ نامشخص: <b>{int(db.get('uncertain_fulfillments') or 0)}</b>" if int(db.get('uncertain_fulfillments') or 0) else "")
        + "\n"
    )
    slow = snap.get("slow_operations") or []
    if slow:
        last = slow[-1]
        text += f"\nآخرین عملیات کند: <code>{html.escape(str(last.get('name') or ''))}</code> — {float(last.get('seconds') or 0):.2f}s"
    lanes = snap.get("executor_lanes") or {name: lane.snapshot() for name, lane in BLOCKING_LANES.items()}
    if lanes:
        lane_text = []
        for name in ("mikrotik", "xui", "zarinpal", "db", "backup"):
            row = lanes.get(name) or {}
            lane_text.append(
                f"{name}: {int(row.get('active') or 0)}/{int(row.get('capacity') or 0)}"
                + (f" ردشده={int(row.get('rejected') or 0)}" if int(row.get('rejected') or 0) else "")
            )
        text += "\n\n<b>صف‌های مستقل:</b>\n<code>" + html.escape("\n".join(lane_text)) + "</code>"
    health_rows = [[InlineKeyboardButton("🔄 تست زنده", callback_data="admin_health_live")]]
    if int(db.get("incomplete_fulfillments") or 0):
        health_rows.append([InlineKeyboardButton("⚠️ تحویل‌های تکمیل‌نشده", callback_data="admin_fulfillments")])
    health_rows.append([InlineKeyboardButton("🔙 سیستم و سرورها", callback_data="admin_system_menu")])
    markup = InlineKeyboardMarkup(health_rows)
    await message.edit_text(text, parse_mode="HTML", reply_markup=markup)


async def show_wallet(message, tg_id: int):
    if is_reseller(tg_id):
        await show_reseller_debt(message, tg_id)
        return
    balance, reserved, latest = await asyncio.gather(
        run_blocking(wallet_balance, tg_id),
        run_blocking(reserved_wallet_for_user, tg_id),
        run_blocking(latest_pending_for_user, tg_id),
    )
    _, pending = latest
    available = max(int(balance) - int(reserved), 0)
    text = (
        "💰 <b>کیف پول</b>\n\n"
        f"موجودی کل: <b>{balance:,} تومان</b>\n"
    )
    if reserved:
        text += f"رزرو سفارش‌های در انتظار: <b>{reserved:,} تومان</b>\nموجودی قابل استفاده: <b>{available:,} تومان</b>\n"
    if pending and not reserved:
        text += "سفارش در حال تکمیل: <b>۱ مورد</b>\n"
    text += "\nکیف پول امکان شارژ دستی ندارد و اعتبار آن فقط از طریق دعوت دوستان اضافه می‌شود."
    if pending:
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton(
                "✅ ادامه تحویل سفارش" if _pending_is_local(pending) else "✅ بررسی آخرین پرداخت",
                callback_data="payment|check",
            )],
            [InlineKeyboardButton("❌ لغو آخرین سفارش", callback_data="payment|cancel")],
            [InlineKeyboardButton("🔙 بازگشت", callback_data="home")],
        ])
    else:
        markup = wallet_keyboard()
    await message.edit_text(text, parse_mode="HTML", reply_markup=markup)


async def show_referral(message, tg_id: int):
    if is_reseller(tg_id):
        await show_reseller_debt(message, tg_id)
        return
    if not (await run_blocking(has_completed_purchase, tg_id)):
        await message.edit_text(
            "🔒 این بخش بعد از اولین خرید موفق برای شما فعال می‌شود.\nاکانت تست جزو خرید محسوب نمی‌شود.",
            reply_markup=wallet_keyboard(),
        )
        return
    code = (await run_blocking(get_or_create_referral_code, tg_id))
    balance = (await run_blocking(wallet_balance, tg_id))
    text = (
        "🎁 <b>دعوت دوستان و دریافت اکانت رایگان</b>\n\n"
        "کد معرف اختصاصی شما:\n"
        f"{text_code_block(code)}"
        f"دوستان شما اگر <b>اولین خرید</b> خود را با این کد انجام دهند، <b>{current_referral_discount_percent()}٪ تخفیف</b> می‌گیرند.\n"
        f"بعد از خرید موفق دوست شما، معادل <b>{current_referral_reward_percent()}٪ مبلغ اصلی بسته</b> به کیف پول شما اضافه می‌شود.\n\n"
        f"💰 موجودی فعلی کیف پول: <b>{balance:,} تومان</b>"
    )
    await message.edit_text(text, parse_mode="HTML", reply_markup=referral_keyboard(code))


async def show_reseller_debt(message, tg_id: int):
    reseller = reseller_record(tg_id)
    if not reseller:
        await message.edit_text(
            "⚠️ دسترسی ریسلر برای این حساب فعال نیست.",
            reply_markup=await main_menu_keyboard(tg_id),
        )
        return
    rate = int(reseller.get("price_per_gb_toman") or 0)
    debt = int(reseller.get("debt_toman") or 0)
    text = (
        "📒 <b>بدهی ریسلر</b>\n\n"
        f"نام: <b>{html.escape(str(reseller.get('name') or ''))}</b>\n"
        f"هزینه هر گیگ: <b>{rate:,} تومان</b>\n"
        f"بدهی فعلی: <b>{debt:,} تومان</b>"
    )
    if rate <= 0:
        text += "\n\n⚠️ هزینه هر گیگ هنوز توسط مدیر تنظیم نشده و خرید امکان‌پذیر نیست."
    await message.edit_text(
        text, parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🔙 بازگشت", callback_data="home")
        ]]),
    )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    schedule_telegram_profile(context, update.effective_user)
    context.user_data.clear()
    text = welcome_text()
    if update.message:
        await update.message.reply_text(text, reply_markup=await main_menu_keyboard(update.effective_user.id))
    else:
        q = update.callback_query
        await safe_callback_answer(q)
        await q.message.edit_text(text, reply_markup=await main_menu_keyboard(q.from_user.id))


async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    data = q.data or ""
    try:
        allowed, limit_message = CALLBACK_LIMITER.allow(q.from_user.id, data)
        if not allowed:
            await safe_callback_answer(q, limit_message, show_alert=False)
            return
        if (
            not is_admin(q.from_user.id)
            and current_maintenance_mode()
            and _maintenance_blocks_callback(data)
        ):
            await safe_callback_answer(q, MAINTENANCE_MESSAGE, show_alert=True)
            return
        schedule_telegram_profile(context, q.from_user)
        await safe_callback_answer(q)
        parts = data.split("|")
    except ServiceBusyError as exc:
        logger.warning(
            "callback preamble busy tg_id=%s data=%s error=%s",
            getattr(q.from_user, "id", 0), data[:120], exc,
        )
        await safe_callback_answer(q, str(exc), show_alert=False)
        return
    except Exception:
        logger.exception(
            "callback preamble error tg_id=%s data=%s",
            getattr(q.from_user, "id", 0), data[:120],
        )
        await callback_failure_reply(q, getattr(q.from_user, "id", 0))
        return

    try:
        if data == "home":
            context.user_data.clear()
            await q.message.edit_text(welcome_text(), reply_markup=await main_menu_keyboard(q.from_user.id))
            return

        if data in {"openvpn_connections", "openvpn_connections_url"}:
            current_url = str(get_app_setting("openvpn_connections_url", "0") or "0")
            parsed = urlsplit(current_url if current_url != "0" else "")
            if parsed.scheme not in {"http", "https"} or not parsed.netloc:
                await q.message.edit_text(
                    "⚠️ لینک راهنمای OpenVPN در حال حاضر غیرفعال است.",
                    reply_markup=back_service("openvpn"),
                )
                return
            await q.message.edit_text(
                "⬇️ دریافت کانکشن‌های OpenVPN",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("باز کردن لینک", url=current_url)],
                    [InlineKeyboardButton("🔙 بازگشت", callback_data="svc|openvpn")],
                ]),
            )
            return

        if parts[0] == "paymethod" and len(parts) == 2:
            method = parts[1]
            order = dict(context.user_data.get("payment_method_order") or {})
            if method not in {"zarinpal", "card_transfer"} or not order:
                await safe_edit_text(
                    q.message,
                    "⚠️ اطلاعات انتخاب پرداخت منقضی شده است؛ بسته را دوباره انتخاب کنید.",
                    reply_markup=await main_menu_keyboard(q.from_user.id),
                )
                return
            if not payment_gateway_enabled(method):
                # A stale button must never enter a gateway disabled by Admin.
                await start_order(
                    q, context, str(order.get("service") or ""),
                    str(order.get("action") or ""), str(order.get("plan_key") or ""),
                    str(order.get("identifier") or ""),
                    referral_code=str(order.get("referral_code") or ""),
                    referrer_tg_id=int(order.get("referrer_tg_id") or 0),
                )
                return
            await start_order(
                q, context, str(order.get("service") or ""),
                str(order.get("action") or ""), str(order.get("plan_key") or ""),
                str(order.get("identifier") or ""),
                referral_code=str(order.get("referral_code") or ""),
                referrer_tg_id=int(order.get("referrer_tg_id") or 0),
                payment_method=method,
            )
            return

        if parts[0] == "cardpay" and len(parts) == 3 and parts[1] == "cancel":
            request_id = int(parts[2])
            try:
                cancelled = await run_blocking(
                    cancel_card_transfer_request, request_id, tg_id=q.from_user.id
                )
            except ValueError as exc:
                await safe_callback_answer(q, str(exc), show_alert=True)
                return
            if not cancelled:
                await safe_callback_answer(q, "درخواست پیدا نشد.", show_alert=True)
                return
            context.user_data.clear()
            await safe_edit_text(
                q.message, "✅ درخواست کارت به کارت لغو شد و رزرو کیف پول آزاد گردید.",
                reply_markup=await main_menu_keyboard(q.from_user.id),
            )
            return

        if data == "admin_tools":
            if not is_admin(q.from_user.id):
                return
            context.user_data.clear()
            await show_admin_tools(q.message, q.from_user.id)
            return

        if data == "admin_users_menu":
            if not is_admin(q.from_user.id):
                return
            context.user_data.clear()
            await show_admin_users_menu(q.message, q.from_user.id)
            return

        if data == "admin_payments_menu":
            if not is_admin(q.from_user.id):
                return
            context.user_data.clear()
            await show_admin_payments_menu(q.message, q.from_user.id)
            return

        if data == "admin_reports_menu":
            if not is_admin(q.from_user.id):
                return
            context.user_data.clear()
            await show_admin_reports_menu(q.message, q.from_user.id)
            return

        if data == "admin_dashboard":
            if not is_admin(q.from_user.id):
                return
            await show_admin_dashboard(q.message, q.from_user.id)
            return

        if data == "admin_referral_report":
            if not is_admin(q.from_user.id):
                return
            await show_admin_referral_report(q.message, q.from_user.id)
            return

        if data == "admin_system_menu":
            if not is_admin(q.from_user.id):
                return
            context.user_data.clear()
            await show_admin_system_menu(q.message, q.from_user.id)
            return

        if data == "admin_database":
            if not is_admin(q.from_user.id):
                return
            await show_admin_database(q.message, q.from_user.id)
            return

        if data == "admin_settings_menu":
            if not is_admin(q.from_user.id):
                return
            context.user_data.clear()
            await show_admin_settings_menu(q.message, q.from_user.id)
            return

        if data == "admin_gateways":
            if not is_admin(q.from_user.id):
                return
            context.user_data.pop("awaiting", None)
            context.user_data.pop("admin_config_edit", None)
            await show_admin_payment_gateways(q.message, q.from_user.id)
            return

        if parts[0] == "admin_gateway_toggle" and len(parts) == 2:
            if not is_admin(q.from_user.id):
                return
            gateway = parts[1]
            if gateway not in {"zarinpal", "card_transfer"}:
                return
            desired = not payment_gateway_enabled(gateway)
            try:
                await run_blocking(
                    set_payment_gateway_enabled, gateway, desired,
                    admin_tg_id=q.from_user.id, _lane="db",
                )
            except ValueError as exc:
                await safe_callback_answer(q, str(exc), show_alert=True)
            await show_admin_config_group(
                q.message, q.from_user.id,
                "zarinpal" if gateway == "zarinpal" else "card",
            )
            return

        if parts[0] == "admin_cfg" and len(parts) == 2:
            if not is_admin(q.from_user.id):
                return
            context.user_data.pop("awaiting", None)
            context.user_data.pop("admin_config_edit", None)
            await show_admin_config_group(q.message, q.from_user.id, parts[1])
            return

        if parts[0] == "admin_service_sales" and len(parts) == 2:
            if not is_admin(q.from_user.id):
                return
            service = parts[1]
            if service not in {"openvpn", "v2ray"}:
                return
            desired = not service_sales_enabled(service)
            try:
                await run_blocking(
                    set_service_sales_enabled, service, desired,
                    admin_tg_id=q.from_user.id, _lane="db",
                )
            except ValueError as exc:
                await q.message.reply_text(f"⚠️ {html.escape(str(exc))}", parse_mode="HTML")
            await show_admin_config_group(
                q.message, q.from_user.id,
                "mikrotik" if service == "openvpn" else "xui",
            )
            return

        if data in {"admin_resellers", "admin_admins"}:
            if not is_admin(q.from_user.id):
                return
            context.user_data.pop("awaiting", None)
            context.user_data.pop("reseller_draft", None)
            context.user_data.pop("reseller_debt_edit", None)
            await show_admin_resellers(q.message, q.from_user.id)
            return

        if data == "admin_root_info":
            if not is_admin(q.from_user.id):
                return
            await safe_callback_answer(q, "مدیر اصلی از ADMIN_IDS ENV خوانده می‌شود و قابل حذف نیست.", show_alert=True)
            return

        if parts[0] == "rs" and len(parts) == 2:
            if not is_admin(q.from_user.id):
                return
            context.user_data.pop("awaiting", None)
            await show_admin_reseller_detail(q.message, q.from_user.id, int(parts[1]))
            return

        if data == "rsadd":
            if not is_admin(q.from_user.id):
                return
            context.user_data["reseller_draft"] = {}
            context.user_data["awaiting"] = {"kind": "reseller_add_name"}
            await q.message.edit_text(
                "➕ <b>افزودن ریسلر</b>\n\nنام ریسلر را ارسال کنید:",
                parse_mode="HTML", reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("❌ انصراف", callback_data="admin_resellers")
                ]]),
            )
            return

        if data == "rsaddok":
            if not is_admin(q.from_user.id):
                return
            draft = dict(context.user_data.get("reseller_draft") or {})
            if not {"name", "tg_id", "price_per_gb_toman", "trial_enabled"}.issubset(draft):
                await show_admin_resellers(q.message, q.from_user.id)
                return
            try:
                await run_blocking(
                    add_reseller, name=draft["name"], tg_id=int(draft["tg_id"]),
                    price_per_gb_toman=int(draft["price_per_gb_toman"]),
                    trial_enabled=bool(draft["trial_enabled"]),
                    admin_tg_id=q.from_user.id, _lane="db",
                )
            except ValueError as exc:
                await safe_callback_answer(q, str(exc), show_alert=True)
                return
            context.user_data.pop("reseller_draft", None)
            context.user_data.pop("awaiting", None)
            await show_admin_resellers(q.message, q.from_user.id)
            return

        if parts[0] == "rsaddtrial" and len(parts) == 2:
            if not is_admin(q.from_user.id):
                return
            if parts[1] not in {"0", "1"}:
                await show_admin_resellers(q.message, q.from_user.id)
                return
            draft = dict(context.user_data.get("reseller_draft") or {})
            if not {"name", "tg_id", "price_per_gb_toman"}.issubset(draft):
                await show_admin_resellers(q.message, q.from_user.id)
                return
            draft["trial_enabled"] = parts[1] == "1"
            context.user_data["reseller_draft"] = draft
            context.user_data.pop("awaiting", None)
            await q.message.edit_text(
                "⚠️ <b>تأیید افزودن ریسلر</b>\n\n"
                f"نام: <b>{html.escape(str(draft['name']))}</b>\n"
                f"Telegram ID: <code>{int(draft['tg_id'])}</code>\n"
                f"هزینه هر گیگ: <b>{int(draft['price_per_gb_toman']):,} تومان</b>\n"
                f"اکانت تست: <b>{'فعال ✅' if draft['trial_enabled'] else 'غیرفعال ⛔'}</b>",
                parse_mode="HTML", reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ افزودن ریسلر", callback_data="rsaddok")],
                    [InlineKeyboardButton("❌ انصراف", callback_data="admin_resellers")],
                ]),
            )
            return

        if parts[0] == "rsedit" and len(parts) == 3:
            if not is_admin(q.from_user.id):
                return
            reseller_id = int(parts[1])
            field = parts[2]
            reseller = _runtime_reseller_by_id(reseller_id)
            if not reseller or field not in {"n", "i", "p"}:
                await show_admin_resellers(q.message, q.from_user.id)
                return
            prompts = {
                "n": "نام جدید ریسلر را ارسال کنید:",
                "i": "Telegram ID عددی جدید ریسلر را ارسال کنید:",
                "p": "هزینه جدید هر گیگ را به تومان و فقط به صورت عدد ارسال کنید:",
            }
            context.user_data["awaiting"] = {
                "kind": "reseller_edit", "reseller_id": reseller_id, "field": field,
            }
            await q.message.edit_text(
                "✏️ <b>ویرایش ریسلر</b>\n\n" + prompts[field],
                parse_mode="HTML", reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("❌ انصراف", callback_data=f"rs|{reseller_id}")
                ]]),
            )
            return

        if parts[0] == "rstrial" and len(parts) == 3:
            if not is_admin(q.from_user.id):
                return
            reseller_id = int(parts[1])
            if parts[2] not in {"0", "1"} or not _runtime_reseller_by_id(reseller_id):
                await show_admin_resellers(q.message, q.from_user.id)
                return
            await run_blocking(
                edit_reseller, reseller_id, trial_enabled=parts[2] == "1",
                admin_tg_id=q.from_user.id, _lane="db",
            )
            await show_admin_reseller_detail(q.message, q.from_user.id, reseller_id)
            return

        if parts[0] == "rsdebt" and len(parts) == 2:
            if not is_admin(q.from_user.id):
                return
            context.user_data.pop("awaiting", None)
            await show_admin_reseller_debt(q.message, q.from_user.id, int(parts[1]))
            return

        if parts[0] == "rsset" and len(parts) == 2:
            if not is_admin(q.from_user.id):
                return
            reseller_id = int(parts[1])
            if not _runtime_reseller_by_id(reseller_id):
                await show_admin_resellers(q.message, q.from_user.id)
                return
            context.user_data["awaiting"] = {
                "kind": "reseller_set_debt", "reseller_id": reseller_id,
            }
            await q.message.edit_text(
                "📒 <b>تغییر بدهی</b>\n\nمبلغ جدید بدهی را به تومان ارسال کنید؛ عدد 0 نیز مجاز است:",
                parse_mode="HTML", reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("❌ انصراف", callback_data=f"rsdebt|{reseller_id}")
                ]]),
            )
            return

        if parts[0] == "rssetok" and len(parts) == 2:
            if not is_admin(q.from_user.id):
                return
            reseller_id = int(parts[1])
            edit_data = dict(context.user_data.get("reseller_debt_edit") or {})
            if int(edit_data.get("reseller_id") or 0) != reseller_id:
                await show_admin_reseller_debt(q.message, q.from_user.id, reseller_id)
                return
            operation_id = f"rs-set:{reseller_id}:{getattr(q.message, 'chat_id', 0)}:{getattr(q.message, 'message_id', 0)}"
            try:
                await run_blocking(
                    change_reseller_debt, reseller_id, int(edit_data.get("debt_toman") or 0),
                    admin_tg_id=q.from_user.id, operation_id=operation_id, _lane="db",
                )
            except ValueError as exc:
                await safe_callback_answer(q, str(exc), show_alert=True)
                return
            context.user_data.pop("reseller_debt_edit", None)
            await show_admin_reseller_debt(q.message, q.from_user.id, reseller_id)
            return

        if parts[0] == "rszero" and len(parts) == 2:
            if not is_admin(q.from_user.id):
                return
            reseller_id = int(parts[1])
            reseller = _runtime_reseller_by_id(reseller_id)
            if not reseller:
                await show_admin_resellers(q.message, q.from_user.id)
                return
            await q.message.edit_text(
                f"⚠️ بدهی <b>{html.escape(str(reseller.get('name') or ''))}</b> از "
                f"<b>{int(reseller.get('debt_toman') or 0):,} تومان</b> به صفر تغییر کند؟",
                parse_mode="HTML", reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ بله، صفر شود", callback_data=f"rszerook|{reseller_id}")],
                    [InlineKeyboardButton("❌ انصراف", callback_data=f"rsdebt|{reseller_id}")],
                ]),
            )
            return

        if parts[0] == "rszerook" and len(parts) == 2:
            if not is_admin(q.from_user.id):
                return
            reseller_id = int(parts[1])
            operation_id = f"rs-zero:{reseller_id}:{getattr(q.message, 'chat_id', 0)}:{getattr(q.message, 'message_id', 0)}"
            await run_blocking(
                change_reseller_debt, reseller_id, 0, admin_tg_id=q.from_user.id,
                operation_id=operation_id, _lane="db",
            )
            await show_admin_reseller_debt(q.message, q.from_user.id, reseller_id)
            return

        if parts[0] == "rsdel" and len(parts) == 2:
            if not is_admin(q.from_user.id):
                return
            reseller_id = int(parts[1])
            reseller = _runtime_reseller_by_id(reseller_id)
            if not reseller:
                await show_admin_resellers(q.message, q.from_user.id)
                return
            debt = int(reseller.get("debt_toman") or 0)
            await q.message.edit_text(
                "⚠️ <b>حذف ریسلر</b>\n\n"
                f"{html.escape(str(reseller.get('name') or ''))} — <code>{int(reseller.get('tg_id') or 0)}</code>\n"
                f"بدهی ثبت‌شده: <b>{debt:,} تومان</b>\n\n"
                "دسترسی ریسلر فوراً حذف می‌شود؛ سابقه بدهی در دیتابیس باقی می‌ماند.",
                parse_mode="HTML", reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ بله، حذف شود", callback_data=f"rsdelok|{reseller_id}")],
                    [InlineKeyboardButton("❌ انصراف", callback_data=f"rs|{reseller_id}")],
                ]),
            )
            return

        if parts[0] == "rsdelok" and len(parts) == 2:
            if not is_admin(q.from_user.id):
                return
            await run_blocking(
                remove_reseller, int(parts[1]), admin_tg_id=q.from_user.id, _lane="db"
            )
            await show_admin_resellers(q.message, q.from_user.id)
            return

        if parts[0] == "admin_cfg_edit" and len(parts) == 2:
            if not is_admin(q.from_user.id):
                return
            short_key = parts[1]
            field = _ADMIN_CONFIG_FIELDS.get(short_key)
            if not field:
                return
            key, label, group = field
            if short_key in {"umscheme", "xuscheme", "xutls", "zpsandbox"}:
                await q.message.edit_text(
                    f"<b>{html.escape(label)}</b>\n\nگزینهٔ موردنظر را انتخاب کنید:",
                    parse_mode="HTML", reply_markup=_config_choice_markup(short_key, group),
                )
                return
            context.user_data["awaiting"] = {
                "kind": "admin_config_value", "short_key": short_key,
                "key": key, "label": label, "group": group,
            }
            hints = {
                "mtuser": "نام کاربری کامل RouterOS را دقیقاً همان‌طور که ساخته‌اید وارد کنید.",
                "ovpnurl": "یک URL کامل HTTP/HTTPS یا 0 برای Disabled وارد کنید.",
                "xusub": "یک URL کامل HTTP/HTTPS یا 0 برای حفظ URL اصلی 3x-ui وارد کنید.",
                "xupath": "مثال: admin که به /admin/ نرمال می‌شود.",
                "umpath": "مثال: um؛ اسلش‌های اضافی خودکار حذف می‌شوند.",
                "cardnum": "شماره کارت 16 رقمی را وارد کنید؛ فاصله و خط تیره خودکار حذف می‌شود.",
                "cardholder": "نامی را وارد کنید که باید کنار شماره کارت به کاربر نمایش داده شود.",
            }
            await q.message.edit_text(
                f"✏️ <b>{html.escape(label)}</b>\n\nمقدار جدید را ارسال کنید.\n{hints.get(short_key, '')}",
                parse_mode="HTML", reply_markup=_admin_config_back(group),
            )
            return

        if parts[0] == "admin_cfg_set" and len(parts) == 3:
            if not is_admin(q.from_user.id):
                return
            short_key, raw_value = parts[1], parts[2]
            field = _ADMIN_CONFIG_FIELDS.get(short_key)
            if not field or short_key not in {"umscheme", "xuscheme", "xutls", "zpsandbox"}:
                return
            key, _label, group = field
            value = raw_value if short_key in {"umscheme", "xuscheme"} else (raw_value == "1")
            await run_blocking(update_setting, key, value, admin_tg_id=q.from_user.id, _lane="db")
            await show_admin_config_group(q.message, q.from_user.id, group)
            return

        if parts[0] == "admin_cfg_confirm" and len(parts) == 2:
            if not is_admin(q.from_user.id):
                return
            short_key = parts[1]
            pending = dict(context.user_data.get("admin_config_edit") or {})
            field = _ADMIN_CONFIG_FIELDS.get(short_key)
            if not field or pending.get("short_key") != short_key or "value" not in pending:
                await show_admin_settings_menu(q.message, q.from_user.id)
                return
            key, _label, group = field
            await run_blocking(
                update_setting, key, pending["value"], admin_tg_id=q.from_user.id, _lane="db"
            )
            context.user_data.pop("admin_config_edit", None)
            context.user_data.pop("awaiting", None)
            await show_admin_config_group(q.message, q.from_user.id, group)
            return

        if data == "admin_inbounds":
            if not is_admin(q.from_user.id):
                return
            context.user_data.pop("awaiting", None)
            await show_admin_inbounds(q.message, q.from_user.id)
            return

        if parts[0] == "admin_inbound" and len(parts) == 2:
            if not is_admin(q.from_user.id):
                return
            await show_admin_inbound_detail(q.message, q.from_user.id, int(parts[1]))
            return

        if data == "admin_inbound_add":
            if not is_admin(q.from_user.id):
                return
            context.user_data["awaiting"] = {"kind": "admin_inbound_add"}
            await q.message.edit_text(
                "➕ <b>Add Inbound</b>\n\nنام دقیق Inbound را ارسال کنید:",
                parse_mode="HTML", reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 Client Inbounds", callback_data="admin_inbounds")
                ]]),
            )
            return

        if parts[0] == "admin_inbound_rename" and len(parts) == 2:
            if not is_admin(q.from_user.id):
                return
            inbound_id = int(parts[1])
            context.user_data["awaiting"] = {"kind": "admin_inbound_rename", "inbound_id": inbound_id}
            await q.message.edit_text(
                "✏️ <b>Rename Inbound</b>\n\nنام جدید را ارسال کنید:",
                parse_mode="HTML", reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 انصراف", callback_data=f"admin_inbound|{inbound_id}")
                ]]),
            )
            return

        if parts[0] == "admin_inbound_rename_confirm" and len(parts) == 2:
            if not is_admin(q.from_user.id):
                return
            inbound_id = int(parts[1])
            pending = dict(context.user_data.get("admin_inbound_edit") or {})
            if int(pending.get("inbound_id") or 0) != inbound_id or not pending.get("remark"):
                await show_admin_inbounds(q.message, q.from_user.id)
                return
            await run_blocking(
                rename_inbound, inbound_id, pending["remark"],
                admin_tg_id=q.from_user.id, _lane="db",
            )
            context.user_data.pop("admin_inbound_edit", None)
            await show_admin_inbound_detail(q.message, q.from_user.id, inbound_id)
            return

        if parts[0] == "admin_inbound_delete" and len(parts) == 2:
            if not is_admin(q.from_user.id):
                return
            inbound_id = int(parts[1])
            found = next((item for item in inbound_records() if item[0] == inbound_id), None)
            if not found:
                await show_admin_inbounds(q.message, q.from_user.id)
                return
            await q.message.edit_text(
                f"⚠️ حذف Inbound زیر را تأیید می‌کنید؟\n\n• {html.escape(found[1])}",
                parse_mode="HTML", reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ بله، حذف شود", callback_data=f"admin_inbound_delete_confirm|{inbound_id}")],
                    [InlineKeyboardButton("❌ انصراف", callback_data=f"admin_inbound|{inbound_id}")],
                ]),
            )
            return

        if parts[0] == "admin_inbound_delete_confirm" and len(parts) == 2:
            if not is_admin(q.from_user.id):
                return
            await run_blocking(
                delete_inbound, int(parts[1]), admin_tg_id=q.from_user.id, _lane="db"
            )
            await show_admin_inbounds(q.message, q.from_user.id)
            return

        if parts[0] == "admin_cfg_test" and len(parts) == 2:
            if not is_admin(q.from_user.id):
                return
            target = parts[1]
            await q.message.edit_text("⏳ در حال اجرای تست غیرمخرب اتصال…")
            if target == "mikrotik":
                result = await run_blocking(mikrotik.test_connection)
                text = (
                    "🧪 <b>MikroTik Test Connection</b>\n\n"
                    f"RouterOS API: {'✅' if result.get('routeros_ok') else '❌'} {html.escape(str(result.get('routeros_detail') or ''))}"
                )
            elif target == "xui":
                result = await run_blocking(XUIClient().test_connection, _lane="xui")
                text = (
                    "🧪 <b>XUI Test Connection</b>\n\n"
                    f"Connectivity/Auth: {'✅' if result.get('connectivity_ok') else '❌'}\n"
                    f"Configured Inbounds: {'✅' if result.get('inbounds_ok') else '❌'}\n"
                    f"{html.escape(str(result.get('detail') or ''))}"
                )
            elif target == "zarinpal":
                result = await run_zarinpal(test_zarinpal_connection)
                text = (
                    "🧪 <b>ZarinPal Test Connection</b>\n\n"
                    f"Configuration: {'✅' if result.get('configured') else '❌'}\n"
                    f"Reachability: {'✅' if result.get('reachable') else '❌'}\n"
                    f"Sandbox: {'Enabled' if result.get('sandbox') else 'Disabled'}\n\n"
                    f"{html.escape(str(result.get('detail') or ''))}\n"
                    "<i>هیچ Payment، Authority، Pending یا تراکنشی ایجاد نشد.</i>"
                )
            else:
                return
            await q.message.edit_text(text, parse_mode="HTML", reply_markup=_admin_config_back(target))
            return

        if data == "admin_backup_settings":
            if not is_admin(q.from_user.id):
                return
            await show_admin_backup_settings(q.message, q.from_user.id)
            return

        if parts[0] == "admin_auto_backup_set" and len(parts) == 2:
            if not is_admin(q.from_user.id):
                return
            desired = parts[1] == "1"
            await run_blocking(
                set_auto_backup_enabled, desired, admin_tg_id=q.from_user.id
            )
            await show_admin_backup_settings(q.message, q.from_user.id)
            return

        if data == "admin_plans" or (parts[0] == "admin_plans" and len(parts) == 2):
            if not is_admin(q.from_user.id):
                return
            context.user_data.pop("awaiting", None)
            context.user_data.pop("admin_plan_draft", None)
            context.user_data.pop("admin_plan_edit", None)
            page = 0
            if len(parts) == 2:
                try:
                    page = max(int(parts[1]), 0)
                except Exception:
                    page = 0
            await show_admin_plans(q.message, q.from_user.id, page)
            return

        if parts[0] == "admin_plans_service" and len(parts) == 3:
            if not is_admin(q.from_user.id):
                return
            service = parts[1]
            if service not in {"openvpn", "v2ray"}:
                await show_admin_plans(q.message, q.from_user.id)
                return
            try:
                page = max(int(parts[2]), 0)
            except Exception:
                page = 0
            context.user_data.pop("awaiting", None)
            await show_admin_service_plans(q.message, q.from_user.id, service, page)
            return

        if data == "admin_trial_view":
            if not is_admin(q.from_user.id):
                return
            context.user_data.pop("awaiting", None)
            context.user_data.pop("admin_trial_edit", None)
            await show_admin_trial_detail(q.message, q.from_user.id)
            return

        if parts[0] == "admin_trial_edit" and len(parts) == 2:
            if not is_admin(q.from_user.id):
                return
            field = parts[1]
            if field not in {"gb", "days", "openvpn_profile"}:
                await show_admin_trial_detail(q.message, q.from_user.id)
                return
            labels = {
                "gb": "حجم جدید تست را به GB ارسال کنید. مثال: 1",
                "days": "مدت جدید تست را به روز ارسال کنید. مثال: 1 یا 3",
                "openvpn_profile": "نام دقیق پکیج تست در MikroTik User Manager را ارسال کنید.",
            }
            context.user_data["awaiting"] = {"kind": "admin_trial_edit", "field": field}
            context.user_data.pop("admin_trial_edit", None)
            await q.message.edit_text(
                "✏️ <b>ویرایش بسته تست</b>\n\n" + labels[field],
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 لغو", callback_data="admin_trial_view")]]),
            )
            return

        if parts[0] == "admin_trial_edit_confirm" and len(parts) == 2:
            if not is_admin(q.from_user.id):
                return
            field = parts[1]
            pending = dict(context.user_data.get("admin_trial_edit") or {})
            if pending.get("field") != field or "value" not in pending:
                await show_admin_trial_detail(q.message, q.from_user.id)
                return
            row = await run_blocking(
                update_trial_plan, field=field, value=pending["value"], admin_tg_id=q.from_user.id
            )
            refresh_test_plan(row)
            context.user_data.pop("admin_trial_edit", None)
            context.user_data.pop("awaiting", None)
            await show_admin_trial_detail(q.message, q.from_user.id)
            return

        if parts[0] == "admin_trial_toggle" and len(parts) == 2:
            if not is_admin(q.from_user.id):
                return
            desired = parts[1] == "1"
            row = await run_blocking(set_trial_plan_enabled, desired, admin_tg_id=q.from_user.id)
            refresh_test_plan(row)
            context.user_data.pop("awaiting", None)
            context.user_data.pop("admin_trial_edit", None)
            await show_admin_trial_detail(q.message, q.from_user.id)
            return

        if parts[0] == "admin_plan_view" and len(parts) == 3:
            if not is_admin(q.from_user.id):
                return
            context.user_data.pop("awaiting", None)
            service = parts[1]
            if service not in {"openvpn", "v2ray"}:
                await show_admin_plans(q.message, q.from_user.id)
                return
            try:
                plan_key = _resolve_admin_plan_ref(service, parts[2])
            except RuntimeError:
                await show_admin_service_plans(q.message, q.from_user.id, service, 0)
                return
            await show_admin_plan_detail(q.message, q.from_user.id, service, plan_key)
            return

        if parts[0] == "admin_plan_add" and len(parts) == 2:
            if not is_admin(q.from_user.id):
                return
            service = parts[1]
            if service not in {"openvpn", "v2ray"} or not service_sales_enabled(service):
                await show_admin_plans(q.message, q.from_user.id)
                return
            draft = {"service": service}
            context.user_data["admin_plan_draft"] = draft
            context.user_data["awaiting"] = {"kind": "admin_plan_add", "step": "gb", "service": service}
            steps = 4 if service == "openvpn" else 3
            await q.message.edit_text(
                f"➕ <b>افزودن بسته {SERVICE_LABEL[service]}</b>\n\n1/{steps} — حجم بسته را به <b>GB</b> ارسال کنید.\nمثال: <code>50</code>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 لغو", callback_data=f"admin_plans_service|{service}|0")]]),
            )
            return

        if data == "admin_plan_add_confirm" or (
            parts[0] == "admin_plan_add_confirm" and len(parts) == 2
        ):
            if not is_admin(q.from_user.id):
                return
            draft = dict(context.user_data.get("admin_plan_draft") or {})
            service = str(draft.get("service") or "")
            if service not in {"openvpn", "v2ray"} or not service_sales_enabled(service):
                await show_admin_plans(q.message, q.from_user.id)
                return
            copy_to_v2ray = data == "admin_plan_add_confirm" or parts[1] == "1"
            if copy_to_v2ray and (
                service != "openvpn" or not service_sales_enabled("v2ray")
            ):
                await q.message.reply_text("⚠️ کپی بسته ممکن نیست؛ فروش V2Ray دیگر فعال نیست.")
                await show_admin_service_plans(q.message, q.from_user.id, service, 0)
                return
            required = {"gb", "months", "price_toman"}
            if service == "openvpn":
                required.add("openvpn_profile")
            if not required.issubset(draft):
                await q.message.edit_text(
                    "⚠️ اطلاعات بسته ناقص است. دوباره بسته را اضافه کنید.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("➕ افزودن بسته", callback_data=f"admin_plan_add|{service}")], [InlineKeyboardButton("🔙 بسته‌ها", callback_data=f"admin_plans_service|{service}|0")]]),
                )
                return
            created = None
            for _ in range(8):
                key = "P" + secrets.token_hex(5).upper()
                try:
                    created = await run_blocking(
                        create_sale_plan,
                        plan_key=key,
                        gb=int(draft["gb"]),
                        months=int(draft["months"]),
                        price_toman=int(draft["price_toman"]),
                        openvpn_profile=str(draft.get("openvpn_profile") or ""),
                        service=service,
                        copy_to_v2ray=copy_to_v2ray,
                        admin_tg_id=q.from_user.id,
                    )
                    break
                except ValueError as exc:
                    if "تکراری" not in str(exc) and "از قبل" not in str(exc):
                        raise
            if not created:
                raise RuntimeError("ساخت شناسه داخلی یکتای بسته ممکن نشد")
            refresh_plans(await run_blocking(list_service_sale_plans), service_aware=True)
            context.user_data.pop("admin_plan_draft", None)
            context.user_data.pop("awaiting", None)
            await q.message.edit_text(
                f"✅ بسته با موفقیت برای <b>{'OpenVPN و V2Ray' if copy_to_v2ray else SERVICE_LABEL[service]}</b> ثبت شد.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📦 مشاهده بسته", callback_data=f"admin_plan_view|{service}|{admin_plan_ref(created['plan_key'], service)}")], [InlineKeyboardButton("🔙 بسته‌ها", callback_data=f"admin_plans_service|{service}|0")]]),
            )
            return

        if parts[0] == "admin_plan_add_restart" and len(parts) == 2:
            if not is_admin(q.from_user.id):
                return
            service = parts[1]
            if service not in {"openvpn", "v2ray"} or not service_sales_enabled(service):
                await show_admin_plans(q.message, q.from_user.id)
                return
            context.user_data.pop("admin_plan_draft", None)
            context.user_data.pop("awaiting", None)
            draft = {"service": service}
            context.user_data["admin_plan_draft"] = draft
            context.user_data["awaiting"] = {"kind": "admin_plan_add", "step": "gb", "service": service}
            await q.message.edit_text(
                f"➕ <b>افزودن بسته {SERVICE_LABEL[service]}</b>\n\n1/{4 if service == 'openvpn' else 3} — حجم بسته را به <b>GB</b> ارسال کنید.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 لغو", callback_data=f"admin_plans_service|{service}|0")]]),
            )
            return

        if parts[0] == "admin_plan_edit" and len(parts) == 4:
            if not is_admin(q.from_user.id):
                return
            service, plan_ref, field = parts[1], parts[2], parts[3]
            if service not in {"openvpn", "v2ray"}:
                await show_admin_plans(q.message, q.from_user.id)
                return
            try:
                plan_key = _resolve_admin_plan_ref(service, plan_ref)
            except RuntimeError:
                await show_admin_service_plans(q.message, q.from_user.id, service, 0)
                return
            plan = plans_for(service).get(plan_key)
            allowed_fields = {"gb", "months", "price_toman"}
            if service == "openvpn":
                allowed_fields.add("openvpn_profile")
            if not plan or field not in allowed_fields:
                await show_admin_service_plans(q.message, q.from_user.id, service, 0)
                return
            labels = {
                "gb": "حجم جدید را به GB ارسال کنید. مثال: 50",
                "months": "مدت جدید را به ماه ارسال کنید. مثال: 3",
                "price_toman": "قیمت جدید را به تومان ارسال کنید. مثال: 450000",
                "openvpn_profile": "نام دقیق پکیج MikroTik را ارسال کنید. حروف بزرگ/کوچک و فاصله‌ها عیناً ذخیره می‌شوند.",
            }
            context.user_data["awaiting"] = {"kind": "admin_plan_edit", "service": service, "plan_key": plan_key, "field": field}
            context.user_data.pop("admin_plan_edit", None)
            await q.message.edit_text(
                "✏️ <b>ویرایش بسته</b>\n\n" + labels[field],
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 لغو", callback_data=f"admin_plan_view|{service}|{plan_ref}")]]),
            )
            return

        if parts[0] == "admin_plan_edit_confirm" and len(parts) == 4:
            if not is_admin(q.from_user.id):
                return
            service, plan_ref, field = parts[1], parts[2], parts[3]
            try:
                plan_key = _resolve_admin_plan_ref(service, plan_ref)
            except RuntimeError:
                await show_admin_service_plans(q.message, q.from_user.id, service, 0)
                return
            pending_edit = dict(context.user_data.get("admin_plan_edit") or {})
            if pending_edit.get("service") != service or pending_edit.get("plan_key") != plan_key or pending_edit.get("field") != field or "value" not in pending_edit:
                await show_admin_plan_detail(q.message, q.from_user.id, service, plan_key)
                return
            await run_blocking(
                update_sale_plan, plan_key, field=field, value=pending_edit["value"],
                service=service, admin_tg_id=q.from_user.id
            )
            refresh_plans(await run_blocking(list_service_sale_plans), service_aware=True)
            context.user_data.pop("admin_plan_edit", None)
            context.user_data.pop("awaiting", None)
            await show_admin_plan_detail(q.message, q.from_user.id, service, plan_key)
            return

        if parts[0] == "admin_plan_delete" and len(parts) == 3:
            if not is_admin(q.from_user.id):
                return
            service, plan_ref = parts[1], parts[2]
            try:
                plan_key = _resolve_admin_plan_ref(service, plan_ref)
            except RuntimeError:
                await show_admin_service_plans(q.message, q.from_user.id, service, 0)
                return
            plan = plans_for(service).get(plan_key)
            if not plan:
                await show_admin_service_plans(q.message, q.from_user.id, service, 0)
                return
            await q.message.edit_text(
                "🗑 <b>حذف بسته</b>\n\n"
                f"{int(plan['gb'])}GB | {html.escape(_plan_duration_text(plan))} | {int(plan['price_toman']):,} تومان\n"
                + (f"MikroTik: <code>{html.escape(str(plan['openvpn_profile']))}</code>\n" if service == "openvpn" else "")
                + f"\n⚠️ بسته فقط از منوی خرید و تمدید {SERVICE_LABEL[service]} حذف می‌شود. سفارش‌های پرداخت‌شده قبلی با Snapshot خودشان محفوظ می‌مانند.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ بله، حذف شود", callback_data=f"admin_plan_delete_confirm|{service}|{plan_ref}")],
                    [InlineKeyboardButton("❌ انصراف", callback_data=f"admin_plan_view|{service}|{plan_ref}")],
                ]),
            )
            return

        if parts[0] == "admin_plan_delete_confirm" and len(parts) == 3:
            if not is_admin(q.from_user.id):
                return
            service, plan_ref = parts[1], parts[2]
            try:
                plan_key = _resolve_admin_plan_ref(service, plan_ref)
            except RuntimeError:
                await show_admin_service_plans(q.message, q.from_user.id, service, 0)
                return
            await run_blocking(delete_sale_plan, plan_key, service=service, admin_tg_id=q.from_user.id)
            refresh_plans(await run_blocking(list_service_sale_plans), service_aware=True)
            context.user_data.pop("awaiting", None)
            context.user_data.pop("admin_plan_edit", None)
            await q.message.edit_text(
                "✅ بسته حذف شد.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 بسته‌ها", callback_data=f"admin_plans_service|{service}|0")]]),
            )
            return

        if data == "admin_referral_settings":
            if not is_admin(q.from_user.id):
                return
            context.user_data.pop("awaiting", None)
            context.user_data.pop("admin_referral_edit", None)
            await show_admin_referral_settings(q.message, q.from_user.id)
            return

        if parts[0] == "admin_referral_edit" and len(parts) == 2:
            if not is_admin(q.from_user.id):
                return
            kind = parts[1]
            if kind not in {"discount", "reward"}:
                return
            current = current_referral_discount_percent() if kind == "discount" else current_referral_reward_percent()
            title = "پاداش خریدار / تخفیف خرید اول" if kind == "discount" else "پاداش معرف"
            maximum = 100 if kind == "discount" else 10_000
            context.user_data["awaiting"] = {"kind": "admin_referral_percent", "field": kind}
            await q.message.edit_text(
                f"✏️ <b>{title}</b>\n\nمقدار فعلی: <b>{current}%</b>\nدرصد جدید را به‌صورت عدد صحیح بین <b>0 تا {maximum:,}</b> ارسال کنید:",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 لغو", callback_data="admin_referral_settings")]]),
            )
            return

        if parts[0] == "admin_referral_confirm" and len(parts) == 3:
            if not is_admin(q.from_user.id):
                return
            kind = parts[1]
            try:
                value = int(parts[2])
            except Exception:
                await show_admin_referral_settings(q.message, q.from_user.id)
                return
            pending = dict(context.user_data.get("admin_referral_edit") or {})
            if pending.get("field") != kind or int(pending.get("value", -1)) != value:
                await show_admin_referral_settings(q.message, q.from_user.id)
                return
            settings = await run_blocking(
                set_referral_percent, kind, value,
                admin_tg_id=q.from_user.id,
                default_discount_percent=REFERRAL_DISCOUNT_PERCENT,
                default_reward_percent=REFERRAL_REWARD_PERCENT,
            )
            _apply_referral_settings(settings)
            context.user_data.pop("admin_referral_edit", None)
            context.user_data.pop("awaiting", None)
            await show_admin_referral_settings(q.message, q.from_user.id)
            return

        if parts[0] == "admin_card_requests" and len(parts) == 2:
            if not is_admin(q.from_user.id):
                return
            await show_admin_card_requests(q.message, q.from_user.id, int(parts[1]))
            return

        if parts[0] == "cardview" and len(parts) == 2:
            if not is_admin(q.from_user.id):
                return
            await show_admin_card_request_detail(q.message, q.from_user.id, int(parts[1]))
            return

        if parts[0] == "cardadm" and len(parts) == 3:
            if not is_admin(q.from_user.id):
                return
            action = parts[1]
            request_id = int(parts[2])
            request = await run_blocking(get_card_transfer_request, request_id)
            if not request:
                await safe_callback_answer(q, "درخواست پیدا نشد.", show_alert=True)
                return
            if action == "receipt":
                kind = str(request.get("receipt_kind") or "")
                file_id = str(request.get("receipt_file_id") or "")
                if kind == "photo" and file_id:
                    await context.bot.send_photo(
                        chat_id=q.from_user.id, photo=file_id,
                        caption=_card_transfer_admin_text(request, include_receipt=False)[:1024],
                        parse_mode="HTML", reply_markup=_card_transfer_admin_markup(request),
                    )
                elif kind == "document" and file_id:
                    await context.bot.send_document(
                        chat_id=q.from_user.id, document=file_id,
                        caption=_card_transfer_admin_text(request, include_receipt=False)[:1024],
                        parse_mode="HTML", reply_markup=_card_transfer_admin_markup(request),
                    )
                else:
                    await safe_callback_answer(q, "رسید تصویری برای این درخواست وجود ندارد.", show_alert=True)
                return
            if action == "reject":
                if str(request.get("status") or "") != "submitted":
                    await safe_callback_answer(q, "این درخواست دیگر قابل رد کردن نیست.", show_alert=True)
                    return
                await q.message.reply_text(
                    "❌ آیا می‌خواهید علت رد درخواست را برای کاربر بنویسید؟",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("✍️ بله، نوشتن علت", callback_data=f"cardadm|reason|{request_id}")],
                        [InlineKeyboardButton("رد بدون علت", callback_data=f"cardadm|rejectnow|{request_id}")],
                        [InlineKeyboardButton("🔙 انصراف", callback_data=f"cardview|{request_id}")],
                    ]),
                )
                return
            if action == "reason":
                if str(request.get("status") or "") != "submitted":
                    await safe_callback_answer(q, "این درخواست دیگر قابل رد کردن نیست.", show_alert=True)
                    return
                context.user_data["awaiting"] = {
                    "kind": "admin_card_reject_reason", "request_id": request_id,
                }
                await q.message.reply_text(
                    "✍️ علت رد را ارسال کنید. این متن بعد از پیام ردشدن برای کاربر فرستاده می‌شود.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(
                        "رد بدون علت", callback_data=f"cardadm|rejectnow|{request_id}"
                    )]]),
                )
                return
            if action == "rejectnow":
                try:
                    await _reject_card_transfer_and_notify(
                        context, request_id, admin_tg_id=q.from_user.id,
                    )
                except ValueError as exc:
                    await safe_callback_answer(q, str(exc), show_alert=True)
                    return
                context.user_data.pop("awaiting", None)
                await q.message.reply_text(
                    "✅ درخواست کارت به کارت بدون علت رد شد و نتیجه برای کاربر ارسال گردید.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(
                        "🧾 رسیدهای کارت به کارت", callback_data="admin_card_requests|0"
                    )]]),
                )
                return
            if action == "approve":
                try:
                    completed = await _approve_card_transfer_request(
                        context, request_id, admin_tg_id=q.from_user.id,
                        admin_message=q.message,
                    )
                except Exception as exc:
                    logger.exception("card transfer approval failed request=%s", request_id)
                    await q.message.reply_text(
                        "⚠️ تأیید/تحویل کامل نشد. درخواست محفوظ مانده است؛ پس از بررسی خطا، دوباره تأیید را بزنید.\n"
                        f"<code>{html.escape(str(exc)[:800])}</code>",
                        parse_mode="HTML", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(
                            "🔄 تلاش مجدد", callback_data=f"cardadm|approve|{request_id}"
                        )]]),
                    )
                    return
                await q.message.reply_text(
                    "✅ رسید تأیید شد، اکانت ساخته شد و در پیام جداگانه برای کاربر ارسال گردید.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(
                        "🧾 رسیدهای کارت به کارت", callback_data="admin_card_requests|0"
                    )]]),
                )
                return

        if parts[0] == "admin_pending" and len(parts) == 2:
            if not is_admin(q.from_user.id):
                return
            try:
                page = int(parts[1])
            except Exception:
                page = 0
            await show_admin_pending_payments(q.message, q.from_user.id, page)
            return

        if parts[0] == "admin_pending_view" and len(parts) == 2:
            if not is_admin(q.from_user.id):
                return
            await show_admin_pending_detail(q.message, q.from_user.id, int(parts[1]))
            return

        if parts[0] in {"admin_pending_verify", "admin_pending_cancel"} and len(parts) == 2:
            if not is_admin(q.from_user.id):
                return
            pending_id = int(parts[1])
            item = await run_blocking(get_admin_pending_payment_by_id, pending_id)
            if not item:
                await q.message.edit_text(
                    "✅ این سفارش دیگر Pending نیست.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Pendingها", callback_data="admin_pending|0")]]),
                )
                return
            payload = item.get("payload") or {}
            authority = str(item.get("authority") or "")
            payment_kind = str(payload.get("payment_kind") or "gateway")
            amount_rial = int(payload.get("amount_rial") or 0)
            if payment_kind in {"wallet", "admin", "owner", "reseller_debt", "preflight", "card_transfer"} or not authority or amount_rial <= 0:
                await q.message.edit_text(
                    "⚠️ این سفارش از نوع پرداخت مستقیم زرین‌پال نیست و از این بخش قابل بررسی/حذف نیست.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 سفارش", callback_data=f"admin_pending_view|{pending_id}")]]),
                )
                return
            try:
                verify_func = verify_payment_for_cancel if parts[0] == "admin_pending_cancel" else verify_payment
                result = await run_zarinpal(verify_func, authority, amount_rial)
                code = _zarinpal_result_code(result)
            except Exception as exc:
                logger.warning("admin pending gateway check failed id=%s: %s", pending_id, exc)
                await q.message.edit_text(
                    "⚠️ ارتباط مطمئن با زرین‌پال برقرار نشد؛ هیچ تغییری در سفارش انجام نشد.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 سفارش", callback_data=f"admin_pending_view|{pending_id}")]]),
                )
                return
            if code in (100, 101):
                await q.message.edit_text(
                    f"✅ زرین‌پال این Authority را <b>پرداخت‌شده</b> اعلام کرد (Code {code}).\n\n"
                    "برای جلوگیری از از دست رفتن پرداخت، رکورد Pending حذف نشد.",
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("👤 مشاهده کاربر", callback_data=f"admin_user|{int(item.get('tg_id') or 0)}")],
                        [InlineKeyboardButton("🔙 سفارش", callback_data=f"admin_pending_view|{pending_id}")],
                    ]),
                )
                return
            if _zarinpal_is_definitely_unpaid(result):
                if parts[0] == "admin_pending_cancel":
                    removed = await run_blocking(pop_pending, authority)
                    if removed:
                        await run_blocking(
                            record_admin_audit,
                            admin_tg_id=q.from_user.id,
                            target_tg_id=int(item.get("tg_id") or 0),
                            action="pending_gateway_released",
                            meta={"authority": authority, "code": code, "pending_id": pending_id},
                        )
                    await q.message.edit_text(
                        "✅ زرین‌پال پرداخت‌نشدن سفارش را تأیید کرد و Pending با موفقیت آزاد شد.",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("👤 مشاهده کاربر", callback_data=f"admin_user|{int(item.get('tg_id') or 0)}")],
                            [InlineKeyboardButton("🔙 Pendingها", callback_data="admin_pending|0")],
                        ]),
                    )
                else:
                    await q.message.edit_text(
                        f"❌ این سفارش پرداخت نشده است (Code {code}).\nهیچ تغییری در دیتابیس انجام نشد.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 سفارش", callback_data=f"admin_pending_view|{pending_id}")]]),
                    )
                return
            await q.message.edit_text(
                f"⚠️ وضعیت زرین‌پال قطعی نیست (Code {code if code is not None else '-'}).\nبرای امنیت مالی، سفارش بدون تغییر نگه داشته شد.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 سفارش", callback_data=f"admin_pending_view|{pending_id}")]]),
            )
            return

        if data == "admin_global_search":
            if not is_admin(q.from_user.id):
                return
            context.user_data["awaiting"] = {"kind": "admin_global_search"}
            await q.message.edit_text(
                "🔎 <b>جستجوی کامل کاربر</b>\n\nTelegram ID، نام، @username، شماره/ایمیل یا نام اکانت OpenVPN/V2Ray را ارسال کنید:",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 کاربران", callback_data="admin_users_menu")]]),
            )
            return

        if parts[0] == "admin_user" and len(parts) == 2:
            if not is_admin(q.from_user.id):
                return
            context.user_data.clear()
            await show_admin_user_detail(q.message, q.from_user.id, int(parts[1]))
            return

        if parts[0] == "admin_user_tx" and len(parts) == 3:
            if not is_admin(q.from_user.id):
                return
            await show_admin_user_transactions(q.message, q.from_user.id, int(parts[1]), int(parts[2]))
            return

        if parts[0] == "admin_audit" and len(parts) == 2:
            if not is_admin(q.from_user.id):
                return
            await show_admin_audit(q.message, q.from_user.id, int(parts[1]))
            return

        if data == "admin_maintenance_toggle":
            if not is_admin(q.from_user.id):
                return
            current_maintenance = current_maintenance_mode()
            _before, after = await run_blocking(
                set_maintenance_mode, not current_maintenance,
                admin_tg_id=q.from_user.id,
            )
            _apply_maintenance_mode(after)
            await show_admin_settings_menu(q.message, q.from_user.id)
            return

        if parts[0] == "admin_maintenance_set" and len(parts) == 2:
            if not is_admin(q.from_user.id):
                return
            desired = parts[1] == "1"
            _before, after = await run_blocking(
                set_maintenance_mode, desired, admin_tg_id=q.from_user.id
            )
            _apply_maintenance_mode(after)
            await show_admin_settings_menu(q.message, q.from_user.id)
            return

        if data in {"admin_health", "admin_health_live"}:
            if not is_admin(q.from_user.id):
                return
            await show_admin_health(q.message, q.from_user.id, live=(data == "admin_health_live"))
            return

        if data == "admin_fulfillments":
            if not is_admin(q.from_user.id):
                return
            await show_incomplete_fulfillments(q.message, q.from_user.id)
            return

        if data == "admin_backup":
            if not is_admin(q.from_user.id):
                return
            await q.message.edit_text("⏳ در حال آماده‌سازی فایل دیتابیس برای ارسال…")
            result = {}
            try:
                result = await run_blocking(export_database_snapshot, _lane="backup")
                snapshot_path = str(result.get("path") or "")
                filename = str(result.get("filename") or "vpn_bot_v2.sqlite3")
                with open(snapshot_path, "rb") as fp:
                    await q.message.reply_document(
                        document=InputFile(fp, filename=filename),
                        caption=(
                            "💾 بک‌آپ دیتابیس\n\n"
                            f"فایل: <code>{html.escape(filename)}</code>\n"
                            f"حجم: <b>{human_bytes(int(result.get('size_bytes') or 0))}</b>"
                        ),
                        parse_mode="HTML",
                    )
                await run_blocking(
                    record_admin_audit, admin_tg_id=q.from_user.id, action="manual_backup_sent",
                    meta={"filename": filename, "size_bytes": int(result.get("size_bytes") or 0)},
                )
                await q.message.edit_text(
                    "✅ فایل دیتابیس با نام اصلی برای شما در تلگرام ارسال شد.\nهیچ بک‌آپ دائمی جدیدی روی MikroTik ساخته نشد.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 سیستم و سرورها", callback_data="admin_system_menu")]]),
                )
            except Exception as exc:
                logger.exception("manual Telegram database backup failed: %s", exc)
                await q.message.edit_text(
                    "❌ ارسال فایل دیتابیس ناموفق بود. لطفاً دوباره تلاش کنید.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 سیستم و سرورها", callback_data="admin_system_menu")]]),
                )
            finally:
                temp_dir = str(result.get("temp_dir") or "")
                if temp_dir:
                    await run_blocking(shutil.rmtree, temp_dir, True, _lane="backup")
            return

        if parts[0] == "admin_tx" and len(parts) == 2:
            if not is_admin(q.from_user.id):
                await safe_callback_answer(q, "دسترسی غیرمجاز", show_alert=True)
                return
            try:
                page = int(parts[1])
            except Exception:
                page = 0
            await show_admin_transactions(q.message, q.from_user.id, page)
            return

        if parts[0] == "admin_wallet_pos" and len(parts) == 2:
            if not is_admin(q.from_user.id):
                await safe_callback_answer(q, "دسترسی غیرمجاز", show_alert=True)
                return
            try:
                page = int(parts[1])
            except Exception:
                page = 0
            context.user_data.clear()
            await show_admin_positive_wallets(q.message, q.from_user.id, page)
            return

        if data == "admin_wallet_manage":
            if not is_admin(q.from_user.id):
                await safe_callback_answer(q, "دسترسی غیرمجاز", show_alert=True)
                return
            context.user_data.clear()
            await show_admin_wallet_manage(q.message, q.from_user.id)
            return

        if data == "admin_wallet_search":
            if not is_admin(q.from_user.id):
                await safe_callback_answer(q, "دسترسی غیرمجاز", show_alert=True)
                return
            context.user_data["awaiting"] = {"kind": "admin_user_search"}
            await q.message.edit_text(
                "🔎 <b>جستجوی کاربر</b>\n\nنام، یوزرنیم یا Telegram ID کاربر را ارسال کنید:",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 بازگشت", callback_data="admin_wallet_manage")]]),
            )
            return

        if parts[0] == "admin_users" and len(parts) == 2:
            if not is_admin(q.from_user.id):
                await safe_callback_answer(q, "دسترسی غیرمجاز", show_alert=True)
                return
            try:
                page = int(parts[1])
            except Exception:
                page = 0
            context.user_data.clear()
            await show_admin_all_users(q.message, q.from_user.id, page)
            return

        if parts[0] == "admin_wallet_user" and len(parts) == 2:
            if not is_admin(q.from_user.id):
                await safe_callback_answer(q, "دسترسی غیرمجاز", show_alert=True)
                return
            context.user_data.clear()
            await show_admin_wallet_user(q.message, q.from_user.id, int(parts[1]))
            return

        if parts[0] in {"admin_wallet_inc", "admin_wallet_dec"} and len(parts) == 2:
            if not is_admin(q.from_user.id):
                await safe_callback_answer(q, "دسترسی غیرمجاز", show_alert=True)
                return
            user_tg_id = int(parts[1])
            action = "inc" if parts[0].endswith("inc") else "dec"
            context.user_data["awaiting"] = {"kind": "admin_wallet_amount", "action": action, "user_tg_id": user_tg_id}
            profile = await run_blocking(get_user_profile, user_tg_id)
            label = admin_user_label(user_tg_id, profile)
            verb = "افزایش" if action == "inc" else "کاهش"
            icon = "➕" if action == "inc" else "➖"
            await q.message.edit_text(
                f"{icon} <b>{verb} موجودی</b>\n\nکاربر: <b>{html.escape(label)}</b>\nمبلغ را به <b>تومان</b> و فقط به صورت عدد ارسال کنید:",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 لغو", callback_data=f"admin_wallet_user|{user_tg_id}")]]),
            )
            return

        if parts[0] in {"admin_wallet_apply", "aw"} and len(parts) in {4, 5}:
            if not is_admin(q.from_user.id):
                await safe_callback_answer(q, "دسترسی غیرمجاز", show_alert=True)
                return
            action = parts[1]
            user_tg_id = int(parts[2])
            amount = int(parts[3])
            operation_id = parts[4] if len(parts) == 5 else account_ref(
                f"legacy:{getattr(q.message, 'message_id', 0)}:{data}"
            )
            if (
                action not in {"inc", "dec"}
                or amount <= 0
                or amount > MAX_ADMIN_WALLET_ADJUST_TOMAN
            ):
                await safe_callback_answer(q, "درخواست نامعتبر", show_alert=True)
                return
            delta = amount if action == "inc" else -amount
            try:
                before, after = await run_blocking(
                    admin_adjust_wallet,
                    user_tg_id,
                    delta,
                    admin_tg_id=q.from_user.id,
                    operation_id=operation_id,
                )
            except Exception as e:
                await safe_callback_answer(q, str(e), show_alert=True)
                return
            context.user_data.clear()
            profile = await run_blocking(get_user_profile, user_tg_id)
            label = admin_user_label(user_tg_id, profile)
            verb = "افزایش" if action == "inc" else "کاهش"
            await q.message.edit_text(
                f"✅ <b>{verb} موجودی انجام شد</b>\n\n"
                f"👤 {html.escape(label)}\n"
                f"مبلغ: <b>{amount:,} تومان</b>\n"
                f"موجودی قبلی: <b>{before:,} تومان</b>\n"
                f"موجودی جدید: <b>{after:,} تومان</b>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 بازگشت به کاربر", callback_data=f"admin_wallet_user|{user_tg_id}")]]),
            )
            return

        if parts[0] == "admnot_less" and len(parts) == 4:
            if not is_admin(q.from_user.id):
                await safe_callback_answer(q, "دسترسی غیرمجاز", show_alert=True)
                return
            service = parts[1]
            owner_tg_id = int(parts[2])
            ref = parts[3]
            header = q.message.text_html or q.message.text or ""
            header = header.split("\n\n🔐 <b>اطلاعات کامل اکانت</b>", 1)[0]
            await q.message.edit_text(
                header, parse_mode="HTML", disable_web_page_preview=True,
                reply_markup=_admin_notification_account_keyboard(
                    service, owner_tg_id, ref
                ),
            )
            return

        if parts[0] in {"admacc_ref", "admnot_ref"} and len(parts) == 4:
            if not is_admin(q.from_user.id):
                await safe_callback_answer(q, "دسترسی غیرمجاز", show_alert=True)
                return
            notification_view = parts[0] == "admnot_ref"
            service = parts[1]
            owner_tg_id = int(parts[2])
            identifier = await run_blocking(
                _resolve_account_ref, owner_tg_id, service, parts[3]
            )
            account = await run_blocking(
                _account_record, owner_tg_id, service, identifier,
                _lane="xui" if service == "v2ray" else "db",
            )
            if not account:
                await safe_callback_answer(q, "اطلاعات اکانت پیدا نشد", show_alert=True)
                return

            header = q.message.text_html or q.message.text or ""
            header = header.split("\n\n🔐 <b>اطلاعات کامل اکانت</b>", 1)[0]
            detail = "\n\n🔐 <b>اطلاعات کامل اکانت</b>\n"
            if service == "openvpn":
                username = str(account.get("username") or identifier)
                password = str(account.get("password") or "")
                profile = str(account.get("profile") or "")
                detail += f"یوزرنیم: <code>{html.escape(username)}</code>\n"
                if password:
                    detail += f"پسورد: <code>{html.escape(password)}</code>\n"
                if profile:
                    detail += f"Profile: <code>{html.escape(profile)}</code>\n"
            else:
                email = str(account.get("identifier") or identifier)
                sub_url = str(account.get("sub_url") or "")
                links = account.get("links") if isinstance(account.get("links"), list) else []
                detail += f"نام اکانت: <code>{html.escape(email)}</code>\n"
                if sub_url:
                    detail += "Subscription:\n" + text_code_block(sub_url)
                configs = format_vless_configs(
                    [str(x) for x in links], max_chars=max(3700 - len(header) - len(detail), 0)
                )
                if configs:
                    detail += "VLESS:\n" + configs
            plan_key = str(account.get("plan_key") or "")
            if plan_key:
                detail += f"پلن داخلی: <code>{html.escape(plan_key)}</code>\n"
            detail += f"نوع: {'تست' if account.get('is_test') else 'واقعی'}"
            await q.message.edit_text(
                header + detail, parse_mode="HTML", disable_web_page_preview=True,
                reply_markup=(
                    InlineKeyboardMarkup([[
                        InlineKeyboardButton(
                            "نمایش کمتر",
                            callback_data=f"admnot_less|{service}|{owner_tg_id}|{parts[3]}",
                        )
                    ]])
                    if notification_view else
                    InlineKeyboardMarkup([[InlineKeyboardButton("👤 صفحه کاربر", callback_data=f"admin_user|{owner_tg_id}")]])
                ),
            )
            return

        # Compatibility with buttons sent by v2.1 and older.
        if parts[0] == "admacc" and len(parts) >= 4:
            if not is_admin(q.from_user.id):
                await safe_callback_answer(q, "دسترسی غیرمجاز", show_alert=True)
                return
            service = parts[1]
            owner_tg_id = int(parts[2])
            identifier = "|".join(parts[3:])
            account = await run_blocking(
                _account_record, owner_tg_id, service, identifier,
                _lane="xui" if service == "v2ray" else "db",
            )
            if not account:
                await safe_callback_answer(q, "اطلاعات اکانت پیدا نشد", show_alert=True)
                return

            header = q.message.text_html or q.message.text or ""
            # Avoid duplicating details if the button is clicked again from an already expanded message.
            header = header.split("\n\n🔐 <b>اطلاعات کامل اکانت</b>", 1)[0]
            detail = "\n\n🔐 <b>اطلاعات کامل اکانت</b>\n"
            if service == "openvpn":
                username = str(account.get("username") or identifier)
                password = str(account.get("password") or "")
                profile = str(account.get("profile") or "")
                detail += f"یوزرنیم: <code>{html.escape(username)}</code>\n"
                if password:
                    detail += f"پسورد: <code>{html.escape(password)}</code>\n"
                if profile:
                    detail += f"Profile: <code>{html.escape(profile)}</code>\n"
            else:
                email = str(account.get("identifier") or identifier)
                sub_url = str(account.get("sub_url") or "")
                links = account.get("links") if isinstance(account.get("links"), list) else []
                detail += f"نام اکانت: <code>{html.escape(email)}</code>\n"
                if sub_url:
                    detail += "Subscription:\n" + text_code_block(sub_url)
                configs = format_vless_configs(
                    [str(x) for x in links], max_chars=max(3700 - len(header) - len(detail), 0)
                )
                if configs:
                    detail += "VLESS:\n" + configs
            plan_key = str(account.get("plan_key") or "")
            if plan_key:
                detail += f"پلن داخلی: <code>{html.escape(plan_key)}</code>\n"
            detail += f"نوع: {'تست' if account.get('is_test') else 'واقعی'}"
            await q.message.edit_text(
                header + detail, parse_mode="HTML", disable_web_page_preview=True,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("👤 صفحه کاربر", callback_data=f"admin_user|{owner_tg_id}")]]),
            )
            return

        if parts[0] == "menu" and len(parts) == 2:
            if parts[1] == "services":
                context.user_data.clear()
                enabled = enabled_sales_services()
                if len(enabled) == 1:
                    await q.message.edit_text(welcome_text(), reply_markup=await main_menu_keyboard(q.from_user.id))
                else:
                    await q.message.edit_text("🛍 سرویس مورد نظرتون رو انتخاب کنید:", reply_markup=service_choose_keyboard())
            elif parts[1] == "wallet":
                if is_reseller(q.from_user.id):
                    await safe_callback_answer(q, "کیف پول برای ریسلر فعال نیست.", show_alert=True)
                    await show_reseller_debt(q.message, q.from_user.id)
                else:
                    await show_wallet(q.message, q.from_user.id)
            elif parts[1] == "referral":
                if is_reseller(q.from_user.id):
                    await safe_callback_answer(q, "Referral برای ریسلر فعال نیست.", show_alert=True)
                    await show_reseller_debt(q.message, q.from_user.id)
                else:
                    await show_referral(q.message, q.from_user.id)
            elif parts[1] == "reseller_debt":
                await show_reseller_debt(q.message, q.from_user.id)
            return

        if parts[0] == "svc" and len(parts) == 2:
            service = parts[1]
            if not service_sales_enabled(service):
                await q.message.edit_text(
                    "⚠️ فروش این سرویس در حال حاضر غیرفعال است.",
                    reply_markup=await main_menu_keyboard(q.from_user.id),
                )
                return
            context.user_data.clear()
            context.user_data["service"] = service
            await q.message.edit_text(
                f"{('🔵' if service == 'openvpn' else '🟣')} {SERVICE_LABEL[service]}\n\nیکی از گزینه‌های زیر را انتخاب کنید:",
                reply_markup=service_menu_keyboard(service, q.from_user.id),
            )
            return

        if parts[0] == "act" and len(parts) == 3:
            action, service = parts[1], parts[2]
            context.user_data["service"] = service
            if action in {"buy", "renew", "test"} and not service_sales_enabled(service):
                await q.message.edit_text(
                    "⚠️ فروش این سرویس در حال حاضر غیرفعال است.",
                    reply_markup=await main_menu_keyboard(q.from_user.id),
                )
                return
            if action == "buy":
                if not plans_for(service):
                    await q.message.edit_text(
                        "⚠️ در حال حاضر هیچ بسته‌ای برای فروش تعریف نشده است.",
                        reply_markup=back_service(service),
                    )
                else:
                    await q.message.edit_text("📦 پلن موردنظر را انتخاب کنید:", reply_markup=plans_keyboard("buy", service))
            elif action in ("status", "renew"):
                # Keep the old renewal/status picker compatible with older messages.
                await show_account_picker(q.message, q.from_user.id, service, action)
            elif action == "accounts":
                await show_my_accounts(q.message, q.from_user.id, service)
            elif action == "test":
                await create_test(q, context, service)
            return

        if parts[0] == "acct" and len(parts) >= 4:
            action, service = parts[1], parts[2]
            identifier = "|".join(parts[3:])
            context.user_data["selected_identifier"] = identifier
            context.user_data["service"] = service
            if action == "status":
                await show_status(q.message, q.from_user.id, service, identifier)
            else:
                if not service_sales_enabled(service):
                    await q.message.edit_text("⚠️ تمدید این سرویس در حال حاضر غیرفعال است.", reply_markup=await main_menu_keyboard(q.from_user.id))
                    return
                await q.message.edit_text(
                    f"♻️ تمدید <code>{html.escape(identifier)}</code>\n\nپلن جدید را انتخاب کنید:",
                    parse_mode="HTML",
                    reply_markup=plans_keyboard("renew", service),
                )
            return

        if parts[0] == "acctref" and len(parts) == 4:
            action, service = parts[1], parts[2]
            identifier = await run_blocking(
                _resolve_account_ref, q.from_user.id, service, parts[3]
            )
            context.user_data["selected_identifier"] = identifier
            context.user_data["service"] = service
            if action == "status":
                await show_status(q.message, q.from_user.id, service, identifier)
            else:
                if not service_sales_enabled(service):
                    await q.message.edit_text("⚠️ تمدید این سرویس در حال حاضر غیرفعال است.", reply_markup=await main_menu_keyboard(q.from_user.id))
                    return
                await q.message.edit_text(
                    f"♻️ تمدید <code>{html.escape(identifier)}</code>\n\nپلن جدید را انتخاب کنید:",
                    parse_mode="HTML",
                    reply_markup=plans_keyboard("renew", service),
                )
            return

        if parts[0] == "myacct" and len(parts) >= 3:
            service = parts[1]
            identifier = "|".join(parts[2:])
            await show_my_account_detail(q.message, q.from_user.id, service, identifier)
            return

        if parts[0] == "myacctref" and len(parts) == 3:
            service = parts[1]
            identifier = await run_blocking(
                _resolve_account_ref, q.from_user.id, service, parts[2]
            )
            await show_my_account_detail(q.message, q.from_user.id, service, identifier)
            return

        if parts[0] == "myact" and len(parts) >= 4:
            action, service = parts[1], parts[2]
            identifier = "|".join(parts[3:])
            context.user_data["selected_identifier"] = identifier
            context.user_data["service"] = service
            if action in {"status", "refresh"}:
                await show_status(
                    q.message, q.from_user.id, service, identifier,
                    markup=my_account_status_keyboard(service, identifier),
                    force_refresh=(action == "refresh"),
                )
            elif action == "renew":
                if not service_sales_enabled(service):
                    await q.message.edit_text("⚠️ تمدید این سرویس در حال حاضر غیرفعال است.", reply_markup=await main_menu_keyboard(q.from_user.id))
                    return
                await q.message.edit_text(
                    f"♻️ تمدید <code>{html.escape(identifier)}</code>\n\nپلن جدید را انتخاب کنید:",
                    parse_mode="HTML",
                    reply_markup=plans_keyboard("renew", service, f"myacct|{service}|{identifier}"),
                )
            return

        if parts[0] == "myactref" and len(parts) == 4:
            action, service = parts[1], parts[2]
            identifier = await run_blocking(
                _resolve_account_ref, q.from_user.id, service, parts[3]
            )
            context.user_data["selected_identifier"] = identifier
            context.user_data["service"] = service
            if action in {"status", "refresh"}:
                await show_status(
                    q.message, q.from_user.id, service, identifier,
                    markup=my_account_status_keyboard(service, identifier),
                    force_refresh=(action == "refresh"),
                )
            elif action == "renew":
                if not service_sales_enabled(service):
                    await q.message.edit_text("⚠️ تمدید این سرویس در حال حاضر غیرفعال است.", reply_markup=await main_menu_keyboard(q.from_user.id))
                    return
                await q.message.edit_text(
                    f"♻️ تمدید <code>{html.escape(identifier)}</code>\n\nپلن جدید را انتخاب کنید:",
                    parse_mode="HTML",
                    reply_markup=plans_keyboard(
                        "renew", service,
                        f"myacctref|{service}|{account_ref(identifier)}",
                    ),
                )
            return

        if parts[0] == "manual" and len(parts) == 3:
            action, service = parts[1], parts[2]
            if action == "renew" and not service_sales_enabled(service):
                await q.message.edit_text("⚠️ تمدید این سرویس در حال حاضر غیرفعال است.", reply_markup=await main_menu_keyboard(q.from_user.id))
                return
            context.user_data["awaiting"] = {"action": action, "service": service}
            label = "یوزرنیم OpenVPN" if service == "openvpn" else "نام Client در V2Ray"
            await q.message.edit_text(f"✍️ {label} را ارسال کنید:", reply_markup=back_service(service))
            return

        if parts[0] == "plan" and len(parts) == 4:
            action, service, plan_key = parts[1], parts[2], parts[3]
            if not service_sales_enabled(service):
                await q.message.edit_text("⚠️ فروش این سرویس در حال حاضر غیرفعال است.", reply_markup=await main_menu_keyboard(q.from_user.id))
                return
            if plan_key not in plans_for(service):
                raise RuntimeError("پلن نامعتبر است")
            identifier = context.user_data.get("selected_identifier", "") if action == "renew" else ""
            if action == "renew" and not identifier:
                raise RuntimeError("اکانت برای تمدید انتخاب نشده است")

            if (
                action == "buy"
                and not is_admin(q.from_user.id)
                and not is_reseller(q.from_user.id)
                and not (await run_blocking(has_completed_purchase, q.from_user.id))
            ):
                authority, existing = (await run_blocking(pending_first_purchase_for_user, q.from_user.id))
                if existing and _pending_is_preflight(existing):
                    await run_blocking(pop_pending, authority)
                    existing = None
                if existing and pending_plan_is_stale(existing):
                    state = await reconcile_stale_pending(authority, existing, q.message, context)
                    if state in ("paid", "blocked"):
                        return
                    existing = None
                if existing:
                    pending_url = str(existing.get("payment_url") or "")
                    pending_amount = int(existing.get("gateway_toman") or (int(existing.get("amount_rial", 0) or 0) // 10))
                    pending_text = (
                        "⚠️ یک سفارش خرید اول در انتظار دارید. برای جلوگیری از استفاده چندباره از تخفیف خرید اول، "
                        "ابتدا همان سفارش را پرداخت یا لغو کنید."
                    )
                    if pending_amount:
                        pending_text += f"\n\n💳 مبلغ درگاه: {pending_amount:,} تومان"
                    if pending_url:
                        pending_text += f"\n{pending_url}"
                    await q.message.edit_text(
                        pending_text,
                        reply_markup=pay_keyboard(pending_url),
                        disable_web_page_preview=True,
                    )
                    return
                context.user_data["first_buy_order"] = {
                    "service": service, "action": action, "plan_key": plan_key, "identifier": identifier
                }
                await q.message.edit_text(
                    "🎟 <b>تخفیف خرید اول</b>\n\n"
                    f"اگر خرید اولتان است، با وارد کردن کد معرف <b>{current_referral_discount_percent()}٪ تخفیف</b> بگیرید.\n\n"
                    f"بعد از این خرید هم می‌توانید با معرفی دوستانتان، به آنها {current_referral_discount_percent()}٪ تخفیف بدهید و خودتان نیز "
                    f"معادل {current_referral_reward_percent()}٪ مبلغ اصلی بسته خریداری‌شده، اعتبار رایگان دریافت کنید.",
                    parse_mode="HTML",
                    reply_markup=first_purchase_referral_keyboard(service),
                )
                return

            await start_order(q, context, service, action, plan_key, identifier)
            return

        if parts[0] == "ref" and len(parts) == 2:
            if is_reseller(q.from_user.id):
                context.user_data.pop("first_buy_order", None)
                context.user_data.pop("awaiting", None)
                await safe_callback_answer(q, "Referral برای ریسلر فعال نیست.", show_alert=True)
                await show_reseller_debt(q.message, q.from_user.id)
                return
            order = context.user_data.get("first_buy_order") or {}
            if not order:
                await q.message.edit_text("⚠️ اطلاعات سفارش پیدا نشد. لطفاً دوباره پلن را انتخاب کنید.", reply_markup=await main_menu_keyboard(q.from_user.id))
                return
            if not service_sales_enabled(str(order.get("service") or "")):
                context.user_data.pop("first_buy_order", None)
                context.user_data.pop("awaiting", None)
                await q.message.edit_text("⚠️ فروش این سرویس در حال حاضر غیرفعال است.", reply_markup=await main_menu_keyboard(q.from_user.id))
                return
            if (await run_blocking(has_completed_purchase, q.from_user.id)):
                # If a purchase completed from another session/message, referral discount is no longer valid.
                await start_order(q, context, order["service"], order["action"], order["plan_key"], order.get("identifier", ""))
                return
            if parts[1] == "have":
                context.user_data["awaiting"] = {"kind": "referral_code", **order}
                await q.message.edit_text(
                    "🎟 کد معرف را ارسال کنید:",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 بدون کد معرف", callback_data="ref|none")]]),
                )
            elif parts[1] == "none":
                context.user_data.pop("awaiting", None)
                await start_order(q, context, order["service"], order["action"], order["plan_key"], order.get("identifier", ""))
            return

        if data == "payment|check":
            await verify_latest(q, context)
            return

        if data == "payment|cancel":
            await cancel_latest_payment(q, context)
            return
    except BadRequest as exc:
        if "message is not modified" in str(exc).lower():
            return
        logger.exception("callback Telegram request rejected")
        await callback_failure_reply(q, q.from_user.id)
    except ServiceBusyError as exc:
        logger.warning(
            "callback lane busy tg_id=%s data=%s error=%s",
            q.from_user.id, data[:120], exc,
        )
        await safe_callback_answer(q, str(exc), show_alert=False)
        try:
            await q.message.reply_text(f"⏳ {exc}")
        except Exception:
            logger.warning("callback busy reply failed", exc_info=True)
    except Exception:
        logger.exception("callback error")
        await callback_failure_reply(q, q.from_user.id)


async def show_account_picker(message, tg_id: int, service: str, action: str):
    accounts = await run_blocking(
        _sync_user_accounts, tg_id, service,
        _lane="xui" if service == "v2ray" else "db",
    )

    rows = []
    for a in accounts[:90]:
        ident = str(a.get("identifier"))
        rows.append([InlineKeyboardButton(
            f"👤 {ident}"[:64],
            callback_data=f"acctref|{action}|{service}|{account_ref(ident)}",
        )])
    rows.append([InlineKeyboardButton("✍️ وارد کردن دستی", callback_data=f"manual|{action}|{service}")])
    rows.append([InlineKeyboardButton("🔙 بازگشت", callback_data=f"svc|{service}")])
    prompt = "📊 اکانت موردنظر را انتخاب کنید:" if action == "status" else "♻️ اکانت موردنظر برای تمدید را انتخاب کنید:"
    await message.edit_text(prompt, reply_markup=InlineKeyboardMarkup(rows))


async def show_my_accounts(message, tg_id: int, service: str):
    accounts = await run_blocking(
        _sync_user_accounts, tg_id, service,
        _lane="xui" if service == "v2ray" else "db",
    )
    rows = []
    for a in accounts[:90]:
        ident = str(a.get("identifier") or "")
        if not ident:
            continue
        prefix = "🎁" if a.get("is_test") else "👤"
        suffix = " (تست)" if a.get("is_test") else ""
        rows.append([InlineKeyboardButton(
            f"{prefix} {ident}{suffix}"[:64],
            callback_data=f"myacctref|{service}|{account_ref(ident)}",
        )])

    if not rows:
        await message.edit_text(
            f"👤 هنوز اکانت {SERVICE_LABEL[service]} برای شما ثبت نشده است.",
            reply_markup=back_service(service),
        )
        return

    rows.append([InlineKeyboardButton("🔙 بازگشت", callback_data=f"svc|{service}")])
    omitted_notice = (
        f"\n\n<i>{len(accounts) - 90} اکانت قدیمی‌تر برای رعایت محدودیت کیبورد تلگرام نمایش داده نشد.</i>"
        if len(accounts) > 90 else ""
    )
    await message.edit_text(
        f"👤 <b>اکانت‌های {SERVICE_LABEL[service]} من</b>\n\nاکانت موردنظر را انتخاب کنید:{omitted_notice}",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(rows),
    )


async def show_my_account_detail(message, tg_id: int, service: str, identifier: str):
    account = await run_blocking(
        _account_record, tg_id, service, identifier,
        _lane="xui" if service == "v2ray" else "db",
    )
    if not account:
        await message.edit_text(
            "❌ این اکانت در لیست اکانت‌های شما پیدا نشد.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 بازگشت", callback_data=f"act|accounts|{service}")]]),
        )
        return

    account_type = "🎁 اکانت تست" if account.get("is_test") else "✅ اکانت خریداری‌شده"

    if service == "openvpn":
        username = str(account.get("username") or identifier)
        password = str(account.get("password") or "")

        # Older/imported records may not contain the password locally. Try to
        # hydrate it from User Manager once, then cache it for future views.
        if not password:
            try:
                info = await run_blocking_retry(mikrotik.fetch_usage_and_expiry, identifier)
                if info.get("found"):
                    username = str(info.get("matched_name") or username)
                    password = str(info.get("password") or "")
                    if password:
                        (await run_blocking(upsert_account, tg_id, "openvpn", identifier, username=username, password=password))
            except Exception:
                pass

        text = (
            f"🔵 <b>OpenVPN</b>\n{account_type}\n\n"
            f"👤 <b>یوزرنیم</b>\n{text_code_block(username)}"
        )
        if password:
            text += f"🔑 <b>پسورد</b>\n{text_code_block(password)}"
        else:
            text += "🔑 <b>پسورد</b>\n⚠️ پسورد این اکانت در اطلاعات ذخیره‌شده موجود نیست.\n"

    else:
        email = identifier
        sub_id = str(account.get("sub_id") or "")
        sub_url = str(account.get("sub_url") or "")
        cached_links = account.get("links")
        links = [str(x) for x in cached_links] if isinstance(cached_links, list) else []

        # Cache-first: normal account-detail views must be instant. Refresh from
        # 3x-ui only when an old/imported record is missing data. Live quota and
        # expiry are still fetched by the dedicated status button.
        if not sub_id or not sub_url or not links:
            try:
                def _hydrate_missing_v2ray_detail():
                    xui = XUIClient()
                    hydrated = xui.get_client(email)
                    client = hydrated.get("client") or {}
                    live_sub_id = str(client.get("subId") or "")
                    live_links = xui.links(email)
                    live_sub_url = xui.subscription_url(live_sub_id) if live_sub_id else ""
                    return live_sub_id, live_sub_url, live_links

                live_sub_id, live_sub_url, live_links = await run_blocking(
                    _hydrate_missing_v2ray_detail, _lane="xui"
                )
                if live_sub_id:
                    sub_id = live_sub_id
                if live_sub_url:
                    sub_url = live_sub_url
                if live_links:
                    links = live_links
                (await run_blocking(upsert_account, tg_id, "v2ray", email, sub_id=sub_id, sub_url=sub_url, links=links))
            except Exception as e:
                logger.warning("V2Ray account detail cache fill failed for %s: %s", email, e)

        text = (
            f"🟣 <b>V2Ray</b>\n{account_type}\n\n"
            f"👤 <b>نام اکانت</b>\n{text_code_block(email)}"
        )
        if sub_url:
            text += f"🔗 <b>Subscription</b>\n{text_code_block(sub_url)}"
        else:
            text += "🔗 <b>Subscription</b>\n⚠️ لینک سابسکریپشن فعلاً در دسترس نیست.\n"

        configs = format_vless_configs(links, max_chars=max(3800 - len(text), 0))
        if configs:
            text += f"⚙️ <b>لینک‌های VLESS</b>\n{configs}"
        else:
            text += "⚙️ <b>لینک‌های VLESS</b>\n⚠️ لینک‌های کانفیگ فعلاً در دسترس نیستند.\n"

    await message.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=my_account_keyboard(
            service, identifier,
            username=username if service == "openvpn" else "",
            password=password if service == "openvpn" else "",
        ),
        disable_web_page_preview=True,
    )


async def _submit_card_receipt_and_notify(
    update: Update, context: ContextTypes.DEFAULT_TYPE, request: dict, *,
    receipt_kind: str, receipt_text: str = "", receipt_file_id: str = "",
    receipt_file_unique_id: str = "",
):
    request_id = int(request.get("id") or 0)
    if not request_id or str(request.get("status") or "") != "awaiting_receipt":
        await update.effective_message.reply_text(
            "⚠️ این درخواست دیگر در وضعیت دریافت رسید نیست."
        )
        return
    try:
        submitted = await run_blocking(
            submit_card_transfer_receipt, request_id, update.effective_user.id,
            receipt_kind=receipt_kind, receipt_text=receipt_text,
            receipt_file_id=receipt_file_id,
            receipt_file_unique_id=receipt_file_unique_id,
        )
    except ValueError as exc:
        await update.effective_message.reply_text(f"⚠️ {str(exc)}")
        return
    context.user_data.pop("awaiting", None)
    await update.effective_message.reply_text(
        "✅ رسید شما ثبت شد و برای ادمین ارسال گردید.\n\n"
        "پس از بررسی، نتیجه برای شما ارسال می‌شود. تا قبل از تأیید، هیچ اکانتی ساخته یا رزرو نمی‌شود.",
        reply_markup=await main_menu_keyboard(update.effective_user.id),
    )
    await notify_card_transfer_admins(context, submitted)


async def card_receipt_media_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    schedule_telegram_profile(context, update.effective_user)
    awaiting = dict(context.user_data.get("awaiting") or {})
    request = {}
    if awaiting.get("kind") == "card_transfer_receipt":
        request = await run_blocking(
            get_card_transfer_request, int(awaiting.get("request_id") or 0)
        )
    if not request:
        request = await run_blocking(
            active_card_transfer_request_for_user, update.effective_user.id
        )
    if not request or str(request.get("status") or "") != "awaiting_receipt":
        return
    if update.message.photo:
        media = update.message.photo[-1]
        kind = "photo"
    elif update.message.document:
        media = update.message.document
        kind = "document"
    else:
        return
    await _submit_card_receipt_and_notify(
        update, context, request, receipt_kind=kind,
        receipt_file_id=str(media.file_id or ""),
        receipt_file_unique_id=str(media.file_unique_id or ""),
    )


async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    schedule_telegram_profile(context, update.effective_user)
    value = (update.message.text or "").strip()
    awaiting = dict(context.user_data.get("awaiting") or {})

    if awaiting.get("kind") == "admin_card_reject_reason":
        context.user_data.pop("awaiting", None)
        if not is_admin(update.effective_user.id):
            return
        try:
            await _reject_card_transfer_and_notify(
                context, int(awaiting.get("request_id") or 0),
                admin_tg_id=update.effective_user.id, reason=value,
            )
        except ValueError as exc:
            await update.message.reply_text(
                f"⚠️ {html.escape(str(exc))}", parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(
                    "🧾 رسیدهای کارت به کارت", callback_data="admin_card_requests|0"
                )]]),
            )
            return
        await update.message.reply_text(
            "✅ درخواست رد شد و علت برای کاربر ارسال گردید.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(
                "🧾 رسیدهای کارت به کارت", callback_data="admin_card_requests|0"
            )]]),
        )
        return

    card_request = {}
    if awaiting.get("kind") == "card_transfer_receipt":
        card_request = await run_blocking(
            get_card_transfer_request, int(awaiting.get("request_id") or 0)
        )
    elif not awaiting:
        # Restarts discard Telegram conversation memory, not the durable order.
        card_request = await run_blocking(
            active_card_transfer_request_for_user, update.effective_user.id
        )
    if card_request and str(card_request.get("status") or "") == "awaiting_receipt":
        if not value:
            return
        await _submit_card_receipt_and_notify(
            update, context, card_request, receipt_kind="text", receipt_text=value,
        )
        return

    awaiting = context.user_data.pop("awaiting", None)
    if not awaiting:
        return

    if not value:
        context.user_data["awaiting"] = awaiting
        return

    if not is_admin(update.effective_user.id) and current_maintenance_mode():
        if awaiting.get("kind") == "referral_code" or awaiting.get("action") == "renew":
            context.user_data.clear()
            await update.message.reply_text(MAINTENANCE_MESSAGE, reply_markup=await main_menu_keyboard(update.effective_user.id))
            return

    if awaiting.get("kind") == "admin_config_value":
        if not is_admin(update.effective_user.id):
            return
        short_key = str(awaiting.get("short_key") or "")
        key = str(awaiting.get("key") or "")
        label = str(awaiting.get("label") or "")
        group = str(awaiting.get("group") or "")
        field = _ADMIN_CONFIG_FIELDS.get(short_key)
        if not field or field[0] != key or field[2] != group:
            return
        try:
            normalized = normalize_setting(key, value)
        except ValueError as exc:
            context.user_data["awaiting"] = awaiting
            await update.message.reply_text(
                f"❌ {html.escape(str(exc))}\nلطفاً مقدار معتبر دیگری ارسال کنید.",
                parse_mode="HTML", reply_markup=_admin_config_back(group),
            )
            return
        old = APP_SETTINGS.get(key)
        if key == "xui_api_token":
            old_display = _masked_secret(str(old or ""))
            new_display = _masked_secret(str(normalized or ""))
        elif key == "api_pass":
            # Explicit UX requirement: MikroTik password is visible to an
            # authorized admin, but it never enters callback data or audit data.
            old_display = str(old or "")
            new_display = str(normalized or "")
        else:
            old_display = str(old)
            new_display = str(normalized)
        context.user_data["admin_config_edit"] = {
            "short_key": short_key, "key": key, "value": normalized,
        }
        await update.message.reply_text(
            f"⚠️ <b>تأیید تغییر {html.escape(label)}</b>\n\n"
            f"قبلی: <code>{html.escape(old_display)}</code>\n"
            f"جدید: <code>{html.escape(new_display)}</code>",
            parse_mode="HTML", reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ تأیید تغییر", callback_data=f"admin_cfg_confirm|{short_key}")],
                [InlineKeyboardButton("❌ انصراف", callback_data=f"admin_cfg|{group}")],
            ]),
        )
        return

    if awaiting.get("kind") == "reseller_add_name":
        if not is_admin(update.effective_user.id):
            return
        if not value or len(value) > 100:
            context.user_data["awaiting"] = awaiting
            await update.message.reply_text("❌ نام ریسلر باید بین 1 تا 100 کاراکتر باشد.")
            return
        context.user_data["reseller_draft"] = {"name": value}
        context.user_data["awaiting"] = {"kind": "reseller_add_tg_id"}
        await update.message.reply_text("Telegram ID عددی ریسلر را ارسال کنید:")
        return

    if awaiting.get("kind") == "reseller_add_tg_id":
        if not is_admin(update.effective_user.id):
            return
        if not value.isdigit() or int(value) <= 0 or int(value) > 9_223_372_036_854_775_807:
            context.user_data["awaiting"] = awaiting
            await update.message.reply_text("❌ Telegram ID باید فقط یک عدد مثبت باشد.")
            return
        new_id = int(value)
        if new_id == root_admin_id() or reseller_record(new_id):
            context.user_data["awaiting"] = awaiting
            await update.message.reply_text("⚠️ این ID مدیر اصلی است یا از قبل به‌عنوان ریسلر ثبت شده است.")
            return
        draft = dict(context.user_data.get("reseller_draft") or {})
        if not draft.get("name"):
            await update.message.reply_text("⚠️ اطلاعات ثبت ریسلر منقضی شده است.")
            return
        draft["tg_id"] = new_id
        context.user_data["reseller_draft"] = draft
        context.user_data["awaiting"] = {"kind": "reseller_add_rate"}
        await update.message.reply_text("هزینه هر گیگ این ریسلر را به تومان و فقط به صورت عدد ارسال کنید:")
        return

    if awaiting.get("kind") == "reseller_add_rate":
        if not is_admin(update.effective_user.id):
            return
        raw = value.replace(",", "").replace("٬", "").replace(" ", "")
        rate = int(raw) if raw.isdigit() else 0
        if rate <= 0 or rate > MAX_RESELLER_MONEY_TOMAN:
            context.user_data["awaiting"] = awaiting
            await update.message.reply_text("❌ هزینه هر گیگ باید یک عدد مثبت و معتبر به تومان باشد.")
            return
        draft = dict(context.user_data.get("reseller_draft") or {})
        if not draft.get("name") or not int(draft.get("tg_id") or 0):
            await update.message.reply_text("⚠️ اطلاعات ثبت ریسلر منقضی شده است.")
            return
        draft["price_per_gb_toman"] = rate
        context.user_data["reseller_draft"] = draft
        await update.message.reply_text(
            "🎁 <b>دسترسی به اکانت تست</b>\n\n"
            "آیا این ریسلر امکان دریافت اکانت تست رایگان را داشته باشد؟",
            parse_mode="HTML", reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("✅ بله", callback_data="rsaddtrial|1"),
                    InlineKeyboardButton("⛔ خیر", callback_data="rsaddtrial|0"),
                ],
                [InlineKeyboardButton("❌ انصراف", callback_data="admin_resellers")],
            ]),
        )
        return

    if awaiting.get("kind") == "reseller_edit":
        if not is_admin(update.effective_user.id):
            return
        reseller_id = int(awaiting.get("reseller_id") or 0)
        field = str(awaiting.get("field") or "")
        if not _runtime_reseller_by_id(reseller_id) or field not in {"n", "i", "p"}:
            await update.message.reply_text("⚠️ ریسلر پیدا نشد.")
            return
        kwargs = {}
        if field == "n":
            if not value or len(value) > 100:
                context.user_data["awaiting"] = awaiting
                await update.message.reply_text("❌ نام باید بین 1 تا 100 کاراکتر باشد.")
                return
            kwargs["name"] = value
        elif field == "i":
            if not value.isdigit() or int(value) <= 0 or int(value) > 9_223_372_036_854_775_807:
                context.user_data["awaiting"] = awaiting
                await update.message.reply_text("❌ Telegram ID باید فقط یک عدد مثبت باشد.")
                return
            kwargs["tg_id"] = int(value)
        else:
            raw = value.replace(",", "").replace("٬", "").replace(" ", "")
            rate = int(raw) if raw.isdigit() else 0
            if rate <= 0 or rate > MAX_RESELLER_MONEY_TOMAN:
                context.user_data["awaiting"] = awaiting
                await update.message.reply_text("❌ هزینه هر گیگ باید یک عدد مثبت و معتبر باشد.")
                return
            kwargs["price_per_gb_toman"] = rate
        try:
            await run_blocking(
                edit_reseller, reseller_id, admin_tg_id=update.effective_user.id,
                _lane="db", **kwargs,
            )
        except ValueError as exc:
            context.user_data["awaiting"] = awaiting
            await update.message.reply_text(f"❌ {html.escape(str(exc))}", parse_mode="HTML")
            return
        await update.message.reply_text(
            "✅ اطلاعات ریسلر به‌روزرسانی شد.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("👤 مشاهده ریسلر", callback_data=f"rs|{reseller_id}")
            ]]),
        )
        return

    if awaiting.get("kind") == "reseller_set_debt":
        if not is_admin(update.effective_user.id):
            return
        reseller_id = int(awaiting.get("reseller_id") or 0)
        reseller = _runtime_reseller_by_id(reseller_id)
        raw = value.replace(",", "").replace("٬", "").replace(" ", "")
        if not reseller or not raw.isdigit():
            context.user_data["awaiting"] = awaiting
            await update.message.reply_text("❌ مبلغ بدهی باید فقط عدد صحیح و غیرمنفی باشد.")
            return
        debt = int(raw)
        if debt > MAX_RESELLER_MONEY_TOMAN:
            context.user_data["awaiting"] = awaiting
            await update.message.reply_text("❌ مبلغ بدهی از سقف مجاز بیشتر است.")
            return
        context.user_data["reseller_debt_edit"] = {
            "reseller_id": reseller_id, "debt_toman": debt,
        }
        await update.message.reply_text(
            "⚠️ <b>تأیید تغییر بدهی</b>\n\n"
            f"قبلی: <b>{int(reseller.get('debt_toman') or 0):,} تومان</b>\n"
            f"جدید: <b>{debt:,} تومان</b>",
            parse_mode="HTML", reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ تأیید تغییر", callback_data=f"rssetok|{reseller_id}")],
                [InlineKeyboardButton("❌ انصراف", callback_data=f"rsdebt|{reseller_id}")],
            ]),
        )
        return

    if awaiting.get("kind") == "admin_inbound_add":
        if not is_admin(update.effective_user.id):
            return
        try:
            await run_blocking(add_inbound, value, admin_tg_id=update.effective_user.id, _lane="db")
        except ValueError as exc:
            context.user_data["awaiting"] = awaiting
            await update.message.reply_text(f"❌ {str(exc)}")
            return
        await update.message.reply_text(
            "✅ Inbound اضافه شد.", reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("Client Inbounds", callback_data="admin_inbounds")
            ]]),
        )
        return

    if awaiting.get("kind") == "admin_inbound_rename":
        if not is_admin(update.effective_user.id):
            return
        inbound_id = int(awaiting.get("inbound_id") or 0)
        remark = value.strip()
        if not remark or len(remark) > 128:
            context.user_data["awaiting"] = awaiting
            await update.message.reply_text("❌ نام Inbound باید بین 1 تا 128 کاراکتر باشد.")
            return
        context.user_data["admin_inbound_edit"] = {
            "inbound_id": inbound_id, "remark": remark,
        }
        await update.message.reply_text(
            f"⚠️ تغییر نام Inbound به «{html.escape(remark)}» را تأیید می‌کنید؟",
            parse_mode="HTML", reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ تأیید Rename", callback_data=f"admin_inbound_rename_confirm|{inbound_id}")],
                [InlineKeyboardButton("❌ انصراف", callback_data=f"admin_inbound|{inbound_id}")],
            ]),
        )
        return

    if awaiting.get("kind") == "admin_global_search":
        if not is_admin(update.effective_user.id):
            return
        value = value[:MAX_ADMIN_SEARCH_CHARS]
        results = await run_blocking(search_known_users, value, limit=20)
        if not results:
            context.user_data["awaiting"] = awaiting
            await update.message.reply_text(
                "❌ کاربری پیدا نشد. Telegram ID، نام، @username یا نام اکانت را دوباره ارسال کنید.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 کاربران", callback_data="admin_users_menu")]]),
            )
            return
        rows = []
        for user in results:
            tg_id = int(user.get("tg_id") or 0)
            label = str(user.get("label") or tg_id)
            matched = str(user.get("matched_account") or "")
            button = label + (f" | {matched}" if matched else "")
            rows.append([InlineKeyboardButton(button[:64], callback_data=f"admin_user|{tg_id}")])
        rows.append([InlineKeyboardButton("🔙 کاربران", callback_data="admin_users_menu")])
        await update.message.reply_text(
            f"🔎 <b>نتایج جستجو</b> — {len(results)} مورد",
            parse_mode="HTML", reply_markup=InlineKeyboardMarkup(rows),
        )
        return

    if awaiting.get("kind") == "admin_user_search":
        if not is_admin(update.effective_user.id):
            return
        value = value[:MAX_ADMIN_SEARCH_CHARS]
        results = await run_blocking(search_known_users, value, limit=20)
        if not results:
            context.user_data["awaiting"] = awaiting
            await update.message.reply_text(
                "❌ کاربری با این مشخصات پیدا نشد. دوباره نام، یوزرنیم یا Telegram ID را ارسال کنید.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 بازگشت", callback_data="admin_wallet_manage")]]),
            )
            return
        rows = []
        for user in results:
            tg_id = int(user.get("tg_id") or 0)
            label = str(user.get("label") or tg_id)
            rows.append([InlineKeyboardButton(label[:64], callback_data=f"admin_wallet_user|{tg_id}")])
        rows.append([InlineKeyboardButton("🔙 مدیریت کیف پول", callback_data="admin_wallet_manage")])
        await update.message.reply_text(
            f"🔎 <b>نتایج جستجو</b> — {len(results)} مورد\n\nکاربر موردنظر را انتخاب کنید:",
            parse_mode="HTML", reply_markup=InlineKeyboardMarkup(rows),
        )
        return

    if awaiting.get("kind") == "admin_wallet_amount":
        if not is_admin(update.effective_user.id):
            return
        raw = value.replace(",", "").replace("٬", "").replace(" ", "")
        try:
            amount = int(raw)
        except Exception:
            amount = 0
        if amount <= 0:
            context.user_data["awaiting"] = awaiting
            await update.message.reply_text("❌ مبلغ معتبر نیست. یک عدد بیشتر از صفر به تومان ارسال کنید.")
            return
        if amount > MAX_ADMIN_WALLET_ADJUST_TOMAN:
            context.user_data["awaiting"] = awaiting
            await update.message.reply_text(
                f"❌ حداکثر مبلغ هر تغییر {MAX_ADMIN_WALLET_ADJUST_TOMAN:,} تومان است. مبلغ دیگری ارسال کنید."
            )
            return
        user_tg_id = int(awaiting.get("user_tg_id") or 0)
        action = str(awaiting.get("action") or "")
        if action not in {"inc", "dec"} or user_tg_id <= 0:
            await update.message.reply_text("❌ اطلاعات عملیات نامعتبر است.", reply_markup=await main_menu_keyboard(update.effective_user.id))
            return
        before = (await run_blocking(wallet_balance, user_tg_id))
        reserved = (await run_blocking(reserved_wallet_for_user, user_tg_id))
        reducible = max(before - reserved, 0)
        if action == "dec" and amount > reducible:
            context.user_data["awaiting"] = awaiting
            await update.message.reply_text(
                f"❌ حداکثر مبلغ قابل کاهش <b>{reducible:,} تومان</b> است.\nمبلغ دیگری ارسال کنید:",
                parse_mode="HTML",
            )
            return
        after = before + (amount if action == "inc" else -amount)
        profile = await run_blocking(get_user_profile, user_tg_id)
        label = admin_user_label(user_tg_id, profile)
        verb = "افزایش" if action == "inc" else "کاهش"
        operation_id = secrets.token_hex(6)
        await update.message.reply_text(
            f"⚠️ <b>تأیید تغییر موجودی</b>\n\n"
            f"👤 {html.escape(label)}\n"
            f"عملیات: <b>{verb}</b>\n"
            f"مبلغ: <b>{amount:,} تومان</b>\n"
            f"موجودی قبلی: <b>{before:,} تومان</b>\n"
            f"موجودی جدید: <b>{after:,} تومان</b>",
            parse_mode="HTML",
            reply_markup=admin_wallet_confirm_keyboard(action, user_tg_id, amount, operation_id),
        )
        return

    if awaiting.get("kind") == "admin_referral_percent":
        if not is_admin(update.effective_user.id):
            return
        field = str(awaiting.get("field") or "")
        raw = value.replace(",", "").replace("٬", "").replace(" ", "")
        try:
            percent = int(raw)
        except Exception:
            percent = -1
        maximum = 100 if field == "discount" else 10_000
        if field not in {"discount", "reward"} or percent < 0 or percent > maximum:
            context.user_data["awaiting"] = awaiting
            await update.message.reply_text(
                f"❌ درصد معتبر نیست. یک عدد صحیح بین 0 تا {maximum:,} ارسال کنید.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 لغو", callback_data="admin_referral_settings")]]),
            )
            return
        current = current_referral_discount_percent() if field == "discount" else current_referral_reward_percent()
        title = "پاداش خریدار / تخفیف خرید اول" if field == "discount" else "پاداش معرف"
        context.user_data["admin_referral_edit"] = {"field": field, "value": percent}
        await update.message.reply_text(
            f"⚠️ <b>تأیید تغییر Referral</b>\n\n"
            f"{title}\n"
            f"مقدار قبلی: <b>{current}%</b>\n"
            f"مقدار جدید: <b>{percent}%</b>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ تأیید تغییر", callback_data=f"admin_referral_confirm|{field}|{percent}")],
                [InlineKeyboardButton("❌ انصراف", callback_data="admin_referral_settings")],
            ]),
        )
        return

    if awaiting.get("kind") == "admin_trial_edit":
        if not is_admin(update.effective_user.id):
            return
        field = str(awaiting.get("field") or "")
        if field not in {"gb", "days", "openvpn_profile"}:
            await update.message.reply_text(
                "⚠️ عملیات ویرایش تست منقضی شده است.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 بسته تست", callback_data="admin_trial_view")]]),
            )
            return
        if field == "openvpn_profile":
            new_value = value.strip()
            if not new_value or len(new_value) > MAX_ADMIN_PLAN_PROFILE_CHARS:
                context.user_data["awaiting"] = awaiting
                await update.message.reply_text(
                    f"❌ نام پکیج باید بین 1 تا {MAX_ADMIN_PLAN_PROFILE_CHARS} کاراکتر باشد. دوباره ارسال کنید."
                )
                return
            old_value = str(TEST_PLAN.get("openvpn_profile") or "")
            display_old = html.escape(old_value)
            display_new = html.escape(new_value)
            label = "پکیج MikroTik"
        else:
            raw = value.replace(",", "").replace("٬", "").replace(" ", "")
            try:
                new_value = int(raw)
            except Exception:
                new_value = 0
            maximum = MAX_ADMIN_PLAN_GB if field == "gb" else MAX_ADMIN_TRIAL_DAYS
            if new_value < 1 or new_value > maximum:
                context.user_data["awaiting"] = awaiting
                unit = "GB" if field == "gb" else "روز"
                await update.message.reply_text(
                    f"❌ مقدار معتبر نیست. عددی بین 1 و {maximum:,} {unit} ارسال کنید."
                )
                return
            old_value = int(TEST_PLAN.get(field) or 0)
            display_old = f"{old_value:,}"
            display_new = f"{new_value:,}"
            label = "حجم (GB)" if field == "gb" else "مدت (روز)"
        context.user_data["admin_trial_edit"] = {"field": field, "value": new_value}
        await update.message.reply_text(
            "⚠️ <b>تأیید ویرایش بسته تست</b>\n\n"
            f"{label}\n"
            f"قبلی: <code>{display_old}</code>\n"
            f"جدید: <code>{display_new}</code>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ تأیید تغییر", callback_data=f"admin_trial_edit_confirm|{field}")],
                [InlineKeyboardButton("❌ انصراف", callback_data="admin_trial_view")],
            ]),
        )
        return

    if awaiting.get("kind") == "admin_plan_add":
        if not is_admin(update.effective_user.id):
            return
        step = str(awaiting.get("step") or "")
        draft = dict(context.user_data.get("admin_plan_draft") or {})
        legacy_shared_flow = not draft.get("service") and not awaiting.get("service")
        service = str(draft.get("service") or awaiting.get("service") or "openvpn")
        if service not in {"openvpn", "v2ray"} or not service_sales_enabled(service):
            await update.message.reply_text(
                "⚠️ فروش این سرویس غیرفعال شده است.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 مدیریت بسته‌ها", callback_data="admin_plans|0")]]),
            )
            return
        draft["service"] = service
        total_steps = 4 if service == "openvpn" else 3
        if step in {"gb", "months", "price_toman"}:
            raw = value.replace(",", "").replace("٬", "").replace(" ", "")
            try:
                number = int(raw)
            except Exception:
                number = 0
            limits = {
                "gb": (1, MAX_ADMIN_PLAN_GB, "حجم به GB"),
                "months": (1, MAX_ADMIN_PLAN_MONTHS, "مدت به ماه"),
                "price_toman": (1, MAX_ADMIN_PLAN_PRICE_TOMAN, "قیمت به تومان"),
            }
            minimum, maximum, label = limits[step]
            if number < minimum or number > maximum:
                context.user_data["awaiting"] = awaiting
                await update.message.reply_text(
                    f"❌ {label} معتبر نیست. عددی بین {minimum:,} و {maximum:,} ارسال کنید."
                )
                return
            draft[step] = number
            context.user_data["admin_plan_draft"] = draft
            if step == "gb":
                context.user_data["awaiting"] = {"kind": "admin_plan_add", "step": "months", "service": service}
                await update.message.reply_text(
                    f"2/{total_steps} — مدت بسته را به <b>ماه</b> ارسال کنید.\nمثال: <code>1</code> یا <code>3</code>",
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 لغو", callback_data=f"admin_plans_service|{service}|0")]]),
                )
                return
            if step == "months":
                context.user_data["awaiting"] = {"kind": "admin_plan_add", "step": "price_toman", "service": service}
                await update.message.reply_text(
                    f"3/{total_steps} — قیمت بسته را به <b>تومان</b> ارسال کنید.\nمثال: <code>450000</code>",
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 لغو", callback_data=f"admin_plans_service|{service}|0")]]),
                )
                return
            if service == "v2ray":
                context.user_data.pop("awaiting", None)
                await update.message.reply_text(
                    _plan_draft_summary(draft), parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("✅ ثبت بسته V2Ray", callback_data="admin_plan_add_confirm|0")],
                        [InlineKeyboardButton("🔄 از ابتدا", callback_data="admin_plan_add_restart|v2ray")],
                        [InlineKeyboardButton("❌ انصراف", callback_data="admin_plans_service|v2ray|0")],
                    ]),
                )
                return
            context.user_data["awaiting"] = {"kind": "admin_plan_add", "step": "openvpn_profile", "service": service}
            await update.message.reply_text(
                "4/4 — <b>نام دقیق پکیج در MikroTik User Manager</b> را ارسال کنید.\n\n"
                "نام دقیقاً همان‌طور که ارسال می‌کنید ذخیره می‌شود و ربات از روی نام آن حجم یا مدت را تشخیص نمی‌دهد.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 لغو", callback_data="admin_plans_service|openvpn|0")]]),
            )
            return
        if step == "openvpn_profile":
            profile = value.strip()
            if not profile or len(profile) > MAX_ADMIN_PLAN_PROFILE_CHARS:
                context.user_data["awaiting"] = awaiting
                await update.message.reply_text(
                    f"❌ نام پکیج باید بین 1 تا {MAX_ADMIN_PLAN_PROFILE_CHARS} کاراکتر باشد. دوباره ارسال کنید."
                )
                return
            draft["openvpn_profile"] = profile
            context.user_data["admin_plan_draft"] = draft
            context.user_data.pop("awaiting", None)
            if legacy_shared_flow:
                rows = [[InlineKeyboardButton("✅ ثبت بسته", callback_data="admin_plan_add_confirm")]]
            elif service_sales_enabled("v2ray"):
                rows = [
                    [InlineKeyboardButton("✅ بله؛ برای هر دو ثبت شود", callback_data="admin_plan_add_confirm|1")],
                    [InlineKeyboardButton("فقط برای OpenVPN ثبت شود", callback_data="admin_plan_add_confirm|0")],
                ]
            else:
                rows = [[InlineKeyboardButton("✅ ثبت بسته OpenVPN", callback_data="admin_plan_add_confirm|0")]]
            rows.extend([
                [InlineKeyboardButton("🔄 از ابتدا", callback_data="admin_plan_add_restart|openvpn")],
                [InlineKeyboardButton("❌ انصراف", callback_data="admin_plans_service|openvpn|0")],
            ])
            await update.message.reply_text(
                _plan_draft_summary(draft),
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(rows),
            )
            return
        await update.message.reply_text("❌ مرحله افزودن بسته نامعتبر شد؛ دوباره شروع کنید.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("➕ افزودن بسته", callback_data=f"admin_plan_add|{service}")]]))
        return

    if awaiting.get("kind") == "admin_plan_edit":
        if not is_admin(update.effective_user.id):
            return
        service = str(awaiting.get("service") or "")
        plan_key = str(awaiting.get("plan_key") or "")
        field = str(awaiting.get("field") or "")
        plan = plans_for(service).get(plan_key) if service in {"openvpn", "v2ray"} else None
        allowed_fields = {"gb", "months", "price_toman"}
        if service == "openvpn":
            allowed_fields.add("openvpn_profile")
        if not plan or field not in allowed_fields:
            await update.message.reply_text("⚠️ بسته پیدا نشد یا عملیات منقضی شده است.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 بسته‌ها", callback_data="admin_plans|0")]]))
            return
        if field == "openvpn_profile":
            new_value = value.strip()
            if not new_value or len(new_value) > MAX_ADMIN_PLAN_PROFILE_CHARS:
                context.user_data["awaiting"] = awaiting
                await update.message.reply_text(f"❌ نام پکیج باید بین 1 تا {MAX_ADMIN_PLAN_PROFILE_CHARS} کاراکتر باشد. دوباره ارسال کنید.")
                return
            old_value = str(plan.get(field) or "")
            display_old = html.escape(old_value)
            display_new = html.escape(new_value)
        else:
            raw = value.replace(",", "").replace("٬", "").replace(" ", "")
            try:
                new_value = int(raw)
            except Exception:
                new_value = 0
            limits = {
                "gb": (1, MAX_ADMIN_PLAN_GB),
                "months": (1, MAX_ADMIN_PLAN_MONTHS),
                "price_toman": (1, MAX_ADMIN_PLAN_PRICE_TOMAN),
            }
            minimum, maximum = limits[field]
            if new_value < minimum or new_value > maximum:
                context.user_data["awaiting"] = awaiting
                await update.message.reply_text(f"❌ مقدار معتبر نیست. عددی بین {minimum:,} و {maximum:,} ارسال کنید.")
                return
            old_value = int(plan.get(field) or 0)
            display_old = f"{old_value:,}"
            display_new = f"{new_value:,}"
        context.user_data["admin_plan_edit"] = {"service": service, "plan_key": plan_key, "field": field, "value": new_value}
        labels = {"gb": "حجم (GB)", "months": "مدت (ماه)", "price_toman": "قیمت (تومان)", "openvpn_profile": "پکیج MikroTik"}
        await update.message.reply_text(
            "⚠️ <b>تأیید ویرایش بسته</b>\n\n"
            f"{labels[field]}\n"
            f"قبلی: <code>{display_old}</code>\n"
            f"جدید: <code>{display_new}</code>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ تأیید تغییر", callback_data=f"admin_plan_edit_confirm|{service}|{admin_plan_ref(plan_key, service)}|{field}")],
                [InlineKeyboardButton("❌ انصراف", callback_data=f"admin_plan_view|{service}|{admin_plan_ref(plan_key, service)}")],
            ]),
        )
        return

    # First-purchase referral code entry.
    if awaiting.get("kind") == "referral_code":
        try:
            tg_id = update.effective_user.id
            if is_reseller(tg_id):
                context.user_data.pop("first_buy_order", None)
                await update.message.reply_text(
                    "⚠️ Referral برای ریسلر فعال نیست.",
                    reply_markup=await main_menu_keyboard(tg_id),
                )
                return
            if (await run_blocking(has_completed_purchase, tg_id)) or (await run_blocking(referral_already_used, tg_id)):
                await update.message.reply_text(
                    "⚠️ تخفیف معرف فقط برای اولین خرید قابل استفاده است.",
                    reply_markup=await main_menu_keyboard(tg_id),
                )
                return
            referrer_tg_id = (await run_blocking(find_referrer_by_code, value))
            if not referrer_tg_id:
                context.user_data["awaiting"] = awaiting
                await update.message.reply_text(
                    "❌ کد معرف معتبر نیست. دوباره کد را ارسال کنید یا گزینه «بدون کد معرف» را بزنید.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("➡️ بدون کد معرف", callback_data="ref|none")]]),
                )
                return
            if int(referrer_tg_id) == int(tg_id):
                context.user_data["awaiting"] = awaiting
                await update.message.reply_text("❌ نمی‌توانید از کد معرف خودتان استفاده کنید.")
                return

            context.user_data["first_buy_order"] = {
                "service": awaiting["service"],
                "action": awaiting["action"],
                "plan_key": awaiting["plan_key"],
                "identifier": awaiting.get("identifier", ""),
            }
            await start_order_message(
                update.message, context, tg_id,
                awaiting["service"], awaiting["action"], awaiting["plan_key"], awaiting.get("identifier", ""),
                referral_code=value, referrer_tg_id=int(referrer_tg_id), edit=False,
            )
        except Exception:
            logger.exception("referral code flow error")
            await update.message.reply_text(
                "❌ انجام این عملیات با خطا روبه‌رو شد. لطفاً دوباره تلاش کنید.",
                reply_markup=await main_menu_keyboard(update.effective_user.id),
            )
        return

    # Existing manual account lookup flow.
    action = awaiting["action"]
    service = awaiting["service"]
    identifier = value
    if len(identifier) > MAX_ACCOUNT_IDENTIFIER_CHARS:
        context.user_data["awaiting"] = awaiting
        await update.message.reply_text(
            f"❌ شناسه اکانت نباید بیشتر از {MAX_ACCOUNT_IDENTIFIER_CHARS} کاراکتر باشد. دوباره ارسال کنید.",
            reply_markup=back_service(service),
        )
        return
    try:
        if service == "openvpn":
            info = await run_blocking_retry(mikrotik.fetch_usage_and_expiry, identifier)
            if not info.get("found"):
                await update.message.reply_text("❌ این یوزرنیم پیدا نشد.", reply_markup=back_service(service))
                return
            (await run_blocking(upsert_account, update.effective_user.id, "openvpn", info.get("matched_name") or identifier, username=info.get("matched_name") or identifier, password=info.get("password") or "", profile=info.get("profile") or ""))
            identifier = info.get("matched_name") or identifier
        else:
            hydrated = await run_blocking_retry(XUIClient().get_client, identifier)
            c = hydrated["client"]
            (await run_blocking(upsert_account, update.effective_user.id, "v2ray", identifier, sub_id=c.get("subId", "")))

        context.user_data["selected_identifier"] = identifier
        context.user_data["service"] = service
        if action == "status":
            await show_status(update.message, update.effective_user.id, service, identifier, edit=False)
        else:
            await update.message.reply_text(
                f"♻️ تمدید <code>{html.escape(identifier)}</code>\n\nپلن جدید را انتخاب کنید:",
                parse_mode="HTML",
                reply_markup=plans_keyboard("renew", service),
            )
    except Exception:
        logger.exception("manual lookup error")
        await update.message.reply_text(
            "❌ دریافت اطلاعات اکانت با خطا روبه‌رو شد؛ لطفاً دوباره تلاش کنید.",
            reply_markup=back_service(service),
        )


async def show_status(message, tg_id: int, service: str, identifier: str, edit: bool = True, markup=None, force_refresh: bool = False):
    cache_key = (str(service), str(identifier))
    cached = None if force_refresh else STATUS_CACHE.get(cache_key)

    if service == "openvpn":
        info = cached if isinstance(cached, dict) else None
        if info is None:
            info = await run_blocking_retry(mikrotik.fetch_usage_and_expiry, identifier)
            STATUS_CACHE.set(cache_key, info, STATUS_CACHE_TTL_SECONDS)
        if not info.get("found"):
            text = "❌ اکانت پیدا نشد."
        else:
            local = next((a for a in (await run_blocking(list_accounts, tg_id, "openvpn")) if a.get("identifier") == identifier), {})
            text = render_openvpn_status(info, identifier, local)
    else:
        st = cached if isinstance(cached, dict) else None
        if st is None:
            st = await run_blocking_retry(XUIClient().status, identifier)
            STATUS_CACHE.set(cache_key, st, STATUS_CACHE_TTL_SECONDS)
        days = math.ceil(st["remaining_days_float"]) if st["remaining_days_float"] > 0 else 0
        if st.get("waiting_first_use"):
            state_line = "🟡 <b>فعال نشده</b>\n⏱ شروع اعتبار: <b>از اولین استفاده</b>"
        elif st["active"]:
            state_line = "✅ <b>فعال</b>"
        else:
            state_line = "❌ <b>منقضی</b>"
        text = (
            f"📊 <b>وضعیت V2Ray</b>\n\n"
            f"👤 <code>{html.escape(identifier)}</code>\n"
            f"📦 حجم باقی‌مانده: <b>{human_bytes(st['remaining_bytes'])}</b>\n"
            f"📅 روز باقی‌مانده: <b>{days} روز</b>\n"
            f"{state_line}"
        )
    markup = markup or back_service(service)
    if edit and hasattr(message, "edit_text"):
        await message.edit_text(text, parse_mode="HTML", reply_markup=markup)
    else:
        await message.reply_text(text, parse_mode="HTML", reply_markup=markup)


async def unique_openvpn_username() -> str:
    for _ in range(20):
        candidate = generate_username()
        if not await run_blocking(mikrotik.user_exists, candidate):
            return candidate
    raise RuntimeError("نام کاربری یکتا ساخته نشد")


async def unique_v2ray_email(xui: XUIClient) -> str:
    for _ in range(20):
        candidate = generate_username()
        found = await run_blocking_retry(xui.get_client_optional, candidate)
        if found is None:
            return candidate
    raise RuntimeError("نام Client یکتا ساخته نشد")


async def _recover_prepared_buy(journal: dict, service: str):
    """Resolve an 'executing' BUY after a worker crash without duplicating a remote write."""
    ident = str(journal.get("delivery_identifier") or "")
    if not ident:
        return None
    if service == "openvpn":
        # OpenVPN creation is idempotent in services.mikrotik. Re-running it can
        # safely repair a user that was added just before a crash but never got
        # its profile assignment.
        return False
    xui = XUIClient()
    found = await run_blocking_retry(xui.get_client_optional, ident)
    if found is None:
        return False
    client = found.get("client") if isinstance(found, dict) else None
    expected_tg_id = int(journal.get("tg_id") or 0)
    actual_tg_id = int((client or {}).get("tgId") or 0) if isinstance(client, dict) else 0
    comment = str((client or {}).get("comment") or "") if isinstance(client, dict) else ""
    if expected_tg_id and (actual_tg_id == expected_tg_id or comment == f"Telegram:{expected_tg_id}"):
        return found
    raise RuntimeError("شناسه V2Ray این سفارش متعلق به Client دیگری است؛ نیاز به بررسی مدیر دارد.")


def _journal_age_seconds(journal: dict) -> float:
    try:
        updated = datetime.fromisoformat(str(journal.get("updated_at") or "").replace("Z", "+00:00"))
        if updated.tzinfo is None:
            updated = updated.replace(tzinfo=timezone.utc)
        return max((datetime.now(timezone.utc) - updated).total_seconds(), 0.0)
    except Exception:
        return 0.0


def _claim_remote_write_sync(order_id: str):
    """Synchronous write barrier called inside a service worker.

    Service clients invoke this after all safe preliminary reads and immediately
    before their first mutation, minimizing false ambiguous journals.
    """
    if not order_id:
        return
    if mark_fulfillment_executing(order_id):
        return
    current = get_fulfillment(order_id)
    state = str((current or {}).get("state") or "")
    raise RuntimeError(
        f"این سفارش هم‌اکنون در حال پردازش است (وضعیت: {state or 'نامشخص'}). "
        "چند لحظه بعد دوباره بررسی کنید."
    )


def _write_claim_callback(order_id: str):
    return functools.partial(_claim_remote_write_sync, order_id) if order_id else None


def _remote_done_callback(order_id: str):
    return functools.partial(mark_fulfillment_remote_done, order_id) if order_id else None


def _delivery_from_journal(journal: dict):
    result = dict(journal.get("result") or {})
    text = str(result.get("text") or "")
    if not text:
        return None
    service = str(result.get("service") or journal.get("service") or "")
    identifier = str(result.get("identifier") or journal.get("delivery_identifier") or "")
    if service == "openvpn":
        password = str(result.get("password") or (journal.get("secret") or {}).get("password") or "")
        markup = openvpn_credentials_keyboard(identifier, password) if identifier and password else back_service("openvpn")
    else:
        markup = back_service("v2ray")
    return text, markup


async def fulfill(
    service: str,
    action: str,
    plan_key: str,
    tg_id: int,
    identifier: str,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    plan_override: dict | None = None,
    order_id: str = "",
    is_test: bool = False,
):
    """Provision/renew exactly once per financial order as far as remote APIs permit.

    Remote writes are journaled. A provisioned/completed order is replayed from
    SQLite without touching RouterOS/3x-ui again. If the worker dies while a
    renewal write is in-flight, v2 intentionally refuses an automatic second
    write because V2Ray renewal is additive and could otherwise double quota.
    """
    p = dict(plan_override or plans_for(service)[plan_key])
    order_id = str(order_id or "").strip()
    journal = (await run_blocking(get_fulfillment, order_id)) if order_id else None

    if journal and journal.get("state") in {"provisioned", "completed"}:
        cached = _delivery_from_journal(journal)
        if cached:
            return cached

    if journal and journal.get("state") == "executing":
        # BUY has a stable generated identifier, so we can safely inspect whether
        # it exists remotely. RENEW is potentially additive; never guess/retry it.
        if action != "buy":
            raise RuntimeError(
                f"سفارش {order_id} هنگام تمدید در وضعیت نامشخص متوقف شده است؛ "
                "برای جلوگیری از تمدید دوباره، نیاز به بررسی مدیر دارد."
            )
        if _journal_age_seconds(journal) < 120:
            raise RuntimeError(
                "ساخت اکانت این سفارش هنوز در حال پردازش است؛ دو دقیقه بعد دوباره بررسی کنید."
            )
        recovered = await _recover_prepared_buy(journal, service)
        if recovered:
            (await run_blocking(mark_fulfillment_remote_done, order_id, {"recovered_after_restart": True}))
            journal = (await run_blocking(get_fulfillment, order_id))
        else:
            # A successful authoritative read proved that the generated account
            # does not exist; it is safe to attempt this BUY once.
            (await run_blocking(mark_fulfillment_prepared, order_id, {"safe_retry_after_remote_absence": True}))
            journal = (await run_blocking(get_fulfillment, order_id))

    result_markup = back_service(service)
    remote_done = bool(journal and journal.get("state") == "remote_done")

    if service == "openvpn":
        if action == "buy":
            if order_id:
                if not journal:
                    username = await unique_openvpn_username()
                    password = generate_password_numeric()
                    journal = (await run_blocking(prepare_fulfillment, order_id, tg_id=tg_id, service=service, action=action, requested_identifier=identifier, delivery_identifier=username, secret={"password": password}, meta={"plan_key": plan_key, "plan": p, "is_test": bool(is_test)}))
                username = str(journal.get("delivery_identifier") or "")
                password = str((journal.get("secret") or {}).get("password") or "")
                if not username or not password:
                    raise RuntimeError("اطلاعات تحویل OpenVPN در Journal ناقص است")
                remote_done = journal.get("state") == "remote_done"
            else:
                username = await unique_openvpn_username()
                password = generate_password_numeric()

            if not remote_done:
                await run_blocking(
                    mikrotik.create_user_with_profile,
                    username,
                    password,
                    p["openvpn_profile"],
                    before_write=_write_claim_callback(order_id),
                    after_write=_remote_done_callback(order_id),
                )
                if order_id:
                    (await run_blocking(mark_fulfillment_remote_done, order_id))
        else:
            username = identifier
            if order_id and not journal:
                journal = (await run_blocking(prepare_fulfillment, order_id, tg_id=tg_id, service=service, action=action, requested_identifier=identifier, delivery_identifier=username, meta={"plan_key": plan_key, "plan": p}))
            remote_done = bool(journal and journal.get("state") == "remote_done")
            if not remote_done:
                if not await run_blocking_retry(mikrotik.user_exists, username):
                    raise RuntimeError("اکانت OpenVPN پیدا نشد")
                password = await run_blocking(
                    mikrotik.ensure_user_exists_and_assign,
                    username, p["openvpn_profile"], generate_password_numeric,
                    before_write=_write_claim_callback(order_id),
                    after_write=_remote_done_callback(order_id),
                )
                if order_id:
                    (await run_blocking(prepare_fulfillment, order_id, tg_id=tg_id, service=service, action=action, requested_identifier=identifier, delivery_identifier=username, secret={"password": password}))
                    (await run_blocking(mark_fulfillment_remote_done, order_id))
            else:
                password = str((journal.get("secret") or {}).get("password") or "")
                if not password:
                    info = await run_blocking_retry(mikrotik.fetch_usage_and_expiry, username)
                    password = str(info.get("password") or "")
                    if password and order_id:
                        (await run_blocking(prepare_fulfillment, order_id, tg_id=tg_id, service=service, action=action, requested_identifier=identifier, delivery_identifier=username, secret={"password": password}))

        (await run_blocking(upsert_account, tg_id, "openvpn", username, username=username, password=password, profile=p["openvpn_profile"], plan_key=plan_key, is_test=bool(is_test)))
        STATUS_CACHE.invalidate(("openvpn", username))
        openvpn_title = (
            "🎁 <b>اکانت تست OpenVPN</b>\n\n"
            if is_test
            else f"✅ اکانت OpenVPN {'تمدید' if action == 'renew' else 'ساخته'} شد.\n\n"
        )
        result_text = (
            openvpn_title +
            f"👤 <b>یوزرنیم</b>\n{text_code_block(username)}"
            f"🔑 <b>پسورد</b>\n{text_code_block(password)}"
            f"📦 {p['gb']} گیگ - {_plan_duration_text(p)}"
        )
        result_markup = openvpn_credentials_keyboard(username, password)
        delivery_identifier = username
        result_for_journal = {
            "service": "openvpn", "identifier": username, "password": password,
            "text": result_text, "plan_key": plan_key,
        }
    else:
        xui = XUIClient()
        if action == "buy":
            if order_id:
                if not journal:
                    email = await unique_v2ray_email(xui)
                    journal = (await run_blocking(prepare_fulfillment, order_id, tg_id=tg_id, service=service, action=action, requested_identifier=identifier, delivery_identifier=email, meta={"plan_key": plan_key, "plan": p, "is_test": bool(is_test)}))
                email = str(journal.get("delivery_identifier") or "")
                if not email:
                    raise RuntimeError("شناسه تحویل V2Ray در Journal ناقص است")
                remote_done = journal.get("state") == "remote_done"
            else:
                email = await unique_v2ray_email(xui)

            hydrated = None
            if remote_done:
                hydrated = await run_blocking_retry(xui.get_client, email)
            else:
                existing = await run_blocking_retry(xui.get_client_optional, email)
                if existing is not None:
                    existing_client = existing.get("client") if isinstance(existing, dict) else None
                    actual_tg_id = int((existing_client or {}).get("tgId") or 0) if isinstance(existing_client, dict) else 0
                    comment = str((existing_client or {}).get("comment") or "") if isinstance(existing_client, dict) else ""
                    if actual_tg_id != int(tg_id) and comment != f"Telegram:{int(tg_id)}":
                        raise RuntimeError("نام Client هم‌زمان توسط اکانت دیگری استفاده شده است؛ دوباره تلاش کنید.")
                    hydrated = existing
                else:
                    hydrated = await run_blocking(
                        xui.create_client, email, tg_id, p["gb"], p["days"],
                        before_write=_write_claim_callback(order_id),
                        after_write=_remote_done_callback(order_id),
                    )
                if order_id:
                    (await run_blocking(mark_fulfillment_remote_done, order_id))
        else:
            email = identifier
            if order_id and not journal:
                journal = (await run_blocking(prepare_fulfillment, order_id, tg_id=tg_id, service=service, action=action, requested_identifier=identifier, delivery_identifier=email, meta={"plan_key": plan_key, "plan": p}))
            remote_done = bool(journal and journal.get("state") == "remote_done")
            if remote_done:
                hydrated = await run_blocking_retry(xui.get_client, email)
            else:
                # renew() can be additive. Journal 'executing' before the call and
                # 'remote_done' immediately after it returns successfully.
                await run_blocking(
                    xui.renew, email, p["gb"], p["days"],
                    before_write=_write_claim_callback(order_id),
                    after_write=_remote_done_callback(order_id),
                )
                if order_id:
                    (await run_blocking(mark_fulfillment_remote_done, order_id))
                hydrated = await run_blocking_retry(xui.get_client, email)

        c = hydrated["client"]
        sub_id = c.get("subId") or ""
        links = await run_blocking_retry(xui.links, email)
        # The public subscription base is snapshotted into the account row at
        # creation. Renewals must preserve that exact historical URL even when
        # Admin changes Change Subscription URL later.
        existing_sub_url = ""
        if action == "renew":
            existing_rows = await run_blocking(list_accounts, tg_id, "v2ray")
            existing_local = next(
                (row for row in existing_rows if str(row.get("identifier") or "") == email),
                {},
            )
            existing_sub_url = str(existing_local.get("sub_url") or "")
        sub_url = existing_sub_url or await run_blocking_retry(xui.subscription_url, sub_id)
        (await run_blocking(upsert_account, tg_id, "v2ray", email, sub_id=sub_id, sub_url=sub_url, links=links, plan_key=plan_key, is_test=bool(is_test)))
        STATUS_CACHE.invalidate(("v2ray", email))
        result_text = v2ray_delivery_text(
            "🎁 <b>اکانت تست V2Ray</b>" if is_test else f"✅ اکانت V2Ray {'تمدید' if action == 'renew' else 'ساخته'} شد.",
            email, p["gb"], p["days"], sub_url, links,
        )
        delivery_identifier = email
        result_for_journal = {
            "service": "v2ray", "identifier": email, "sub_id": sub_id,
            "sub_url": sub_url, "links": links, "text": result_text, "plan_key": plan_key,
        }

    if order_id:
        saved = await run_blocking(
            mark_fulfillment_provisioned, order_id, result_for_journal
        )
        if not saved:
            # Never claim a recoverable delivery unless its replay payload is
            # durably in SQLite. A concurrent handler may already have advanced
            # it; in that case use the stored result instead.
            current = await run_blocking(get_fulfillment, order_id)
            cached = _delivery_from_journal(current or {})
            if cached:
                return cached
            raise RuntimeError("ثبت امن نتیجه تحویل در دیتابیس کامل نشد؛ سفارش محفوظ ماند.")

    if is_test:
        buyer_profile = await run_blocking(get_user_profile, tg_id)
        await notify_admins(
            context,
            "🎁 <b>اکانت تست ساخته شد</b>\n"
            f"سرویس: {SERVICE_LABEL[service]}\n"
            f"پلن: {p['gb']}GB / {p['days']} روز\n\n"
            f"{buyer_info_text(tg_id, buyer_profile)}",
            reply_markup=admin_account_keyboard(service, tg_id, delivery_identifier),
        )
    if is_admin(tg_id):
        await run_blocking(
            record_admin_audit, admin_tg_id=tg_id, target_tg_id=tg_id,
            action=f"admin_{'test' if is_test else action}_{service}",
            after={"identifier": delivery_identifier, "plan_key": plan_key, "order_id": order_id},
        )
    return result_text, result_markup


async def finalize_successful_order(payload: dict):
    """Finalize successful order bookkeeping idempotently after service delivery succeeds."""
    financial_result = {}
    if str(payload.get("payment_kind") or "") == "reseller_debt":
        financial_result = await run_blocking(charge_reseller_order, payload, _lane="db")
    # v1.8 admin ledger: both successful buys and renewals are transactions.
    (await run_blocking(record_transaction, payload))
    if payload.get("action") != "buy":
        return financial_result

    tg_id = int(payload["tg_id"])
    order_id = str(payload.get("order_id") or f"legacy-{tg_id}-{payload.get('ts', 0)}")
    base_price = int(
        payload.get("base_price_toman")
        or plans_for(str(payload.get("service") or "openvpn"))[payload["plan_key"]]["price_toman"]
    )
    referral_code = str(payload.get("referral_code") or "")
    referrer_tg_id = int(payload.get("referrer_tg_id") or 0)

    if referral_code and referrer_tg_id:
        if referrer_tg_id == tg_id:
            raise RuntimeError("کد معرف خود کاربر قابل استفاده نیست")
        # Same-order retries are harmless; a different prior referral is rejected.
        (await run_blocking(mark_referral_used, tg_id, order_id=order_id, code=referral_code, referrer_tg_id=referrer_tg_id))

    (await run_blocking(record_purchase, tg_id, order_id, service=payload["service"], plan_key=payload["plan_key"], base_price_toman=base_price, referral_code=referral_code, referrer_tg_id=referrer_tg_id))
    _PURCHASE_STATUS_CACHE.set(tg_id, True, 300.0)

    if referral_code and referrer_tg_id:
        reward_percent = int(payload.get("referral_reward_percent") if payload.get("referral_reward_percent") is not None else REFERRAL_REWARD_PERCENT)
        reward_amount = (base_price * reward_percent) // 100
        (await run_blocking(credit_referral_reward, referrer_tg_id, reward_amount, order_id=order_id, buyer_tg_id=tg_id))
    return financial_result


def successful_order_admin_text(
    payload: dict, paid_plan: dict, buyer_profile: dict, financial_result: dict | None = None
) -> str:
    """Render one exact financial receipt for the sole Admin."""
    financial = dict(financial_result) if isinstance(financial_result, dict) else {}
    payment_kind = str(payload.get("payment_kind") or "gateway").strip().lower()
    action = "تمدید" if str(payload.get("action") or "") == "renew" else "خرید"
    service = str(payload.get("service") or "")
    base = int(payload.get("base_price_toman") or paid_plan.get("price_toman") or 0)
    wallet = int(payload.get("wallet_used_toman") or 0)
    gateway = int(payload.get("gateway_toman") or 0)
    discount = int(payload.get("referral_discount_toman") or 0)

    if payment_kind == "owner":
        sale_type = "خرید توسط مدیر"
        financial_lines = ["مبلغ خرید: <b>رایگان</b>"]
    elif payment_kind == "reseller_debt":
        sale_type = "فروش ریسلر"
        added = int(financial.get("added_toman") or payload.get("reseller_charge_toman") or 0)
        after = int(financial.get("after_toman") or 0)
        financial_lines = [
            f"ریسلر: <b>{html.escape(str(payload.get('reseller_name') or ''))}</b> "
            f"(<code>{int(payload.get('reseller_tg_id') or payload.get('tg_id') or 0)}</code>)",
            f"هزینه هر گیگ: <b>{int(payload.get('reseller_price_per_gb_toman') or 0):,} تومان</b>",
            f"مبلغ افزوده‌شده به بدهی: <b>{added:,} تومان</b>",
            f"بدهی جدید ریسلر: <b>{after:,} تومان</b>",
        ]
    else:
        sale_type = "فروش مستقیم"
        components = []
        method = str(payload.get("payment_authorization_method") or "").strip().lower()
        if gateway:
            gateway_label = "کارت به کارت" if method == "card_transfer" else "زرین‌پال"
            components.append(f"{gateway:,} تومان {gateway_label}")
        if wallet:
            components.append(f"{wallet:,} تومان اعتبار کیف پول")
        if not components:
            components.append("0 تومان")
        financial_lines = [f"روش پرداخت: <b>{' + '.join(components)}</b>"]
        if discount:
            financial_lines.append(f"تخفیف Referral: <b>{discount:,} تومان</b>")

    return (
        f"🧾 <b>{action} موفق</b>\n"
        f"نوع فروش: <b>{sale_type}</b>\n"
        f"سرویس: <b>{html.escape(SERVICE_LABEL.get(service, service))}</b>\n"
        f"پلن: <b>{int(paid_plan.get('gb') or 0)}GB / {int(paid_plan.get('days') or 0)} روز</b>\n"
        f"قیمت عمومی بسته: <b>{base:,} تومان</b>\n"
        + "\n".join(financial_lines)
        + "\n\n"
        + buyer_info_text(int(payload.get("tg_id") or 0), buyer_profile)
        + f"\nOrder: <code>{html.escape(str(payload.get('order_id') or ''))}</code>"
    )


async def order_price_breakdown(
    tg_id: int, plan_key: str, *, service: str = "openvpn", referral_code: str = ""
) -> dict:
    base_price = int(plans_for(service)[plan_key]["price_toman"])
    discount_percent = current_referral_discount_percent()
    referral_discount = (base_price * discount_percent) // 100 if referral_code else 0
    after_discount = max(base_price - referral_discount, 0)
    available = await run_blocking(wallet_available, tg_id)
    wallet_used = min(available, after_discount)
    gateway_toman = max(after_discount - wallet_used, 0)
    return {
        "base_price_toman": base_price,
        "referral_discount_percent": discount_percent if referral_code else 0,
        "referral_discount_toman": referral_discount,
        "after_discount_toman": after_discount,
        "wallet_used_toman": wallet_used,
        "gateway_toman": gateway_toman,
    }


def order_summary_text(
    plan_key: str, breakdown: dict, *, service: str = "openvpn",
    referral_code: str = "", gateway_url: str = "",
    plan_override: dict | None = None,
) -> str:
    p = dict(plan_override or plans_for(service)[plan_key])
    text = (
        f"📦 <b>{p['gb']} گیگ - {_plan_duration_text(p)}</b>\n\n"
        f"💵 مبلغ اصلی: <b>{breakdown['base_price_toman']:,} تومان</b>\n"
    )
    if referral_code:
        text += f"🎟 تخفیف کد معرف: <b>-{breakdown['referral_discount_toman']:,} تومان</b>\n"
    if breakdown["wallet_used_toman"]:
        text += f"💰 پرداخت از کیف پول: <b>-{breakdown['wallet_used_toman']:,} تومان</b>\n"
    if breakdown["gateway_toman"]:
        text += f"💳 مبلغ قابل پرداخت در درگاه: <b>{breakdown['gateway_toman']:,} تومان</b>\n"
    else:
        text += "✅ مبلغ قابل پرداخت در درگاه: <b>۰ تومان</b>\n"
    if gateway_url:
        text += f"\nپرداخت را انجام دهید و سپس روی «پرداخت کردم» بزنید:\n{gateway_url}"
    return text


def pending_payment_text(pending: dict, notice: str = "") -> str:
    """Rebuild the current payment page so gateway feedback edits one message."""
    paid_plan = snapshot_for_delivery(pending)
    gateway_toman = int(pending.get("gateway_toman") or (int(pending.get("amount_rial") or 0) // 10))
    breakdown = {
        "base_price_toman": int(pending.get("base_price_toman") or paid_plan.get("price_toman") or gateway_toman),
        "referral_discount_toman": int(pending.get("referral_discount_toman") or 0),
        "wallet_used_toman": int(pending.get("wallet_used_toman") or 0),
        "gateway_toman": gateway_toman,
    }
    text = order_summary_text(
        str(pending.get("plan_key") or ""),
        breakdown,
        service=str(pending.get("service") or "openvpn"),
        referral_code=str(pending.get("referral_code") or ""),
        gateway_url=str(pending.get("payment_url") or ""),
        plan_override=paid_plan,
    )
    if notice:
        text += "\n\n" + str(notice)
    return text


def _pending_is_local(pending: dict) -> bool:
    return str((pending or {}).get("payment_kind") or "") in {
        "wallet", "admin", "owner", "reseller_debt"
    }


def _pending_is_preflight(pending: dict) -> bool:
    return str((pending or {}).get("payment_kind") or "") == "preflight"


def _pending_is_card_transfer(pending: dict) -> bool:
    return str((pending or {}).get("payment_kind") or "") == "card_transfer"


def _local_authority(payment_kind: str, order_id: str) -> str:
    return f"local-{str(payment_kind)}-{str(order_id)}"


def _pending_amount_rial(pending: dict) -> int:
    amount = int(pending.get("amount_rial") or 0)
    if amount > 0:
        return amount
    gateway_toman = int(pending.get("gateway_toman") or 0)
    if gateway_toman > 0:
        return gateway_toman * 10
    plan_key = str(pending.get("plan_key") or "")
    service = str(pending.get("service") or "openvpn")
    if service in {"openvpn", "v2ray"} and plan_key in plans_for(service):
        return price_rial(plan_key, service)
    raise RuntimeError("مبلغ سفارش قدیمی قابل تشخیص نیست")


async def reconcile_stale_pending(authority: str, pending: dict, message, context) -> str:
    """Handle an ENV-changed pending order safely using v2.0.4 gateway semantics."""
    if not pending_plan_is_stale(pending):
        return "current"

    if _pending_is_local(pending):
        try:
            # A wallet/admin order is already authorized locally. Honor the paid
            # package snapshot instead of asking ZarinPal about a synthetic ID.
            await _deliver_verified_pending(authority, pending, message, context)
            return "paid"
        except Exception as exc:
            logger.warning("stale local order delivery failed for %s: %s", authority, exc)
            await message.reply_text(
                "⚠️ سفارش قبلی محفوظ است اما تحویل آن کامل نشد. از بخش کیف پول، «ادامه تحویل سفارش» را بزنید."
            )
            return "blocked"

    if _pending_is_card_transfer(pending):
        request = await run_blocking(get_card_transfer_request_by_authority, authority)
        status = str((request or {}).get("status") or "")
        await message.reply_text(
            "🧾 درخواست کارت به کارت قبلی شما "
            + ("در انتظار ارسال رسید است." if status == "awaiting_receipt" else "در انتظار بررسی ادمین است.")
            + "\nابتدا همان درخواست را تکمیل کنید.",
        )
        return "blocked"

    try:
        result = await run_zarinpal(verify_payment, authority, _pending_amount_rial(pending))
        code = _zarinpal_result_code(result)
    except Exception as exc:
        logger.warning("stale pending verification failed for %s: %s", authority, exc)
        await message.reply_text(
            "⚠️ مشخصات یا قیمت این سفارش تغییر کرده، اما فعلاً نتوانستم وضعیت پرداخت قبلی را بررسی کنم. "
            "برای جلوگیری از مشکل مالی، سفارش قبلی فعلاً نگه داشته شد؛ دوباره «پرداخت کردم» را بزنید.",
            reply_markup=pay_keyboard(str(pending.get("payment_url") or "")),
        )
        return "blocked"

    if code in (100, 101):
        await _authorize_zarinpal_and_deliver(
            authority, pending, message, context, verification_code=code
        )
        return "paid"

    if _zarinpal_is_definitely_unpaid(result):
        await run_blocking(pop_pending, authority)
        return "removed"

    logger.warning("stale pending verification inconclusive for %s: %r", authority, result)
    await message.reply_text(
        "⚠️ وضعیت پرداخت قبلی هنوز قطعی نیست و برای جلوگیری از حذف اشتباه سفارش، "
        "سفارش نگه داشته شد. چند لحظه بعد دوباره «پرداخت کردم» را بزنید.",
        reply_markup=pay_keyboard(str(pending.get("payment_url") or "")),
    )
    return "blocked"

async def start_order(q, context, service: str, action: str, plan_key: str, identifier: str,
                      referral_code: str = "", referrer_tg_id: int = 0,
                      payment_method: str = ""):
    await start_order_message(
        q.message, context, q.from_user.id,
        service, action, plan_key, identifier,
        referral_code=referral_code, referrer_tg_id=referrer_tg_id,
        payment_method=payment_method, edit=True,
    )


async def start_order_message(message, context, tg_id: int, service: str, action: str, plan_key: str, identifier: str,
                              *, referral_code: str = "", referrer_tg_id: int = 0,
                              payment_method: str = "", edit: bool = True):
    tg_id = int(tg_id)
    if not service_sales_enabled(service):
        text = "⚠️ فروش این سرویس در حال حاضر غیرفعال است."
        markup = await main_menu_keyboard(tg_id)
        if edit and hasattr(message, "edit_text"):
            await safe_edit_text(message, text, reply_markup=markup)
        else:
            await message.reply_text(text, reply_markup=markup)
        return
    registry = plans_for(service)
    if plan_key not in registry:
        raise RuntimeError("پلن نامعتبر است")
    p = registry[plan_key]
    current_plan_snapshot = plan_snapshot(plan_key, service)

    # Gateway orders follow v2.0.4 semantics: an old gateway pending does not
    # globally block every later order.  The only gateway lock kept by v2.0.4
    # is the first-purchase lock (handled below).  Local wallet/admin orders are
    # different: they may be mid-fulfillment and must remain recoverable.
    existing_authority, existing = await run_blocking(latest_pending_for_user, tg_id)
    if existing and _pending_is_preflight(existing):
        # Compatibility cleanup for interrupted v2.2 gateway-link preflights.
        await run_blocking(pop_pending, existing_authority)
        existing = None
    if existing and _pending_is_local(existing):
        pending_kind = str(existing.get("payment_kind") or "")
        text = "⚠️ یک سفارش قبلی هنوز در انتظار تکمیل است. ابتدا همان سفارش را تحویل بگیرید یا لغو کنید."
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 ادامه تحویل سفارش", callback_data="payment|check")],
            [InlineKeyboardButton("❌ لغو سفارش قبلی", callback_data="payment|cancel")],
            [InlineKeyboardButton("🔙 منوی اصلی", callback_data="home")],
        ])
        if edit and hasattr(message, "edit_text"):
            await safe_edit_text(message, text, reply_markup=markup)
        else:
            await message.reply_text(text, reply_markup=markup)
        return
    if existing and _pending_is_card_transfer(existing):
        request = await run_blocking(get_card_transfer_request_by_authority, existing_authority)
        status = str((request or {}).get("status") or "")
        text = (
            "🧾 یک درخواست کارت به کارت قبلی در انتظار ارسال رسید است."
            if status == "awaiting_receipt"
            else "⏳ رسید کارت به کارت قبلی شما در انتظار بررسی ادمین است."
        )
        rows = []
        if status == "awaiting_receipt" and int((request or {}).get("id") or 0):
            rows.append([InlineKeyboardButton(
                "❌ لغو درخواست", callback_data=f"cardpay|cancel|{int(request['id'])}"
            )])
        rows.append([InlineKeyboardButton("🔙 منوی اصلی", callback_data="home")])
        markup = InlineKeyboardMarkup(rows)
        if edit and hasattr(message, "edit_text"):
            await safe_edit_text(message, text, reply_markup=markup)
        else:
            await message.reply_text(text, reply_markup=markup)
        return

    # The sole ENV Admin and active resellers bypass payment gateways. Their
    # authorization is persisted before provisioning, so retries remain safe.
    role = "owner" if is_admin(tg_id) else ("reseller" if is_reseller(tg_id) else "")
    if role:
        order_id = f"ord-{tg_id}-{secrets.token_hex(10)}"
        common_payload = {
            "tg_id": tg_id,
            "service": service,
            "action": action,
            "plan_key": plan_key,
            "identifier": identifier,
            "order_id": order_id,
            "first_purchase": False,
            "base_price_toman": int(p.get("price_toman") or 0),
            "referral_discount_toman": 0,
            "wallet_used_toman": 0,
            "gateway_toman": 0,
            "wallet_committed": False,
            "referral_code": "",
            "referrer_tg_id": 0,
            "referral_discount_percent": 0,
            "referral_reward_percent": 0,
            "plan_snapshot": current_plan_snapshot,
            "ts": int(time.time()),
        }
        authority = _local_authority(
            "owner" if role == "owner" else "reseller", order_id
        )
        if role == "owner":
            common_payload.update({
                "payment_kind": "owner",
                "payment_authorized": True,
                "payment_authorization_method": "owner",
                "payment_authorized_at": datetime.now(timezone.utc).isoformat(),
            })
            await run_blocking(add_pending, authority, common_payload)
        else:
            common_payload = await run_blocking(
                create_reseller_pending, authority, common_payload, _lane="db"
            )
        await _deliver_verified_pending(
            authority, common_payload, message, context, edit=edit
        )
        context.user_data.pop("first_buy_order", None)
        context.user_data.pop("awaiting", None)
        context.user_data.pop("payment_method_order", None)
        return

    first_purchase = action == "buy" and not (await run_blocking(has_completed_purchase, tg_id))

    # Referral discount is valid only on the first successful BUY, never renewal.
    if referral_code:
        if not first_purchase or (await run_blocking(referral_already_used, tg_id)):
            raise RuntimeError("کد معرف فقط برای اولین خرید قابل استفاده است")
        actual_referrer = (await run_blocking(find_referrer_by_code, referral_code))
        if not actual_referrer or int(actual_referrer) != int(referrer_tg_id or 0):
            raise RuntimeError("کد معرف معتبر نیست")
        if int(actual_referrer) == tg_id:
            raise RuntimeError("نمی‌توانید از کد معرف خودتان استفاده کنید")
        referrer_tg_id = int(actual_referrer)

    # Same first-purchase lock as v2.0.4.  This is intentionally narrower than
    # the v2.2 global pending lock: only an unfinished FIRST purchase can block
    # another first-purchase attempt.
    if first_purchase:
        existing_authority, existing = await run_blocking(pending_first_purchase_for_user, tg_id)
        if existing and _pending_is_preflight(existing):
            await run_blocking(pop_pending, existing_authority)
            existing = None
        if existing and _pending_is_local(existing):
            text = "⚠️ خرید اول قبلی هنوز در حال تکمیل است؛ ابتدا همان سفارش را تحویل بگیرید یا لغو کنید."
            markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 ادامه تحویل سفارش", callback_data="payment|check")],
                [InlineKeyboardButton("❌ لغو سفارش قبلی", callback_data="payment|cancel")],
                [InlineKeyboardButton("🔙 منوی اصلی", callback_data="home")],
            ])
            if edit and hasattr(message, "edit_text"):
                await safe_edit_text(message, text, reply_markup=markup)
            else:
                await message.reply_text(text, reply_markup=markup)
            return
        if existing and _pending_is_card_transfer(existing):
            request = await run_blocking(
                get_card_transfer_request_by_authority, existing_authority
            )
            status = str((request or {}).get("status") or "")
            await message.reply_text(
                "🧾 خرید اول کارت به کارت قبلی شما "
                + ("در انتظار ارسال رسید است." if status == "awaiting_receipt" else "در انتظار بررسی ادمین است.")
                + " ابتدا همان درخواست را تکمیل کنید."
            )
            return
        if existing and pending_plan_is_stale(existing):
            state = await reconcile_stale_pending(existing_authority, existing, message, context)
            if state in {"paid", "blocked"}:
                return
            existing = None
        if existing:
            raise RuntimeError("یک سفارش خرید اول در انتظار دارید؛ ابتدا آن را پرداخت یا لغو کنید")

    breakdown = await order_price_breakdown(
        tg_id, plan_key, service=service, referral_code=referral_code
    )
    selected_gateway = str(payment_method or "").strip().lower()
    if breakdown["gateway_toman"] > 0:
        gateways = enabled_payment_gateways()
        if not gateways:
            raise RuntimeError("هیچ درگاه پرداخت فعالی وجود ندارد")
        if selected_gateway not in gateways:
            if len(gateways) == 1:
                selected_gateway = gateways[0]
            else:
                context.user_data["payment_method_order"] = {
                    "service": service,
                    "action": action,
                    "plan_key": plan_key,
                    "identifier": identifier,
                    "referral_code": referral_code,
                    "referrer_tg_id": int(referrer_tg_id or 0),
                }
                chooser_text = order_summary_text(
                    plan_key, breakdown, service=service, referral_code=referral_code
                ) + "\n\n💳 <b>روش پرداخت را انتخاب کنید:</b>"
                chooser = InlineKeyboardMarkup([
                    [InlineKeyboardButton("💳 زرین‌پال", callback_data="paymethod|zarinpal")],
                    [InlineKeyboardButton("🏦 کارت به کارت", callback_data="paymethod|card_transfer")],
                    [InlineKeyboardButton("🔙 منوی اصلی", callback_data="home")],
                ])
                if edit and hasattr(message, "edit_text"):
                    await safe_edit_text(message, chooser_text, parse_mode="HTML", reply_markup=chooser)
                else:
                    await message.reply_text(chooser_text, parse_mode="HTML", reply_markup=chooser)
                return
    order_id = f"ord-{tg_id}-{secrets.token_hex(10)}"
    common_payload = {
        "tg_id": tg_id,
        "service": service,
        "action": action,
        "plan_key": plan_key,
        "identifier": identifier,
        "order_id": order_id,
        "first_purchase": bool(first_purchase),
        "base_price_toman": breakdown["base_price_toman"],
        "referral_discount_toman": breakdown["referral_discount_toman"],
        "wallet_used_toman": breakdown["wallet_used_toman"],
        "gateway_toman": breakdown["gateway_toman"],
        "wallet_committed": False,
        "referral_code": str(referral_code or "").upper().strip(),
        "referrer_tg_id": int(referrer_tg_id or 0),
        "referral_discount_percent": int(breakdown.get("referral_discount_percent") or 0),
        "referral_reward_percent": current_referral_reward_percent() if referral_code else 0,
        "plan_snapshot": current_plan_snapshot,
    }

    # Wallet covers the whole payable amount: no gateway at all.
    if breakdown["gateway_toman"] <= 0:
        common_payload.update({
            "payment_kind": "wallet",
            "ts": int(time.time()),
        })
        authority = _local_authority("wallet", order_id)
        # Persist the recoverable order before the idempotent debit. If the worker
        # stops at any later line, the wallet page can resume exactly this order.
        await run_blocking(add_pending, authority, common_payload)
        await _deliver_verified_pending(
            authority, common_payload, message, context, edit=edit
        )
        context.user_data.pop("first_buy_order", None)
        context.user_data.pop("awaiting", None)
        return

    if selected_gateway == "card_transfer":
        common_payload.update({
            "payment_kind": "card_transfer",
            "amount_rial": int(breakdown["gateway_toman"]) * 10,
            "ts": int(time.time()),
        })
        request = await run_blocking(create_card_transfer_request, common_payload)
        request_id = int(request.get("id") or 0)
        if not request_id:
            raise RuntimeError("ثبت درخواست کارت به کارت کامل نشد")
        snap = APP_SETTINGS.snapshot()
        card_number = str(snap.get("card_transfer_card_number") or "")
        card_holder = str(snap.get("card_transfer_card_holder") or "")
        if not card_number or not card_holder:
            await run_blocking(cancel_card_transfer_request, request_id, tg_id=tg_id)
            raise RuntimeError("اطلاعات کارت به کارت کامل تنظیم نشده است")
        context.user_data.pop("payment_method_order", None)
        context.user_data.pop("first_buy_order", None)
        context.user_data["awaiting"] = {
            "kind": "card_transfer_receipt", "request_id": request_id,
        }
        text = order_summary_text(
            plan_key, breakdown, service=service, referral_code=referral_code
        ) + (
            "\n\n🏦 <b>پرداخت کارت به کارت</b>\n"
            f"شماره کارت: <code>{html.escape(_format_card_number(card_number))}</code>\n"
            f"به نام: <b>{html.escape(card_holder)}</b>\n\n"
            "پس از واریز، تصویر رسید یا متن رسید/شماره پیگیری را همین‌جا ارسال کنید.\n"
            "اکانت فقط پس از تأیید ادمین ساخته و در یک پیام جداگانه تحویل می‌شود."
        )
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton(
                "📋 کپی شماره کارت", copy_text=CopyTextButton(text=card_number)
            )],
            [InlineKeyboardButton("❌ لغو درخواست", callback_data=f"cardpay|cancel|{request_id}")],
            [InlineKeyboardButton("🔙 منوی اصلی", callback_data="home")],
        ])
        if edit and hasattr(message, "edit_text"):
            await safe_edit_text(message, text, parse_mode="HTML", reply_markup=markup)
        else:
            await message.reply_text(text, parse_mode="HTML", reply_markup=markup)
        return

    # ZarinPal payment: preserve the proven v2.0.4 request/persistence flow.
    common_payload["payment_kind"] = "gateway"
    pay_url, _ = await run_zarinpal(create_payment,
        tg_id=tg_id,
        service=service,
        action=action,
        plan_key=plan_key,
        identifier=identifier,
        amount_rial=int(breakdown["gateway_toman"]) * 10,
        order_id=order_id,
        extra_payload=common_payload,
    )
    context.user_data.pop("first_buy_order", None)
    context.user_data.pop("awaiting", None)
    context.user_data.pop("payment_method_order", None)
    text = order_summary_text(
        plan_key, breakdown, service=service,
        referral_code=referral_code, gateway_url=pay_url,
    )
    sender = message.edit_text if edit else message.reply_text
    await sender(
        text,
        parse_mode="HTML",
        reply_markup=pay_keyboard(pay_url),
        disable_web_page_preview=True,
    )

async def _deliver_verified_pending(authority: str, pending: dict, message, context, *, edit: bool = False):
    order_id = str((pending or {}).get("order_id") or f"legacy-{authority}")
    async with ORDER_LOCKS.hold(order_id):
        fresh = await run_blocking(get_pending, authority)
        if not fresh:
            return False
        return await _deliver_verified_pending_unlocked(
            authority, fresh, message, context, edit=edit
        )


async def _authorize_zarinpal_and_deliver(
    authority: str, pending: dict, message, context, *, verification_code: int,
    edit: bool = False,
):
    """Persist successful gateway verification before entering fulfillment."""
    authorized = await run_blocking(
        authorize_pending_payment, authority,
        method="zarinpal", verification_code=int(verification_code),
    )
    return await _deliver_verified_pending(
        authority, authorized, message, context, edit=edit
    )


def _assert_delivery_payment_authorized(payload: dict) -> None:
    """Fail closed unless this order has a durable, valid payment proof."""
    payment_kind = str(payload.get("payment_kind") or "gateway").strip().lower()
    gateway_toman = int(payload.get("gateway_toman") or 0)
    amount_rial = int(payload.get("amount_rial") or 0)

    if payment_kind == "wallet":
        paid_plan = snapshot_for_delivery(payload)
        base_price = int(
            payload.get("base_price_toman") or paid_plan.get("price_toman") or 0
        )
        wallet_used = int(payload.get("wallet_used_toman") or 0)
        discount = int(payload.get("referral_discount_toman") or 0)
        if (
            base_price <= 0
            or wallet_used < 0
            or discount < 0
            or gateway_toman > 0
            or amount_rial > 0
            or wallet_used + discount < base_price
        ):
            raise RuntimeError("پوشش کامل مالی سفارش کیف پول تأیید نشد")
        return

    if payment_kind == "owner":
        if (
            int(payload.get("tg_id") or 0) != root_admin_id()
            or payload.get("payment_authorized") is not True
            or str(payload.get("payment_authorization_method") or "").strip().lower()
            != "owner"
            or gateway_toman != 0
            or amount_rial != 0
            or int(payload.get("wallet_used_toman") or 0) != 0
        ):
            raise RuntimeError("مجوز خرید رایگان مدیر معتبر نیست")
        return

    if payment_kind == "reseller_debt":
        gb = int(payload.get("reseller_gb") or 0)
        rate = int(payload.get("reseller_price_per_gb_toman") or 0)
        charge = int(payload.get("reseller_charge_toman") or 0)
        if (
            payload.get("payment_authorized") is not True
            or str(payload.get("payment_authorization_method") or "").strip().lower()
            != "reseller_debt"
            or int(payload.get("reseller_id") or 0) <= 0
            or int(payload.get("reseller_tg_id") or 0) != int(payload.get("tg_id") or 0)
            or gb <= 0
            or rate <= 0
            or charge != gb * rate
            or gateway_toman != 0
            or amount_rial != 0
            or int(payload.get("wallet_used_toman") or 0) != 0
        ):
            raise RuntimeError("مجوز خرید بدهی ریسلر معتبر نیست")
        return

    if payment_kind in {"admin", "preflight"}:
        raise RuntimeError("سفارش بدون پرداخت اجازه تحویل ندارد")

    expected_method = "card_transfer" if payment_kind == "card_transfer" else "zarinpal"
    if (
        payload.get("payment_authorized") is not True
        or str(payload.get("payment_authorization_method") or "").strip().lower()
        != expected_method
    ):
        raise RuntimeError("پرداخت این سفارش هنوز تأیید نشده است")
    if gateway_toman <= 0 and amount_rial <= 0:
        raise RuntimeError("مبلغ تأییدشده پرداخت نامعتبر است")
    if amount_rial > 0 and gateway_toman > 0 and amount_rial != gateway_toman * 10:
        raise RuntimeError("مبلغ تأییدشده با سفارش مطابقت ندارد")


async def _deliver_verified_pending_unlocked(
    authority: str, pending: dict, message, context, *, edit: bool = False,
    delivery_chat_id: int = 0, remove_pending: bool = True,
    delivery_prefix: str = "",
):
    payload = dict(pending)
    _assert_delivery_payment_authorized(payload)
    tg_id = int(payload["tg_id"])
    order_id = str(payload.get("order_id") or f"legacy-{authority}")
    wallet_used = int(payload.get("wallet_used_toman", 0) or 0)

    # Commit reserved wallet exactly once after gateway confirmation.
    if wallet_used and not payload.get("wallet_committed"):
        (await run_blocking(debit_wallet, tg_id, wallet_used, order_id=order_id))
        payload["wallet_committed"] = True
        (await run_blocking(update_pending, authority, wallet_committed=True))
    elif wallet_used and not await run_blocking(wallet_order_debited, order_id):
        raise RuntimeError("ثبت برداشت کیف پول این سفارش پیدا نشد")

    paid_plan = snapshot_for_delivery(payload)
    text, markup = await fulfill(
        payload["service"], payload["action"], payload["plan_key"],
        tg_id, payload.get("identifier", ""), context,
        plan_override=paid_plan, order_id=order_id,
    )
    bookkeeping_ok = False
    financial_result = {}
    try:
        financial_result = await finalize_successful_order(payload)
        completed = await run_blocking(mark_fulfillment_completed, order_id)
        if not completed:
            raise RuntimeError("ثبت وضعیت نهایی سفارش کامل نشد")
        bookkeeping_ok = True
    except Exception as e:
        # Keep the pending payment. A later "پرداخت کردم" replays the already
        # provisioned result from the journal and retries only local bookkeeping.
        logger.exception("post-delivery bookkeeping failed for %s", order_id)
        await notify_admins(
            context,
            f"⚠️ <b>خطای ثبت مالی بعد از تحویل</b>\nOrder: <code>{html.escape(order_id)}</code>\n"
            f"User: <code>{tg_id}</code>\nError: <code>{html.escape(str(e)[:1000])}</code>",
        )
    if bookkeeping_ok:
        journal = await run_blocking(get_fulfillment, order_id)
        delivery_identifier = str((journal or {}).get("delivery_identifier") or "")
        buyer_profile = await run_blocking(get_user_profile, tg_id)
        await notify_admins(
            context,
            successful_order_admin_text(
                payload, paid_plan, buyer_profile, financial_result
            ),
            reply_markup=(
                admin_account_keyboard(
                    str(payload.get("service") or ""), tg_id, delivery_identifier
                )
                if delivery_identifier else None
            ),
        )
    if wallet_used or payload.get("referral_discount_toman"):
        breakdown = {
            "base_price_toman": int(payload.get("base_price_toman") or paid_plan["price_toman"]),
            "referral_discount_toman": int(payload.get("referral_discount_toman", 0) or 0),
            "wallet_used_toman": wallet_used,
            "gateway_toman": int(payload.get("gateway_toman") or (int(payload.get("amount_rial", 0)) // 10)),
        }
        text = order_summary_text(
            payload["plan_key"], breakdown,
            service=str(payload.get("service") or "openvpn"),
            referral_code=payload.get("referral_code", ""),
            plan_override=paid_plan,
        ) + "\n\n" + text

    if delivery_prefix:
        text = str(delivery_prefix).rstrip() + "\n\n" + text
    if delivery_chat_id:
        await context.bot.send_message(
            chat_id=int(delivery_chat_id), text=text, parse_mode="HTML",
            reply_markup=markup, disable_web_page_preview=True,
        )
    else:
        sender = message.edit_text if edit else message.reply_text
        await sender(text, parse_mode="HTML", reply_markup=markup, disable_web_page_preview=True)
    if bookkeeping_ok and remove_pending:
        # Keep the recoverable pending row until Telegram confirms delivery. A
        # transient send failure can then replay the journaled result safely.
        (await run_blocking(pop_pending, authority))
    return bookkeeping_ok


async def verify_latest(q, context):
    authority, pending = (await run_blocking(latest_pending_for_user, q.from_user.id))
    if not pending:
        await safe_edit_text(
            q.message,
            "پرداختی در انتظار برای شما پیدا نشد.",
            reply_markup=await main_menu_keyboard(q.from_user.id),
        )
        return

    if _pending_is_preflight(pending):
        await run_blocking(pop_pending, authority)
        await safe_edit_text(
            q.message,
            "⚠️ ساخت لینک پرداخت قبلی کامل نشده بود و رزرو آن آزاد شد. لطفاً سفارش را دوباره ایجاد کنید.",
            reply_markup=await main_menu_keyboard(q.from_user.id),
        )
        return

    if _pending_is_card_transfer(pending):
        request = await run_blocking(get_card_transfer_request_by_authority, authority)
        status = str((request or {}).get("status") or "")
        text = (
            "🧾 هنوز رسید کارت به کارت را ارسال نکرده‌اید. تصویر یا متن رسید را همین‌جا بفرستید."
            if status == "awaiting_receipt"
            else "⏳ رسید کارت به کارت شما ثبت شده و در انتظار بررسی ادمین است."
        )
        await safe_edit_text(
            q.message, text, reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 منوی اصلی", callback_data="home")
            ]]),
        )
        return

    if _pending_is_local(pending):
        try:
            await _deliver_verified_pending(authority, pending, q.message, context)
            context.user_data.pop("first_buy_order", None)
            context.user_data.pop("awaiting", None)
        except Exception as exc:
            logger.exception("local order delivery failed authority=%s", authority)
            await safe_edit_text(
                q.message,
                "⚠️ سفارش شما محفوظ است اما تحویل هنوز کامل نشده؛ چند لحظه بعد دوباره «ادامه تحویل سفارش» را بزنید.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 ادامه تحویل سفارش", callback_data="payment|check")],
                    [InlineKeyboardButton("🔙 منوی اصلی", callback_data="home")],
                ]),
            )
        return

    try:
        amount = _pending_amount_rial(pending)
        result = await run_zarinpal(verify_payment, authority, amount)
    except Exception as exc:
        logger.warning("verify gateway failed authority=%s: %s", authority, exc)
        await safe_edit_text(
            q.message,
            pending_payment_text(
                pending,
                "⚠️ در حال حاضر ارتباط با زرین‌پال برقرار نشد؛ لطفاً چند لحظه بعد دوباره تلاش کنید.",
            ),
            parse_mode="HTML",
            reply_markup=pay_keyboard(str(pending.get("payment_url") or "")),
            disable_web_page_preview=True,
        )
        return

    code = _zarinpal_result_code(result)
    if code not in (100, 101):
        if pending_plan_is_stale(pending):
            if _zarinpal_is_definitely_unpaid(result):
                (await run_blocking(pop_pending, authority))
                context.user_data.clear()
                await safe_edit_text(
                    q.message,
                    "⚠️ قیمت یا مشخصات این بسته تغییر کرده و پرداخت قبلی انجام نشده بود؛ "
                    "سفارش قدیمی لغو شد. لطفاً بسته را دوباره با قیمت فعلی انتخاب کنید.",
                    reply_markup=await main_menu_keyboard(q.from_user.id),
                )
            else:
                await safe_edit_text(
                    q.message,
                    pending_payment_text(
                        pending,
                        "⚠️ وضعیت پرداخت هنوز قطعی نیست؛ سفارش برای جلوگیری از خطای مالی محفوظ مانده است. "
                        "چند لحظه بعد دوباره تلاش کنید.",
                    ),
                    parse_mode="HTML",
                    reply_markup=pay_keyboard(str(pending.get("payment_url") or "")),
                    disable_web_page_preview=True,
                )
            return
        notice = (
            "❌ پرداخت شما انجام نشده، لطفاً پرداخت خود را تکمیل کنید."
            if _zarinpal_is_definitely_unpaid(result)
            else "⚠️ وضعیت پرداخت هنوز قطعی نیست؛ چند لحظه بعد دوباره تلاش کنید."
        )
        await safe_edit_text(
            q.message,
            pending_payment_text(pending, notice),
            parse_mode="HTML",
            reply_markup=pay_keyboard(str(pending.get("payment_url") or "")),
            disable_web_page_preview=True,
        )
        return

    # If the ENV changed after the payment link was created but the old authority
    # was paid, honor the exact package/payment snapshot attached to it.
    try:
        await _authorize_zarinpal_and_deliver(
            authority, pending, q.message, context, verification_code=code
        )
    except Exception as exc:
        logger.exception("verified payment delivery failed authority=%s", authority)
        await safe_edit_text(
            q.message,
            "✅ پرداخت شما تأیید شد.\n\n"
            "⚠️ تحویل اکانت کامل نشد و سفارش برای بررسی محفوظ مانده است. "
            "چند لحظه بعد دوباره روی «بررسی و تحویل مجدد» بزنید.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 بررسی و تحویل مجدد", callback_data="payment|check")],
                [InlineKeyboardButton("🔙 منوی اصلی", callback_data="home")],
            ]),
        )
        await notify_admins(
            context,
            f"⚠️ <b>پرداخت تأیید شد ولی تحویل ناموفق بود</b>\n"
            f"Authority: <code>{html.escape(authority)}</code>\n"
            f"User: <code>{int(pending.get('tg_id') or 0)}</code>\n"
            f"Error: <code>{html.escape(str(exc)[:1000])}</code>",
        )


def _zarinpal_result_code(result) -> int | None:
    """Extract a ZarinPal code from both success and v4 error response shapes."""
    if not isinstance(result, dict):
        return None

    candidates = [result]
    for key in ("data", "errors"):
        branch = result.get(key)
        if isinstance(branch, dict):
            candidates.append(branch)

    for item in candidates:
        value = item.get("code")
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            pass

    # Inquiry validation errors may use a nested v4 shape such as
    # {"errors": {"authority": ["Invalid authority.", "-54"]}}.
    stack = [result.get("errors")]
    while stack:
        item = stack.pop()
        if isinstance(item, dict):
            stack.extend(item.values())
        elif isinstance(item, (list, tuple)):
            stack.extend(item)
        elif isinstance(item, str):
            value = item.strip()
            if value.lstrip("-").isdigit():
                try:
                    return int(value)
                except ValueError:
                    pass
    return None




def _zarinpal_is_definitely_unpaid(result) -> bool:
    """Return True only for gateway results that prove no successful payment."""
    code = _zarinpal_result_code(result)

    # -51: unsuccessful, -54: invalid authority, -55: request not found.
    if code in (-51, -54, -55):
        return True

    # Be resilient to equivalent v4 messages while remaining conservative.
    text = str(result).lower()
    safe_phrases = (
        "session is not paid",
        "session is not payed",
        "payment unsuccessful",
        "payment failed",
        "invalid authority",
        "transaction not found",
    )
    return any(phrase in text for phrase in safe_phrases)


async def _cancel_local_pending(authority: str, pending: dict, q, context):
    order_id = str(pending.get("order_id") or "")
    async with ORDER_LOCKS.hold(order_id):
        fresh = await run_blocking(get_pending, authority)
        if not fresh:
            await safe_edit_text(
                q.message,
                "این سفارش قبلاً تکمیل یا لغو شده است.",
                reply_markup=await main_menu_keyboard(q.from_user.id),
            )
            return
        pending = dict(fresh)
        journal, debited = await asyncio.gather(
            run_blocking(get_fulfillment, order_id),
            run_blocking(wallet_order_debited, order_id),
        )
        state = str((journal or {}).get("state") or "")
        if state in {"executing", "remote_done", "provisioned", "completed"}:
            await safe_edit_text(
                q.message,
                "⚠️ عملیات ساخت/تمدید این سفارش شروع شده و برای جلوگیری از تحویل یا بازگشت وجه تکراری قابل لغو نیست. "
                "روی «ادامه تحویل سفارش» بزنید.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 ادامه تحویل سفارش", callback_data="payment|check")],
                    [InlineKeyboardButton("🔙 منوی اصلی", callback_data="home")],
                ]),
            )
            return
        wallet_used = int(pending.get("wallet_used_toman") or 0)
        if debited and wallet_used:
            await run_blocking(
                refund_wallet,
                int(pending.get("tg_id") or q.from_user.id),
                wallet_used,
                order_id=order_id,
            )
        await run_blocking(pop_pending, authority)
        if pending.get("first_purchase"):
            await _cleanup_duplicate_unpaid_first_purchase_pendings(
                q.from_user.id, exclude_authority=authority
            )
        context.user_data.clear()
        await safe_edit_text(
            q.message,
            "✅ سفارش لغو شد و مبلغ کسرشده/رزروشده کیف پول آزاد شد.",
            reply_markup=await main_menu_keyboard(q.from_user.id),
        )


async def _cleanup_duplicate_unpaid_first_purchase_pendings(tg_id: int, *, exclude_authority: str = "") -> int:
    """Remove legacy duplicate unpaid first-purchase rows left by v2.2.

    v2.0.4 allowed only one pending first purchase, but the v2.2 preflight/global
    order changes could leave more than one row in an upgraded database.  We
    never delete a paid or inconclusive gateway authority here.
    """
    removed = 0
    rows = await run_blocking(list_pending_for_user, int(tg_id))
    for authority, pending in rows:
        authority = str(authority or "")
        pending = dict(pending or {})
        if not authority or authority == str(exclude_authority or ""):
            continue
        if not pending.get("first_purchase"):
            continue
        if _pending_is_preflight(pending):
            if await run_blocking(pop_pending, authority):
                removed += 1
            continue
        if _pending_is_local(pending) or pending.get("wallet_committed"):
            continue
        if _pending_is_card_transfer(pending):
            # A submitted receipt is a manual financial claim and must never be
            # discarded or queried against ZarinPal by compatibility cleanup.
            continue
        try:
            result = await run_zarinpal(
                verify_payment_for_cancel, authority, _pending_amount_rial(pending)
            )
        except Exception as exc:
            logger.warning("duplicate pending cleanup verify failed authority=%s: %s", authority, exc)
            continue
        code = _zarinpal_result_code(result)
        if code in (100, 101):
            # Never discard money: a paid duplicate stays recoverable.
            continue
        if _zarinpal_is_definitely_unpaid(result):
            if await run_blocking(pop_pending, authority):
                removed += 1
    return removed


async def cancel_latest_payment(q, context):
    authority, pending = (await run_blocking(latest_pending_for_user, q.from_user.id))
    if not pending:
        await safe_edit_text(
            q.message,
            "سفارش در انتظاری پیدا نشد.",
            reply_markup=await main_menu_keyboard(q.from_user.id),
        )
        return

    # Compatibility cleanup for an interrupted v2.2 preflight row that may already
    # exist in an upgraded database. New gateway orders no longer create these.
    if _pending_is_preflight(pending):
        await run_blocking(pop_pending, authority)
        if pending.get("first_purchase"):
            await _cleanup_duplicate_unpaid_first_purchase_pendings(
                q.from_user.id, exclude_authority=authority
            )
        context.user_data.clear()
        await safe_edit_text(
            q.message,
            "✅ درخواست ناتمام ساخت لینک پرداخت لغو و رزرو کیف پول آزاد شد.",
            reply_markup=await main_menu_keyboard(q.from_user.id),
        )
        return

    if _pending_is_card_transfer(pending):
        request = await run_blocking(get_card_transfer_request_by_authority, authority)
        if not request:
            await safe_edit_text(
                q.message, "⚠️ اطلاعات درخواست کارت به کارت پیدا نشد.",
                reply_markup=await main_menu_keyboard(q.from_user.id),
            )
            return
        try:
            await run_blocking(
                cancel_card_transfer_request, int(request.get("id") or 0),
                tg_id=q.from_user.id,
            )
        except ValueError as exc:
            await safe_edit_text(
                q.message, f"⚠️ {str(exc)}",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 منوی اصلی", callback_data="home")
                ]]),
            )
            return
        context.user_data.clear()
        await safe_edit_text(
            q.message, "✅ درخواست کارت به کارت لغو شد و رزرو کیف پول آزاد گردید.",
            reply_markup=await main_menu_keyboard(q.from_user.id),
        )
        return

    if _pending_is_local(pending):
        await _cancel_local_pending(authority, pending, q, context)
        return

    if pending.get("wallet_committed"):
        await safe_edit_text(
            q.message,
            "⚠️ پرداخت این سفارش قبلاً تأیید شده است و قابل لغو خودکار نیست. "
            "روی «پرداخت کردم» بزنید تا تحویل ادامه پیدا کند.",
            reply_markup=pay_keyboard(str(pending.get("payment_url") or "")),
        )
        return

    try:
        amount = _pending_amount_rial(pending)
        result = await run_zarinpal(verify_payment_for_cancel, authority, amount)
        code = _zarinpal_result_code(result)
        if code in (100, 101):
            # Same safety rule as v2.0.4: a paid authority is delivered, never cancelled.
            await _authorize_zarinpal_and_deliver(
                authority, pending, q.message, context, verification_code=code
            )
            return
        if not _zarinpal_is_definitely_unpaid(result):
            logger.warning("cancel verification inconclusive for %s: %r", authority, result)
            await safe_edit_text(
                q.message,
                "⚠️ وضعیت این پرداخت هنوز قطعی نیست و برای جلوگیری از حذف اشتباه سفارش، لغو انجام نشد. چند لحظه بعد دوباره امتحان کنید.",
                reply_markup=pay_keyboard(str(pending.get("payment_url") or "")),
            )
            return
    except Exception as e:
        logger.warning("cancel verify failed for %s: %s", authority, e)
        await safe_edit_text(
            q.message,
            "⚠️ فعلاً نتوانستم وضعیت درگاه را با اطمینان بررسی کنم؛ برای جلوگیری از حذف اشتباه سفارش، لغو انجام نشد.",
            reply_markup=pay_keyboard(str(pending.get("payment_url") or "")),
        )
        return

    await run_blocking(pop_pending, authority)
    if pending.get("first_purchase"):
        await _cleanup_duplicate_unpaid_first_purchase_pendings(
            q.from_user.id, exclude_authority=authority
        )
    context.user_data.clear()
    await safe_edit_text(
        q.message,
        "✅ سفارش لغو شد و اعتبار رزروشده کیف پول آزاد شد.\n\n⚠️ لینک پرداخت سفارش لغوشده را دیگر استفاده نکنید.",
        reply_markup=await main_menu_keyboard(q.from_user.id),
    )

async def verify_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("/verify AUTHORITY")
        return
    authority = context.args[0].strip()
    pending = (await run_blocking(get_pending, authority))
    if not pending:
        await update.message.reply_text("Authority پیدا نشد یا قبلاً استفاده شده.")
        return
    if int(pending.get("tg_id", 0)) != update.effective_user.id and not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ این پرداخت متعلق به شما نیست.")
        return
    try:
        if _pending_is_card_transfer(pending):
            request = await run_blocking(get_card_transfer_request_by_authority, authority)
            status = str((request or {}).get("status") or "")
            await update.message.reply_text(
                "🧾 درخواست کارت به کارت در انتظار ارسال رسید است."
                if status == "awaiting_receipt"
                else "⏳ رسید کارت به کارت در انتظار بررسی ادمین است."
            )
            return
        if _pending_is_local(pending):
            await _deliver_verified_pending(authority, pending, update.message, context)
            return
        amount = _pending_amount_rial(pending)
        result = await run_zarinpal(verify_payment, authority, amount)
        code = _zarinpal_result_code(result)
        if code not in (100, 101):
            if pending_plan_is_stale(pending):
                if _zarinpal_is_definitely_unpaid(result):
                    (await run_blocking(pop_pending, authority))
                    await update.message.reply_text(
                        "⚠️ قیمت یا مشخصات این بسته تغییر کرده و سفارش قدیمی پرداخت نشده بود. "
                        "سفارش لغو شد؛ بسته را دوباره با قیمت فعلی انتخاب کنید."
                    )
                else:
                    await update.message.reply_text(
                        "⚠️ وضعیت پرداخت هنوز قطعی نیست؛ سفارش حذف نشد. چند لحظه بعد دوباره تلاش کنید."
                    )
                return
            if _zarinpal_is_definitely_unpaid(result):
                await update.message.reply_text("❌ پرداخت شما انجام نشده، لطفاً پرداخت خود را تکمیل کنید.")
            else:
                await update.message.reply_text("⚠️ وضعیت پرداخت هنوز قطعی نیست؛ چند لحظه بعد دوباره تلاش کنید.")
            return
        await _authorize_zarinpal_and_deliver(
            authority, pending, update.message, context, verification_code=code
        )
    except Exception:
        logger.exception("verify cmd")
        await update.message.reply_text("⚠️ بررسی یا تحویل پرداخت کامل نشد؛ لطفاً دوباره تلاش کنید.")


async def create_test(q, context, service: str):
    tg_id = q.from_user.id
    if not service_sales_enabled(service):
        await q.message.edit_text(
            "⚠️ فروش این سرویس در حال حاضر غیرفعال است.",
            reply_markup=await main_menu_keyboard(tg_id),
        )
        return
    reseller = reseller_record(tg_id)
    if reseller and not bool(reseller.get("trial_enabled", True)):
        await q.message.edit_text(
            "⛔ دریافت اکانت تست برای حساب ریسلر شما غیرفعال است.",
            reply_markup=back_service(service),
        )
        return
    if not test_plan_enabled():
        await q.message.edit_text(
            "⛔ در حال حاضر دریافت اکانت تست غیرفعال است.",
            reply_markup=back_service(service),
        )
        return
    unlimited_trial = is_admin(tg_id) or bool(reseller)
    if not unlimited_trial and (await run_blocking(has_test, tg_id, service)):
        await q.message.edit_text("⚠️ اکانت تست این سرویس را قبلاً دریافت کرده‌اید.", reply_markup=back_service(service))
        return
    try:
        order_id = (
            f"test-{service}-{tg_id}-{secrets.token_hex(8)}"
            if unlimited_trial
            else f"test-{service}-{tg_id}"
        )
        text, result_markup = await fulfill(
            service,
            "buy",
            f"__test_{service}__",
            tg_id,
            "",
            context,
            plan_override=TEST_PLAN,
            order_id=order_id,
            is_test=True,
        )
        if not unlimited_trial:
            (await run_blocking(mark_test, tg_id, service, True))
        completed = await run_blocking(mark_fulfillment_completed, order_id)
        if not completed:
            raise RuntimeError("ثبت وضعیت نهایی اکانت تست کامل نشد")
        await q.message.edit_text(text, parse_mode="HTML", reply_markup=result_markup, disable_web_page_preview=True)
    except Exception:
        logger.exception("create test")
        await q.message.edit_text(
            "❌ ساخت اکانت تست کامل نشد؛ لطفاً دوباره تلاش کنید.",
            reply_markup=back_service(service),
        )


async def contact_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    schedule_telegram_profile(context, update.effective_user)
    contact = update.message.contact if update.message else None
    if not contact:
        return
    # Only persist a phone number when the user shares their own Telegram contact.
    if contact.user_id is None or int(contact.user_id) == int(update.effective_user.id):
        await run_blocking(update_user_profile, update.effective_user.id, phone_number=contact.phone_number or "")



async def health(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    snap, db = await live_health_snapshot()
    services = snap.get("service_health") or {}
    mt = services.get("mikrotik") or {}
    xu = services.get("xui") or {}
    rows = [
        f"✅ OpenVPN plans loaded: {len(plans_for('openvpn'))} ({', '.join(plans_for('openvpn').keys())})",
        f"✅ V2Ray plans loaded: {len(plans_for('v2ray'))} ({', '.join(plans_for('v2ray').keys())})",
        f"{'✅' if mt.get('ok') else '❌'} MikroTik API: {mt.get('detail') or '-'}",
        f"{'✅' if xu.get('ok') else '❌'} 3x-ui API: {xu.get('detail') or '-'}",
        f"{'✅' if db.get('quick_check') == 'ok' else '❌'} SQLite: {db.get('quick_check')}",
        f"Heartbeat age: {float(snap.get('heartbeat_age_seconds',0)):.1f}s",
        f"In-flight updates: {int(snap.get('in_flight',0))}",
        f"Total updates: {int(snap.get('total_updates',0))}",
    ]
    await update.message.reply_text("\n".join(rows))


async def _heartbeat_loop():
    while True:
        RUNTIME.heartbeat()
        await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)


async def _health_probe_loop():
    # External probes are deliberately separate from the heartbeat. A slow/down
    # panel must never make the watchdog think the Telegram event loop is dead.
    await asyncio.sleep(3)
    while True:
        try:
            await live_health_snapshot()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("background health probe failed: %s", exc)
        await asyncio.sleep(HEALTH_PROBE_INTERVAL_SECONDS)


def _backup_timezone():
    try:
        return ZoneInfo(APP_TIMEZONE)
    except Exception:
        # Iran no longer observes DST; this fallback keeps 06:00 deterministic
        # even on a minimal Ubuntu installation without system tzdata.
        logger.warning("timezone %s unavailable; using UTC+03:30 for backups", APP_TIMEZONE)
        return timezone(timedelta(hours=3, minutes=30))


def _seconds_until_next_backup(now: datetime | None = None) -> float:
    tz = _backup_timezone()
    current = now.astimezone(tz) if now is not None else datetime.now(tz)
    target = current.replace(hour=APP_BACKUP_HOUR, minute=0, second=0, microsecond=0)
    if target <= current:
        target += timedelta(days=1)
    return max((target - current).total_seconds(), 1.0)


async def _backup_loop():
    # The task itself is intentionally always alive so changing the persistent
    # admin switch takes effect without restarting the service. While waiting
    # it consumes no worker thread and essentially no CPU.
    while True:
        try:
            await asyncio.sleep(_seconds_until_next_backup())
            if not (await run_blocking(auto_backup_enabled)):
                logger.info("scheduled SQLite backup skipped: disabled by admin")
                continue
            result = await run_blocking(
                backup_database, force=True, keep=BACKUP_KEEP, _lane="backup"
            )
            if result.get("created"):
                logger.info("scheduled SQLite backup created: %s", result.get("path"))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("automatic backup failed: %s", exc)
            # Avoid a tight retry loop if clock/timezone or storage fails.
            await asyncio.sleep(60)


async def post_init(application):
    RUNTIME.heartbeat()
    if WATCHDOG_ENABLED:
        RUNTIME.start_watchdog(WATCHDOG_STALE_SECONDS, logger=logger)
    tasks = [
        asyncio.create_task(_heartbeat_loop(), name="heartbeat"),
        asyncio.create_task(_health_probe_loop(), name="health-probe"),
    ]
    # Always keep the lightweight scheduler task alive; the persistent admin
    # switch decides whether the 06:00 backup actually runs.
    tasks.append(asyncio.create_task(_backup_loop(), name="sqlite-backup"))
    for task in tasks:
        task.add_done_callback(
            lambda done: logger.error(
                "background task stopped unexpectedly name=%s error=%r",
                done.get_name(),
                None if done.cancelled() else done.exception(),
            ) if not done.cancelled() else None
        )
    application.bot_data["background_tasks"] = tasks
    logger.info(
        "Account Sales Bot v1.0.0 runtime initialized watchdog=%s sqlite=%s cache_ttl=%.1fs workers=%d queue_cap=%d "
        "lanes=misc:%d,db:%d,mikrotik:%d,xui:%d,zarinpal:%d read_deadline=%.1fs",
        WATCHDOG_ENABLED, (await run_blocking(database_stats)).get("quick_check"), STATUS_CACHE_TTL_SECONDS,
        BOT_CONCURRENT_UPDATES, BOT_UPDATE_QUEUE_CAP,
        BOT_IO_WORKERS, BOT_DB_WORKERS, BOT_MIKROTIK_WORKERS, BOT_XUI_WORKERS, BOT_ZARINPAL_WORKERS,
        SERVICE_READ_TOTAL_TIMEOUT_SECONDS,
    )


async def post_shutdown(application):
    RUNTIME.stop_watchdog()
    for task in application.bot_data.get("background_tasks", []):
        task.cancel()
    if application.bot_data.get("background_tasks"):
        await asyncio.gather(*application.bot_data["background_tasks"], return_exceptions=True)
    for lane in BLOCKING_LANES.values():
        lane.shutdown()


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    error = context.error
    logger.error(
        "Unhandled error",
        exc_info=(type(error), error, error.__traceback__) if error else None,
    )
    message = getattr(update, "effective_message", None)
    if message is not None:
        try:
            await message.reply_text(
                "❌ این درخواست کامل نشد. لطفاً دوباره تلاش کنید یا /start را بزنید."
            )
        except Exception:
            logger.warning("unhandled-error fallback message failed", exc_info=True)


def main():
    app = (ApplicationBuilder()
           .token(BOT_TOKEN)
           .post_init(post_init)
           .post_shutdown(post_shutdown)
           .concurrent_updates(PerUserUpdateProcessor(BOT_CONCURRENT_UPDATES))
           .connection_pool_size(max(32, BOT_CONCURRENT_UPDATES * 4))
           .connect_timeout(5.0)
           .read_timeout(15.0)
           .write_timeout(15.0)
           .pool_timeout(5.0)
           .build())
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("verify", verify_cmd))
    app.add_handler(CommandHandler("health", health))
    app.add_handler(CallbackQueryHandler(callback_router))
    app.add_handler(MessageHandler(filters.CONTACT, contact_router))
    app.add_handler(MessageHandler(filters.PHOTO | filters.Document.IMAGE, card_receipt_media_router))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_router))
    app.add_error_handler(error_handler)
    logger.info("Account Sales Bot v1.0.0 started")
    app.run_polling(
        allowed_updates=[Update.MESSAGE, Update.CALLBACK_QUERY],
        bootstrap_retries=-1,
    )


if __name__ == "__main__":
    main()
