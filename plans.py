import os
import re
import threading
from collections.abc import Mapping, Iterator

from storage import (
    get_trial_plan,
    initialize_sale_plans,
    initialize_service_sale_plans,
    initialize_trial_plan,
    list_service_sale_plans,
)

# v3.0: sale plans are persisted in SQLite and managed from the Telegram admin
# panel. Legacy PLAN_* environment variables are read exactly once, only to
# migrate an existing v2.x installation into the new database-backed registry.
# After that marker is written, PLAN_* no longer repopulates or override plans.

_PLAN_PREFIX = "PLAN_"
_PLAN_KEY_RE = re.compile(r"^[A-Za-z0-9_-]{1,32}$")
_MAX_PLAN_GB = 1_000_000
_MAX_PLAN_DAYS = 36_500
_MAX_PLAN_PRICE_TOMAN = 1_000_000_000_000
_MAX_PLAN_COUNT = 90

# Compatibility for historical pending/paid gateway orders that predate plan
# snapshots. These values are never used to populate the v3 sale menu.
_LEGACY_V15_PLANS = {
    "10": {"title": "10 گیگ - 30 روزه", "gb": 10, "months": 1, "days": 30, "price_toman": 150_000, "openvpn_profile": "1M-10G"},
    "25": {"title": "25 گیگ - 30 روزه", "gb": 25, "months": 1, "days": 30, "price_toman": 350_000, "openvpn_profile": "1M-25G"},
    "50": {"title": "50 گیگ - 30 روزه", "gb": 50, "months": 1, "days": 30, "price_toman": 650_000, "openvpn_profile": "1M-50G"},
    "100": {"title": "100 گیگ - 30 روزه", "gb": 100, "months": 1, "days": 30, "price_toman": 980_000, "openvpn_profile": "1M-100G"},
}


def _parse_positive_int(raw: str, *, field: str, env_name: str, maximum: int | None = None) -> int:
    try:
        value = int(str(raw).strip())
    except Exception as exc:
        raise RuntimeError(f"{env_name}: {field} باید عدد صحیح باشد") from exc
    if value <= 0:
        raise RuntimeError(f"{env_name}: {field} باید بزرگ‌تر از صفر باشد")
    if maximum is not None and value > int(maximum):
        raise RuntimeError(f"{env_name}: {field} از حداکثر مجاز ({int(maximum):,}) بیشتر است")
    return value


def _legacy_env_seed() -> list[dict]:
    """Parse legacy PLAN_* only for the one-time v2 -> v3 migration."""
    result = []
    for env_name, raw_value in os.environ.items():
        if not env_name.startswith(_PLAN_PREFIX):
            continue
        key = env_name[len(_PLAN_PREFIX):].strip()
        if not key or not _PLAN_KEY_RE.fullmatch(key):
            raise RuntimeError(
                f"نام پلن نامعتبر است: {env_name}. بعد از PLAN_ فقط حروف انگلیسی، عدد، _ و - مجاز است."
            )
        parts = [part.strip() for part in str(raw_value or "").split("|")]
        if len(parts) != 4:
            raise RuntimeError(f"{env_name} نامعتبر است. فرمت قدیمی درست: GB|DAYS|PRICE_TOMAN|OPENVPN_PROFILE")
        gb = _parse_positive_int(parts[0], field="GB", env_name=env_name, maximum=_MAX_PLAN_GB)
        days = _parse_positive_int(parts[1], field="DAYS", env_name=env_name, maximum=_MAX_PLAN_DAYS)
        price = _parse_positive_int(parts[2], field="PRICE_TOMAN", env_name=env_name, maximum=_MAX_PLAN_PRICE_TOMAN)
        profile = parts[3]
        if not profile:
            raise RuntimeError(f"{env_name}: OPENVPN_PROFILE خالی است")
        if len(profile) > 128:
            raise RuntimeError(f"{env_name}: OPENVPN_PROFILE نباید بیشتر از 128 کاراکتر باشد")
        # Existing v2 packages are normally 30/60/90/... days. Preserve exact
        # days for financial/delivery compatibility; months is presentation and
        # becomes authoritative only for packages created/edited in v3.
        months = days // 30 if days % 30 == 0 else 0
        result.append({
            "plan_key": key,
            "gb": gb,
            "months": months,
            "days": days,
            "price_toman": price,
            "openvpn_profile": profile,
        })
    result.sort(key=lambda p: (p["days"], p["gb"], p["price_toman"], p["plan_key"].lower()))
    if len(result) > _MAX_PLAN_COUNT:
        raise RuntimeError(f"تعداد PLAN_* نباید بیشتر از {_MAX_PLAN_COUNT} باشد")
    return result


def _title(plan: dict) -> str:
    months = int(plan.get("months") or 0)
    if months > 0:
        return f"{int(plan['gb'])} گیگ - {months} ماهه"
    return f"{int(plan['gb'])} گیگ - {int(plan['days'])} روزه"


def _row_to_plan(row: dict) -> dict:
    plan = {
        "gb": int(row["gb"]),
        "months": int(row.get("months") or 0),
        "days": int(row["days"]),
        "price_toman": int(row["price_toman"]),
        "openvpn_profile": str(row.get("openvpn_profile") or ""),
    }
    plan["title"] = _title(plan)
    return plan


class PlanRegistry(Mapping):
    """Thread-safe, zero-I/O plan snapshot shared by all worker lanes.

    Admin writes build a new dict and swap the reference under one lock, so a
    ZarinPal/XUI/RouterOS worker can never observe the transient empty registry
    that a dict.clear()+update() refresh would create.
    """
    def __init__(self):
        self._lock = threading.RLock()
        self._data: dict[str, dict] = {}

    def replace(self, fresh: dict[str, dict]):
        with self._lock:
            self._data = dict(fresh)

    def snapshot(self) -> dict[str, dict]:
        with self._lock:
            return dict(self._data)

    def __getitem__(self, key: str) -> dict:
        with self._lock:
            return self._data[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.snapshot())

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)

    def get(self, key, default=None):
        with self._lock:
            return self._data.get(key, default)

    def items(self):
        return self.snapshot().items()

    def keys(self):
        return self.snapshot().keys()

    def values(self):
        return self.snapshot().values()


# One-time migration and then a zero-I/O in-memory registry for all user paths.
initialize_sale_plans(_legacy_env_seed())
initialize_service_sale_plans()
OPENVPN_PLANS = PlanRegistry()
V2RAY_PLANS = PlanRegistry()
SERVICE_PLANS = {
    "openvpn": OPENVPN_PLANS,
    "v2ray": V2RAY_PLANS,
}
# Compatibility alias for stable extensions which historically imported PLANS.
PLANS = OPENVPN_PLANS


def plans_for(service: str) -> PlanRegistry:
    normalized = str(service or "").strip().lower()
    if normalized not in SERVICE_PLANS:
        raise KeyError(normalized)
    return SERVICE_PLANS[normalized]


def refresh_plans(rows: list[dict] | None = None, *, service_aware: bool = False):
    from_database = rows is None
    rows = list_service_sale_plans() if from_database else list(rows)
    fresh = {"openvpn": {}, "v2ray": {}}
    for row in rows:
        # Rows without a service are accepted only for compatibility with old
        # tests/extensions and represent the historical shared package model.
        services = (
            (str(row["service"]),)
            if (from_database or service_aware) and row.get("service")
            else ("openvpn", "v2ray")
        )
        for service in services:
            if service in fresh:
                fresh[service][str(row["plan_key"])] = _row_to_plan(row)
    OPENVPN_PLANS.replace(fresh["openvpn"])
    V2RAY_PLANS.replace(fresh["v2ray"])
    return SERVICE_PLANS


refresh_plans()

# v3.1: TEST_PLAN is migrated from ENV once, then becomes a persistent
# admin-managed trial package in SQLite. User paths read only the in-memory
# registry, so showing service menus or creating a trial adds no DB read.
def _load_test_plan_seed() -> dict:
    raw = os.getenv("TEST_PLAN", "1|1|1D-1G-Test")
    parts = [part.strip() for part in str(raw).split("|")]
    if len(parts) != 3:
        raise RuntimeError("TEST_PLAN نامعتبر است. فرمت درست: GB|DAYS|OPENVPN_PROFILE")
    gb = _parse_positive_int(parts[0], field="GB", env_name="TEST_PLAN", maximum=_MAX_PLAN_GB)
    days = _parse_positive_int(parts[1], field="DAYS", env_name="TEST_PLAN", maximum=_MAX_PLAN_DAYS)
    profile = parts[2]
    if not profile:
        raise RuntimeError("TEST_PLAN: OPENVPN_PROFILE خالی است")
    if len(profile) > 128:
        raise RuntimeError("TEST_PLAN: OPENVPN_PROFILE نباید بیشتر از 128 کاراکتر باشد")
    return {
        "title": f"{gb} گیگ - {days} روزه",
        "gb": gb,
        "months": days // 30 if days % 30 == 0 else 0,
        "days": days,
        "price_toman": 0,
        "openvpn_profile": profile,
    }


class TrialPlanRegistry(Mapping):
    def __init__(self, initial: dict | None = None):
        self._lock = threading.RLock()
        self._data = dict(initial or {})

    def replace(self, fresh: dict):
        with self._lock:
            self._data = dict(fresh or {})

    def snapshot(self) -> dict:
        with self._lock:
            return dict(self._data)

    def __getitem__(self, key: str):
        with self._lock:
            return self._data[key]

    def __iter__(self):
        return iter(self.snapshot())

    def __len__(self):
        with self._lock:
            return len(self._data)

    def get(self, key, default=None):
        with self._lock:
            return self._data.get(key, default)


_trial_seed = _load_test_plan_seed()
initialize_trial_plan({**_trial_seed, "enabled": True})
TEST_PLAN = TrialPlanRegistry(get_trial_plan(_trial_seed))

def refresh_test_plan(row: dict | None = None):
    fresh = get_trial_plan(_trial_seed) if row is None else dict(row)
    fresh["title"] = f"{int(fresh['gb'])} گیگ - {int(fresh['days'])} روزه"
    fresh["months"] = 0
    fresh["price_toman"] = 0
    TEST_PLAN.replace(fresh)
    return TEST_PLAN

refresh_test_plan()


def reload_plan_registries():
    """Re-run idempotent migrations and atomically reload plans after restore."""
    initialize_sale_plans(_legacy_env_seed())
    initialize_service_sale_plans()
    initialize_trial_plan({**_trial_seed, "enabled": True})
    refresh_plans()
    refresh_test_plan()
    return {
        "openvpn": len(OPENVPN_PLANS),
        "v2ray": len(V2RAY_PLANS),
        "trial_enabled": test_plan_enabled(),
    }

def test_plan_enabled() -> bool:
    return bool(TEST_PLAN.get("enabled", True))


def plan_snapshot(plan_key: str, service: str = "openvpn") -> dict:
    p = plans_for(service)[plan_key]
    return {
        "plan_key": str(plan_key),
        "title": str(p.get("title") or ""),
        "gb": int(p["gb"]),
        "months": int(p.get("months") or 0),
        "days": int(p["days"]),
        "price_toman": int(p["price_toman"]),
        "openvpn_profile": str(p["openvpn_profile"]),
    }


def plan_signature(plan: dict | None) -> tuple:
    if not isinstance(plan, dict):
        return ()
    try:
        return (
            int(plan.get("gb") or 0),
            int(plan.get("days") or 0),
            int(plan.get("price_toman") or 0),
            str(plan.get("openvpn_profile") or ""),
        )
    except Exception:
        return ()


def pending_plan_is_stale(payload: dict) -> bool:
    """True when an unpaid order no longer matches the current admin plan."""
    key = str(payload.get("plan_key") or "")
    service = str(payload.get("service") or "openvpn").lower()
    current = plans_for(service).get(key) if service in SERVICE_PLANS else None
    if not current:
        return True
    snap = payload.get("plan_snapshot")
    if isinstance(snap, dict) and snap:
        return plan_signature(snap) != plan_signature(current)
    old_price = int(payload.get("base_price_toman") or 0)
    legacy = _LEGACY_V15_PLANS.get(key)
    if legacy and (not old_price or old_price == int(legacy["price_toman"])):
        return plan_signature(legacy) != plan_signature(current)
    if old_price and old_price != int(current.get("price_toman") or 0):
        return True
    return False


def snapshot_for_delivery(payload: dict) -> dict:
    """Paid orders use their immutable snapshot even if admin later edits/deletes a plan."""
    snap = payload.get("plan_snapshot")
    if isinstance(snap, dict) and snap:
        days = int(snap.get("days") or 0)
        months = int(snap.get("months") or (days // 30 if days and days % 30 == 0 else 0))
        return {
            "title": str(snap.get("title") or ""),
            "gb": int(snap.get("gb") or 0),
            "months": months,
            "days": days,
            "price_toman": int(snap.get("price_toman") or 0),
            "openvpn_profile": str(snap.get("openvpn_profile") or ""),
        }
    key = str(payload["plan_key"])
    old_price = int(payload.get("base_price_toman") or 0)
    legacy = _LEGACY_V15_PLANS.get(key)
    if legacy and (not old_price or old_price == int(legacy["price_toman"])):
        return dict(legacy)
    service = str(payload.get("service") or "openvpn").lower()
    return dict(plans_for(service)[key])


def price_rial(plan_key: str, service: str = "openvpn") -> int:
    return int(plans_for(service)[plan_key]["price_toman"]) * 10


def gb_to_bytes(gb: int | float) -> int:
    return int(float(gb) * 1024 * 1024 * 1024)
