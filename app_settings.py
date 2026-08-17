"""SQLite-backed application settings with an atomic memory snapshot.

Only bootstrap/infrastructure values (BOT_TOKEN, root ADMIN_IDS entry and
DATA_DIR) remain ENV-driven. Service and business paths read this module's
immutable snapshot and never query SQLite per Telegram action.
"""

from __future__ import annotations

import os
import re
import threading
from collections.abc import Mapping
from types import MappingProxyType
from urllib.parse import urlsplit

import config
from storage import (
    add_reseller as _db_add_reseller,
    add_xui_inbound as _db_add_xui_inbound,
    delete_reseller as _db_delete_reseller,
    delete_xui_inbound as _db_delete_xui_inbound,
    get_app_settings_state,
    initialize_app_settings,
    initialize_feature_toggles,
    initialize_v101_expiry_notifications,
    initialize_v34_sales_settings,
    initialize_v35_payment_settings,
    initialize_v36_resellers,
    record_reseller_debt_charge as _db_record_reseller_debt_charge,
    rename_xui_inbound as _db_rename_xui_inbound,
    set_reseller_debt as _db_set_reseller_debt,
    set_app_settings as _db_set_app_settings,
    update_reseller as _db_update_reseller,
)


APP_TIMEZONE = "Asia/Tehran"
APP_BACKUP_HOUR = 6
_PREFIX_RE = re.compile(r"^[A-Za-z0-9_-]+$")

DEFAULTS = MappingProxyType({
    "bot_brand_name": "Account Sales Bot",
    "account_username_prefix": "accountbot",
    "referral_code_prefix": "ASB",
    "referral_enabled": True,
    "wallet_enabled": True,
    "openvpn_connections_url": "0",
    "openvpn_sales_enabled": True,
    "api_ip": "127.0.0.1",
    "api_port": 8728,
    "api_user": "",
    "api_pass": "",
    "um_scheme": "http",
    "um_path": "um",
    # Hidden compatibility values. Empty means derive from api_ip/default port.
    "um_host_legacy": "",
    "um_port_legacy": "",
    "xui_api_token": "",
    "xui_scheme": "https",
    "xui_host": "127.0.0.1",
    "xui_port": 2053,
    "xui_base_path": "/admin/",
    "xui_verify_tls": False,
    "xui_sub_public_base": "0",
    "v2ray_sales_enabled": True,
    "zarinpal_sandbox": False,
    "zarinpal_merchant_id": "xxxx-xxx-xxx-xxx-xxxx",
    "zarinpal_enabled": True,
    "card_transfer_enabled": False,
    "card_transfer_card_number": "",
    "card_transfer_card_holder": "",
    "account_expiry_notifications_enabled": True,
    "account_expiry_check_interval_minutes": 30,
})

def _bool_value(value, *, strict: bool = True) -> bool:
    if isinstance(value, bool):
        return value
    raw = str(value or "").strip().lower()
    if raw in {"1", "true", "yes", "on", "enabled"}:
        return True
    if raw in {"0", "false", "no", "off", "disabled", ""}:
        return False
    if strict:
        raise ValueError("مقدار باید Enabled یا Disabled باشد")
    return False


def _port(value, label: str) -> int:
    try:
        result = int(str(value).strip())
    except Exception as exc:
        raise ValueError(f"{label} باید عدد صحیح باشد") from exc
    if result < 1 or result > 65535:
        raise ValueError(f"{label} باید بین 1 و 65535 باشد")
    return result


def _host(value, label: str) -> str:
    result = str(value or "").strip()
    if not result or len(result) > 255:
        raise ValueError(f"{label} معتبر نیست")
    if any(ch.isspace() for ch in result) or "://" in result or "/" in result:
        raise ValueError(f"در {label} فقط IP یا hostname را وارد کنید")
    return result


def _http_url_or_zero(value, label: str, *, trailing_slash: bool = False) -> str:
    result = str(value or "").strip()
    if result == "0":
        return "0"
    parsed = urlsplit(result)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(f"{label} باید یک URL کامل HTTP/HTTPS یا مقدار 0 باشد")
    if len(result) > 2000:
        raise ValueError(f"{label} بیش از حد طولانی است")
    if trailing_slash:
        result = result.rstrip("/") + "/"
    return result


def _api_user_exact(value: str, *, allow_empty: bool = False) -> str:
    """Validate a complete RouterOS username without rewriting it."""
    result = str(value or "").strip()
    if not result and allow_empty:
        return ""
    if not result or len(result) > 64 or any(ch in result for ch in "\r\n\0"):
        raise ValueError("Mikrotik Username باید بین 1 تا 64 کاراکتر و بدون خط جدید باشد")
    return result


def normalize_setting(key: str, value, *, from_env: bool = False):
    key = str(key or "").strip()
    if key not in DEFAULTS:
        raise ValueError("تنظیم ناشناخته است")
    if key == "bot_brand_name":
        result = str(value or "").strip()
        if not result or len(result) > 100:
            raise ValueError("نام برند باید بین 1 تا 100 کاراکتر باشد")
        return result
    if key == "account_username_prefix":
        result = str(value or "").strip()
        if not result or len(result) > 32 or not _PREFIX_RE.fullmatch(result):
            raise ValueError("پیشوند نام اکانت باید 1 تا 32 کاراکتر انگلیسی/عدد/_/- باشد")
        return result
    if key == "referral_code_prefix":
        result = str(value or "").strip().upper()
        if not result or len(result) > 32 or not result.isalnum():
            raise ValueError("پیشوند Referral باید 1 تا 32 حرف یا عدد باشد")
        return result
    if key == "openvpn_connections_url":
        return _http_url_or_zero(value, "OPENVPN_CONNECTIONS_URL")
    if key == "api_ip":
        return _host(value, "Mikrotik IP")
    if key == "api_port":
        return _port(value, "Mikrotik API Port")
    if key == "api_user":
        return _api_user_exact(value, allow_empty=from_env)
    if key == "api_pass":
        result = str(value or "")
        if not from_env and not result:
            raise ValueError("Mikrotik Password نمی‌تواند خالی باشد")
        if len(result) > 512:
            raise ValueError("Mikrotik Password بیش از حد طولانی است")
        return result
    if key in {"um_scheme", "xui_scheme"}:
        result = str(value or "").strip().lower()
        if result not in {"http", "https"}:
            raise ValueError("Connection Type فقط HTTP یا HTTPS است")
        return result
    if key == "um_path":
        result = str(value or "").strip().strip("/")
        if not result or len(result) > 256:
            raise ValueError("User Manager Path معتبر نیست")
        return "/".join(part for part in result.split("/") if part)
    if key == "um_host_legacy":
        result = str(value or "").strip()
        return "" if not result else _host(result, "legacy User Manager host")
    if key == "um_port_legacy":
        result = str(value or "").strip()
        return "" if not result else str(_port(result, "legacy User Manager port"))
    if key == "xui_api_token":
        result = str(value or "").strip()
        if not from_env and not result:
            raise ValueError("XUI API Token نمی‌تواند خالی باشد")
        if len(result) > 4096:
            raise ValueError("XUI API Token بیش از حد طولانی است")
        return result
    if key == "xui_host":
        return _host(value, "XUI IP")
    if key == "xui_port":
        return _port(value, "XUI Panel Port")
    if key == "xui_base_path":
        raw = str(value or "").strip()
        parts = [part for part in raw.split("/") if part]
        result = "/" + "/".join(parts) + "/" if parts else "/"
        if len(result) > 512:
            raise ValueError("XUI Panel Path بیش از حد طولانی است")
        return result
    if key in {
        "xui_verify_tls", "zarinpal_sandbox",
        "openvpn_sales_enabled", "v2ray_sales_enabled",
        "zarinpal_enabled", "card_transfer_enabled",
        "referral_enabled", "wallet_enabled",
        "account_expiry_notifications_enabled",
    }:
        return _bool_value(value, strict=not from_env)
    if key == "account_expiry_check_interval_minutes":
        try:
            result = int(str(value).strip())
        except Exception as exc:
            raise ValueError("فاصله بررسی اعلان‌ها باید عدد صحیح باشد") from exc
        if result < 5 or result > 1440:
            raise ValueError("فاصله بررسی اعلان‌ها باید بین 5 و 1440 دقیقه باشد")
        return result
    if key == "xui_sub_public_base":
        return _http_url_or_zero(value, "Change Subscription URL", trailing_slash=True)
    if key == "zarinpal_merchant_id":
        result = str(value or "").strip()
        if not result or len(result) > 128:
            raise ValueError("Merchant ID معتبر نیست")
        return result
    if key == "card_transfer_card_number":
        result = re.sub(r"[\s-]+", "", str(value or "").strip())
        if from_env and not result:
            return ""
        if not result.isdigit() or len(result) != 16:
            raise ValueError("شماره کارت باید دقیقاً 16 رقم باشد")
        return result
    if key == "card_transfer_card_holder":
        result = str(value or "").strip()
        if from_env and not result:
            return ""
        if not result or len(result) > 100:
            raise ValueError("نام صاحب کارت باید بین 1 تا 100 کاراکتر باشد")
        return result
    raise ValueError("تنظیم ناشناخته است")


def _env_get(source: Mapping | None, name: str, default):
    if source is None:
        return os.getenv(name, default)
    return source.get(name, default)


def build_migration_seed(source: Mapping | None = None) -> dict:
    api_ip = normalize_setting(
        "api_ip", _env_get(source, "API_IP", DEFAULTS["api_ip"]), from_env=True
    )
    um_host_raw = str(_env_get(source, "UM_HOST", api_ip) or "").strip()
    um_host_legacy = "" if not um_host_raw or um_host_raw == api_ip else um_host_raw
    openvpn_url = _env_get(
        source, "OPENVPN_CONNECTIONS_URL", DEFAULTS["openvpn_connections_url"]
    )
    if not str(openvpn_url or "").strip():
        openvpn_url = "0"
    subscription_base = _env_get(
        source, "XUI_SUB_PUBLIC_BASE", DEFAULTS["xui_sub_public_base"]
    )
    if not str(subscription_base or "").strip():
        subscription_base = "0"
    raw = {
        "bot_brand_name": _env_get(source, "BOT_BRAND_NAME", DEFAULTS["bot_brand_name"]),
        "account_username_prefix": _env_get(source, "ACCOUNT_USERNAME_PREFIX", DEFAULTS["account_username_prefix"]),
        "referral_code_prefix": _env_get(source, "REFERRAL_CODE_PREFIX", DEFAULTS["referral_code_prefix"]),
        "referral_enabled": DEFAULTS["referral_enabled"],
        "wallet_enabled": DEFAULTS["wallet_enabled"],
        "openvpn_connections_url": openvpn_url,
        "openvpn_sales_enabled": DEFAULTS["openvpn_sales_enabled"],
        "api_ip": api_ip,
        "api_port": _env_get(source, "API_PORT", DEFAULTS["api_port"]),
        "api_user": _env_get(source, "API_USER", ""),
        "api_pass": _env_get(source, "API_PASS", ""),
        "um_scheme": _env_get(source, "UM_SCHEME", DEFAULTS["um_scheme"]),
        "um_path": _env_get(source, "UM_PATH", DEFAULTS["um_path"]),
        "um_host_legacy": um_host_legacy,
        "um_port_legacy": _env_get(source, "UM_PORT", ""),
        "xui_api_token": _env_get(source, "XUI_API_TOKEN", ""),
        "xui_scheme": _env_get(source, "XUI_SCHEME", DEFAULTS["xui_scheme"]),
        "xui_host": _env_get(source, "XUI_HOST", DEFAULTS["xui_host"]),
        "xui_port": _env_get(source, "XUI_PORT", DEFAULTS["xui_port"]),
        "xui_base_path": _env_get(source, "XUI_BASE_PATH", DEFAULTS["xui_base_path"]),
        "xui_verify_tls": _env_get(source, "XUI_VERIFY_TLS", DEFAULTS["xui_verify_tls"]),
        "xui_sub_public_base": subscription_base,
        "v2ray_sales_enabled": DEFAULTS["v2ray_sales_enabled"],
        "zarinpal_sandbox": _env_get(source, "ZARINPAL_SANDBOX", DEFAULTS["zarinpal_sandbox"]),
        "zarinpal_merchant_id": _env_get(source, "ZARINPAL_MERCHANT_ID", DEFAULTS["zarinpal_merchant_id"]),
        "zarinpal_enabled": DEFAULTS["zarinpal_enabled"],
        "card_transfer_enabled": DEFAULTS["card_transfer_enabled"],
        "card_transfer_card_number": DEFAULTS["card_transfer_card_number"],
        "card_transfer_card_holder": DEFAULTS["card_transfer_card_holder"],
        "account_expiry_notifications_enabled": DEFAULTS["account_expiry_notifications_enabled"],
        "account_expiry_check_interval_minutes": DEFAULTS["account_expiry_check_interval_minutes"],
    }
    return {key: normalize_setting(key, value, from_env=True) for key, value in raw.items()}


def parse_admin_ids(raw: str) -> tuple[int, ...]:
    result = []
    seen = set()
    for item in str(raw or "").split(","):
        item = item.strip()
        if not item.isdigit():
            continue
        value = int(item)
        if 0 < value <= 9_223_372_036_854_775_807 and value not in seen:
            seen.add(value)
            result.append(value)
    return tuple(result)


def parse_inbound_remarks(raw: str) -> tuple[str, ...]:
    result = []
    seen = set()
    for item in str(raw or "").split("|"):
        remark = item.strip()
        folded = remark.casefold()
        if remark and folded not in seen:
            seen.add(folded)
            result.append(remark)
    return tuple(result)


class RuntimeSettingsRegistry:
    """Lock-protected atomic reference swap; all exposed values are immutable."""

    def __init__(self):
        self._lock = threading.RLock()
        self._snapshot = MappingProxyType(dict(DEFAULTS))

    def replace(self, state: dict, *, root_admin_id: int) -> Mapping:
        values = dict(DEFAULTS)
        values.update(dict((state or {}).get("settings") or {}))
        reseller_rows = []
        for raw in (state or {}).get("resellers", ()):
            if len(raw) < 7:
                continue
            reseller_id, tg_id, name, rate, debt, created_at, updated_at = raw[:7]
            trial_enabled = bool(raw[7]) if len(raw) >= 8 else True
            if int(reseller_id) <= 0 or int(tg_id) <= 0:
                continue
            reseller_rows.append(MappingProxyType({
                "id": int(reseller_id),
                "tg_id": int(tg_id),
                "name": str(name),
                "price_per_gb_toman": int(rate),
                "debt_toman": int(debt),
                "trial_enabled": trial_enabled,
                "created_at": str(created_at),
                "updated_at": str(updated_at),
            }))
        reseller_records = tuple(reseller_rows)
        reseller_by_tg_id = MappingProxyType({
            int(row["tg_id"]): row for row in reseller_records
        })
        inbound_records = tuple(
            (int(item[0]), str(item[1]))
            for item in (state or {}).get("inbounds", ())
            if len(item) == 2 and str(item[1]).strip()
        )
        # v3.6 has exactly one Admin: the first valid ADMIN_IDS ENV entry.
        effective = (int(root_admin_id),) if int(root_admin_id or 0) > 0 else ()
        values.update({
            "root_admin_id": int(root_admin_id or 0),
            "dynamic_admin_ids": (),
            "effective_admin_ids": effective,
            "reseller_records": reseller_records,
            "reseller_by_tg_id": reseller_by_tg_id,
            "xui_inbound_records": inbound_records,
            "xui_inbound_remarks": tuple(item[1] for item in inbound_records),
            "migration_version": str((state or {}).get("migration_version") or ""),
        })
        fresh = MappingProxyType(values)
        with self._lock:
            self._snapshot = fresh
        return fresh

    def snapshot(self) -> Mapping:
        with self._lock:
            return self._snapshot

    def get(self, key: str, default=None):
        return self.snapshot().get(key, default)

    def is_admin(self, tg_id: int) -> bool:
        try:
            value = int(tg_id)
        except Exception:
            return False
        return value in self.get("effective_admin_ids", ())


APP_SETTINGS = RuntimeSettingsRegistry()
_MUTATION_LOCK = threading.RLock()


def initialize_runtime_settings(
    *,
    source: Mapping | None = None,
    root_admin_id: int | None = None,
    env_admin_ids: tuple[int, ...] | None = None,
    inbound_remarks: tuple[str, ...] | None = None,
) -> Mapping:
    ids = tuple(config.ENV_ADMIN_IDS if env_admin_ids is None else env_admin_ids)
    root = int(config.ROOT_ADMIN_ID if root_admin_id is None else root_admin_id or 0)
    if root <= 0 and ids:
        root = int(ids[0])
    with _MUTATION_LOCK:
        existing = get_app_settings_state()
        if existing.get("migration_version"):
            # Once migrated, covered ENV values are not even parsed. A stale or
            # invalid legacy value therefore cannot overwrite Admin data or
            # prevent a later restart. The root ID remains ENV-controlled.
            state = initialize_v34_sales_settings(DEFAULTS)
            state = initialize_v35_payment_settings(DEFAULTS)
            state = initialize_v36_resellers(
                root_admin_id=root,
                env_admin_ids=tuple(x for x in ids if int(x) != root),
            )
            state = initialize_feature_toggles(DEFAULTS)
            state = initialize_v101_expiry_notifications(DEFAULTS)
            return APP_SETTINGS.replace(state, root_admin_id=root)
        seed = build_migration_seed(source)
        remarks = (
            parse_inbound_remarks(str(_env_get(source, "XUI_INBOUND_REMARKS", "")))
            if inbound_remarks is None
            else tuple(inbound_remarks)
        )
        state = initialize_app_settings(
            seed,
            extra_admin_ids=tuple(x for x in ids if int(x) != root),
            inbound_remarks=remarks,
        )
        state = initialize_v34_sales_settings(DEFAULTS)
        state = initialize_v35_payment_settings(DEFAULTS)
        state = initialize_v36_resellers(
            root_admin_id=root,
            env_admin_ids=tuple(x for x in ids if int(x) != root),
        )
        state = initialize_feature_toggles(DEFAULTS)
        state = initialize_v101_expiry_notifications(DEFAULTS)
        return APP_SETTINGS.replace(state, root_admin_id=root)


def refresh_runtime_settings(*, root_admin_id: int | None = None) -> Mapping:
    root = int(
        APP_SETTINGS.get("root_admin_id", config.ROOT_ADMIN_ID)
        if root_admin_id is None else root_admin_id or 0
    )
    with _MUTATION_LOCK:
        return APP_SETTINGS.replace(get_app_settings_state(), root_admin_id=root)


def settings_snapshot() -> Mapping:
    return APP_SETTINGS.snapshot()


def get_setting(key: str, default=None):
    return APP_SETTINGS.get(key, default)


def effective_admin_ids() -> tuple[int, ...]:
    return tuple(APP_SETTINGS.get("effective_admin_ids", ()))


def reseller_records() -> tuple[dict, ...]:
    return tuple(dict(row) for row in APP_SETTINGS.get("reseller_records", ()))


def reseller_record(tg_id: int) -> dict:
    try:
        value = int(tg_id)
    except Exception:
        return {}
    row = APP_SETTINGS.get("reseller_by_tg_id", {}).get(value)
    return dict(row) if row else {}


def is_reseller(tg_id: int) -> bool:
    return bool(reseller_record(tg_id))


def root_admin_id() -> int:
    return int(APP_SETTINGS.get("root_admin_id", 0))


def is_admin(tg_id: int) -> bool:
    return APP_SETTINGS.is_admin(tg_id)


def update_setting(key: str, value, *, admin_tg_id: int = 0) -> Mapping:
    normalized = normalize_setting(key, value, from_env=False)
    with _MUTATION_LOCK:
        if key in {"openvpn_sales_enabled", "v2ray_sales_enabled"} and not normalized:
            other_key = (
                "v2ray_sales_enabled"
                if key == "openvpn_sales_enabled"
                else "openvpn_sales_enabled"
            )
            if not bool(APP_SETTINGS.get(other_key, True)):
                raise ValueError("حداقل فروش یکی از سرویس‌های OpenVPN یا V2Ray باید فعال باشد")
        if key in {"zarinpal_enabled", "card_transfer_enabled"} and not normalized:
            other_key = (
                "card_transfer_enabled"
                if key == "zarinpal_enabled"
                else "zarinpal_enabled"
            )
            if not bool(APP_SETTINGS.get(other_key, False)):
                raise ValueError("حداقل یکی از درگاه‌های زرین‌پال یا کارت به کارت باید فعال باشد")
        if key == "card_transfer_enabled" and normalized:
            if not str(APP_SETTINGS.get("card_transfer_card_number", "") or "").strip():
                raise ValueError("ابتدا شماره کارت را تنظیم کنید")
            if not str(APP_SETTINGS.get("card_transfer_card_holder", "") or "").strip():
                raise ValueError("ابتدا نام صاحب کارت را تنظیم کنید")
        if key == "wallet_enabled" and not normalized:
            if bool(APP_SETTINGS.get("referral_enabled", True)):
                raise ValueError("تا وقتی Referral فعال است، کیف پول نمی‌تواند غیرفعال شود")
        updates = {key: normalized}
        if key == "referral_enabled" and normalized:
            # Referral rewards are paid into the wallet. Enabling Referral must
            # therefore enable both settings in one SQLite transaction and one
            # immutable snapshot replacement.
            updates["wallet_enabled"] = True
        state = _db_set_app_settings(updates, admin_tg_id=int(admin_tg_id or 0))
        return APP_SETTINGS.replace(state, root_admin_id=root_admin_id())


def referral_enabled() -> bool:
    return bool(APP_SETTINGS.get("referral_enabled", True))


def wallet_enabled() -> bool:
    return bool(APP_SETTINGS.get("wallet_enabled", True))


def set_referral_enabled(enabled: bool, *, admin_tg_id: int = 0) -> Mapping:
    return update_setting("referral_enabled", bool(enabled), admin_tg_id=admin_tg_id)


def set_wallet_enabled(enabled: bool, *, admin_tg_id: int = 0) -> Mapping:
    return update_setting("wallet_enabled", bool(enabled), admin_tg_id=admin_tg_id)


def service_sales_enabled(service: str) -> bool:
    normalized = str(service or "").strip().lower()
    if normalized == "openvpn":
        return bool(APP_SETTINGS.get("openvpn_sales_enabled", True))
    if normalized == "v2ray":
        return bool(APP_SETTINGS.get("v2ray_sales_enabled", True))
    return False


def enabled_sales_services() -> tuple[str, ...]:
    return tuple(
        service for service in ("openvpn", "v2ray")
        if service_sales_enabled(service)
    )


def set_service_sales_enabled(
    service: str, enabled: bool, *, admin_tg_id: int = 0
) -> Mapping:
    normalized = str(service or "").strip().lower()
    if normalized not in {"openvpn", "v2ray"}:
        raise ValueError("سرویس فروش نامعتبر است")
    return update_setting(
        f"{normalized}_sales_enabled", bool(enabled),
        admin_tg_id=admin_tg_id,
    )


def payment_gateway_enabled(gateway: str) -> bool:
    normalized = str(gateway or "").strip().lower()
    if normalized == "zarinpal":
        return bool(APP_SETTINGS.get("zarinpal_enabled", True))
    if normalized == "card_transfer":
        return bool(APP_SETTINGS.get("card_transfer_enabled", False))
    return False


def enabled_payment_gateways() -> tuple[str, ...]:
    return tuple(
        gateway for gateway in ("zarinpal", "card_transfer")
        if payment_gateway_enabled(gateway)
    )


def set_payment_gateway_enabled(
    gateway: str, enabled: bool, *, admin_tg_id: int = 0
) -> Mapping:
    normalized = str(gateway or "").strip().lower()
    if normalized not in {"zarinpal", "card_transfer"}:
        raise ValueError("درگاه پرداخت نامعتبر است")
    return update_setting(
        f"{normalized}_enabled", bool(enabled), admin_tg_id=admin_tg_id
    )


def add_reseller(
    *, name: str, tg_id: int, price_per_gb_toman: int,
    trial_enabled: bool = True, admin_tg_id: int = 0,
) -> Mapping:
    with _MUTATION_LOCK:
        state = _db_add_reseller(
            name=name, tg_id=tg_id, price_per_gb_toman=price_per_gb_toman,
            trial_enabled=bool(trial_enabled),
            admin_tg_id=int(admin_tg_id or 0), protected_tg_id=root_admin_id(),
        )
        return APP_SETTINGS.replace(state, root_admin_id=root_admin_id())


def edit_reseller(
    reseller_id: int, *, admin_tg_id: int = 0,
    name=None, tg_id=None, price_per_gb_toman=None, trial_enabled=None,
) -> Mapping:
    with _MUTATION_LOCK:
        state = _db_update_reseller(
            int(reseller_id), admin_tg_id=int(admin_tg_id or 0),
            protected_tg_id=root_admin_id(), name=name, tg_id=tg_id,
            price_per_gb_toman=price_per_gb_toman,
            trial_enabled=trial_enabled,
        )
        return APP_SETTINGS.replace(state, root_admin_id=root_admin_id())


def remove_reseller(reseller_id: int, *, admin_tg_id: int = 0) -> Mapping:
    with _MUTATION_LOCK:
        state = _db_delete_reseller(
            int(reseller_id), admin_tg_id=int(admin_tg_id or 0),
            protected_tg_id=root_admin_id(),
        )
        return APP_SETTINGS.replace(state, root_admin_id=root_admin_id())


def change_reseller_debt(
    reseller_id: int, debt_toman: int, *, admin_tg_id: int = 0,
    operation_id: str = "",
) -> tuple[int, int]:
    with _MUTATION_LOCK:
        result = _db_set_reseller_debt(
            int(reseller_id), int(debt_toman), admin_tg_id=int(admin_tg_id or 0),
            operation_id=operation_id,
        )
        APP_SETTINGS.replace(get_app_settings_state(), root_admin_id=root_admin_id())
        return result


def charge_reseller_order(payload: dict) -> dict:
    with _MUTATION_LOCK:
        result = _db_record_reseller_debt_charge(payload)
        APP_SETTINGS.replace(get_app_settings_state(), root_admin_id=root_admin_id())
        return result


def inbound_records() -> tuple[tuple[int, str], ...]:
    return tuple(APP_SETTINGS.get("xui_inbound_records", ()))


def add_inbound(remark: str, *, admin_tg_id: int = 0) -> Mapping:
    with _MUTATION_LOCK:
        state = _db_add_xui_inbound(remark, admin_tg_id=int(admin_tg_id or 0))
        return APP_SETTINGS.replace(state, root_admin_id=root_admin_id())


def rename_inbound(inbound_id: int, remark: str, *, admin_tg_id: int = 0) -> Mapping:
    with _MUTATION_LOCK:
        state = _db_rename_xui_inbound(
            int(inbound_id), remark, admin_tg_id=int(admin_tg_id or 0)
        )
        return APP_SETTINGS.replace(state, root_admin_id=root_admin_id())


def delete_inbound(inbound_id: int, *, admin_tg_id: int = 0) -> Mapping:
    with _MUTATION_LOCK:
        state = _db_delete_xui_inbound(int(inbound_id), admin_tg_id=int(admin_tg_id or 0))
        return APP_SETTINGS.replace(state, root_admin_id=root_admin_id())


initialize_runtime_settings()
