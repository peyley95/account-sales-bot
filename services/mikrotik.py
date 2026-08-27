import re
from datetime import datetime, timezone, timedelta

import requests
import routeros_api
from routeros_api.exceptions import (
    FatalRouterOsApiError,
    RouterOsApiConnectionError,
    RouterOsApiFatalCommunicationError,
)

from config import (
    MIKROTIK_API_TIMEOUT_SECONDS,
    UM_CONNECT_TIMEOUT_SECONDS, UM_READ_TIMEOUT_SECONDS,
)
from app_settings import settings_snapshot


def _build_um_base(settings=None):
    current = settings_snapshot() if settings is None else settings
    scheme = str(current.get("um_scheme") or "http").lower()
    host = str(current.get("um_host_legacy") or current.get("api_ip") or "127.0.0.1").strip()
    port = str(current.get("um_port_legacy") or "").strip()
    path = "/".join(part for part in str(current.get("um_path") or "um").split("/") if part)
    base = f"{scheme}://{host}"
    if port:
        base += f":{port}"
    if path:
        base += f"/{path}"
    return base.rstrip("/")


def _is_transport_failure(exc: Exception) -> bool:
    """Errors that make further calls on the same RouterOS socket unsafe/slow."""
    return isinstance(
        exc,
        (
            OSError,
            TimeoutError,
            FatalRouterOsApiError,
            RouterOsApiConnectionError,
            RouterOsApiFatalCommunicationError,
        ),
    )


def _is_missing_resource(exc: Exception) -> bool:
    text = str(exc or "").lower()
    return any(
        phrase in text
        for phrase in (
            "no such command",
            "unknown command",
            "unknown path",
            "not enough permissions",
        )
    )


# Numeric values returned by the User Manager Web API. Keep the codes in the
# service result so the Telegram UI never has to guess the package state from a
# missing end-time (a profile that starts on first use intentionally has none).
UM_PROFILE_STATE_LABELS = {
    0: "فعال نشده",
    1: "اتمام حجم بسته",
    2: "فعال شده",
    3: "منقضی شده",
}
UM_PROFILE_STARTS_AT_LABELS = {
    0: "از اولین استفاده",
    1: "بلافاصله",
}


# Kept from the original bot: RouterOsApiPool + plaintext_login=True.
def connect_mikrotik(settings=None):
    current = settings_snapshot() if settings is None else settings
    pool = None
    try:
        pool = routeros_api.RouterOsApiPool(
            host=str(current.get("api_ip") or "127.0.0.1"),
            username=str(current.get("api_user") or ""),
            password=str(current.get("api_pass") or ""),
            port=int(current.get("api_port") or 8728),
            plaintext_login=True,
        )
        # RouterOS-api defaults to 15 seconds per socket operation. A status
        # lookup performs several operations, so set the timeout before the
        # real socket is opened by get_api().
        pool.socket_timeout = float(MIKROTIK_API_TIMEOUT_SECONDS)
        api = pool.get_api()
    except Exception:
        if pool is not None:
            try:
                pool.disconnect()
            except Exception:
                pass
        raise RuntimeError("اتصال یا احراز هویت MikroTik RouterOS ناموفق بود") from None
    return pool, api


def healthcheck() -> bool:
    """Open and close one RouterOS connection in the same worker."""
    current = settings_snapshot()
    pool = None
    try:
        pool, _ = connect_mikrotik(current)
        return True
    finally:
        if pool is not None:
            pool.disconnect()


def test_connection() -> dict:
    """Non-destructive RouterOS API connectivity/authentication test."""
    current = settings_snapshot()
    result = {
        "routeros_ok": False,
        "routeros_detail": "RouterOS API authentication failed",
    }
    pool = None
    try:
        pool, api = connect_mikrotik(current)
        # Read-only resource lookup proves the authenticated API session works.
        api.get_resource("/system/resource").get()
        result["routeros_ok"] = True
        result["routeros_detail"] = "RouterOS API connectivity/authentication succeeded"
    except Exception:
        pass
    finally:
        if pool is not None:
            pool.disconnect()
    return result


def _rec_id(d: dict):
    return d.get(".id") or d.get("id")


def find_profile_exact_or_casefold(api, desired: str):
    prof = api.get_resource("/user-manager/profile")
    res = prof.get(name=desired)
    if res:
        return desired
    names = [p.get("name", "") for p in prof.get()]
    low = desired.casefold()
    for n in names:
        if n and n.casefold() == low:
            return n
    return None


def _safe_remove_user_profile(user_profile, rec):
    rid = _rec_id(rec)
    if rid:
        try:
            user_profile.remove(id=rid)
            return
        except Exception:
            pass
    user = rec.get("user")
    profile = rec.get("profile")
    if user and profile:
        user_profile.remove(user=user, profile=profile)


def user_exists(username: str) -> bool:
    current = settings_snapshot()
    pool, api = connect_mikrotik(current)
    try:
        return bool(api.get_resource("/user-manager/user").get(name=username))
    finally:
        pool.disconnect()


def create_user_with_profile(username: str, password: str, profile_name: str, *,
                             before_write=None, after_write=None):
    """Idempotently create a bot-owned user and ensure its profile assignment.

    Never delete an existing username. A check/create race in older versions
    could otherwise remove an unrelated User Manager account.
    """
    current = settings_snapshot()
    pool, api = connect_mikrotik(current)
    try:
        prof_name = find_profile_exact_or_casefold(api, profile_name)
        if not prof_name:
            raise RuntimeError(f"پروفایل «{profile_name}» در User Manager پیدا نشد.")
        userman = api.get_resource("/user-manager/user")
        user_profile = api.get_resource("/user-manager/user-profile")
        existing = userman.get(name=username)
        assigned = user_profile.get(user=username) if existing else []
        assigned_rows = assigned if isinstance(assigned, list) else ([assigned] if assigned else [])
        if existing:
            rec = existing[0] if isinstance(existing, list) else existing
            existing_password = str(rec.get("password") or "") if isinstance(rec, dict) else ""
            if existing_password and existing_password != str(password):
                raise RuntimeError("نام کاربری هم‌زمان توسط اکانت دیگری استفاده شده است؛ دوباره تلاش کنید.")
            already_assigned = any(
                isinstance(ap, dict)
                and str(ap.get("profile") or "").casefold() == str(prof_name).casefold()
                for ap in assigned_rows
            )
            if already_assigned:
                return
            if not existing_password:
                # Some RouterOS permission sets hide passwords. Without either
                # the expected password or the expected assignment we cannot
                # prove this pre-existing username belongs to this order.
                raise RuntimeError(
                    "مالکیت نام کاربری موجود قابل تأیید نیست؛ برای جلوگیری از تغییر اکانت دیگر، عملیات متوقف شد."
                )
        else:
            if before_write is not None:
                before_write()
            userman.add(name=username, password=password, group="default", shared_users="0")
            assigned_rows = []
        if any(
            isinstance(ap, dict)
            and str(ap.get("profile") or "").casefold() == str(prof_name).casefold()
            for ap in assigned_rows
        ):
            return
        if existing and before_write is not None:
            before_write()
        for ap in assigned_rows:
            if isinstance(ap, dict):
                _safe_remove_user_profile(user_profile, ap)
        user_profile.add(user=username, profile=prof_name)
        if after_write is not None:
            after_write()
    finally:
        pool.disconnect()


def ensure_user_exists_and_assign(username: str, profile_name: str, password_factory, *,
                                  before_write=None, after_write=None) -> str:
    # Same renewal behavior as the original bot: keep existing password, replace profile assignment.
    current = settings_snapshot()
    pool, api = connect_mikrotik(current)
    try:
        prof_name = find_profile_exact_or_casefold(api, profile_name)
        if not prof_name:
            raise RuntimeError(f"پروفایل «{profile_name}» در User Manager پیدا نشد.")
        userman = api.get_resource("/user-manager/user")
        user_profile = api.get_resource("/user-manager/user-profile")
        users = userman.get(name=username)
        if not users:
            password = password_factory()
            if before_write is not None:
                before_write()
            userman.add(name=username, password=password, group="default", shared_users="0")
        else:
            urec = users[0] if isinstance(users, list) else users
            password = (urec.get("password") if isinstance(urec, dict) else None) or password_factory()
        assigned = user_profile.get(user=username)
        if users and before_write is not None:
            before_write()
        for ap in (assigned if isinstance(assigned, list) else ([assigned] if assigned else [])):
            if isinstance(ap, dict):
                _safe_remove_user_profile(user_profile, ap)
        user_profile.add(user=username, profile=prof_name)
        if after_write is not None:
            after_write()
        return password
    finally:
        pool.disconnect()


def _without_realm(u: str) -> str:
    return (u or "").split("@", 1)[0]


def _find_user_and_password(api, username: str):
    # RouterOS 7 uses /user-manager/user. The legacy path is attempted only if
    # the current resource itself fails; it must not add a full socket timeout to
    # every normal status request.
    paths = ["/user-manager/user", "/tool/user-manager/user"]
    name_in = (username or "").strip()
    for p in paths:
        try:
            resource = api.get_resource(p)
            recs = resource.get(name=name_in)
            if recs:
                rec = recs[0] if isinstance(recs, list) else recs
                return rec.get("name", name_in), rec.get("password")

            # Exact lookup can miss usernames with a realm suffix. Only list the
            # working resource once, rather than scanning both current and legacy
            # paths and swallowing two independent timeouts.
            items = resource.get()
            items = items if isinstance(items, list) else [items]
            base = _without_realm(name_in)
            low = name_in.casefold()
            for it in items:
                if isinstance(it, dict) and _without_realm(str(it.get("name", ""))) == base:
                    return str(it.get("name", "")), it.get("password")
            for it in items:
                if isinstance(it, dict) and str(it.get("name", "")).casefold() == low:
                    return str(it.get("name", "")), it.get("password")
            return None, None
        except Exception as exc:
            # The legacy path is useful only when this RouterOS build genuinely
            # lacks /user-manager/user. A timeout/closed socket must propagate;
            # otherwise one network failure becomes another full timeout and is
            # finally misreported to the user as "account not found".
            if p == paths[0] and _is_missing_resource(exc):
                continue
            raise
    return None, None


def _parse_dt(value: str):
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%b/%d/%Y %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(value.strip(), fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            continue
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc)
    except Exception:
        return None


def _parse_exp_after(s: str):
    if not s:
        return None
    total = timedelta(0)
    for num, unit in re.findall(r"(\d+)([wdhms])", s):
        v = int(num)
        if unit == "w": total += timedelta(days=7 * v)
        elif unit == "d": total += timedelta(days=v)
        elif unit == "h": total += timedelta(hours=v)
        elif unit == "m": total += timedelta(minutes=v)
        elif unit == "s": total += timedelta(seconds=v)
    return total if total.total_seconds() > 0 else None


def _um_session(settings=None):
    current = settings_snapshot() if settings is None else settings
    s = requests.Session()
    try:
        connect_timeout, read_timeout = _um_timeout(current)
        with s.get(
            f"{_build_um_base(current)}/login_dynamic.html",
            timeout=(min(connect_timeout, 1.5), min(read_timeout, 2.0)),
        ) as response:
            # Loading the page primes the same cookies as the original bot. Its
            # body is irrelevant; the API login below remains authoritative.
            response.content
    except requests.RequestException:
        # The API login below is authoritative. A missing/slow HTML page must not
        # consume the full status deadline before the real request starts.
        pass
    return s


def _um_timeout(_settings=None):
    return (float(UM_CONNECT_TIMEOUT_SECONDS), float(UM_READ_TIMEOUT_SECONDS))


def _um_login(s: requests.Session, username: str, password: str, settings=None):
    current = settings_snapshot() if settings is None else settings
    with s.post(
        f"{_build_um_base(current)}/api/login",
        data={"username": username, "password": password},
        timeout=_um_timeout(current),
    ) as r:
        r.raise_for_status()
        jr = r.json()
    if not jr.get("success"):
        raise RuntimeError(f"UM login failed: {jr}")


def _um_get_user(s: requests.Session, settings=None):
    current = settings_snapshot() if settings is None else settings
    with s.post(f"{_build_um_base(current)}/api/getUser", data={}, timeout=_um_timeout(current)) as r:
        r.raise_for_status()
        return r.json()


def _um_get_user_profiles(s: requests.Session, settings=None):
    current = settings_snapshot() if settings is None else settings
    with s.post(f"{_build_um_base(current)}/api/getUserProfiles", data={}, timeout=_um_timeout(current)) as r:
        r.raise_for_status()
        return r.json()


def _numeric_um_code(value, valid_codes):
    """Return a documented User Manager numeric code from int/string input."""
    if isinstance(value, bool) or value is None:
        return None
    try:
        if isinstance(value, float):
            if not value.is_integer():
                return None
            code = int(value)
        else:
            code = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    return code if code in valid_codes else None


def _um_profile_state_code(value):
    return _numeric_um_code(value, UM_PROFILE_STATE_LABELS)


def _um_profile_starts_at_code(value):
    return _numeric_um_code(value, UM_PROFILE_STARTS_AT_LABELS)


def _profile_field(profile: dict, *wanted):
    """Read camelCase, snake_case or kebab-case Web API field variants."""
    normalized = {
        str(key).replace("_", "").replace("-", "").lower(): value
        for key, value in profile.items()
    }
    for name in wanted:
        key = name.replace("_", "").replace("-", "").lower()
        if key in normalized:
            return normalized[key]
    return None


def _web_profile_name(profile: dict) -> str:
    value = _profile_field(profile, "profileName", "profile", "name")
    if isinstance(value, dict):
        value = value.get("name") or value.get("profileName")
    return str(value or "").strip()


def _web_profile_metadata(obj, preferred_profile: str = "") -> dict:
    """Select the relevant Web API profile and decode state/startsAt exactly."""
    empty = {"state": None, "starts_at": None, "expiry": None}
    try:
        if not (isinstance(obj, dict) and obj.get("success")):
            return empty
        profiles = obj.get("data", {}).get("profiles", [])
        profiles = profiles if isinstance(profiles, list) else []
        preferred = str(preferred_profile or "").strip().casefold()
        candidates = []
        state_priority = {2: 4, 0: 3, 1: 2, 3: 1, None: 0}
        for index, profile in enumerate(profiles):
            if not isinstance(profile, dict):
                continue
            state = _um_profile_state_code(_profile_field(profile, "state"))
            starts_at = _um_profile_starts_at_code(_profile_field(profile, "startsAt"))
            exp_after = _profile_field(profile, "expAfter")
            duration = _parse_exp_after(exp_after) if isinstance(exp_after, str) else None
            name = _web_profile_name(profile).casefold()
            name_match = int(bool(preferred and name and name == preferred))
            candidates.append((name_match, state_priority[state], index, state, starts_at, duration))
        if not candidates:
            return empty
        candidates.sort(key=lambda item: (item[0], item[1], -item[2]), reverse=True)
        _, _, _, state, starts_at, duration = candidates[0]
        return {
            "state": state,
            "starts_at": starts_at,
            "expiry": datetime.now(timezone.utc) + duration if duration else None,
        }
    except Exception:
        return empty


def _parse_routeros_bytes(value):
    """Convert RouterOS byte counters such as 2553.3MiB to integer bytes."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)

    text = str(value).strip().replace(" ", "")
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        pass

    m = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)([KMGTPE]?i?B)", text, re.IGNORECASE)
    if not m:
        return None

    number = float(m.group(1))
    unit = m.group(2).upper()
    powers = {
        "B": 0,
        "KB": 1, "KIB": 1,
        "MB": 2, "MIB": 2,
        "GB": 3, "GIB": 3,
        "TB": 4, "TIB": 4,
        "PB": 5, "PIB": 5,
        "EB": 6, "EIB": 6,
    }
    power = powers.get(unit)
    if power is None:
        return None
    return int(number * (1024 ** power))


def _normalize_profile_state(state: str) -> str:
    if state is None:
        return ""
    return str(state).strip().lower().replace("_", "-").replace(" ", "-")


def _profile_state_priority(state: str) -> int:
    normalized = _normalize_profile_state(state)
    numeric = _um_profile_state_code(normalized)
    if numeric == 2:
        return 4
    if numeric == 0:
        return 3
    if numeric in {1, 3}:
        return 1
    if normalized == "running-active":
        return 4
    if normalized == "waiting":
        return 3
    if normalized in {"running", "used"}:
        return 1
    return 0


def _routeros_profile_state_code(state):
    numeric = _um_profile_state_code(state)
    if numeric is not None:
        return numeric
    normalized = _normalize_profile_state(state)
    # User Manager's textual enum mirrors the Web UI JavaScript mapping:
    # Waiting=0, Running=1, Running active=2, Used=3.  Keep this exact;
    # treating "running" as a never-started profile was the v2.3.0 status bug.
    if normalized == "waiting":
        return 0
    if normalized == "running":
        return 1
    if normalized == "running-active":
        return 2
    if normalized == "used":
        return 3
    return None


def _routeros_starts_at_code(value):
    numeric = _um_profile_starts_at_code(value)
    if numeric is not None:
        return numeric
    normalized = _normalize_profile_state(value)
    if normalized in {"first-auth", "first-authentication"}:
        return 0
    if normalized in {"assigned", "immediately", "immediate"}:
        return 1
    return None


def _routeros_usage_and_profile(api, username: str):
    """Read authoritative User Manager traffic counters and active profile."""
    dl = ul = None
    actual_profile = None
    expiry = None
    profile_state = None
    profile_starts_at = None

    try:
        userman = api.get_resource("/user-manager/user")
        users = userman.get(name=username)
        users = users if isinstance(users, list) else ([users] if users else [])
        if users:
            rec = users[0]
            rid = _rec_id(rec) if isinstance(rec, dict) else None
            if rid:
                monitored = userman.call("monitor", {"numbers": rid, "once": None})
                monitored = monitored if isinstance(monitored, list) else ([monitored] if monitored else [])
                if monitored and isinstance(monitored[0], dict):
                    mon = monitored[0]
                    dl = _parse_routeros_bytes(mon.get("total-download"))
                    ul = _parse_routeros_bytes(mon.get("total-upload"))
                    actual_profile = mon.get("actual-profile") or None
    except Exception as exc:
        if _is_transport_failure(exc):
            raise

    try:
        assigned = api.get_resource("/user-manager/user-profile").get(user=username)
        rows = assigned if isinstance(assigned, list) else ([assigned] if assigned else [])
        candidates = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            dt = _parse_dt(row.get("end-time"))
            state = _normalize_profile_state(row.get("state"))
            candidates.append((_profile_state_priority(state), dt, row.get("profile"), state))

        if candidates:
            # Prefer running-active, then waiting, then terminal states.
            candidates.sort(key=lambda x: (x[0], x[1] or datetime.min.replace(tzinfo=timezone.utc)), reverse=True)
            _, expiry, row_profile, profile_state = candidates[0]
            # Immediately after renewing an expired first-use account, User
            # Manager's user monitor can temporarily keep the old
            # ``actual-profile`` while user-profile already contains the new
            # waiting assignment.  The assigned row selected above is the
            # authoritative current package; preferring the stale monitor value
            # can make the Web fallback read the expired profile and emit a
            # false zero-volume notification before the first new connection.
            actual_profile = row_profile or actual_profile
    except Exception as exc:
        if _is_transport_failure(exc):
            raise

    # starts-when is a property of /user-manager/profile, not user-profile.
    # Reading it over RouterOS avoids a slow Web login for normal status views.
    if actual_profile:
        try:
            profiles = api.get_resource("/user-manager/profile").get(name=actual_profile)
            profiles = profiles if isinstance(profiles, list) else ([profiles] if profiles else [])
            if profiles and isinstance(profiles[0], dict):
                row = profiles[0]
                profile_starts_at = _routeros_starts_at_code(
                    row.get("starts-when", row.get("starts_at", row.get("startsAt")))
                )
        except Exception as exc:
            if _is_transport_failure(exc):
                raise

    return dl, ul, actual_profile, expiry, profile_state, profile_starts_at


def fetch_usage_and_expiry(username: str):
    # RouterOS User Manager is authoritative for traffic counters and end-time.
    # The Web API remains only as a fallback for older/different UM setups.
    current = settings_snapshot()
    pool, api = connect_mikrotik(current)
    try:
        matched, pwd = _find_user_and_password(api, username)
        if not matched:
            return {"found": False}
        dl, ul, profile, exp_dt, profile_state, profile_starts_at = _routeros_usage_and_profile(api, matched)
    finally:
        pool.disconnect()

    um_profile_state = _routeros_profile_state_code(profile_state)
    um_profile_starts_at = _um_profile_starts_at_code(profile_starts_at)

    # A never-started profile has authoritative zero counters and deliberately
    # has no end-time. Do not make it wait on the Web API just to rediscover 0.
    need_web_counters = (dl is None or ul is None) and um_profile_state != 0
    need_web_profile = (
        um_profile_state is None
        or um_profile_starts_at is None
        or (
            exp_dt is None
            and (
                um_profile_state in {1, 2}
                or (um_profile_state == 0 and um_profile_starts_at == 1)
            )
        )
    )
    web_error = None
    if need_web_counters or need_web_profile:
        s = None
        try:
            s = _um_session(current)
            _um_login(s, matched, pwd or "", current)
            if need_web_counters:
                j_user = _um_get_user(s, current)
                if j_user.get("success"):
                    data = j_user.get("data", {})
                    if dl is None:
                        dl = _parse_routeros_bytes(data.get("download"))
                    if ul is None:
                        ul = _parse_routeros_bytes(data.get("upload"))
            if need_web_profile:
                metadata = _web_profile_metadata(_um_get_user_profiles(s, current), profile)
                # RouterOS user-profile is authoritative when it supplied a
                # state.  The Web endpoint can lag briefly after a renewal, so
                # it may only fill missing fields and must never replace a
                # freshly observed waiting state with the old terminal state.
                if um_profile_state is None and metadata.get("state") is not None:
                    um_profile_state = metadata["state"]
                if um_profile_starts_at is None and metadata.get("starts_at") is not None:
                    um_profile_starts_at = metadata["starts_at"]
                if exp_dt is None and metadata.get("expiry") is not None:
                    exp_dt = metadata["expiry"]
        except Exception as exc:
            web_error = exc
        finally:
            if s is not None:
                s.close()

    usage_available = dl is not None and ul is not None
    status_available = um_profile_state is not None or exp_dt is not None
    if not usage_available and not status_available and not profile:
        raise RuntimeError("دریافت وضعیت User Manager کامل نشد؛ لطفاً دوباره تلاش کنید.") from web_error

    return {
        "found": True,
        "matched_name": matched,
        "password": pwd,
        "profile": profile,
        "total_download": dl or 0,
        "total_upload": ul or 0,
        "usage_available": usage_available,
        "status_available": status_available,
        "expiry": exp_dt,
        "profile_state": profile_state,
        "um_profile_state": um_profile_state,
        "um_profile_state_label": UM_PROFILE_STATE_LABELS.get(um_profile_state, ""),
        "um_profile_starts_at": um_profile_starts_at,
        "um_profile_starts_at_label": UM_PROFILE_STARTS_AT_LABELS.get(um_profile_starts_at, ""),
    }
