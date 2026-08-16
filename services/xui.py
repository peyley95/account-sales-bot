import secrets
import time
import uuid
from urllib.parse import quote, urlsplit, urlunsplit

import requests

from config import XUI_TIMEOUT, XUI_CONNECT_TIMEOUT_SECONDS, XUI_SUB_FALLBACK_PATH
from app_settings import settings_snapshot
from plans import gb_to_bytes


class XUIError(RuntimeError):
    pass


MILLISECONDS_PER_DAY = 86_400_000


def delayed_expiry_ms(days: int) -> int:
    """3x-ui Start-After-First-Use encoding: negative duration in ms."""
    return -max(int(days), 0) * MILLISECONDS_PER_DAY


class XUIClient:
    def __init__(self, settings=None):
        # Hold one complete immutable snapshot for this client operation.
        current = settings_snapshot() if settings is None else settings
        scheme = str(current.get("xui_scheme") or "https").lower()
        host = str(current.get("xui_host") or "127.0.0.1")
        port = int(current.get("xui_port") or 2053)
        raw_path = str(current.get("xui_base_path") or "/")
        base_path = "/" + raw_path.strip("/") if raw_path.strip("/") else ""
        self.origin = f"{scheme}://{host}:{port}"
        self.base = self.origin + base_path
        self.api_token = str(current.get("xui_api_token") or "")
        self.verify_tls = bool(current.get("xui_verify_tls", False))
        self.inbound_remarks = tuple(current.get("xui_inbound_remarks") or ())
        self.sub_public_base = str(current.get("xui_sub_public_base") or "0").strip()
        self.headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if self.api_token:
            self.headers["Authorization"] = f"Bearer {self.api_token}"

    def _request(self, method: str, path: str, **kwargs):
        if not self.api_token:
            raise XUIError("XUI_API_TOKEN تنظیم نشده است.")
        url = self.base + path
        # A client is often short-lived (for example a status button). Keeping a
        # Session on every instance leaked connection pools until GC ran. The
        # context manager closes sockets deterministically after each response.
        with requests.Session() as session:
            session.headers.update(self.headers)
            with session.request(
                method, url,
                timeout=(float(XUI_CONNECT_TIMEOUT_SECONDS), float(XUI_TIMEOUT)),
                verify=self.verify_tls,
                **kwargs,
            ) as r:
                try:
                    data = r.json()
                except Exception:
                    data = None
                status_code = int(r.status_code)
        if status_code >= 400:
            raise XUIError(f"3x-ui HTTP {status_code}")
        if not isinstance(data, dict):
            raise XUIError("پاسخ نامعتبر از 3x-ui")
        if data.get("success") is False:
            raise XUIError(data.get("msg") or "3x-ui request failed")
        return data

    def get(self, path: str):
        return self._request("GET", path)

    def post(self, path: str, payload=None):
        return self._request("POST", path, json=payload if payload is not None else {})

    def healthcheck(self):
        return self.get("/panel/api/inbounds/options")

    def inbound_ids(self) -> list[int]:
        obj = self.get("/panel/api/inbounds/options").get("obj") or []
        found = []
        missing = []
        for wanted in self.inbound_remarks:
            row = next((x for x in obj if str(x.get("remark", "")) == wanted), None)
            if not row:
                missing.append(wanted)
                continue
            if str(row.get("protocol", "")).lower() != "vless":
                raise XUIError(f"Inbound {wanted} پروتکل VLESS نیست.")
            found.append(int(row["id"]))
        if missing:
            raise XUIError("Inbound پیدا نشد: " + " | ".join(missing))
        return found

    @staticmethod
    def _is_not_found_error(exc: Exception) -> bool:
        msg = str(exc or "").lower()
        return (
            "not found" in msg
            or "پیدا نشد" in msg
            or "does not exist" in msg
            or "不存在" in msg
        )

    def get_client_optional(self, email: str):
        try:
            return self.get_client(email)
        except XUIError as exc:
            if self._is_not_found_error(exc):
                return None
            raise

    def client_exists(self, email: str) -> bool:
        return self.get_client_optional(email) is not None

    def create_client(self, email: str, tg_id: int, gb: int, days: int, *,
                      before_write=None, after_write=None) -> dict:
        sub_id = secrets.token_hex(8)  # 16 chars, same length as the v3 UI default.
        client_uuid = str(uuid.uuid4())
        inbound_ids = self.inbound_ids()
        if not inbound_ids:
            raise XUIError("هیچ Inbound برای ساخت اکانت V2Ray تنظیم نشده است.")
        payload = {
            "client": {
                "email": email,
                "subId": sub_id,
                "id": client_uuid,
                "flow": "",
                "security": "auto",
                "totalGB": gb_to_bytes(gb),
                # 3x-ui native "Start After First Use": a negative expiryTime
                # is a duration. The panel converts it to an absolute timestamp
                # after the first real traffic is recorded.
                "expiryTime": delayed_expiry_ms(days),
                "limitIp": 0,
                "tgId": int(tg_id),
                "reset": 0,
                "comment": f"Telegram:{tg_id}",
                "enable": True,
            },
            "inboundIds": inbound_ids,
        }
        if before_write is not None:
            before_write()
        self.post("/panel/api/clients/add", payload)
        if after_write is not None:
            after_write()
        return self.get_client(email)

    def get_client(self, email: str) -> dict:
        obj = self.get(f"/panel/api/clients/get/{quote(email, safe='')}").get("obj")
        if not isinstance(obj, dict) or not isinstance(obj.get("client"), dict):
            raise XUIError("Client در 3x-ui پیدا نشد.")
        return obj

    def get_by_tg_id(self, tg_id: int) -> list[dict]:
        obj = self.get(f"/panel/api/clients/get/tgId/{int(tg_id)}").get("obj") or []
        return obj if isinstance(obj, list) else []

    def get_traffic(self, email: str) -> dict:
        obj = self.get(f"/panel/api/clients/traffic/{quote(email, safe='')}").get("obj") or {}
        return obj if isinstance(obj, dict) else {}

    def links(self, email: str) -> list[str]:
        obj = self.get(f"/panel/api/clients/links/{quote(email, safe='')}").get("obj") or []
        return [str(x) for x in obj] if isinstance(obj, list) else []

    def sub_links(self, sub_id: str) -> list[str]:
        obj = self.get(f"/panel/api/clients/subLinks/{quote(sub_id, safe='')}").get("obj") or []
        return [str(x) for x in obj] if isinstance(obj, list) else []

    def _sub_uri_from_panel(self) -> str:
        try:
            obj = self.post("/panel/api/setting/defaultSettings").get("obj") or {}
            uri = str(obj.get("subURI") or "").strip()
            return uri
        except Exception:
            return ""

    def subscription_url(self, sub_id: str) -> str:
        # v3 UI builds subscription as subURI + subId. Keep that exact path/query,
        # but replace the public scheme+host with the requested domain.
        panel_uri = self._sub_uri_from_panel()
        if panel_uri:
            full = panel_uri + sub_id
            src = urlsplit(full)
            if self.sub_public_base == "0":
                if src.scheme and src.netloc:
                    return full
                return self.origin.rstrip("/") + "/" + full.lstrip("/")
            public = urlsplit(self.sub_public_base)
            scheme = public.scheme or "https"
            netloc = public.netloc or public.path
            return urlunsplit((scheme, netloc, src.path, src.query, src.fragment))
        path = "/" + XUI_SUB_FALLBACK_PATH.strip("/") + "/" + sub_id
        public = self.origin if self.sub_public_base == "0" else self.sub_public_base.rstrip("/")
        return public + path

    def test_connection(self) -> dict:
        """Read-only API authentication and configured-inbound resolution."""
        result = {"connectivity_ok": False, "inbounds_ok": False, "detail": ""}
        try:
            obj = self.healthcheck().get("obj") or []
            result["connectivity_ok"] = True
            result["detail"] = f"3x-ui API authentication succeeded; {len(obj)} inbounds available"
            self.inbound_ids()
            result["inbounds_ok"] = True
        except Exception as exc:
            result["detail"] = str(exc)[:300]
        return result

    @staticmethod
    def status_from(client: dict, traffic: dict) -> dict:
        total = int(client.get("totalGB") or traffic.get("total") or 0)
        up = int(traffic.get("up") or 0)
        down = int(traffic.get("down") or 0)
        used = up + down
        remaining = max(total - used, 0) if total > 0 else 0
        expiry = int(client.get("expiryTime") or traffic.get("expiryTime") or 0)
        now_ms = int(time.time() * 1000)

        waiting_first_use = expiry < 0
        if waiting_first_use:
            # Negative expiry is a duration, not an already-expired timestamp.
            remaining_ms = abs(expiry)
        elif expiry > 0:
            remaining_ms = max(expiry - now_ms, 0)
        else:
            remaining_ms = 0

        remaining_days_float = remaining_ms / MILLISECONDS_PER_DAY
        enabled = bool(client.get("enable", True))
        traffic_valid = total <= 0 or remaining > 0
        time_valid = waiting_first_use or expiry == 0 or remaining_ms > 0
        # "active" means the timed package has actually started. A delayed
        # client is connectable but deliberately shown as "فعال نشده".
        active = enabled and traffic_valid and time_valid and not waiting_first_use
        return {
            "total_bytes": total,
            "used_bytes": used,
            "remaining_bytes": remaining,
            "expiry_ms": expiry,
            "remaining_days_float": remaining_days_float,
            "active": active,
            "waiting_first_use": bool(enabled and traffic_valid and waiting_first_use),
            "enabled": enabled,
        }

    def status(self, email: str) -> dict:
        hydrated = self.get_client(email)
        client = hydrated["client"]
        traffic = self.get_traffic(email)
        s = self.status_from(client, traffic)
        s.update({"client": client, "traffic": traffic, "inboundIds": hydrated.get("inboundIds", [])})
        return s

    def _update_payload(self, client: dict, *, total_bytes: int, expiry_ms: int, enable: bool = True):
        payload = {
            "email": client.get("email", ""),
            "subId": client.get("subId", ""),
            "id": client.get("uuid") or client.get("id") or "",
            "password": client.get("password", ""),
            "auth": client.get("auth", ""),
            "flow": client.get("flow") or "",
            "security": client.get("security") or "auto",
            "totalGB": int(total_bytes),
            "expiryTime": int(expiry_ms),
            "limitIp": int(client.get("limitIp") or 0),
            "tgId": int(client.get("tgId") or 0),
            "reset": int(client.get("reset") or 0),
            "group": client.get("group") or "",
            "comment": client.get("comment") or "",
            "enable": bool(enable),
        }
        reverse = client.get("reverse")
        if isinstance(reverse, dict) and reverse.get("tag"):
            payload["reverse"] = {"tag": reverse["tag"]}
        return payload

    def renew(self, email: str, gb: int, days: int, *, before_write=None, after_write=None) -> None:
        """Apply only the remote renewal writes; do not append a fallible final read.

        The after_write barrier journals remote_done in the same worker immediately
        after the last successful mutation. The caller performs hydration separately,
        so a later GET failure cannot leave an additive renewal ambiguous.
        """
        status = self.status(email)
        if before_write is not None:
            before_write()
        if status.get("waiting_first_use"):
            # Renewing before first use must not accidentally start the timer.
            # Extend both the delayed duration and quota in-place.
            client = status.get("client") or self.get_client(email)["client"]
            current_total = int(client.get("totalGB") or status.get("total_bytes") or 0)
            current_delay = abs(int(client.get("expiryTime") or status.get("expiry_ms") or 0))
            payload = self._update_payload(
                client,
                total_bytes=current_total + gb_to_bytes(gb),
                expiry_ms=-(current_delay + int(days) * MILLISECONDS_PER_DAY),
                enable=True,
            )
            self.post(f"/panel/api/clients/update/{quote(email, safe='')}", payload)
        elif status["active"]:
            # Already-started account: preserve the existing v2.3.0 additive renewal.
            self.post("/panel/api/clients/bulkAdjust", {
                "emails": [email],
                "addDays": int(days),
                "addBytes": gb_to_bytes(gb),
                "flow": "",
            })
        else:
            # Expired/depleted account: reset the old package, but make the new
            # package start on the subscriber's next real use.
            self.post(f"/panel/api/clients/resetTraffic/{quote(email, safe='')}")
            hydrated = self.get_client(email)
            client = hydrated["client"]
            payload = self._update_payload(
                client,
                total_bytes=gb_to_bytes(gb),
                expiry_ms=delayed_expiry_ms(days),
                enable=True,
            )
            self.post(f"/panel/api/clients/update/{quote(email, safe='')}", payload)
        if after_write is not None:
            after_write()
        return None
