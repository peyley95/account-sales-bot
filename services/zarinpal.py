import time
import requests

from config import ZARINPAL_CONNECT_TIMEOUT_SECONDS, ZARINPAL_READ_TIMEOUT_SECONDS
from app_settings import settings_snapshot
from plans import price_rial
from storage import add_pending

class ZarinpalError(RuntimeError):
    pass


def _gateway_urls(sandbox: bool) -> tuple[str, str, str]:
    if sandbox:
        return (
            "https://sandbox.zarinpal.com/pg/v4/payment/request.json",
            "https://sandbox.zarinpal.com/pg/v4/payment/verify.json",
            "https://sandbox.zarinpal.com/pg/StartPay/{authority}",
        )
    return (
        "https://api.zarinpal.com/pg/v4/payment/request.json",
        "https://api.zarinpal.com/pg/v4/payment/verify.json",
        "https://www.zarinpal.com/pg/StartPay/{authority}",
    )


def _current_gateway_config():
    current = settings_snapshot()
    merchant_id = str(current.get("zarinpal_merchant_id") or "").strip()
    sandbox = bool(current.get("zarinpal_sandbox", False))
    request_url, verify_url, start_url = _gateway_urls(sandbox)
    return merchant_id, sandbox, request_url, verify_url, start_url


def _timeout():
    return (float(ZARINPAL_CONNECT_TIMEOUT_SECONDS), float(ZARINPAL_READ_TIMEOUT_SECONDS))


def _post_json(url: str, payload: dict, *, accept_client_error: bool = False) -> dict:
    """POST JSON with deterministic socket cleanup and Persian-safe failures."""
    try:
        with requests.Session() as session:
            with session.post(url, json=payload, timeout=_timeout()) as response:
                status = int(response.status_code)
                try:
                    data = response.json()
                except ValueError:
                    data = None
    except requests.RequestException as exc:
        raise ZarinpalError("ارتباط با زرین‌پال برقرار نشد؛ لطفاً دوباره تلاش کنید.") from exc

    if status >= 500:
        raise ZarinpalError("زرین‌پال موقتاً پاسخ‌گو نیست؛ لطفاً دوباره تلاش کنید.")
    if 400 <= status < 500 and accept_client_error and isinstance(data, dict):
        result = dict(data)
        result["_http_status"] = status
        return result
    if status >= 400:
        raise ZarinpalError("درخواست درگاه پرداخت پذیرفته نشد؛ لطفاً دوباره تلاش کنید.")
    if not isinstance(data, dict):
        raise ZarinpalError("پاسخ نامعتبر از زرین‌پال دریافت شد؛ لطفاً دوباره تلاش کنید.")
    return data


def create_payment(*, tg_id: int, service: str, action: str, plan_key: str, identifier: str = "",
                   amount_rial: int | None = None, order_id: str = "", extra_payload: dict | None = None):
    merchant_id, _sandbox, request_url, _verify_url, start_url = _current_gateway_config()
    if not merchant_id or merchant_id == "xxxx-xxx-xxx-xxx-xxxx":
        raise ZarinpalError("Merchant ID زرین‌پال تنظیم نشده است.")
    # amount_rial can be lower than the current PLAN_* price after referral discount/wallet.
    amount = int(
        amount_rial if amount_rial is not None else price_rial(plan_key, service)
    )
    if amount <= 0:
        raise ValueError("مبلغ درگاه باید بیشتر از صفر باشد")
    description = "ربات تلگرام"
    callback_url = "https://zarinpal.com"  # Manual 'پرداخت کردم' verification, preserved.
    effective_order_id = str(order_id or f"{tg_id}-{int(time.time())}")
    req_payload = {
        "merchant_id": merchant_id,
        "amount": amount,
        "description": description,
        "callback_url": callback_url,
        "metadata": {"order_id": effective_order_id},
    }
    data = _post_json(request_url, req_payload)
    authority = (data.get("data") or {}).get("authority")
    if not authority:
        raise ZarinpalError("زرین‌پال لینک پرداخت را ایجاد نکرد؛ لطفاً دوباره تلاش کنید.")
    payment_url = start_url.format(authority=authority)
    payload = {
        "tg_id": int(tg_id),
        "service": service,
        "action": action,
        "plan_key": plan_key,
        "identifier": identifier,
        "amount_rial": amount,
        "order_id": effective_order_id,
        "ts": int(time.time()),
        "payment_url": payment_url,
    }
    payload.update(dict(extra_payload or {}))
    add_pending(authority, payload)
    return payment_url, authority


def _verify_request(authority: str, amount_rial: int):
    merchant_id, _sandbox, _request_url, verify_url, _start_url = _current_gateway_config()
    if not merchant_id or merchant_id == "xxxx-xxx-xxx-xxx-xxxx":
        raise ZarinpalError("Merchant ID زرین‌پال تنظیم نشده است.")
    payload = {
        "merchant_id": merchant_id,
        "amount": int(amount_rial),
        "authority": authority,
    }
    # Unpaid sessions are commonly returned as structured 4xx responses. They
    # are valid gateway results, not transport exceptions.
    return _post_json(verify_url, payload, accept_client_error=True)


def test_connection() -> dict:
    """Configuration validation plus bounded reachability; creates no payment."""
    merchant_id, sandbox, request_url, _verify_url, _start_url = _current_gateway_config()
    configured = bool(merchant_id and merchant_id != "xxxx-xxx-xxx-xxx-xxxx")
    result = {
        "configured": configured,
        "sandbox": sandbox,
        "reachable": False,
        "detail": "Merchant ID format/configuration checked; no payment request was created",
    }
    if not configured:
        return result
    try:
        parsed_origin = request_url.split("/pg/", 1)[0] + "/"
        with requests.Session() as session:
            with session.get(parsed_origin, timeout=_timeout()) as response:
                response.content
                status = int(response.status_code)
        result["reachable"] = status < 500
        result["detail"] += f"; gateway endpoint responded ({status})"
    except requests.RequestException:
        result["detail"] += "; gateway endpoint was not reachable"
    return result


def verify_payment(authority: str, amount_rial: int):
    """Strict verification used after the user says the payment is complete."""
    return _verify_request(authority, amount_rial)


def verify_payment_for_cancel(authority: str, amount_rial: int):
    """
    Verification used only by the explicit cancel button.

    ZarinPal may answer an unpaid/invalid session with a 4xx response.  Those
    responses are still useful structured gateway results and must not be
    confused with a network outage. Server-side (5xx) failures remain errors
    so a possibly-paid order is never removed just because the gateway is down.
    """
    return _verify_request(authority, amount_rial)
