import asyncio
import os
import shutil
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


TEST_DATA_DIR = tempfile.mkdtemp(prefix="vpn-bot-v22-tests-")
os.environ.setdefault("BOT_TOKEN", "123456:TEST_TOKEN")
os.environ.setdefault("ZARINPAL_MERCHANT_ID", "00000000-0000-0000-0000-000000000000")
os.environ.setdefault("PLAN_TEST", "10|30|150000|1M-10G")
os.environ["DATA_DIR"] = TEST_DATA_DIR

import bot  # noqa: E402
import plans  # noqa: E402
import runtime  # noqa: E402
import storage  # noqa: E402
from services import mikrotik, xui, zarinpal  # noqa: E402


class FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.closed = True
        return False

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.closed = True
        return False

    def post(self, *args, **kwargs):
        return self.response

    def close(self):
        self.closed = True


class FakeMessage:
    def __init__(self):
        self.edits = []
        self.replies = []

    async def edit_text(self, text, **kwargs):
        self.edits.append((text, kwargs))

    async def reply_text(self, text, **kwargs):
        self.replies.append((text, kwargs))


class ZarinpalTests(unittest.TestCase):
    def test_structured_unpaid_4xx_is_returned_not_raised(self):
        payload = {"errors": {"code": -51, "message": "Session is not paid"}}
        response = FakeResponse(400, payload)
        session = FakeSession(response)
        with patch("services.zarinpal.requests.Session", return_value=session):
            result = zarinpal.verify_payment("A" * 36, 1_500_000)
        self.assertEqual(result["errors"]["code"], -51)
        self.assertEqual(result["_http_status"], 400)
        self.assertTrue(response.closed)
        self.assertTrue(session.closed)  # v2.0.4 closes the Session per request.

    def test_production_endpoints_match_v204(self):
        request_url, verify_url, start_url = zarinpal._gateway_urls(False)
        self.assertEqual(
            request_url,
            "https://api.zarinpal.com/pg/v4/payment/request.json",
        )
        self.assertEqual(
            verify_url,
            "https://api.zarinpal.com/pg/v4/payment/verify.json",
        )
        self.assertEqual(
            start_url,
            "https://www.zarinpal.com/pg/StartPay/{authority}",
        )

    def test_create_payment_persists_pending_like_v204(self):
        response = {"data": {"code": 100, "authority": "A" * 36}}
        saved = []
        with (
            patch.object(zarinpal, "_post_json", return_value=response),
            patch.object(zarinpal, "add_pending", lambda authority, payload: saved.append((authority, payload))),
        ):
            payment_url, authority = zarinpal.create_payment(
                tg_id=42,
                service="openvpn",
                action="renew",
                plan_key="TEST",
                identifier="vpn000042",
                amount_rial=1_500_000,
                order_id="ord-42-test",
            )
        self.assertEqual(authority, "A" * 36)
        self.assertEqual(saved[0][0], "A" * 36)
        self.assertEqual(saved[0][1]["order_id"], "ord-42-test")
        self.assertTrue(payment_url.startswith("https://www.zarinpal.com/pg/StartPay/"))


class FirstPurchaseFlowTests(unittest.IsolatedAsyncioTestCase):
    async def test_new_user_plan_callback_reaches_referral_screen_without_nameerror(self):
        message = FakeMessage()

        query = SimpleNamespace(
            data="plan|buy|openvpn|TEST",
            from_user=SimpleNamespace(id=73001, username="newuser", first_name="New", last_name="User"),
            message=message,
        )

        update = SimpleNamespace(callback_query=query)
        context = SimpleNamespace(user_data={})

        async def fake_blocking(func, *_args, **_kwargs):
            if func is bot.maintenance_mode:
                return False
            if func is bot.has_completed_purchase:
                return False
            if func is bot.pending_first_purchase_for_user:
                return (None, None)
            return None

        with (
            patch.object(bot.CALLBACK_LIMITER, "allow", return_value=(True, "")),
            patch.object(bot, "run_blocking", fake_blocking),
            patch.object(bot, "safe_callback_answer", AsyncMock(return_value=True)),
            patch.object(bot, "schedule_telegram_profile", lambda *_args, **_kwargs: None),
        ):
            await bot.callback_router(update, context)

        self.assertEqual(context.user_data["first_buy_order"]["plan_key"], "TEST")
        self.assertIn("تخفیف خرید اول", message.edits[-1][0])

    async def test_new_first_purchase_can_create_gateway_link(self):
        message = FakeMessage()
        context = SimpleNamespace(user_data={})
        breakdown = {
            "base_price_toman": 150_000,
            "referral_discount_toman": 0,
            "after_discount_toman": 150_000,
            "wallet_used_toman": 0,
            "gateway_toman": 150_000,
        }

        async def fake_gateway(*_args, **_kwargs):
            return "https://www.zarinpal.com/pg/StartPay/" + "N" * 36, "N" * 36

        async def fake_blocking(func, *_args, **_kwargs):
            if func is bot.latest_pending_for_user:
                return (None, None)
            if func is bot.has_completed_purchase:
                return False
            if func is bot.pending_first_purchase_for_user:
                return (None, None)
            return None

        with (
            patch.object(bot, "run_blocking", fake_blocking),
            patch.object(bot, "order_price_breakdown", AsyncMock(return_value=breakdown)),
            patch.object(bot, "run_zarinpal", fake_gateway),
        ):
            await bot.start_order_message(
                message, context, 73002, "openvpn", "buy", "TEST", "", edit=True
            )

        self.assertIn("www.zarinpal.com/pg/StartPay", message.edits[-1][0])


class PaymentUiTests(unittest.IsolatedAsyncioTestCase):
    async def test_unpaid_feedback_edits_same_payment_message_in_persian(self):
        pending = {
            "tg_id": 42,
            "plan_key": "TEST",
            "amount_rial": 1_500_000,
            "gateway_toman": 150_000,
            "base_price_toman": 150_000,
            "payment_url": "https://example.invalid/pay",
            "plan_snapshot": {
                "gb": 10,
                "days": 30,
                "price_toman": 150_000,
                "openvpn_profile": "1M-10G",
            },
        }
        message = FakeMessage()
        query = SimpleNamespace(from_user=SimpleNamespace(id=42), message=message)
        context = SimpleNamespace(user_data={})

        with (
            patch.object(bot, "latest_pending_for_user", lambda _uid: ("AUTH", pending)),
            patch.object(bot, "verify_payment", lambda _authority, _amount: {"errors": {"code": -51}}),
        ):
            await bot.verify_latest(query, context)

        self.assertEqual(len(message.replies), 0)
        self.assertEqual(len(message.edits), 1)
        self.assertIn("پرداخت شما انجام نشده، لطفاً پرداخت خود را تکمیل کنید", message.edits[0][0])

    async def test_inconclusive_gateway_code_is_not_reported_as_unpaid(self):
        pending = {
            "tg_id": 42,
            "plan_key": "TEST",
            "amount_rial": 1_500_000,
            "gateway_toman": 150_000,
            "base_price_toman": 150_000,
            "payment_url": "https://example.invalid/pay",
            "plan_snapshot": {
                "gb": 10,
                "days": 30,
                "price_toman": 150_000,
                "openvpn_profile": "1M-10G",
            },
        }
        message = FakeMessage()
        query = SimpleNamespace(from_user=SimpleNamespace(id=42), message=message)
        context = SimpleNamespace(user_data={})
        with (
            patch.object(bot, "latest_pending_for_user", lambda _uid: ("AUTH", pending)),
            patch.object(bot, "verify_payment", lambda _authority, _amount: {"errors": {"code": -9}}),
        ):
            await bot.verify_latest(query, context)
        self.assertIn("وضعیت پرداخت هنوز قطعی نیست", message.edits[0][0])
        self.assertNotIn("پرداخت شما انجام نشده", message.edits[0][0])

    async def test_gateway_link_is_rendered_after_v204_creation(self):
        message = FakeMessage()
        context = SimpleNamespace(user_data={})
        breakdown = {
            "base_price_toman": 150_000,
            "referral_discount_toman": 0,
            "after_discount_toman": 150_000,
            "wallet_used_toman": 0,
            "gateway_toman": 150_000,
        }

        async def fake_gateway(*_args, **_kwargs):
            return "https://www.zarinpal.com/pg/StartPay/" + "A" * 36, "A" * 36

        async def fake_blocking(func, *_args, **_kwargs):
            if func is bot.latest_pending_for_user:
                return (None, None)
            if func is bot.has_completed_purchase:
                return True
            return None

        with (
            patch.object(bot, "order_price_breakdown", AsyncMock(return_value=breakdown)),
            patch.object(bot, "run_zarinpal", fake_gateway),
            patch.object(bot, "run_blocking", fake_blocking),
        ):
            await bot.start_order_message(
                message,
                context,
                42,
                "openvpn",
                "renew",
                "TEST",
                "vpn000042",
                edit=True,
            )

        rendered_markup = message.edits[-1][1]["reply_markup"]
        self.assertTrue(rendered_markup.inline_keyboard[0][0].url.startswith("https://www.zarinpal.com/"))


class PaymentCancellationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.pending = {
            "tg_id": 42,
            "plan_key": "TEST",
            "amount_rial": 1_500_000,
            "gateway_toman": 150_000,
            "wallet_committed": False,
            "payment_url": "https://www.zarinpal.com/pg/StartPay/" + "A" * 36,
        }
        self.message = FakeMessage()
        self.query = SimpleNamespace(from_user=SimpleNamespace(id=42), message=self.message)
        self.context = SimpleNamespace(user_data={"some": "state"})

    async def test_unpaid_verify_cancels_and_releases_local_order(self):
        popped = []
        with (
            patch.object(bot, "latest_pending_for_user", lambda _uid: ("A" * 36, self.pending)),
            patch.object(bot, "verify_payment_for_cancel", lambda _authority, _amount: {"errors": {"code": -51}}),
            patch.object(bot, "pop_pending", lambda authority: popped.append(authority) or self.pending),
        ):
            await bot.cancel_latest_payment(self.query, self.context)

        self.assertEqual(popped, ["A" * 36])
        self.assertEqual(self.context.user_data, {})
        self.assertIn("سفارش لغو شد", self.message.edits[-1][0])

    async def test_invalid_authority_is_cancelable_like_v204(self):
        popped = []
        result = {"errors": {"code": -54, "message": "Invalid authority"}}
        with (
            patch.object(bot, "latest_pending_for_user", lambda _uid: ("A" * 36, self.pending)),
            patch.object(bot, "verify_payment_for_cancel", lambda _authority, _amount: result),
            patch.object(bot, "pop_pending", lambda authority: popped.append(authority) or self.pending),
        ):
            await bot.cancel_latest_payment(self.query, self.context)
        self.assertEqual(popped, ["A" * 36])

    async def test_inconclusive_verify_does_not_cancel(self):
        popped = []
        with (
            patch.object(bot, "latest_pending_for_user", lambda _uid: ("A" * 36, self.pending)),
            patch.object(bot, "verify_payment_for_cancel", lambda _authority, _amount: {"errors": {"code": -9}}),
            patch.object(bot, "pop_pending", lambda authority: popped.append(authority)),
        ):
            await bot.cancel_latest_payment(self.query, self.context)
        self.assertEqual(popped, [])
        self.assertIn("وضعیت این پرداخت هنوز قطعی نیست", self.message.edits[-1][0])

    async def test_paid_payment_is_delivered_instead_of_cancelled(self):
        authorized = {
            **self.pending,
            "payment_authorized": True,
            "payment_authorization_method": "zarinpal",
        }
        with (
            patch.object(bot, "latest_pending_for_user", lambda _uid: ("A" * 36, self.pending)),
            patch.object(bot, "verify_payment_for_cancel", lambda _authority, _amount: {"data": {"code": 100}}),
            patch.object(bot, "authorize_pending_payment", return_value=authorized) as authorize,
            patch.object(bot, "_deliver_verified_pending", AsyncMock()) as deliver,
        ):
            await bot.cancel_latest_payment(self.query, self.context)
        authorize.assert_called_once()
        deliver.assert_awaited_once()

    async def test_stale_unpaid_order_uses_verify_and_is_removed(self):
        popped = []
        with (
            patch.object(bot, "pending_plan_is_stale", return_value=True),
            patch.object(bot, "verify_payment", lambda _authority, _amount: {"errors": {"code": -51}}),
            patch.object(bot, "pop_pending", lambda authority: popped.append(authority) or self.pending),
        ):
            state = await bot.reconcile_stale_pending("A" * 36, self.pending, self.message, self.context)
        self.assertEqual(state, "removed")
        self.assertEqual(popped, ["A" * 36])

    async def test_v204_create_cancel_create_again_regression(self):
        uid = 72041
        authorities = iter(["E" * 36, "F" * 36])

        def request_result(*_args, **_kwargs):
            return {"data": {"code": 100, "authority": next(authorities)}}

        with patch.object(zarinpal, "_post_json", side_effect=request_result):
            first_url, first_authority = zarinpal.create_payment(
                tg_id=uid, service="openvpn", action="buy", plan_key="TEST",
                amount_rial=1_500_000, order_id="regression-first",
                extra_payload={"first_purchase": True, "gateway_toman": 150_000, "wallet_committed": False},
            )
        self.assertEqual(storage.pending_first_purchase_for_user(uid)[0], first_authority)
        self.assertIn(first_authority, first_url)

        message = FakeMessage()
        query = SimpleNamespace(from_user=SimpleNamespace(id=uid), message=message)
        context = SimpleNamespace(user_data={"flow": "buy"})
        with patch.object(bot, "verify_payment_for_cancel", lambda _authority, _amount: {"errors": {"code": -51}}):
            await bot.cancel_latest_payment(query, context)
        self.assertEqual(storage.pending_first_purchase_for_user(uid), (None, None))

        with patch.object(zarinpal, "_post_json", side_effect=request_result):
            second_url, second_authority = zarinpal.create_payment(
                tg_id=uid, service="openvpn", action="buy", plan_key="TEST",
                amount_rial=1_500_000, order_id="regression-second",
                extra_payload={"first_purchase": True, "gateway_toman": 150_000, "wallet_committed": False},
            )
        self.assertNotEqual(first_authority, second_authority)
        self.assertEqual(storage.pending_first_purchase_for_user(uid)[0], second_authority)
        self.assertIn(second_authority, second_url)

    def test_first_purchase_storage_helper_is_imported_into_bot(self):
        self.assertIs(bot.pending_first_purchase_for_user, storage.pending_first_purchase_for_user)

    async def test_cancel_sweeps_legacy_duplicate_unpaid_first_purchase_rows(self):
        uid = 72043
        now = int(time.time())
        older = "G" * 36
        newer = "H" * 36
        base = {
            "tg_id": uid,
            "first_purchase": True,
            "plan_key": "TEST",
            "amount_rial": 1_500_000,
            "gateway_toman": 150_000,
            "wallet_committed": False,
        }
        storage.add_pending(older, {**base, "ts": now, "payment_url": "https://example.invalid/old"})
        storage.add_pending(newer, {**base, "ts": now + 1, "payment_url": "https://example.invalid/new"})
        message = FakeMessage()
        query = SimpleNamespace(from_user=SimpleNamespace(id=uid), message=message)
        context = SimpleNamespace(user_data={"flow": "buy"})
        with patch.object(bot, "verify_payment_for_cancel", lambda _authority, _amount: {"errors": {"code": -51}}):
            await bot.cancel_latest_payment(query, context)
        self.assertEqual(storage.pending_first_purchase_for_user(uid), (None, None))
        self.assertEqual(storage.list_pending_for_user(uid), [])

    async def test_cancel_preflight_also_cleans_older_unpaid_first_purchase(self):
        uid = 72045
        now = int(time.time())
        older = "L" * 36
        preflight = "preflight-ord-72045-test"
        base = {
            "tg_id": uid,
            "first_purchase": True,
            "plan_key": "TEST",
            "amount_rial": 1_500_000,
            "gateway_toman": 150_000,
            "wallet_committed": False,
        }
        storage.add_pending(older, {**base, "ts": now, "payment_url": "https://example.invalid/old"})
        storage.add_pending(preflight, {**base, "ts": now + 1, "payment_kind": "preflight"})
        message = FakeMessage()
        query = SimpleNamespace(from_user=SimpleNamespace(id=uid), message=message)
        context = SimpleNamespace(user_data={})
        with patch.object(bot, "verify_payment_for_cancel", lambda _authority, _amount: {"errors": {"code": -51}}):
            await bot.cancel_latest_payment(query, context)
        self.assertEqual(storage.list_pending_for_user(uid), [])
        self.assertIn("لغو", message.edits[-1][0])

    async def test_duplicate_cleanup_never_deletes_paid_authority(self):
        uid = 72044
        now = int(time.time())
        unpaid = "J" * 36
        paid = "K" * 36
        base = {
            "tg_id": uid,
            "first_purchase": True,
            "plan_key": "TEST",
            "amount_rial": 1_500_000,
            "gateway_toman": 150_000,
            "wallet_committed": False,
        }
        storage.add_pending(paid, {**base, "ts": now, "payment_url": "https://example.invalid/paid"})
        storage.add_pending(unpaid, {**base, "ts": now + 1, "payment_url": "https://example.invalid/unpaid"})

        def result(authority, _amount):
            if authority == paid:
                return {"data": {"code": 100}}
            return {"errors": {"code": -51}}

        message = FakeMessage()
        query = SimpleNamespace(from_user=SimpleNamespace(id=uid), message=message)
        context = SimpleNamespace(user_data={})
        with patch.object(bot, "verify_payment_for_cancel", result):
            await bot.cancel_latest_payment(query, context)
        self.assertIsNotNone(storage.get_pending(paid))
        self.assertIsNone(storage.get_pending(unpaid))

    async def test_cancelled_pending_no_longer_blocks_new_first_purchase(self):
        uid = 72042
        authority = "C" * 36
        pending = {
            "tg_id": uid,
            "ts": int(time.time()),
            "first_purchase": True,
            "plan_key": "TEST",
            "amount_rial": 1_500_000,
            "gateway_toman": 150_000,
            "wallet_committed": False,
            "payment_url": "https://www.zarinpal.com/pg/StartPay/" + authority,
        }
        storage.add_pending(authority, pending)
        message = FakeMessage()
        query = SimpleNamespace(from_user=SimpleNamespace(id=uid), message=message)
        context = SimpleNamespace(user_data={"stale": "state"})
        with patch.object(bot, "verify_payment_for_cancel", lambda _authority, _amount: {"errors": {"code": -51}}):
            await bot.cancel_latest_payment(query, context)
        self.assertEqual(storage.pending_first_purchase_for_user(uid), (None, None))
        new_authority = "D" * 36
        storage.add_pending(new_authority, {**pending, "ts": int(time.time()) + 1, "payment_url": "new"})
        found_authority, found = storage.pending_first_purchase_for_user(uid)
        self.assertEqual(found_authority, new_authority)
        self.assertIsNotNone(found)


class ExecutorIsolationTests(unittest.IsolatedAsyncioTestCase):
    async def test_timed_out_awaiter_does_not_release_running_lane(self):
        lane = bot.BlockingLane("test", 1, queue_factor=1)
        release = threading.Event()
        try:
            wrapped = lane.submit(lambda: release.wait(2), asyncio.get_running_loop())
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(wrapped, timeout=0.02)

            self.assertEqual(lane.snapshot()["active"], 1)
            with self.assertRaises(bot.ServiceBusyError):
                lane.submit(lambda: None, asyncio.get_running_loop())

            release.set()
            deadline = time.monotonic() + 1
            while lane.snapshot()["active"] and time.monotonic() < deadline:
                await asyncio.sleep(0.01)
            self.assertEqual(lane.snapshot()["active"], 0)
        finally:
            release.set()
            lane.shutdown()

    async def test_mikrotik_saturation_does_not_block_database_lane(self):
        lane = bot.BLOCKING_LANES["mikrotik"]
        release = threading.Event()
        futures = [
            lane.submit(lambda: release.wait(2), asyncio.get_running_loop())
            for _ in range(lane.capacity)
        ]
        try:
            started = time.monotonic()
            stats = await bot.run_blocking(bot.database_stats)
            self.assertEqual(stats["quick_check"], "ok")
            self.assertLess(time.monotonic() - started, 0.5)
        finally:
            release.set()
            await asyncio.gather(*futures, return_exceptions=True)


class BoundedRuntimeStateTests(unittest.TestCase):
    def test_ttl_cache_cannot_grow_without_bound(self):
        cache = runtime.TTLCache(max_entries=16)
        for index in range(200):
            cache.set(index, index, 60)
        self.assertLessEqual(len(cache._data), 16)

    def test_callback_limiter_prunes_idle_users(self):
        limiter = runtime.CallbackRateLimiter(2, 10, 0.1)
        old = time.monotonic() - 20
        limiter._events[1].append(old)
        limiter._last_callback[1] = ("old", old)
        self.assertEqual(limiter.tracked_users(), 0)

    def test_callback_limiter_has_a_hard_user_bound(self):
        limiter = runtime.CallbackRateLimiter(60, 10, 0, max_users=128)
        for user_id in range(500):
            limiter.allow(user_id, "button")
        self.assertLessEqual(limiter.tracked_users(), 128)


class TelegramCallbackTests(unittest.IsolatedAsyncioTestCase):
    async def test_callback_failure_falls_back_to_new_message_when_edit_is_rejected(self):
        replies = []

        class Message:
            async def edit_text(self, *_args, **_kwargs):
                raise RuntimeError("message cannot be edited")

            async def reply_text(self, text, **_kwargs):
                replies.append(text)

        query = SimpleNamespace(message=Message())
        with patch.object(bot, "main_menu_keyboard", AsyncMock(return_value=None)):
            await bot.callback_failure_reply(query, 42)
        self.assertEqual(len(replies), 1)

    async def test_callback_answer_transport_failure_does_not_abort_action(self):
        class Query:
            data = "myact|status|openvpn|user"

            async def answer(self, **kwargs):
                raise RuntimeError("temporary Telegram timeout")

        self.assertFalse(await bot.safe_callback_answer(Query()))

    async def test_callback_alert_text_is_capped_to_telegram_limit(self):
        captured = []

        class Query:
            async def answer(self, **kwargs):
                captured.append(kwargs)

        self.assertTrue(await bot.safe_callback_answer(Query(), "x" * 1000, show_alert=True))
        self.assertLessEqual(len(captured[0]["text"]), 200)

    def test_long_account_identifiers_never_exceed_callback_limit(self):
        identifier = "کاربر-" + ("x" * 300)
        markup = bot.my_account_keyboard("v2ray", identifier)
        callbacks = [button.callback_data for row in markup.inline_keyboard for button in row]
        self.assertTrue(all(len(value.encode("utf-8")) <= 64 for value in callbacks if value))
        self.assertNotIn(identifier, "|".join(value for value in callbacks if value))

    def test_admin_wallet_confirmation_never_exceeds_callback_limit(self):
        markup = bot.admin_wallet_confirm_keyboard(
            "inc", 9_223_372_036_854_775_807,
            bot.MAX_ADMIN_WALLET_ADJUST_TOMAN, "f" * 12,
        )
        callbacks = [button.callback_data for row in markup.inline_keyboard for button in row]
        self.assertTrue(all(len(value.encode("utf-8")) <= 64 for value in callbacks if value))

    def test_vless_delivery_is_capped_without_cutting_html_blocks(self):
        links = [f"vless://{'x' * 900}#{index}" for index in range(12)]
        text = bot.v2ray_delivery_text("✅ ساخته شد", "customer", 10, 30, "https://sub.example/id", links)
        self.assertLess(len(text), 4096)
        self.assertEqual(text.count("<pre>"), text.count("</pre>"))
        self.assertIn("Subscription", text)


class DurableLocalOrderTests(unittest.IsolatedAsyncioTestCase):
    async def test_gateway_creation_does_not_write_temporary_pending_row(self):
        message = FakeMessage()
        context = SimpleNamespace(user_data={})
        breakdown = {
            "base_price_toman": 150_000,
            "referral_discount_toman": 0,
            "after_discount_toman": 150_000,
            "wallet_used_toman": 50_000,
            "gateway_toman": 100_000,
        }
        bot_lane_pending_calls = []

        async def gateway(*_args, **_kwargs):
            return "https://www.zarinpal.com/pg/StartPay/" + "A" * 36, "A" * 36

        with (
            patch.object(bot, "latest_pending_for_user", lambda _uid: (None, None)),
            patch.object(bot, "order_price_breakdown", AsyncMock(return_value=breakdown)),
            patch.object(bot, "add_pending", lambda *args, **kwargs: bot_lane_pending_calls.append((args, kwargs))),
            patch.object(bot, "run_zarinpal", gateway),
        ):
            await bot.start_order_message(
                message, context, 69999, "openvpn", "renew", "TEST", "customer", edit=True
            )
        self.assertEqual(bot_lane_pending_calls, [])
        self.assertIn("www.zarinpal.com/pg/StartPay", message.edits[-1][0])

    async def test_old_gateway_pending_does_not_globally_block_renewal_like_v204(self):
        existing = {
            "tg_id": 70000,
            "payment_kind": "gateway",
            "plan_key": "TEST",
            "plan_snapshot": bot.plan_snapshot("TEST"),
        }
        breakdown = {
            "base_price_toman": 150_000,
            "referral_discount_toman": 0,
            "after_discount_toman": 150_000,
            "wallet_used_toman": 0,
            "gateway_toman": 150_000,
        }
        message = FakeMessage()
        context = SimpleNamespace(user_data={})

        async def gateway(*_args, **_kwargs):
            return "https://www.zarinpal.com/pg/StartPay/" + "B" * 36, "B" * 36

        async def fake_blocking(func, *_args, **_kwargs):
            if func is bot.latest_pending_for_user:
                return ("A" * 36, existing)
            if func is bot.has_completed_purchase:
                return True
            return None

        with (
            patch.object(bot, "run_blocking", fake_blocking),
            patch.object(bot, "order_price_breakdown", AsyncMock(return_value=breakdown)),
            patch.object(bot, "run_zarinpal", gateway),
        ):
            await bot.start_order_message(
                message, context, 70000, "openvpn", "renew", "TEST", "customer", edit=True
            )
        self.assertIn("www.zarinpal.com/pg/StartPay", message.edits[-1][0])

    async def test_full_wallet_order_is_persisted_before_delivery(self):
        message = FakeMessage()
        context = SimpleNamespace(user_data={})
        calls = []
        breakdown = {
            "base_price_toman": 150_000,
            "referral_discount_toman": 0,
            "after_discount_toman": 150_000,
            "wallet_used_toman": 150_000,
            "gateway_toman": 0,
        }

        def remember_pending(authority, payload):
            calls.append(("pending", authority, dict(payload)))

        async def deliver(authority, payload, *_args, **_kwargs):
            self.assertTrue(calls)
            self.assertEqual(calls[0][0], "pending")
            self.assertEqual(payload["payment_kind"], "wallet")
            calls.append(("deliver", authority))

        with (
            patch.object(bot, "order_price_breakdown", AsyncMock(return_value=breakdown)),
            patch.object(bot, "add_pending", remember_pending),
            patch.object(bot, "_deliver_verified_pending", deliver),
        ):
            await bot.start_order_message(
                message, context, 70001, "openvpn", "renew", "TEST", "customer", edit=True
            )
        self.assertEqual([row[0] for row in calls], ["pending", "deliver"])

    async def test_local_pending_resumes_without_calling_zarinpal(self):
        pending = {
            "tg_id": 70002,
            "payment_kind": "wallet",
            "order_id": "ord-local-test",
            "service": "openvpn",
            "action": "renew",
            "plan_key": "TEST",
        }
        message = FakeMessage()
        query = SimpleNamespace(from_user=SimpleNamespace(id=70002), message=message)
        context = SimpleNamespace(user_data={})
        with (
            patch.object(bot, "latest_pending_for_user", lambda _uid: ("local-wallet-order", pending)),
            patch.object(bot, "verify_payment", side_effect=AssertionError("ZarinPal must not run")),
            patch.object(bot, "_deliver_verified_pending", AsyncMock()) as deliver,
        ):
            await bot.verify_latest(query, context)
        deliver.assert_awaited_once()

    async def test_cancel_after_debit_before_flag_update_refunds_safely(self):
        pending = {
            "tg_id": 70003,
            "payment_kind": "wallet",
            "order_id": "ord-crash-window",
            "wallet_used_toman": 50_000,
            "wallet_committed": False,
        }
        refunded = []
        popped = []
        message = FakeMessage()
        query = SimpleNamespace(from_user=SimpleNamespace(id=70003), message=message)
        context = SimpleNamespace(user_data={"x": 1})
        with (
            patch.object(bot, "latest_pending_for_user", lambda _uid: ("local-wallet-crash", pending)),
            patch.object(bot, "get_pending", lambda _authority: pending),
            patch.object(bot, "get_fulfillment", lambda _order: None),
            patch.object(bot, "wallet_order_debited", lambda _order: True),
            patch.object(bot, "refund_wallet", lambda uid, amount, *, order_id: refunded.append((uid, amount, order_id))),
            patch.object(bot, "pop_pending", lambda authority: popped.append(authority)),
            patch.object(bot, "main_menu_keyboard", AsyncMock(return_value=None)),
        ):
            await bot.cancel_latest_payment(query, context)
        self.assertEqual(refunded, [(70003, 50_000, "ord-crash-window")])
        self.assertEqual(popped, ["local-wallet-crash"])


class IdempotencyTests(unittest.IsolatedAsyncioTestCase):
    def test_admin_wallet_confirmation_is_exactly_once(self):
        uid = 71001
        before1, after1 = storage.admin_adjust_wallet(
            uid, 75_000, admin_tg_id=999, operation_id="same-confirmation"
        )
        before2, after2 = storage.admin_adjust_wallet(
            uid, 75_000, admin_tg_id=999, operation_id="same-confirmation"
        )
        self.assertEqual((before1, after1), (0, 75_000))
        self.assertEqual((before2, after2), (0, 75_000))
        self.assertEqual(storage.wallet_balance(uid), 75_000)

    def test_wallet_transaction_id_cannot_be_reused_with_another_amount(self):
        storage.debit_wallet(71006, 0, order_id="same-wallet-order")
        storage.admin_adjust_wallet(
            71006, 100_000, admin_tg_id=999, operation_id="seed-wallet"
        )
        storage.debit_wallet(71006, 10_000, order_id="same-wallet-order")
        with self.assertRaises(ValueError):
            storage.debit_wallet(71006, 20_000, order_id="same-wallet-order")

    def test_gateway_preflight_authority_is_replaced_atomically(self):
        old_authority = "preflight-atomic-order"
        new_authority = "B" * 36
        payload = {
            "tg_id": 71007,
            "ts": 123,
            "order_id": "atomic-order",
            "plan_key": "TEST",
            "wallet_used_toman": 25_000,
            "payment_kind": "preflight",
        }
        storage.add_pending(old_authority, payload)
        final_payload = {
            **payload,
            "payment_kind": "gateway",
            "payment_url": "https://example.invalid/pay",
        }
        storage.replace_pending_authority(old_authority, new_authority, final_payload)
        self.assertIsNone(storage.get_pending(old_authority))
        self.assertEqual(storage.get_pending(new_authority)["order_id"], "atomic-order")
        self.assertEqual(storage.reserved_wallet_for_user(71007), 25_000)

    def test_admin_cannot_reduce_wallet_below_gateway_preflight_reservation(self):
        user_id = 71008
        storage.admin_adjust_wallet(
            user_id, 100_000, admin_tg_id=999, operation_id="seed-reserved-wallet"
        )
        storage.add_pending("preflight-reserved-wallet", {
            "tg_id": user_id,
            "ts": 1,
            "order_id": "reserved-wallet-order",
            "plan_key": "TEST",
            "wallet_used_toman": 80_000,
            "payment_kind": "preflight",
        })
        with self.assertRaises(ValueError):
            storage.admin_adjust_wallet(
                user_id, -30_000, admin_tg_id=999,
                operation_id="invalid-reduction-during-preflight",
            )
        self.assertEqual(storage.wallet_balance(user_id), 100_000)

    def test_only_one_handler_can_claim_a_remote_write(self):
        order_id = "ord-single-remote-writer"
        storage.prepare_fulfillment(
            order_id,
            tg_id=71003,
            service="v2ray",
            action="renew",
            requested_identifier="customer",
            delivery_identifier="customer",
        )
        self.assertTrue(storage.mark_fulfillment_executing(order_id))
        self.assertFalse(storage.mark_fulfillment_executing(order_id))

    def test_late_handler_cannot_move_completed_order_backwards(self):
        order_id = "ord-monotonic-journal"
        storage.prepare_fulfillment(
            order_id,
            tg_id=71004,
            service="openvpn",
            action="buy",
            delivery_identifier="customer",
        )
        self.assertTrue(storage.mark_fulfillment_executing(order_id))
        self.assertTrue(storage.mark_fulfillment_remote_done(order_id))
        self.assertTrue(storage.mark_fulfillment_provisioned(order_id, {"text": "ok"}))
        self.assertTrue(storage.mark_fulfillment_completed(order_id))

        self.assertFalse(storage.mark_fulfillment_remote_done(order_id))
        self.assertFalse(storage.mark_fulfillment_provisioned(order_id, {"text": "late"}))
        journal = storage.get_fulfillment(order_id)
        self.assertEqual(journal["state"], "completed")
        self.assertEqual(journal["result"]["text"], "ok")

    async def test_telegram_send_failure_keeps_pending_for_safe_replay(self):
        pending = {
            "tg_id": 71005,
            "order_id": "ord-send-retry",
            "service": "openvpn",
            "action": "buy",
            "plan_key": "TEST",
            "identifier": "",
            "payment_kind": "wallet",
            "base_price_toman": 150_000,
            "referral_discount_toman": 150_000,
            "gateway_toman": 0,
            "wallet_used_toman": 0,
        }
        popped = []

        class FailedMessage:
            async def reply_text(self, *_args, **_kwargs):
                raise RuntimeError("temporary Telegram send failure")

        with (
            patch.object(bot, "fulfill", AsyncMock(return_value=("delivered", None))),
            patch.object(bot, "finalize_successful_order", AsyncMock()),
            patch.object(bot, "mark_fulfillment_completed", lambda _order_id: True),
            patch.object(bot, "pop_pending", lambda authority: popped.append(authority)),
        ):
            with self.assertRaises(RuntimeError):
                await bot._deliver_verified_pending_unlocked(
                    "local-wallet-retry", pending, FailedMessage(), SimpleNamespace()
                )
        self.assertEqual(popped, [])

    async def test_test_account_uses_stable_fulfillment_order(self):
        message = FakeMessage()
        query = SimpleNamespace(from_user=SimpleNamespace(id=71002), message=message)
        context = SimpleNamespace(user_data={})
        completed = []
        with (
            patch.object(bot, "has_test", lambda *_args: False),
            patch.object(bot, "fulfill", AsyncMock(return_value=("delivered", None))) as provision,
            patch.object(bot, "mark_test", lambda *_args: None),
            patch.object(bot, "mark_fulfillment_completed", lambda order_id: completed.append(order_id) or True),
        ):
            await bot.create_test(query, context, "openvpn")
        kwargs = provision.await_args.kwargs
        self.assertEqual(kwargs["order_id"], "test-openvpn-71002")
        self.assertTrue(kwargs["is_test"])
        self.assertEqual(completed, ["test-openvpn-71002"])


class RemoteWriteBoundaryTests(unittest.IsolatedAsyncioTestCase):
    async def test_v2ray_buy_is_remote_done_before_create_hydration_failure(self):
        order_id = "ord-v2ray-create-read-after-write"

        class FakeXUI:
            def get_client_optional(self, *_args):
                return None

            def create_client(self, *_args, **kwargs):
                kwargs["before_write"]()
                kwargs["after_write"]()
                raise RuntimeError("temporary hydration failure after client add")

        with patch.object(bot, "XUIClient", return_value=FakeXUI()):
            with self.assertRaises(RuntimeError):
                await bot.fulfill(
                    "v2ray", "buy", "TEST", 72000, "",
                    SimpleNamespace(), order_id=order_id,
                )
        journal = storage.get_fulfillment(order_id)
        self.assertEqual(journal["state"], "remote_done")

    async def test_v2ray_renew_is_remote_done_before_fallible_hydration(self):
        order_id = "ord-v2ray-read-after-write"

        class FakeXUI:
            def renew(self, *_args, **kwargs):
                kwargs["before_write"]()
                kwargs["after_write"]()
                return None

            def get_client(self, *_args):
                raise RuntimeError("temporary read failure after successful write")

        with patch.object(bot, "XUIClient", return_value=FakeXUI()):
            with self.assertRaises(RuntimeError):
                await bot.fulfill(
                    "v2ray", "renew", "TEST", 72001, "customer", SimpleNamespace(), order_id=order_id
                )
        journal = storage.get_fulfillment(order_id)
        self.assertEqual(journal["state"], "remote_done")

    async def test_v2ray_prewrite_read_failure_leaves_order_prepared(self):
        order_id = "ord-v2ray-prewrite-read-failure"

        class FakeXUI:
            def renew(self, *_args, **_kwargs):
                raise RuntimeError("status read failed before first write")

        with patch.object(bot, "XUIClient", return_value=FakeXUI()):
            with self.assertRaises(RuntimeError):
                await bot.fulfill(
                    "v2ray", "renew", "TEST", 72002, "customer",
                    SimpleNamespace(), order_id=order_id,
                )
        journal = storage.get_fulfillment(order_id)
        self.assertEqual(journal["state"], "prepared")

    def test_xui_renew_does_not_do_a_final_read_after_write(self):
        client = xui.XUIClient()
        with (
            patch.object(client, "status", return_value={"active": True}),
            patch.object(client, "post") as post,
            patch.object(client, "get_client", side_effect=AssertionError("final read must be separate")),
        ):
            self.assertIsNone(client.renew("customer", 10, 30))
        post.assert_called_once()


class V2RayFirstUseTests(unittest.IsolatedAsyncioTestCase):
    def test_create_client_uses_native_negative_delayed_expiry(self):
        client = xui.XUIClient()
        captured = []
        with (
            patch.object(client, "inbound_ids", return_value=[1, 2]),
            patch.object(client, "post", side_effect=lambda path, payload=None: captured.append((path, payload)) or {"success": True}),
            patch.object(client, "get_client", return_value={"client": {"email": "customer"}, "inboundIds": [1, 2]}),
        ):
            client.create_client("customer", 123, 10, 30)
        payload = captured[0][1]["client"]
        self.assertEqual(payload["expiryTime"], -30 * 86_400_000)
        self.assertEqual(payload["totalGB"], 10 * 1024 ** 3)
        self.assertTrue(payload["enable"])

    def test_status_negative_expiry_is_waiting_not_expired(self):
        status = xui.XUIClient.status_from(
            {"totalGB": 10 * 1024 ** 3, "expiryTime": -30 * 86_400_000, "enable": True},
            {"up": 0, "down": 0},
        )
        self.assertTrue(status["waiting_first_use"])
        self.assertFalse(status["active"])
        self.assertEqual(status["remaining_bytes"], 10 * 1024 ** 3)
        self.assertEqual(status["remaining_days_float"], 30)

    def test_waiting_renew_extends_without_starting_timer(self):
        client = xui.XUIClient()
        existing = {
            "email": "customer", "subId": "sub", "id": "uuid",
            "totalGB": 10 * 1024 ** 3, "expiryTime": -30 * 86_400_000,
            "enable": True, "tgId": 123,
        }
        status = {
            "active": False, "waiting_first_use": True, "client": existing,
            "total_bytes": existing["totalGB"], "expiry_ms": existing["expiryTime"],
        }
        calls = []
        with (
            patch.object(client, "status", return_value=status),
            patch.object(client, "post", side_effect=lambda path, payload=None: calls.append((path, payload)) or {"success": True}),
        ):
            client.renew("customer", 5, 15)
        self.assertEqual(len(calls), 1)
        self.assertIn("/panel/api/clients/update/", calls[0][0])
        self.assertEqual(calls[0][1]["totalGB"], 15 * 1024 ** 3)
        self.assertEqual(calls[0][1]["expiryTime"], -45 * 86_400_000)

    def test_expired_renew_resets_to_first_use_delay(self):
        client = xui.XUIClient()
        calls = []
        old_client = {
            "email": "customer", "subId": "sub", "id": "uuid",
            "totalGB": 10 * 1024 ** 3, "expiryTime": 1, "enable": False, "tgId": 123,
        }
        with (
            patch.object(client, "status", return_value={"active": False, "waiting_first_use": False}),
            patch.object(client, "get_client", return_value={"client": old_client}),
            patch.object(client, "post", side_effect=lambda path, payload=None: calls.append((path, payload)) or {"success": True}),
        ):
            client.renew("customer", 20, 60)
        self.assertEqual(calls[0][0], "/panel/api/clients/resetTraffic/customer")
        self.assertIn("/panel/api/clients/update/", calls[1][0])
        self.assertEqual(calls[1][1]["expiryTime"], -60 * 86_400_000)

    def test_expired_renew_is_not_notified_before_first_use(self):
        client = xui.XUIClient()
        calls = []
        old_client = {
            "email": "customer", "subId": "sub", "id": "uuid",
            "totalGB": 10 * 1024 ** 3, "expiryTime": 1,
            "enable": False, "tgId": 123,
        }
        with (
            patch.object(client, "status", return_value={"active": False, "waiting_first_use": False}),
            patch.object(client, "get_client", return_value={"client": old_client}),
            patch.object(client, "post", side_effect=lambda path, payload=None: calls.append((path, payload)) or {"success": True}),
        ):
            client.renew("customer", 20, 60)

        renewed_payload = calls[1][1]
        status = xui.XUIClient.status_from(renewed_payload, {"up": 0, "down": 0})
        self.assertTrue(status["waiting_first_use"])
        self.assertEqual(status["remaining_bytes"], 20 * 1024 ** 3)
        self.assertIsNone(bot.classify_v2ray_status(status))

    async def test_v2ray_waiting_status_ui_is_explicit(self):
        message = FakeMessage()
        waiting = {
            "remaining_days_float": 30.0,
            "remaining_bytes": 10 * 1024 ** 3,
            "active": False,
            "waiting_first_use": True,
        }
        with patch.object(bot.XUIClient, "status", return_value=waiting):
            await bot.show_status(message, 123, "v2ray", "customer", edit=True, force_refresh=True)
        rendered = message.edits[-1][0]
        self.assertIn("فعال نشده", rendered)
        self.assertIn("از اولین استفاده", rendered)
        self.assertIn("30 روز", rendered)

    def test_openvpn_account_detail_keyboard_has_copy_buttons(self):
        markup = bot.my_account_keyboard("openvpn", "vpn001", username="vpn001", password="123456")
        buttons = [row[0] for row in markup.inline_keyboard[:2]]
        self.assertEqual(buttons[0].copy_text.text, "vpn001")
        self.assertEqual(buttons[1].copy_text.text, "123456")


class OpenVPNUserManagerStatusTests(unittest.TestCase):
    def test_documented_numeric_mappings_accept_int_and_string_codes(self):
        expected_states = {
            0: "فعال نشده",
            1: "اتمام حجم بسته",
            2: "فعال شده",
            3: "منقضی شده",
        }
        expected_starts = {0: "از اولین استفاده", 1: "بلافاصله"}
        self.assertEqual(mikrotik.UM_PROFILE_STATE_LABELS, expected_states)
        self.assertEqual(mikrotik.UM_PROFILE_STARTS_AT_LABELS, expected_starts)
        for code in expected_states:
            self.assertEqual(mikrotik._um_profile_state_code(code), code)
            self.assertEqual(mikrotik._um_profile_state_code(str(code)), code)
        for code in expected_starts:
            self.assertEqual(mikrotik._um_profile_starts_at_code(code), code)
            self.assertEqual(mikrotik._um_profile_starts_at_code(str(code)), code)
        self.assertEqual(mikrotik._routeros_starts_at_code("first-auth"), 0)
        self.assertEqual(mikrotik._routeros_starts_at_code("assigned"), 1)
        self.assertEqual(mikrotik._routeros_profile_state_code("waiting"), 0)
        self.assertEqual(mikrotik._routeros_profile_state_code("running"), 1)
        self.assertEqual(mikrotik._routeros_profile_state_code("running active"), 2)
        self.assertEqual(mikrotik._routeros_profile_state_code("used"), 3)

    def test_web_profile_metadata_prefers_matching_profile_and_reads_field_variants(self):
        payload = {
            "success": True,
            "data": {
                "profiles": [
                    {"name": "other", "state": 2, "startsAt": 1, "expAfter": "2d"},
                    {"profile_name": "1M-10G", "state": "0", "starts_at": "0", "exp_after": "30d"},
                ]
            },
        }
        metadata = mikrotik._web_profile_metadata(payload, "1M-10G")
        self.assertEqual(metadata["state"], 0)
        self.assertEqual(metadata["starts_at"], 0)
        self.assertIsNotNone(metadata["expiry"])

    def test_never_used_routeros_profile_does_not_require_web_fallback(self):
        class Pool:
            disconnected = False

            def disconnect(self):
                self.disconnected = True

        pool = Pool()
        for router_state in ("0", "waiting"):
            with self.subTest(router_state=router_state):
                with (
                    patch.object(mikrotik, "connect_mikrotik", return_value=(pool, object())),
                    patch.object(mikrotik, "_find_user_and_password", return_value=("customer", "123456")),
                    patch.object(
                        mikrotik,
                        "_routeros_usage_and_profile",
                        return_value=(0, 0, "1M-10G", None, router_state, 0),
                    ),
                    patch.object(mikrotik, "_um_session", side_effect=AssertionError("Web fallback must not run")),
                ):
                    result = mikrotik.fetch_usage_and_expiry("customer")

                self.assertTrue(pool.disconnected)
                self.assertEqual(result["um_profile_state"], 0)
                self.assertEqual(result["um_profile_state_label"], "فعال نشده")
                self.assertEqual(result["um_profile_starts_at"], 0)
                self.assertEqual(result["um_profile_starts_at_label"], "از اولین استفاده")

    def test_renewed_waiting_assignment_wins_over_stale_actual_profile(self):
        class Pool:
            def disconnect(self):
                pass

        requested_profiles = []

        class UserResource:
            def get(self, **_kwargs):
                return [{".id": "*1", "name": "customer"}]

            def call(self, _command, _params):
                return [{
                    "total-download": 10 * 1024 ** 3,
                    "total-upload": 0,
                    "actual-profile": "OLD-EXPIRED",
                }]

        class UserProfileResource:
            def get(self, **_kwargs):
                return [
                    {"profile": "OLD-EXPIRED", "state": "used"},
                    {"profile": "NEW-FIRST-USE", "state": "waiting"},
                ]

        class ProfileResource:
            def get(self, **kwargs):
                requested_profiles.append(kwargs.get("name"))
                return [{"name": kwargs.get("name"), "starts-when": "first-auth"}]

        resources = {
            "/user-manager/user": UserResource(),
            "/user-manager/user-profile": UserProfileResource(),
            "/user-manager/profile": ProfileResource(),
        }

        class Api:
            def get_resource(self, path):
                return resources[path]

        with (
            patch.object(mikrotik, "connect_mikrotik", return_value=(Pool(), Api())),
            patch.object(mikrotik, "_find_user_and_password", return_value=("customer", "123456")),
            patch.object(mikrotik, "_um_session", side_effect=AssertionError("waiting profile must not need Web fallback")),
        ):
            result = mikrotik.fetch_usage_and_expiry("customer")

        self.assertEqual(requested_profiles, ["NEW-FIRST-USE"])
        self.assertEqual(result["profile"], "NEW-FIRST-USE")
        self.assertEqual(result["um_profile_state"], 0)
        self.assertEqual(result["um_profile_starts_at"], 0)
        self.assertIsNone(
            bot.classify_openvpn_status(result, quota_bytes=10 * 1024 ** 3)
        )

    def test_stale_web_state_cannot_override_routeros_waiting_state(self):
        class Pool:
            def disconnect(self):
                pass

        class Session:
            def close(self):
                pass

        stale_web_profiles = {
            "success": True,
            "data": {
                "profiles": [{
                    "name": "NEW-FIRST-USE",
                    "state": 3,
                    "startsAt": 0,
                    "expAfter": "0s",
                }]
            },
        }
        with (
            patch.object(mikrotik, "connect_mikrotik", return_value=(Pool(), object())),
            patch.object(mikrotik, "_find_user_and_password", return_value=("customer", "123456")),
            patch.object(
                mikrotik,
                "_routeros_usage_and_profile",
                return_value=(10 * 1024 ** 3, 0, "NEW-FIRST-USE", None, "waiting", None),
            ),
            patch.object(mikrotik, "_um_session", return_value=Session()),
            patch.object(mikrotik, "_um_login"),
            patch.object(mikrotik, "_um_get_user_profiles", return_value=stale_web_profiles),
        ):
            result = mikrotik.fetch_usage_and_expiry("customer")

        self.assertEqual(result["um_profile_state"], 0)
        self.assertEqual(result["um_profile_starts_at"], 0)
        self.assertIsNone(
            bot.classify_openvpn_status(result, quota_bytes=10 * 1024 ** 3)
        )

    def test_never_used_web_profile_is_not_misclassified_as_expired(self):
        class Pool:
            def disconnect(self):
                pass

        class Session:
            closed = False

            def close(self):
                self.closed = True

        session = Session()
        web_profiles = {
            "success": True,
            "data": {
                "profiles": [
                    {"name": "1M-10G", "state": 0, "startsAt": 0, "expAfter": "30d"}
                ]
            },
        }
        with (
            patch.object(mikrotik, "connect_mikrotik", return_value=(Pool(), object())),
            patch.object(mikrotik, "_find_user_and_password", return_value=("customer", "123456")),
            patch.object(
                mikrotik,
                "_routeros_usage_and_profile",
                return_value=(None, None, "1M-10G", None, None, None),
            ),
            patch.object(mikrotik, "_um_session", return_value=session),
            patch.object(mikrotik, "_um_login"),
            patch.object(
                mikrotik,
                "_um_get_user",
                return_value={"success": True, "data": {"download": 0, "upload": 0}},
            ),
            patch.object(mikrotik, "_um_get_user_profiles", return_value=web_profiles),
        ):
            result = mikrotik.fetch_usage_and_expiry("customer")

        self.assertTrue(session.closed)
        self.assertEqual(result["um_profile_state"], 0)
        self.assertEqual(result["um_profile_state_label"], "فعال نشده")
        rendered = bot.render_openvpn_status(
            result,
            "customer",
            {"plan_key": "TEST", "profile": "1M-10G"},
        )
        self.assertIn("فعال نشده", rendered)
        self.assertNotIn("منقضی شده", rendered)

    def test_openvpn_status_renders_all_user_manager_states_and_start_modes(self):
        local = {"plan_key": "TEST", "profile": "1M-10G"}
        base = {
            "found": True,
            "matched_name": "customer",
            "profile": "1M-10G",
            "profile_state": "",
            "total_download": 0,
            "total_upload": 0,
        }
        cases = [
            (0, 0, None, "فعال نشده", "از اولین استفاده", "30 روز", "10.00 GB"),
            (1, 1, datetime.now(timezone.utc) + timedelta(days=5), "اتمام حجم بسته", "بلافاصله", "5 روز", "0 MB"),
            (2, 0, datetime.now(timezone.utc) + timedelta(days=5), "فعال شده", "از اولین استفاده", "5 روز", "10.00 GB"),
            (3, 1, datetime.now(timezone.utc) + timedelta(days=5), "منقضی شده", "بلافاصله", "0 روز", "10.00 GB"),
        ]
        for state, starts_at, expiry, state_label, starts_label, days, traffic in cases:
            with self.subTest(state=state, starts_at=starts_at):
                info = {
                    **base,
                    "um_profile_state": state,
                    "um_profile_starts_at": starts_at,
                    "expiry": expiry,
                }
                rendered = bot.render_openvpn_status(info, "customer", local)
                self.assertIn(f"وضعیت بسته: <b>", rendered)
                self.assertIn(state_label, rendered)
                self.assertIn(f"شروع اعتبار: <b>{starts_label}</b>", rendered)
                self.assertIn(f"روز باقی‌مانده: <b>{days}</b>", rendered)
                self.assertIn(f"حجم باقی‌مانده: <b>{traffic}</b>", rendered)

    def test_never_used_but_immediately_started_profile_uses_live_expiry(self):
        rendered = bot.render_openvpn_status(
            {
                "found": True,
                "matched_name": "customer",
                "profile": "1M-10G",
                "profile_state": "",
                "total_download": 0,
                "total_upload": 0,
                "um_profile_state": 0,
                "um_profile_starts_at": 1,
                "expiry": datetime.now(timezone.utc) + timedelta(days=5),
            },
            "customer",
            {"plan_key": "TEST", "profile": "1M-10G"},
        )
        self.assertIn("فعال نشده", rendered)
        self.assertIn("شروع اعتبار: <b>بلافاصله</b>", rendered)
        self.assertIn("روز باقی‌مانده: <b>5 روز</b>", rendered)

    def test_partial_status_is_never_misreported_as_expired(self):
        rendered = bot.render_openvpn_status(
            {
                "found": True,
                "matched_name": "customer",
                "profile": "1M-10G",
                "profile_state": "",
                "total_download": 0,
                "total_upload": 0,
                "usage_available": False,
                "um_profile_state": None,
                "um_profile_starts_at": None,
                "expiry": None,
            },
            "customer",
            {"plan_key": "TEST", "profile": "1M-10G"},
        )
        self.assertIn("نامشخص", rendered)
        self.assertNotIn("منقضی شده", rendered)


class MikroTikTests(unittest.TestCase):
    def test_profile_lookup_never_uses_ambiguous_prefix(self):
        class Resource:
            def get(self, **kwargs):
                return [] if kwargs else [{"name": "1M-100G"}, {"name": "1M-10G-PLUS"}]

        class Api:
            def get_resource(self, _path):
                return Resource()

        self.assertIsNone(mikrotik.find_profile_exact_or_casefold(Api(), "1M-10G"))

    def test_socket_timeout_is_set_before_connect(self):
        class Pool:
            def __init__(self):
                self.socket_timeout = 15
                self.timeout_seen_by_get_api = None
                self.disconnected = False

            def get_api(self):
                self.timeout_seen_by_get_api = self.socket_timeout
                return object()

            def disconnect(self):
                self.disconnected = True

        pool = Pool()
        with patch("services.mikrotik.routeros_api.RouterOsApiPool", return_value=pool):
            returned_pool, _ = mikrotik.connect_mikrotik()
        self.assertIs(returned_pool, pool)
        self.assertEqual(pool.timeout_seen_by_get_api, 5.0)

    def test_current_user_manager_path_is_not_preceded_by_legacy_timeout(self):
        calls = []

        class Resource:
            def get(self, **kwargs):
                return [{"name": kwargs["name"], "password": "123456"}]

        class Api:
            def get_resource(self, path):
                calls.append(path)
                return Resource()

        name, password = mikrotik._find_user_and_password(Api(), "customer")
        self.assertEqual((name, password), ("customer", "123456"))
        self.assertEqual(calls, ["/user-manager/user"])

    def test_current_path_timeout_propagates_without_legacy_retry(self):
        calls = []

        class Resource:
            def get(self, **_kwargs):
                raise TimeoutError("socket timed out")

        class Api:
            def get_resource(self, path):
                calls.append(path)
                return Resource()

        with self.assertRaises(TimeoutError):
            mikrotik._find_user_and_password(Api(), "customer")
        self.assertEqual(calls, ["/user-manager/user"])

    def test_openvpn_create_never_deletes_existing_matching_user(self):
        removed = []
        added = []

        class Resource:
            def __init__(self, kind):
                self.kind = kind

            def get(self, **_kwargs):
                if self.kind == "profile":
                    return [{"name": "1M-10G"}]
                if self.kind == "user":
                    return [{".id": "*1", "name": "customer", "password": "123456"}]
                return [{".id": "*2", "user": "customer", "profile": "1M-10G"}]

            def add(self, **kwargs):
                added.append((self.kind, kwargs))

            def remove(self, **kwargs):
                removed.append((self.kind, kwargs))

        resources = {
            "/user-manager/profile": Resource("profile"),
            "/user-manager/user": Resource("user"),
            "/user-manager/user-profile": Resource("assignment"),
        }

        class Pool:
            def disconnect(self):
                pass

        class Api:
            def get_resource(self, path):
                return resources[path]

        with patch.object(mikrotik, "connect_mikrotik", return_value=(Pool(), Api())):
            mikrotik.create_user_with_profile("customer", "123456", "1M-10G")
        self.assertEqual(removed, [])
        self.assertEqual(added, [])

    def test_openvpn_create_does_not_take_over_unverifiable_existing_user(self):
        removed = []
        added = []

        class Resource:
            def __init__(self, kind):
                self.kind = kind

            def get(self, **_kwargs):
                if self.kind == "profile":
                    return [{"name": "1M-10G"}]
                if self.kind == "user":
                    return [{".id": "*1", "name": "customer"}]
                return [{".id": "*2", "user": "customer", "profile": "other"}]

            def add(self, **kwargs):
                added.append((self.kind, kwargs))

            def remove(self, **kwargs):
                removed.append((self.kind, kwargs))

        resources = {
            "/user-manager/profile": Resource("profile"),
            "/user-manager/user": Resource("user"),
            "/user-manager/user-profile": Resource("assignment"),
        }

        class Pool:
            def disconnect(self):
                pass

        class Api:
            def get_resource(self, path):
                return resources[path]

        with patch.object(mikrotik, "connect_mikrotik", return_value=(Pool(), Api())):
            with self.assertRaises(RuntimeError):
                mikrotik.create_user_with_profile("customer", "123456", "1M-10G")
        self.assertEqual(removed, [])
        self.assertEqual(added, [])

    def test_user_manager_session_is_closed_after_fallback(self):
        class Pool:
            disconnected = False

            def disconnect(self):
                self.disconnected = True

        class Session:
            closed = False

            def close(self):
                self.closed = True

        pool = Pool()
        session = Session()
        with (
            patch.object(mikrotik, "connect_mikrotik", return_value=(pool, object())),
            patch.object(mikrotik, "_find_user_and_password", return_value=("customer", "123456")),
            patch.object(mikrotik, "_routeros_usage_and_profile", return_value=(None, None, "1M-10G", None, None, None)),
            patch.object(mikrotik, "_um_session", return_value=session),
            patch.object(mikrotik, "_um_login"),
            patch.object(mikrotik, "_um_get_user", return_value={"success": True, "data": {"download": 1, "upload": 2}}),
            patch.object(mikrotik, "_um_get_user_profiles", return_value={"success": False}),
        ):
            result = mikrotik.fetch_usage_and_expiry("customer")
        self.assertTrue(result["found"])
        self.assertTrue(pool.disconnected)
        self.assertTrue(session.closed)


def tearDownModule():
    for lane in bot.BLOCKING_LANES.values():
        lane.shutdown()
    shutil.rmtree(TEST_DATA_DIR, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()


class AdminPanelV23Tests(unittest.IsolatedAsyncioTestCase):
    def test_admin_root_removes_duplicate_reports_section(self):
        markup = bot.admin_tools_keyboard()
        labels = [button.text for row in markup.inline_keyboard for button in row]
        for expected in (
            "👥 کاربران",
            "🧾 سفارش‌ها",
            "📦 فروش و بسته‌ها",
            "🛠 سیستم و نگهداری",
            "⚙️ تنظیمات",
        ):
            self.assertIn(expected, labels)
        self.assertNotIn("📊 آمار و گزارش‌ها", labels)
        payments = bot.admin_payments_menu_keyboard()
        payment_labels = [button.text for row in payments.inline_keyboard for button in row]
        self.assertIn("📊 گزارش‌ها", payment_labels)
        callbacks = [button.callback_data for row in markup.inline_keyboard for button in row]
        self.assertTrue(all(len(value.encode("utf-8")) <= 64 for value in callbacks if value))

    async def test_system_menu_does_not_trigger_live_network_probes(self):
        message = FakeMessage()
        with (
            patch.object(bot, "is_admin", return_value=True),
            patch.object(bot, "live_health_snapshot", AsyncMock(side_effect=AssertionError("must not probe"))),
        ):
            await bot.show_admin_system_menu(message, 1)
        self.assertEqual(len(message.edits), 1)
        self.assertIn("سیستم و نگهداری", message.edits[0][0])

    def test_pending_admin_rows_have_short_numeric_callback_ids(self):
        uid = 88001
        authority = "P" * 36
        storage.add_pending(authority, {
            "tg_id": uid,
            "ts": int(time.time()),
            "service": "openvpn",
            "action": "buy",
            "plan_key": "TEST",
            "amount_rial": 1_500_000,
            "gateway_toman": 150_000,
        })
        rows, total = storage.list_admin_pending_payments(offset=0, limit=10)
        target = next(row for row in rows if row["authority"] == authority)
        self.assertGreaterEqual(total, 1)
        self.assertGreater(int(target["pending_id"]), 0)
        markup = bot.admin_pending_keyboard([target], page=0, total=1)
        callbacks = [button.callback_data for row in markup.inline_keyboard for button in row]
        self.assertTrue(all(len(value.encode("utf-8")) <= 64 for value in callbacks if value))
        self.assertTrue(any(value.startswith("admin_pending_view|") for value in callbacks if value))
        storage.pop_pending(authority)


class AutoBackupV23Tests(unittest.IsolatedAsyncioTestCase):
    def test_auto_backup_switch_is_persistent_and_audited(self):
        old = storage.auto_backup_enabled()
        try:
            storage.set_auto_backup_enabled(False, admin_tg_id=999)
            self.assertFalse(storage.auto_backup_enabled())
            storage.set_auto_backup_enabled(True, admin_tg_id=999)
            self.assertTrue(storage.auto_backup_enabled())
            rows, _ = storage.list_admin_audit(offset=0, limit=10)
            self.assertTrue(any(row.get("action") == "auto_backup_toggle" for row in rows))
        finally:
            storage.set_auto_backup_enabled(old)

    def test_next_backup_uses_configured_hour_in_tehran(self):
        tz = bot._backup_timezone()
        before = datetime(2026, 8, 12, 5, 59, 0, tzinfo=tz)
        after = datetime(2026, 8, 12, 6, 1, 0, tzinfo=tz)
        self.assertAlmostEqual(bot._seconds_until_next_backup(before, hour=6), 60.0, delta=0.1)
        self.assertAlmostEqual(bot._seconds_until_next_backup(after, hour=6), 23 * 3600 + 59 * 60, delta=0.1)
        afternoon = datetime(2026, 8, 12, 15, 59, 0, tzinfo=tz)
        self.assertAlmostEqual(bot._seconds_until_next_backup(afternoon, hour=16), 60.0, delta=0.1)
        midnight = datetime(2026, 8, 12, 23, 59, 0, tzinfo=tz)
        self.assertAlmostEqual(bot._seconds_until_next_backup(midnight, hour=24), 60.0, delta=0.1)

    def test_backup_hour_is_persistent_validated_and_audited(self):
        old = storage.auto_backup_hour()
        try:
            storage.set_auto_backup_hour(16, admin_tg_id=999)
            self.assertEqual(storage.auto_backup_hour(), 16)
            self.assertEqual(storage.auto_backup_status()["hour"], 16)
            rows, _ = storage.list_admin_audit(offset=0, limit=20)
            self.assertTrue(any(
                row.get("action") == "auto_backup_hour_update" for row in rows
            ))
            with self.assertRaises(ValueError):
                storage.set_auto_backup_hour(25, admin_tg_id=999)
            self.assertEqual(storage.auto_backup_hour(), 16)
        finally:
            storage.set_auto_backup_hour(old)

    async def test_scheduled_backup_is_sent_and_delivery_is_recorded(self):
        result = storage.backup_database(force=True, keep=2)
        sender = AsyncMock(return_value=None)
        application = SimpleNamespace(bot=SimpleNamespace(send_document=sender))
        with patch.object(bot, "effective_admin_ids", return_value=(999,)):
            delivery = await bot._send_scheduled_backup(application, result)
        sender.assert_awaited_once()
        self.assertEqual(delivery["delivered_admin_ids"], [999])
        self.assertEqual(delivery["failed_admin_ids"], [])
        status = storage.auto_backup_status()
        self.assertEqual(status["last_delivery"]["delivered_admin_ids"], [999])

    async def test_scheduler_wires_due_backup_to_telegram_delivery(self):
        wake = asyncio.Event()
        application = SimpleNamespace(bot=SimpleNamespace())
        wait_calls = 0

        async def due_then_cancel(awaitable, *, timeout):
            nonlocal wait_calls
            wait_calls += 1
            awaitable.close()
            if wait_calls == 1:
                raise asyncio.TimeoutError
            raise asyncio.CancelledError

        run = AsyncMock(side_effect=[
            {"enabled": True, "hour": 16},
            {"enabled": True, "hour": 16},
            {"created": True, "path": "scheduled.sqlite3", "size_bytes": 10},
            {"enabled": True, "hour": 16},
        ])
        delivery = AsyncMock(return_value={"delivered_admin_ids": [999]})
        with (
            patch.object(bot, "run_blocking", new=run),
            patch.object(bot.asyncio, "wait_for", side_effect=due_then_cancel),
            patch.object(bot, "_send_scheduled_backup", new=delivery),
        ):
            with self.assertRaises(asyncio.CancelledError):
                await bot._backup_loop(application, wake)
        delivery.assert_awaited_once()
        self.assertEqual(delivery.await_args.args[0], application)
        self.assertEqual(delivery.await_args.args[1]["path"], "scheduled.sqlite3")

    async def test_backup_settings_show_configurable_hour_control(self):
        message = FakeMessage()
        old = storage.auto_backup_hour()
        try:
            storage.set_auto_backup_hour(16)
            with patch.object(bot, "is_admin", return_value=True):
                await bot.show_admin_backup_settings(message, 999)
            text, kwargs = message.edits[-1]
            callbacks = [
                button.callback_data
                for row in kwargs["reply_markup"].inline_keyboard
                for button in row
                if button.callback_data
            ]
            self.assertIn("16:00", text)
            self.assertIn("admin_backup_hour", callbacks)
        finally:
            storage.set_auto_backup_hour(old)

    async def test_backup_lane_saturation_does_not_block_database_lane(self):
        lane = bot.BLOCKING_LANES["backup"]
        release = threading.Event()
        future = lane.submit(lambda: release.wait(2), asyncio.get_running_loop())
        try:
            started = time.monotonic()
            stats = await bot.run_blocking(storage.database_stats)
            self.assertEqual(stats["quick_check"], "ok")
            self.assertLess(time.monotonic() - started, 0.5)
        finally:
            release.set()
            await asyncio.gather(future, return_exceptions=True)


class AdminPendingSafetyV23Tests(unittest.IsolatedAsyncioTestCase):
    def _make_query(self, callback_data: str):
        message = FakeMessage()
        query = SimpleNamespace(
            data=callback_data,
            from_user=SimpleNamespace(id=99001, username="admin", first_name="Admin", last_name=""),
            message=message,
            answer=AsyncMock(return_value=None),
        )
        return query, message

    async def test_admin_release_removes_only_definitely_unpaid_gateway_order(self):
        uid = 99011
        authority = "U" * 36
        storage.add_pending(authority, {
            "tg_id": uid, "ts": int(time.time()), "service": "openvpn", "action": "buy",
            "plan_key": "TEST", "amount_rial": 1_500_000, "gateway_toman": 150_000,
        })
        rows, _ = storage.list_admin_pending_payments(offset=0, limit=100)
        pending_id = next(int(r["pending_id"]) for r in rows if r["authority"] == authority)
        query, message = self._make_query(f"admin_pending_cancel|{pending_id}")
        update = SimpleNamespace(callback_query=query)
        context = SimpleNamespace(user_data={})
        with (
            patch.object(bot, "is_admin", return_value=True),
            patch.object(bot.CALLBACK_LIMITER, "allow", return_value=(True, "")),
            patch.object(bot, "schedule_telegram_profile", lambda *_a, **_k: None),
            patch.object(bot, "run_zarinpal", AsyncMock(return_value={"errors": {"code": -51}})),
        ):
            await bot.callback_router(update, context)
        self.assertIsNone(storage.get_pending(authority))
        self.assertIn("آزاد شد", message.edits[-1][0])

    async def test_admin_release_never_deletes_paid_gateway_order(self):
        uid = 99012
        authority = "V" * 36
        storage.add_pending(authority, {
            "tg_id": uid, "ts": int(time.time()), "service": "v2ray", "action": "buy",
            "plan_key": "TEST", "amount_rial": 1_500_000, "gateway_toman": 150_000,
        })
        rows, _ = storage.list_admin_pending_payments(offset=0, limit=100)
        pending_id = next(int(r["pending_id"]) for r in rows if r["authority"] == authority)
        query, message = self._make_query(f"admin_pending_cancel|{pending_id}")
        update = SimpleNamespace(callback_query=query)
        context = SimpleNamespace(user_data={})
        with (
            patch.object(bot, "is_admin", return_value=True),
            patch.object(bot.CALLBACK_LIMITER, "allow", return_value=(True, "")),
            patch.object(bot, "schedule_telegram_profile", lambda *_a, **_k: None),
            patch.object(bot, "run_zarinpal", AsyncMock(return_value={"data": {"code": 100}})),
        ):
            await bot.callback_router(update, context)
        self.assertIsNotNone(storage.get_pending(authority))
        self.assertIn("پرداخت‌شده", message.edits[-1][0])
        storage.pop_pending(authority)


class DynamicReferralV30Tests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.original = storage.get_referral_settings(
            default_discount_percent=bot.REFERRAL_DISCOUNT_PERCENT,
            default_reward_percent=bot.REFERRAL_REWARD_PERCENT,
        )

    def tearDown(self):
        d = storage.set_referral_percent(
            "discount", int(self.original["discount_percent"]), admin_tg_id=999,
            default_discount_percent=bot.REFERRAL_DISCOUNT_PERCENT,
            default_reward_percent=bot.REFERRAL_REWARD_PERCENT,
        )
        d = storage.set_referral_percent(
            "reward", int(self.original["reward_percent"]), admin_tg_id=999,
            default_discount_percent=bot.REFERRAL_DISCOUNT_PERCENT,
            default_reward_percent=bot.REFERRAL_REWARD_PERCENT,
        )
        bot._apply_referral_settings(d)

    async def test_admin_referral_percent_changes_price_without_restart(self):
        settings = storage.set_referral_percent(
            "discount", 25, admin_tg_id=999,
            default_discount_percent=bot.REFERRAL_DISCOUNT_PERCENT,
            default_reward_percent=bot.REFERRAL_REWARD_PERCENT,
        )
        bot._apply_referral_settings(settings)
        with patch.object(bot, "run_blocking", AsyncMock(return_value=0)):
            breakdown = await bot.order_price_breakdown(123, "TEST", referral_code="REFTEST")
        self.assertEqual(breakdown["base_price_toman"], 150_000)
        self.assertEqual(breakdown["referral_discount_percent"], 25)
        self.assertEqual(breakdown["referral_discount_toman"], 37_500)
        self.assertEqual(breakdown["gateway_toman"], 112_500)

    async def test_new_order_snapshots_referral_percentages(self):
        settings = storage.set_referral_percent(
            "discount", 20, admin_tg_id=999,
            default_discount_percent=bot.REFERRAL_DISCOUNT_PERCENT,
            default_reward_percent=bot.REFERRAL_REWARD_PERCENT,
        )
        settings = storage.set_referral_percent(
            "reward", 35, admin_tg_id=999,
            default_discount_percent=bot.REFERRAL_DISCOUNT_PERCENT,
            default_reward_percent=bot.REFERRAL_REWARD_PERCENT,
        )
        bot._apply_referral_settings(settings)
        message = FakeMessage()
        context = SimpleNamespace(user_data={})
        captured = {}

        async def fake_blocking(func, *args, **kwargs):
            if func is bot.latest_pending_for_user:
                return (None, None)
            if func is bot.has_completed_purchase:
                return False
            if func is bot.pending_first_purchase_for_user:
                return (None, None)
            if func is bot.referral_already_used:
                return False
            if func is bot.find_referrer_by_code:
                return 777
            if func is bot.wallet_available:
                return 0
            return None

        async def fake_gateway(func, *args, **kwargs):
            captured.update(kwargs.get("extra_payload") or {})
            return "https://www.zarinpal.com/pg/StartPay/" + "R" * 36, "R" * 36

        with (
            patch.object(bot, "run_blocking", fake_blocking),
            patch.object(bot, "run_zarinpal", fake_gateway),
        ):
            await bot.start_order_message(
                message, context, 778, "openvpn", "buy", "TEST", "",
                referral_code="REFTEST", referrer_tg_id=777, edit=True,
            )
        self.assertEqual(captured["referral_discount_percent"], 20)
        self.assertEqual(captured["referral_reward_percent"], 35)
        self.assertEqual(captured["referral_discount_toman"], 30_000)


class DynamicPlansV30Tests(unittest.TestCase):
    def setUp(self):
        self.key = "PTESTV30"
        try:
            storage.delete_sale_plan(self.key, admin_tg_id=999)
        except Exception:
            pass
        bot.refresh_plans(storage.list_sale_plans())

    def tearDown(self):
        try:
            storage.delete_sale_plan(self.key, admin_tg_id=999)
        except Exception:
            pass
        bot.refresh_plans(storage.list_sale_plans())

    def test_plan_crud_is_database_backed_and_shared_by_both_services(self):
        storage.create_sale_plan(
            plan_key=self.key, gb=37, months=2, price_toman=432_100,
            openvpn_profile="Exact MikroTik Profile 37", admin_tg_id=999,
        )
        bot.refresh_plans(storage.list_sale_plans())
        plan = plans.PLANS[self.key]
        self.assertEqual(plan["gb"], 37)
        self.assertEqual(plan["months"], 2)
        self.assertEqual(plan["days"], 60)
        self.assertEqual(plan["price_toman"], 432_100)
        self.assertEqual(plan["openvpn_profile"], "Exact MikroTik Profile 37")

        ovpn_callbacks = [b.callback_data for row in bot.plans_keyboard("buy", "openvpn").inline_keyboard for b in row if b.callback_data]
        v2_callbacks = [b.callback_data for row in bot.plans_keyboard("buy", "v2ray").inline_keyboard for b in row if b.callback_data]
        self.assertIn(f"plan|buy|openvpn|{self.key}", ovpn_callbacks)
        self.assertIn(f"plan|buy|v2ray|{self.key}", v2_callbacks)

        storage.update_sale_plan(self.key, field="months", value=3, admin_tg_id=999)
        storage.update_sale_plan(self.key, field="price_toman", value=500_000, admin_tg_id=999)
        storage.update_sale_plan(self.key, field="openvpn_profile", value="Profile-NAME-With-No-Quota-Hint", admin_tg_id=999)
        bot.refresh_plans(storage.list_sale_plans())
        edited = plans.PLANS[self.key]
        self.assertEqual(edited["days"], 90)
        self.assertEqual(edited["price_toman"], 500_000)
        self.assertEqual(edited["openvpn_profile"], "Profile-NAME-With-No-Quota-Hint")

        storage.delete_sale_plan(self.key, admin_tg_id=999)
        bot.refresh_plans(storage.list_sale_plans())
        self.assertNotIn(self.key, plans.PLANS)

    def test_legacy_env_seed_never_repopulates_after_v3_marker(self):
        self.assertIsNone(storage.get_sale_plan(self.key))
        storage.initialize_sale_plans([{
            "plan_key": self.key, "gb": 99, "months": 1, "days": 30,
            "price_toman": 999_000, "openvpn_profile": "SHOULD-NOT-RETURN",
        }])
        self.assertIsNone(storage.get_sale_plan(self.key))

    def test_paid_snapshot_survives_plan_delete_or_edit(self):
        storage.create_sale_plan(
            plan_key=self.key, gb=44, months=1, price_toman=444_000,
            openvpn_profile="PROFILE-OLD", admin_tg_id=999,
        )
        bot.refresh_plans(storage.list_sale_plans())
        snap = bot.plan_snapshot(self.key)
        payload = {"plan_key": self.key, "base_price_toman": 444_000, "plan_snapshot": snap}
        storage.update_sale_plan(self.key, field="price_toman", value=555_000, admin_tg_id=999)
        storage.update_sale_plan(self.key, field="openvpn_profile", value="PROFILE-NEW", admin_tg_id=999)
        bot.refresh_plans(storage.list_sale_plans())
        paid = bot.snapshot_for_delivery(payload)
        self.assertEqual(paid["price_toman"], 444_000)
        self.assertEqual(paid["openvpn_profile"], "PROFILE-OLD")
        self.assertTrue(bot.pending_plan_is_stale(payload))


class AdminPlanUiV30Tests(unittest.IsolatedAsyncioTestCase):
    async def test_long_legacy_plan_key_admin_callbacks_stay_under_telegram_limit(self):
        key = "L" * 32
        try:
            storage.delete_sale_plan(key, admin_tg_id=999)
        except Exception:
            pass
        storage.create_sale_plan(
            plan_key=key, gb=7, months=1, price_toman=77_000,
            openvpn_profile="Legacy Exact Profile", admin_tg_id=999,
        )
        bot.refresh_plans(storage.list_sale_plans())
        message = FakeMessage()
        try:
            with patch.object(bot, "is_admin", return_value=True):
                await bot.show_admin_plan_detail(message, 999, key)
            markup = message.edits[-1][1]["reply_markup"]
            callbacks = [button.callback_data for row in markup.inline_keyboard for button in row if button.callback_data]
            self.assertTrue(callbacks)
            self.assertTrue(all(len(value.encode("utf-8")) <= 64 for value in callbacks))
            self.assertTrue(any(value.startswith("admin_plan_edit|") for value in callbacks))
        finally:
            storage.delete_sale_plan(key, admin_tg_id=999)
            bot.refresh_plans(storage.list_sale_plans())

    async def test_referral_settings_screen_has_two_edit_buttons(self):
        message = FakeMessage()
        with patch.object(bot, "is_admin", return_value=True):
            await bot.show_admin_referral_settings(message, 999)
        markup = message.edits[-1][1]["reply_markup"]
        callbacks = [button.callback_data for row in markup.inline_keyboard for button in row if button.callback_data]
        self.assertIn("admin_referral_edit|discount", callbacks)
        self.assertIn("admin_referral_edit|reward", callbacks)


class AdminPlanWizardV30Tests(unittest.IsolatedAsyncioTestCase):
    async def test_add_wizard_asks_only_shared_plan_fields_and_creates_one_shared_plan(self):
        before = set(plans.PLANS.keys())
        context = SimpleNamespace(user_data={"awaiting": {"kind": "admin_plan_add", "step": "gb"}, "admin_plan_draft": {}})
        user = SimpleNamespace(id=999, username="admin", first_name="Admin", last_name="")

        async def send_text(text):
            message = FakeMessage()
            message.text = text
            update = SimpleNamespace(message=message, effective_user=user)
            with (
                patch.object(bot, "is_admin", return_value=True),
                patch.object(bot, "schedule_telegram_profile", lambda *_a, **_k: None),
            ):
                await bot.text_router(update, context)
            return message

        await send_text("73")
        self.assertEqual(context.user_data["awaiting"]["step"], "months")
        await send_text("2")
        self.assertEqual(context.user_data["awaiting"]["step"], "price_toman")
        await send_text("654321")
        self.assertEqual(context.user_data["awaiting"]["step"], "openvpn_profile")
        last = await send_text("Exact-UM-Profile")
        self.assertNotIn("awaiting", context.user_data)
        self.assertIn("admin_plan_draft", context.user_data)
        self.assertIn("OpenVPN و V2Ray", last.replies[-1][0])

        q_message = FakeMessage()
        query = SimpleNamespace(
            data="admin_plan_add_confirm",
            from_user=user,
            message=q_message,
            answer=AsyncMock(return_value=None),
        )
        update = SimpleNamespace(callback_query=query)
        try:
            with (
                patch.object(bot, "is_admin", return_value=True),
                patch.object(bot.CALLBACK_LIMITER, "allow", return_value=(True, "")),
                patch.object(bot, "schedule_telegram_profile", lambda *_a, **_k: None),
            ):
                await bot.callback_router(update, context)
            after = set(plans.PLANS.keys())
            new_keys = list(after - before)
            self.assertEqual(len(new_keys), 1)
            plan = plans.PLANS[new_keys[0]]
            self.assertEqual(plan["gb"], 73)
            self.assertEqual(plan["months"], 2)
            self.assertEqual(plan["days"], 60)
            self.assertEqual(plan["price_toman"], 654321)
            self.assertEqual(plan["openvpn_profile"], "Exact-UM-Profile")
            self.assertNotIn("service", context.user_data.get("admin_plan_draft", {}))
        finally:
            for key in list(set(plans.PLANS.keys()) - before):
                try:
                    storage.delete_sale_plan(key, admin_tg_id=999)
                except Exception:
                    pass
            bot.refresh_plans(storage.list_sale_plans())


class TrialPlanV31Tests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.original = storage.get_trial_plan()

    def tearDown(self):
        storage.update_trial_plan(field="gb", value=self.original["gb"], admin_tg_id=999)
        storage.update_trial_plan(field="days", value=self.original["days"], admin_tg_id=999)
        storage.update_trial_plan(field="openvpn_profile", value=self.original["openvpn_profile"], admin_tg_id=999)
        row = storage.set_trial_plan_enabled(self.original["enabled"], admin_tg_id=999)
        bot.refresh_test_plan(row)

    def test_trial_is_persistent_admin_managed_and_day_based(self):
        storage.update_trial_plan(field="gb", value=3, admin_tg_id=999)
        storage.update_trial_plan(field="days", value=5, admin_tg_id=999)
        row = storage.update_trial_plan(field="openvpn_profile", value="Exact-5D-Test", admin_tg_id=999)
        bot.refresh_test_plan(row)
        self.assertEqual(bot.TEST_PLAN["gb"], 3)
        self.assertEqual(bot.TEST_PLAN["days"], 5)
        self.assertEqual(bot.TEST_PLAN["months"], 0)
        self.assertEqual(bot.TEST_PLAN["price_toman"], 0)
        self.assertEqual(bot.TEST_PLAN["openvpn_profile"], "Exact-5D-Test")
        self.assertEqual(storage.get_trial_plan()["days"], 5)

    def test_disabling_trial_hides_both_service_buttons_and_existing_accounts_remain(self):
        uid = 991731
        storage.upsert_account(uid, "openvpn", "trial-old", username="trial-old", is_test=True, plan_key="__test_openvpn__")
        before = storage.list_accounts(uid, "openvpn")
        self.assertTrue(any(a.get("is_test") for a in before))
        row = storage.set_trial_plan_enabled(False, admin_tg_id=999)
        bot.refresh_test_plan(row)
        for service in ("openvpn", "v2ray"):
            labels = [b.text for r in bot.service_menu_keyboard(service).inline_keyboard for b in r]
            self.assertFalse(any("تست رایگان" in label for label in labels))
        after = storage.list_accounts(uid, "openvpn")
        self.assertTrue(any(a.get("identifier") == "trial-old" and a.get("is_test") for a in after))

    async def test_disabled_trial_rejects_stale_callback_without_remote_provisioning(self):
        row = storage.set_trial_plan_enabled(False, admin_tg_id=999)
        bot.refresh_test_plan(row)
        message = FakeMessage()
        q = SimpleNamespace(from_user=SimpleNamespace(id=12345), message=message)
        context = SimpleNamespace(user_data={})
        with patch.object(bot, "fulfill", AsyncMock(side_effect=AssertionError("must not provision"))):
            await bot.create_test(q, context, "openvpn")
        self.assertIn("غیرفعال", message.edits[-1][0])

    async def test_admin_trial_package_has_edit_toggle_but_no_delete(self):
        message = FakeMessage()
        with patch.object(bot, "is_admin", return_value=True):
            await bot.show_admin_trial_detail(message, 999)
        text, kwargs = message.edits[-1]
        self.assertIn("بسته تست", text)
        callbacks = [b.callback_data for r in kwargs["reply_markup"].inline_keyboard for b in r if b.callback_data]
        self.assertIn("admin_trial_edit|gb", callbacks)
        self.assertIn("admin_trial_edit|days", callbacks)
        self.assertIn("admin_trial_edit|openvpn_profile", callbacks)
        self.assertTrue(any(c.startswith("admin_trial_toggle|") for c in callbacks))
        self.assertFalse(any("delete" in c for c in callbacks))

    async def test_admin_package_list_includes_trial(self):
        markup = bot._admin_plan_list_keyboard(0)
        callbacks = [b.callback_data for r in markup.inline_keyboard for b in r if b.callback_data]
        self.assertIn("admin_trial_view", callbacks)

    def test_restart_seed_cannot_override_admin_managed_trial(self):
        storage.update_trial_plan(field="gb", value=9, admin_tg_id=999)
        storage.update_trial_plan(field="days", value=7, admin_tg_id=999)
        storage.update_trial_plan(field="openvpn_profile", value="ADMIN-TRIAL", admin_tg_id=999)
        persisted = storage.initialize_trial_plan({
            "gb": 99, "days": 99, "openvpn_profile": "ENV-SHOULD-NOT-WIN", "enabled": True,
        })
        self.assertEqual(persisted["gb"], 9)
        self.assertEqual(persisted["days"], 7)
        self.assertEqual(persisted["openvpn_profile"], "ADMIN-TRIAL")

    async def test_enabled_trial_uses_current_admin_managed_snapshot_for_provisioning(self):
        storage.update_trial_plan(field="gb", value=4, admin_tg_id=999)
        storage.update_trial_plan(field="days", value=2, admin_tg_id=999)
        row = storage.update_trial_plan(field="openvpn_profile", value="TRIAL-2D-4G", admin_tg_id=999)
        row = storage.set_trial_plan_enabled(True, admin_tg_id=999)
        bot.refresh_test_plan(row)
        message = FakeMessage()
        q = SimpleNamespace(from_user=SimpleNamespace(id=999), message=message)
        context = SimpleNamespace(user_data={})
        captured = {}

        async def fake_fulfill(*args, **kwargs):
            captured.update(dict(kwargs["plan_override"]))
            return "ok", None

        with (
            patch.object(bot, "is_admin", return_value=True),
            patch.object(bot, "fulfill", fake_fulfill),
            patch.object(bot, "mark_fulfillment_completed", return_value=True),
        ):
            await bot.create_test(q, context, "openvpn")
        self.assertEqual(captured["gb"], 4)
        self.assertEqual(captured["days"], 2)
        self.assertEqual(captured["openvpn_profile"], "TRIAL-2D-4G")
