import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


os.environ.setdefault("BOT_TOKEN", "123456:V35_TEST_TOKEN")
os.environ.setdefault("ADMIN_IDS", "900001")
os.environ.setdefault("DATA_DIR", tempfile.mkdtemp(prefix="vpn-bot-v35-import-"))

import app_settings
import bot
import config
import storage


class FakeMessage:
    def __init__(self):
        self.text = "source"
        self.edits = []
        self.replies = []

    async def edit_text(self, text, **kwargs):
        self.edits.append((text, kwargs))

    async def reply_text(self, text, **kwargs):
        self.replies.append((text, kwargs))


class FakeTelegramBot:
    def __init__(self):
        self.messages = []
        self.photos = []
        self.documents = []

    async def send_message(self, *args, **kwargs):
        if args:
            kwargs = dict(kwargs)
            kwargs.setdefault("chat_id", args[0])
            if len(args) > 1:
                kwargs.setdefault("text", args[1])
        self.messages.append(kwargs)

    async def send_photo(self, **kwargs):
        self.photos.append(kwargs)

    async def send_document(self, **kwargs):
        self.documents.append(kwargs)


def payload(tg_id=71001, order_id="ord-card-1"):
    return {
        "tg_id": tg_id,
        "service": "openvpn",
        "action": "buy",
        "plan_key": "V35",
        "identifier": "",
        "order_id": order_id,
        "first_purchase": True,
        "base_price_toman": 200000,
        "referral_discount_toman": 0,
        "wallet_used_toman": 0,
        "gateway_toman": 200000,
        "wallet_committed": False,
        "plan_snapshot": {
            "plan_key": "V35", "service": "openvpn", "gb": 10,
            "months": 1, "days": 30, "price_toman": 200000,
            "openvpn_profile": "V35-PROFILE",
        },
        "ts": 1700000000,
    }


class TemporaryPaymentDatabase(unittest.TestCase):
    def setUp(self):
        self.original_db = storage.DB_FILE
        self.original_state = app_settings.APP_SETTINGS.snapshot()
        self.tempdir = tempfile.TemporaryDirectory(prefix="vpn-v35-")
        storage.DB_FILE = os.path.join(self.tempdir.name, "vpn.sqlite3")
        storage.initialize_storage()
        storage.initialize_app_settings(dict(app_settings.DEFAULTS))
        storage.initialize_v34_sales_settings(app_settings.DEFAULTS)
        storage.initialize_v35_payment_settings(app_settings.DEFAULTS)
        app_settings.refresh_runtime_settings(root_admin_id=900001)

    def tearDown(self):
        storage.DB_FILE = self.original_db
        app_settings.APP_SETTINGS.replace(
            {"settings": dict(self.original_state), "admins": (), "inbounds": ()},
            root_admin_id=int(self.original_state.get("root_admin_id") or config.ROOT_ADMIN_ID),
        )
        self.tempdir.cleanup()


class PaymentSettingsTests(TemporaryPaymentDatabase):
    def test_additive_migration_defaults_and_marker_are_idempotent(self):
        state = storage.get_app_settings_state()
        self.assertTrue(state["settings"]["zarinpal_enabled"])
        self.assertFalse(state["settings"]["card_transfer_enabled"])
        conn = storage._connect()
        try:
            marker = conn.execute(
                "SELECT value FROM meta WHERE key='app_payment_settings_v35_initialized'"
            ).fetchone()
        finally:
            conn.close()
        self.assertEqual(marker[0], "3.5.0")

        app_settings.update_setting("card_transfer_card_number", "6037-9912-3456-7890")
        app_settings.update_setting("card_transfer_card_holder", "Test Holder")
        app_settings.set_payment_gateway_enabled("card_transfer", True)
        app_settings.set_payment_gateway_enabled("zarinpal", False)
        second = storage.initialize_v35_payment_settings({
            "zarinpal_enabled": True, "card_transfer_enabled": False,
        })
        self.assertFalse(second["settings"]["zarinpal_enabled"])
        self.assertTrue(second["settings"]["card_transfer_enabled"])

    def test_both_gateways_cannot_be_disabled_and_card_requires_details(self):
        with self.assertRaisesRegex(ValueError, "شماره کارت"):
            app_settings.set_payment_gateway_enabled("card_transfer", True)
        with self.assertRaisesRegex(ValueError, "حداقل یکی"):
            app_settings.set_payment_gateway_enabled("zarinpal", False)

        app_settings.update_setting("card_transfer_card_number", "6037991234567890")
        app_settings.update_setting("card_transfer_card_holder", "صاحب کارت")
        app_settings.set_payment_gateway_enabled("card_transfer", True)
        app_settings.set_payment_gateway_enabled("zarinpal", False)
        self.assertEqual(app_settings.enabled_payment_gateways(), ("card_transfer",))
        with self.assertRaisesRegex(ValueError, "حداقل یکی"):
            app_settings.set_payment_gateway_enabled("card_transfer", False)

    def test_gateway_hot_path_reads_only_atomic_snapshot(self):
        with patch.object(storage, "_connect", side_effect=AssertionError("SQLite hot read")):
            self.assertTrue(app_settings.payment_gateway_enabled("zarinpal"))
            self.assertEqual(app_settings.enabled_payment_gateways(), ("zarinpal",))

    def test_card_number_is_normalized_and_not_written_to_audit_values(self):
        number = "6037 9912 3456 7890"
        app_settings.update_setting(
            "card_transfer_card_number", number, admin_tg_id=900001
        )
        self.assertEqual(
            app_settings.get_setting("card_transfer_card_number"), "6037991234567890"
        )
        conn = storage._connect()
        try:
            row = conn.execute(
                "SELECT before_json,after_json FROM admin_audit ORDER BY id DESC LIMIT 1"
            ).fetchone()
        finally:
            conn.close()
        self.assertNotIn("6037991234567890", row[0] + row[1])


class CardTransferStorageTests(TemporaryPaymentDatabase):
    def test_receipt_is_durable_without_account_or_fulfillment(self):
        request = storage.create_card_transfer_request(payload())
        self.assertEqual(request["status"], "awaiting_receipt")
        self.assertEqual(storage.list_accounts(71001, "openvpn"), [])
        self.assertIsNone(storage.get_fulfillment("ord-card-1"))

        submitted = storage.submit_card_transfer_receipt(
            request["id"], 71001, receipt_kind="text",
            receipt_text="پیگیری 123456",
        )
        self.assertEqual(submitted["status"], "submitted")
        self.assertEqual(submitted["receipt_text"], "پیگیری 123456")
        reopened = storage.active_card_transfer_request_for_user(71001)
        self.assertEqual(reopened["id"], request["id"])
        self.assertIsNotNone(storage.get_pending(request["authority"]))
        self.assertEqual(storage.list_accounts(71001, "openvpn"), [])

    def test_reject_releases_pending_and_keeps_no_account(self):
        request = storage.create_card_transfer_request(payload())
        storage.submit_card_transfer_receipt(
            request["id"], 71001, receipt_kind="photo",
            receipt_file_id="photo-file", receipt_file_unique_id="unique",
        )
        rejected = storage.reject_card_transfer_request(
            request["id"], admin_tg_id=900001, reason="مبلغ ناخوانا"
        )
        self.assertEqual(rejected["status"], "rejected")
        self.assertEqual(rejected["rejection_reason"], "مبلغ ناخوانا")
        self.assertIsNone(storage.get_pending(request["authority"]))
        self.assertEqual(storage.list_accounts(71001, "openvpn"), [])

    def test_only_one_active_request_and_approval_claim_is_idempotent(self):
        request = storage.create_card_transfer_request(payload())
        with self.assertRaisesRegex(ValueError, "فعال"):
            storage.create_card_transfer_request(payload(order_id="ord-card-2"))
        storage.submit_card_transfer_receipt(
            request["id"], 71001, receipt_kind="text", receipt_text="receipt"
        )
        first = storage.claim_card_transfer_request(request["id"], admin_tg_id=900001)
        second = storage.claim_card_transfer_request(request["id"], admin_tg_id=900002)
        self.assertEqual(first["status"], "processing")
        self.assertEqual(second["status"], "processing")
        authorized = storage.authorize_pending_payment(
            request["authority"], method="card_transfer", admin_tg_id=900001
        )
        self.assertTrue(authorized["payment_authorized"])
        completed = storage.complete_card_transfer_request(
            request["id"], admin_tg_id=900001
        )
        self.assertEqual(completed["status"], "approved")
        self.assertIsNone(storage.get_pending(request["authority"]))

    def test_schema_upgrade_preserves_existing_business_rows(self):
        storage.update_user_profile(72001, first_name="Existing")
        storage.upsert_account(72001, "openvpn", "existing", password="keep")
        before = storage.list_accounts(72001, "openvpn")
        with storage._tx(immediate=True) as conn:
            conn.execute("DROP TABLE card_transfer_requests")
        storage.initialize_storage()
        self.assertEqual(storage.list_accounts(72001, "openvpn"), before)
        conn = storage._connect()
        try:
            version = conn.execute(
                "SELECT value FROM meta WHERE key='schema_version'"
            ).fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(version, "27")


class PaymentUiTests(TemporaryPaymentDatabase, unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        super().setUp()
        storage.initialize_sale_plans([{
            "plan_key": "V35", "gb": 10, "months": 1, "days": 30,
            "price_toman": 200000, "openvpn_profile": "V35-PROFILE",
        }])
        storage.initialize_service_sale_plans()
        bot.refresh_plans(storage.list_service_sale_plans(), service_aware=True)

    def tearDown(self):
        bot.refresh_plans()
        super().tearDown()

    async def test_both_gateways_show_chooser_without_creating_payment(self):
        app_settings.update_setting("card_transfer_card_number", "6037991234567890")
        app_settings.update_setting("card_transfer_card_holder", "صاحب کارت")
        app_settings.set_payment_gateway_enabled("card_transfer", True)
        message = FakeMessage()
        context = SimpleNamespace(user_data={})
        with patch.object(bot, "run_zarinpal", AsyncMock(side_effect=AssertionError("must not call"))):
            await bot.start_order_message(
                message, context, 73001, "openvpn", "buy", "V35", "", edit=True
            )
        self.assertIn("روش پرداخت", message.edits[-1][0])
        callbacks = [
            button.callback_data
            for row in message.edits[-1][1]["reply_markup"].inline_keyboard
            for button in row if button.callback_data
        ]
        self.assertIn("paymethod|zarinpal", callbacks)
        self.assertIn("paymethod|card_transfer", callbacks)
        self.assertEqual(storage.list_admin_pending_payments()[1], 0)

    async def test_single_zarinpal_gateway_skips_chooser(self):
        message = FakeMessage()
        context = SimpleNamespace(user_data={})
        with patch.object(bot, "run_zarinpal", AsyncMock(return_value=("https://pay.test/1", {}))) as gateway:
            await bot.start_order_message(
                message, context, 73002, "openvpn", "buy", "V35", "", edit=True
            )
        gateway.assert_awaited_once()
        self.assertNotIn("روش پرداخت", message.edits[-1][0])
        self.assertIn("https://pay.test/1", message.edits[-1][0])

    async def test_single_card_gateway_creates_request_but_never_provisions(self):
        app_settings.update_setting("card_transfer_card_number", "6037991234567890")
        app_settings.update_setting("card_transfer_card_holder", "صاحب کارت")
        app_settings.set_payment_gateway_enabled("card_transfer", True)
        app_settings.set_payment_gateway_enabled("zarinpal", False)
        message = FakeMessage()
        context = SimpleNamespace(user_data={})
        with (
            patch.object(bot, "run_zarinpal", AsyncMock(side_effect=AssertionError("must not call"))),
            patch.object(bot, "fulfill", AsyncMock(side_effect=AssertionError("must not provision"))),
        ):
            await bot.start_order_message(
                message, context, 73003, "openvpn", "buy", "V35", "", edit=True
            )
        request = storage.active_card_transfer_request_for_user(73003)
        self.assertEqual(request["status"], "awaiting_receipt")
        self.assertEqual(context.user_data["awaiting"]["kind"], "card_transfer_receipt")
        self.assertIn("6037991234567890", message.edits[-1][0])
        self.assertNotIn("6037-9912-3456-7890", message.edits[-1][0])
        self.assertEqual(storage.list_accounts(73003, "openvpn"), [])

    async def test_admin_purchase_is_free_and_never_enters_payment_gateway(self):
        message = FakeMessage()
        context = SimpleNamespace(user_data={}, bot=FakeTelegramBot())
        with (
            patch.object(
                bot, "run_zarinpal",
                AsyncMock(side_effect=AssertionError("admin must not enter gateway")),
            ) as gateway,
            patch.object(
                bot, "fulfill",
                AsyncMock(return_value=("delivered", None)),
            ) as fulfill,
            patch.object(bot, "finalize_successful_order", AsyncMock(return_value={})),
            patch.object(bot, "mark_fulfillment_completed", return_value=True),
        ):
            await bot.start_order_message(
                message, context, 900001, "openvpn", "renew", "V35", "admin-user",
                edit=True,
            )
        gateway.assert_not_awaited()
        fulfill.assert_awaited_once()
        self.assertIn("delivered", message.edits[-1][0])

    async def test_admin_and_user_card_number_display_is_contiguous(self):
        app_settings.update_setting("card_transfer_card_number", "6037-9912-3456-7890")
        app_settings.update_setting("card_transfer_card_holder", "صاحب کارت")
        admin_message = FakeMessage()
        await bot.show_admin_card_transfer_settings(admin_message, 900001)
        self.assertIn("6037991234567890", admin_message.edits[-1][0])
        self.assertNotIn("6037-9912-3456-7890", admin_message.edits[-1][0])

    async def test_admin_approval_invokes_delivery_only_after_receipt(self):
        request = storage.create_card_transfer_request(payload(tg_id=73004, order_id="ord-card-approve"))
        self.assertEqual(storage.list_accounts(73004, "openvpn"), [])
        storage.submit_card_transfer_receipt(
            request["id"], 73004, receipt_kind="text", receipt_text="receipt"
        )
        telegram_bot = FakeTelegramBot()
        context = SimpleNamespace(bot=telegram_bot, user_data={})
        admin_message = FakeMessage()
        with patch.object(
            bot, "_deliver_verified_pending_unlocked", AsyncMock(return_value=True)
        ) as deliver:
            completed = await bot._approve_card_transfer_request(
                context, request["id"], admin_tg_id=900001,
                admin_message=admin_message,
            )
        self.assertEqual(completed["status"], "approved")
        deliver.assert_awaited_once()
        kwargs = deliver.await_args.kwargs
        self.assertEqual(kwargs["delivery_chat_id"], 73004)
        self.assertFalse(kwargs["remove_pending"])
        delivered_payload = deliver.await_args.args[1]
        self.assertTrue(delivered_payload["payment_authorized"])
        self.assertEqual(
            delivered_payload["payment_authorization_method"], "card_transfer"
        )

    async def test_text_receipt_is_forwarded_to_admin_and_survives_lost_context(self):
        request = storage.create_card_transfer_request(
            payload(tg_id=73006, order_id="ord-card-text")
        )
        message = FakeMessage()
        message.text = "شماره پیگیری 998877"
        telegram_bot = FakeTelegramBot()
        context = SimpleNamespace(bot=telegram_bot, user_data={})
        user = SimpleNamespace(id=73006)
        update = SimpleNamespace(
            effective_user=user, effective_message=message, message=message,
        )
        with patch.object(bot, "schedule_telegram_profile"):
            await bot.text_router(update, context)
        saved = storage.get_card_transfer_request(request["id"])
        self.assertEqual(saved["status"], "submitted")
        self.assertEqual(saved["receipt_text"], "شماره پیگیری 998877")
        self.assertTrue(telegram_bot.messages)
        self.assertEqual(telegram_bot.messages[0]["chat_id"], 900001)

    async def test_rejection_reason_is_sent_without_provisioning(self):
        request = storage.create_card_transfer_request(payload(tg_id=73005, order_id="ord-card-reject"))
        storage.submit_card_transfer_receipt(
            request["id"], 73005, receipt_kind="text", receipt_text="receipt"
        )
        telegram_bot = FakeTelegramBot()
        context = SimpleNamespace(bot=telegram_bot)
        with patch.object(bot, "fulfill", AsyncMock(side_effect=AssertionError("must not provision"))):
            rejected = await bot._reject_card_transfer_and_notify(
                context, request["id"], admin_tg_id=900001, reason="رسید نامعتبر"
            )
        self.assertEqual(rejected["status"], "rejected")
        self.assertIn("رسید نامعتبر", telegram_bot.messages[0]["text"])

    def test_gateway_and_card_callbacks_fit_telegram_limit(self):
        request = {
            "id": 9_223_372_036_854_775_807, "tg_id": 8_888_888_888,
            "status": "submitted", "receipt_kind": "photo",
        }
        markups = [
            bot.admin_settings_menu_keyboard(False, True),
            bot._card_transfer_admin_markup(request),
            bot.admin_payments_menu_keyboard(1, 1, 1),
        ]
        for markup in markups:
            for row in markup.inline_keyboard:
                for button in row:
                    if button.callback_data:
                        self.assertLessEqual(len(button.callback_data.encode("utf-8")), 64)


class PaymentAuthorizationGateTests(TemporaryPaymentDatabase, unittest.IsolatedAsyncioTestCase):
    def _gateway_payload(self, tg_id=74001, order_id="ord-gateway-gate"):
        item = payload(tg_id=tg_id, order_id=order_id)
        item.update({"payment_kind": "gateway", "amount_rial": 2_000_000})
        return item

    def test_zarinpal_authorization_requires_success_code_and_is_durable(self):
        authority = "Z" * 36
        storage.add_pending(authority, self._gateway_payload())
        with self.assertRaisesRegex(RuntimeError, "پاسخ موفق"):
            storage.authorize_pending_payment(
                authority, method="zarinpal", verification_code=-51
            )
        self.assertFalse(bool(storage.get_pending(authority).get("payment_authorized")))
        authorized = storage.authorize_pending_payment(
            authority, method="zarinpal", verification_code=100
        )
        self.assertTrue(authorized["payment_authorized"])
        self.assertEqual(authorized["payment_authorization_method"], "zarinpal")
        self.assertEqual(storage.get_pending(authority)["payment_authorization_code"], 100)

    def test_card_transfer_cannot_be_authorized_before_admin_claim(self):
        request = storage.create_card_transfer_request(
            payload(tg_id=74002, order_id="ord-card-gate")
        )
        with self.assertRaisesRegex(RuntimeError, "هنوز توسط ادمین"):
            storage.authorize_pending_payment(
                request["authority"], method="card_transfer", admin_tg_id=900001
            )
        storage.submit_card_transfer_receipt(
            request["id"], 74002, receipt_kind="text", receipt_text="receipt"
        )
        with self.assertRaisesRegex(RuntimeError, "هنوز توسط ادمین"):
            storage.authorize_pending_payment(
                request["authority"], method="card_transfer", admin_tg_id=900001
            )
        storage.claim_card_transfer_request(request["id"], admin_tg_id=900001)
        authorized = storage.authorize_pending_payment(
            request["authority"], method="card_transfer", admin_tg_id=900001
        )
        self.assertEqual(authorized["payment_authorization_method"], "card_transfer")

    async def test_delivery_refuses_unverified_gateway_before_fulfill(self):
        pending = self._gateway_payload()
        with patch.object(
            bot, "fulfill", AsyncMock(side_effect=AssertionError("must not provision"))
        ) as fulfill:
            with self.assertRaisesRegex(RuntimeError, "هنوز تأیید نشده"):
                await bot._deliver_verified_pending_unlocked(
                    "U" * 36, pending, FakeMessage(), SimpleNamespace()
                )
        fulfill.assert_not_awaited()

    async def test_authorized_gateway_reaches_fulfill(self):
        pending = self._gateway_payload(tg_id=74007, order_id="ord-authorized-zp")
        pending.update({
            "payment_authorized": True,
            "payment_authorization_method": "zarinpal",
            "payment_authorization_code": 100,
        })
        message = FakeMessage()
        with (
            patch.object(bot, "fulfill", AsyncMock(return_value=("delivered", None))) as fulfill,
            patch.object(bot, "finalize_successful_order", AsyncMock()),
            patch.object(bot, "mark_fulfillment_completed", return_value=True),
        ):
            delivered = await bot._deliver_verified_pending_unlocked(
                "P" * 36, pending, message, SimpleNamespace()
            )
        self.assertTrue(delivered)
        fulfill.assert_awaited_once()
        self.assertIn("delivered", message.replies[-1][0])

    async def test_delivery_refuses_unapproved_card_before_fulfill(self):
        pending = payload(tg_id=74003, order_id="ord-unapproved-card")
        pending["payment_kind"] = "card_transfer"
        with patch.object(
            bot, "fulfill", AsyncMock(side_effect=AssertionError("must not provision"))
        ) as fulfill:
            with self.assertRaisesRegex(RuntimeError, "هنوز تأیید نشده"):
                await bot._deliver_verified_pending_unlocked(
                    "card-unapproved", pending, FakeMessage(), SimpleNamespace()
                )
        fulfill.assert_not_awaited()

    async def test_unpaid_zarinpal_result_never_authorizes_or_delivers(self):
        authority = "N" * 36
        pending = self._gateway_payload(tg_id=74004, order_id="ord-unpaid-zp")
        storage.add_pending(authority, pending)
        message = FakeMessage()
        query = SimpleNamespace(from_user=SimpleNamespace(id=74004), message=message)
        with (
            patch.object(bot, "run_zarinpal", AsyncMock(return_value={"errors": {"code": -51}})),
            patch.object(bot, "_deliver_verified_pending", AsyncMock()) as deliver,
        ):
            await bot.verify_latest(query, SimpleNamespace(user_data={}))
        deliver.assert_not_awaited()
        stored = storage.get_pending(authority)
        self.assertTrue(stored is None or not bool(stored.get("payment_authorized")))

    async def test_successful_zarinpal_result_authorizes_before_delivery(self):
        authority = "S" * 36
        pending = self._gateway_payload(tg_id=74005, order_id="ord-paid-zp")
        storage.add_pending(authority, pending)
        message = FakeMessage()
        query = SimpleNamespace(from_user=SimpleNamespace(id=74005), message=message)
        with (
            patch.object(bot, "run_zarinpal", AsyncMock(return_value={"data": {"code": 100}})),
            patch.object(bot, "_deliver_verified_pending", AsyncMock(return_value=True)) as deliver,
        ):
            await bot.verify_latest(query, SimpleNamespace(user_data={}))
        deliver.assert_awaited_once()
        delivered_payload = deliver.await_args.args[1]
        self.assertTrue(delivered_payload["payment_authorized"])
        self.assertEqual(
            storage.get_pending(authority)["payment_authorization_method"], "zarinpal"
        )

    async def test_malformed_wallet_order_cannot_bypass_payment(self):
        pending = payload(tg_id=74006, order_id="ord-fake-wallet")
        pending.update({
            "payment_kind": "wallet", "wallet_used_toman": 0, "gateway_toman": 0,
        })
        with patch.object(
            bot, "fulfill", AsyncMock(side_effect=AssertionError("must not provision"))
        ) as fulfill:
            with self.assertRaisesRegex(RuntimeError, "پوشش کامل مالی"):
                await bot._deliver_verified_pending_unlocked(
                    "local-wallet-fake", pending, FakeMessage(), SimpleNamespace()
                )
        fulfill.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
