import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


os.environ.setdefault("BOT_TOKEN", "123456:V36_TEST_TOKEN")
os.environ.setdefault("ADMIN_IDS", "900001")
os.environ.setdefault("DATA_DIR", tempfile.mkdtemp(prefix="vpn-bot-v36-import-"))

import app_settings
import bot
import config
import storage


class FakeMessage:
    def __init__(self):
        self.edits = []
        self.replies = []
        self.chat_id = 1
        self.message_id = 1

    async def edit_text(self, text, **kwargs):
        self.edits.append((text, kwargs))

    async def reply_text(self, text, **kwargs):
        self.replies.append((text, kwargs))


class FakeTelegramBot:
    def __init__(self):
        self.messages = []

    async def send_message(self, *args, **kwargs):
        self.messages.append((args, kwargs))


def plan_snapshot(gb=10, price=200_000):
    return {
        "plan_key": "V36", "service": "openvpn", "gb": gb,
        "months": 1, "days": 30, "price_toman": price,
        "openvpn_profile": "V36-PROFILE",
    }


class TemporaryResellerDatabase(unittest.TestCase):
    def setUp(self):
        self.original_db = storage.DB_FILE
        self.original_snapshot = app_settings.APP_SETTINGS.snapshot()
        self.tempdir = tempfile.TemporaryDirectory(prefix="vpn-v36-")
        storage.DB_FILE = os.path.join(self.tempdir.name, "vpn.sqlite3")
        storage.initialize_storage()
        storage.initialize_app_settings(dict(app_settings.DEFAULTS))
        storage.initialize_v34_sales_settings(app_settings.DEFAULTS)
        storage.initialize_v35_payment_settings(app_settings.DEFAULTS)
        storage.initialize_v36_resellers(root_admin_id=900001)
        app_settings.refresh_runtime_settings(root_admin_id=900001)

    def tearDown(self):
        storage.DB_FILE = self.original_db
        app_settings.APP_SETTINGS.replace(
            {
                "settings": dict(self.original_snapshot),
                "admins": (), "inbounds": (), "resellers": (),
            },
            root_admin_id=int(
                self.original_snapshot.get("root_admin_id") or config.ROOT_ADMIN_ID
            ),
        )
        self.tempdir.cleanup()

    def add_reseller(self, tg_id=700001, name="Shop", rate=5_000):
        app_settings.add_reseller(
            name=name, tg_id=tg_id, price_per_gb_toman=rate,
            admin_tg_id=900001,
        )
        return app_settings.reseller_record(tg_id)


class ResellerMigrationTests(TemporaryResellerDatabase):
    def test_legacy_admins_and_extra_env_ids_migrate_once_as_zero_rate_resellers(self):
        with storage._tx(immediate=True) as conn:
            conn.execute("DELETE FROM meta WHERE key=?", (storage.RESELLERS_V36_MIGRATION_KEY,))
            conn.execute("INSERT INTO bot_admins VALUES(700010,?,900001)", (storage.now_iso(),))
        first = storage.initialize_v36_resellers(
            root_admin_id=900001, env_admin_ids=(900001, 700011)
        )
        self.assertEqual(first["admins"], ())
        migrated = {row[1]: row for row in first["resellers"]}
        self.assertEqual(set(migrated), {700010, 700011})
        self.assertEqual(migrated[700010][3], 0)
        second = storage.initialize_v36_resellers(
            root_admin_id=900001, env_admin_ids=(700012,)
        )
        self.assertEqual({row[1] for row in second["resellers"]}, {700010, 700011})

    def test_only_root_is_admin_and_reseller_snapshot_changes_immediately(self):
        reseller = self.add_reseller(tg_id=700020)
        self.assertTrue(app_settings.is_admin(900001))
        self.assertFalse(app_settings.is_admin(700020))
        self.assertTrue(app_settings.is_reseller(700020))
        app_settings.edit_reseller(
            reseller["id"], name="New Shop", price_per_gb_toman=7000,
            admin_tg_id=900001,
        )
        self.assertEqual(app_settings.reseller_record(700020)["name"], "New Shop")
        self.assertEqual(app_settings.reseller_record(700020)["price_per_gb_toman"], 7000)
        app_settings.remove_reseller(reseller["id"], admin_tg_id=900001)
        self.assertFalse(app_settings.is_reseller(700020))

    def test_root_cannot_be_added_as_reseller(self):
        with self.assertRaisesRegex(ValueError, "مدیر اصلی"):
            app_settings.add_reseller(
                name="Root", tg_id=900001, price_per_gb_toman=1,
                admin_tg_id=900001,
            )


class ResellerDebtTests(TemporaryResellerDatabase, unittest.IsolatedAsyncioTestCase):
    def _pending(self, *, tg_id=700001, order_id="ord-reseller", gb=10):
        return {
            "tg_id": tg_id, "service": "openvpn", "action": "buy",
            "plan_key": "V36", "identifier": "", "order_id": order_id,
            "base_price_toman": 200_000, "first_purchase": False,
            "plan_snapshot": plan_snapshot(gb), "ts": 1,
        }

    def test_pending_snapshots_formula_and_charge_is_exactly_once(self):
        self.add_reseller(rate=5_000)
        pending = storage.create_reseller_pending(
            "local-reseller-order", self._pending()
        )
        self.assertEqual(pending["reseller_charge_toman"], 50_000)
        first = app_settings.charge_reseller_order(pending)
        second = app_settings.charge_reseller_order(pending)
        self.assertEqual(first, second)
        self.assertEqual(app_settings.reseller_record(700001)["debt_toman"], 50_000)
        conn = storage._connect()
        try:
            count = conn.execute("SELECT COUNT(*) FROM reseller_debt_entries").fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(count, 1)

    def test_zero_rate_migrated_reseller_cannot_provision(self):
        with storage._tx(immediate=True) as conn:
            ts = storage.now_iso()
            conn.execute(
                "INSERT INTO resellers(tg_id,name,price_per_gb_toman,debt_toman,created_at,updated_at) VALUES(700002,'Legacy',0,0,?,?)",
                (ts, ts),
            )
        app_settings.refresh_runtime_settings(root_admin_id=900001)
        with self.assertRaisesRegex(ValueError, "هنوز توسط مدیر تنظیم نشده"):
            storage.create_reseller_pending(
                "local-reseller-zero", self._pending(tg_id=700002)
            )

    def test_admin_can_set_and_zero_debt_idempotently(self):
        reseller = self.add_reseller()
        first = app_settings.change_reseller_debt(
            reseller["id"], 123_000, admin_tg_id=900001, operation_id="set-1"
        )
        retry = app_settings.change_reseller_debt(
            reseller["id"], 123_000, admin_tg_id=900001, operation_id="set-1"
        )
        self.assertEqual(first, (0, 123_000))
        self.assertEqual(retry, first)
        app_settings.change_reseller_debt(
            reseller["id"], 0, admin_tg_id=900001, operation_id="zero-1"
        )
        self.assertEqual(app_settings.reseller_record(700001)["debt_toman"], 0)

    async def test_menu_hides_wallet_and_referral_and_stale_callbacks_are_safe(self):
        self.add_reseller()
        markup = await bot.main_menu_keyboard(700001)
        callbacks = [
            button.callback_data for row in markup.inline_keyboard for button in row
            if button.callback_data
        ]
        self.assertIn("menu|reseller_debt", callbacks)
        self.assertNotIn("menu|wallet", callbacks)
        self.assertNotIn("menu|referral", callbacks)
        message = FakeMessage()
        await bot.show_wallet(message, 700001)
        self.assertIn("بدهی ریسلر", message.edits[-1][0])

    async def test_reseller_order_skips_gateway_and_debt_is_after_success(self):
        self.add_reseller(rate=4_000)
        storage.initialize_sale_plans([{
            "plan_key": "V36", "gb": 10, "months": 1, "days": 30,
            "price_toman": 200_000, "openvpn_profile": "V36-PROFILE",
        }])
        storage.initialize_service_sale_plans()
        bot.refresh_plans(storage.list_service_sale_plans(), service_aware=True)
        message = FakeMessage()
        context = SimpleNamespace(user_data={}, bot=FakeTelegramBot())
        try:
            with (
                patch.object(bot, "run_zarinpal", AsyncMock(side_effect=AssertionError("gateway"))) as gateway,
                patch.object(bot, "fulfill", AsyncMock(return_value=("delivered", None))),
                patch.object(bot, "mark_fulfillment_completed", return_value=True),
                patch.object(bot, "get_fulfillment", return_value={"delivery_identifier": "account"}),
            ):
                await bot.start_order_message(
                    message, context, 700001, "openvpn", "buy", "V36", "", edit=True
                )
            gateway.assert_not_awaited()
            self.assertEqual(app_settings.reseller_record(700001)["debt_toman"], 40_000)
            self.assertTrue(any(
                "فروش ریسلر" in (
                    str(call_args[1]) if len(call_args) > 1 else str(call_kwargs.get("text") or "")
                )
                for call_args, call_kwargs in context.bot.messages
            ))
        finally:
            bot.refresh_plans()

    async def test_provisioning_failure_never_adds_reseller_debt(self):
        self.add_reseller(rate=4_000)
        storage.initialize_sale_plans([{
            "plan_key": "V36", "gb": 10, "months": 1, "days": 30,
            "price_toman": 200_000, "openvpn_profile": "V36-PROFILE",
        }])
        storage.initialize_service_sale_plans()
        bot.refresh_plans(storage.list_service_sale_plans(), service_aware=True)
        try:
            with patch.object(bot, "fulfill", AsyncMock(side_effect=RuntimeError("remote failed"))):
                with self.assertRaisesRegex(RuntimeError, "remote failed"):
                    await bot.start_order_message(
                        FakeMessage(), SimpleNamespace(user_data={}, bot=FakeTelegramBot()),
                        700001, "openvpn", "renew", "V36", "existing", edit=True,
                    )
            self.assertEqual(app_settings.reseller_record(700001)["debt_toman"], 0)
            self.assertEqual(storage.list_admin_pending_payments()[1], 1)
        finally:
            bot.refresh_plans()

    async def test_reseller_admin_callbacks_stay_within_telegram_limit(self):
        reseller = self.add_reseller(name="Long Shop Name", rate=4_000)
        messages = [FakeMessage(), FakeMessage(), FakeMessage()]
        await bot.show_admin_resellers(messages[0], 900001)
        await bot.show_admin_reseller_detail(messages[1], 900001, reseller["id"])
        await bot.show_admin_reseller_debt(messages[2], 900001, reseller["id"])
        callbacks = [
            button.callback_data
            for message in messages
            for row in message.edits[-1][1]["reply_markup"].inline_keyboard
            for button in row if button.callback_data
        ]
        self.assertTrue(callbacks)
        self.assertTrue(all(len(value.encode("utf-8")) <= 64 for value in callbacks))


class FinancialReceiptTests(TemporaryResellerDatabase):
    def test_receipts_show_exact_owner_reseller_and_split_payments(self):
        plan = plan_snapshot()
        owner = bot.successful_order_admin_text(
            {"tg_id": 900001, "service": "openvpn", "action": "buy",
             "order_id": "owner", "payment_kind": "owner", "base_price_toman": 200_000},
            plan, {}, {},
        )
        self.assertIn("خرید توسط مدیر", owner)
        self.assertIn("رایگان", owner)
        reseller = bot.successful_order_admin_text(
            {"tg_id": 700001, "service": "openvpn", "action": "buy",
             "order_id": "reseller", "payment_kind": "reseller_debt",
             "reseller_name": "Shop", "reseller_tg_id": 700001,
             "reseller_price_per_gb_toman": 5_000,
             "reseller_charge_toman": 50_000, "base_price_toman": 200_000},
            plan, {}, {"added_toman": 50_000, "after_toman": 150_000},
        )
        self.assertIn("فروش ریسلر", reseller)
        self.assertIn("50,000 تومان", reseller)
        direct = bot.successful_order_admin_text(
            {"tg_id": 800001, "service": "v2ray", "action": "buy",
             "order_id": "direct", "payment_kind": "gateway",
             "payment_authorization_method": "zarinpal",
             "gateway_toman": 100_000, "wallet_used_toman": 100_000,
             "base_price_toman": 200_000},
            plan, {}, {},
        )
        self.assertIn("100,000 تومان زرین‌پال + 100,000 تومان اعتبار کیف پول", direct)


if __name__ == "__main__":
    unittest.main()
