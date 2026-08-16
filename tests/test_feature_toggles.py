import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


TEST_DATA_DIR = tempfile.mkdtemp(prefix="account-bot-feature-tests-")
os.environ["BOT_TOKEN"] = "123456:FEATURE_TOGGLE_TEST_TOKEN"
os.environ["DATA_DIR"] = TEST_DATA_DIR
os.environ.setdefault("PLAN_TEST", "10|30|150000|1M-10G")

import app_settings
import bot
import config
import storage


class FakeMessage:
    def __init__(self):
        self.edits = []
        self.replies = []

    async def edit_text(self, text, **kwargs):
        self.edits.append((text, kwargs))

    async def reply_text(self, text, **kwargs):
        self.replies.append((text, kwargs))


def callback_values(markup):
    return [
        button.callback_data
        for row in markup.inline_keyboard
        for button in row
        if button.callback_data
    ]


class TemporaryFeatureDatabase(unittest.TestCase):
    def setUp(self):
        self.original_db = storage.DB_FILE
        self.tempdir = tempfile.TemporaryDirectory(prefix="account-feature-db-")
        storage.DB_FILE = os.path.join(self.tempdir.name, "account.sqlite3")
        storage.initialize_storage()
        storage.initialize_app_settings(dict(app_settings.DEFAULTS))
        storage.initialize_feature_toggles(app_settings.DEFAULTS)
        app_settings.refresh_runtime_settings(root_admin_id=900001)

    def tearDown(self):
        storage.DB_FILE = self.original_db
        app_settings.refresh_runtime_settings(root_admin_id=config.ROOT_ADMIN_ID)
        self.tempdir.cleanup()


class FeatureSettingsTests(TemporaryFeatureDatabase):
    def test_defaults_preserve_existing_behavior(self):
        self.assertTrue(app_settings.referral_enabled())
        self.assertTrue(app_settings.wallet_enabled())

    def test_wallet_dependency_and_atomic_referral_enable(self):
        with self.assertRaisesRegex(ValueError, "Referral فعال"):
            app_settings.set_wallet_enabled(False, admin_tg_id=900001)

        app_settings.set_referral_enabled(False, admin_tg_id=900001)
        app_settings.set_wallet_enabled(False, admin_tg_id=900001)
        self.assertFalse(app_settings.referral_enabled())
        self.assertFalse(app_settings.wallet_enabled())

        with patch.object(
            app_settings.APP_SETTINGS, "replace", wraps=app_settings.APP_SETTINGS.replace
        ) as replace:
            app_settings.set_referral_enabled(True, admin_tg_id=900001)
        self.assertEqual(replace.call_count, 1)
        self.assertTrue(app_settings.referral_enabled())
        self.assertTrue(app_settings.wallet_enabled())
        state = storage.get_app_settings_state()["settings"]
        self.assertTrue(state["referral_enabled"])
        self.assertTrue(state["wallet_enabled"])

    def test_additive_migration_is_idempotent(self):
        app_settings.set_referral_enabled(False, admin_tg_id=900001)
        state = storage.initialize_feature_toggles({
            "referral_enabled": True,
            "wallet_enabled": True,
        })
        self.assertFalse(state["settings"]["referral_enabled"])

    def test_disabling_features_preserves_existing_codes_and_balance(self):
        storage.record_purchase(
            7001, "old-order", service="openvpn", plan_key="OLD",
            base_price_toman=100000,
        )
        code = storage.get_or_create_referral_code(7001)
        storage.admin_adjust_wallet(
            7001, 50000, admin_tg_id=900001, operation_id="old-balance"
        )

        app_settings.set_referral_enabled(False, admin_tg_id=900001)
        app_settings.set_wallet_enabled(False, admin_tg_id=900001)

        self.assertEqual(storage.find_referrer_by_code(code), 7001)
        self.assertEqual(storage.wallet_balance(7001), 50000)

    def test_hot_path_flags_read_only_the_snapshot(self):
        with patch.object(
            storage, "get_app_settings_state", side_effect=AssertionError("DB read")
        ):
            self.assertTrue(app_settings.referral_enabled())
            self.assertTrue(app_settings.wallet_enabled())


class FeatureUiTests(TemporaryFeatureDatabase, unittest.IsolatedAsyncioTestCase):
    async def test_disabled_features_are_hidden_from_user_menu(self):
        app_settings.set_referral_enabled(False, admin_tg_id=900001)
        app_settings.set_wallet_enabled(False, admin_tg_id=900001)
        with (
            patch.object(bot, "completed_purchase_for_menu", AsyncMock(return_value=True)),
            patch.object(bot, "is_admin", return_value=False),
        ):
            markup = await bot.main_menu_keyboard(7002)
        callbacks = callback_values(markup)
        self.assertNotIn("menu|wallet", callbacks)
        self.assertNotIn("menu|referral", callbacks)

    async def test_stale_wallet_and_referral_screens_fail_safely(self):
        app_settings.set_referral_enabled(False, admin_tg_id=900001)
        app_settings.set_wallet_enabled(False, admin_tg_id=900001)
        with patch.object(bot, "completed_purchase_for_menu", AsyncMock(return_value=False)):
            wallet_message = FakeMessage()
            await bot.show_wallet(wallet_message, 7003)
            referral_message = FakeMessage()
            await bot.show_referral(referral_message, 7003)
        self.assertIn("غیرفعال", wallet_message.edits[-1][0])
        self.assertIn("غیرفعال", referral_message.edits[-1][0])

    async def test_first_purchase_skips_referral_prompt_when_disabled(self):
        app_settings.set_referral_enabled(False, admin_tg_id=900001)
        message = FakeMessage()
        user = SimpleNamespace(id=7004, username="u", first_name="U", last_name="")
        query = SimpleNamespace(
            data="plan|buy|openvpn|P1", from_user=user, message=message,
            answer=AsyncMock(return_value=None),
        )
        update = SimpleNamespace(callback_query=query)
        context = SimpleNamespace(user_data={})
        with (
            patch.object(bot.CALLBACK_LIMITER, "allow", return_value=(True, "")),
            patch.object(bot, "schedule_telegram_profile", lambda *_a, **_k: None),
            patch.object(bot, "current_maintenance_mode", return_value=False),
            patch.object(bot, "is_admin", return_value=False),
            patch.object(bot, "is_reseller", return_value=False),
            patch.object(bot, "plans_for", return_value={"P1": {"price_toman": 100000}}),
            patch.object(bot, "run_blocking", AsyncMock(return_value=False)),
            patch.object(bot, "start_order", AsyncMock()) as start_order,
        ):
            await bot.callback_router(update, context)
        start_order.assert_awaited_once()
        self.assertFalse(any("کد معرف" in text for text, _ in message.edits))

    async def test_stale_referral_callback_cannot_apply_discount(self):
        app_settings.set_referral_enabled(False, admin_tg_id=900001)
        message = FakeMessage()
        user = SimpleNamespace(id=7005, username="u", first_name="U", last_name="")
        query = SimpleNamespace(
            data="ref|have", from_user=user, message=message,
            answer=AsyncMock(return_value=None),
        )
        context = SimpleNamespace(user_data={
            "first_buy_order": {
                "service": "openvpn", "action": "buy",
                "plan_key": "P1", "identifier": "",
            }
        })
        with (
            patch.object(bot.CALLBACK_LIMITER, "allow", return_value=(True, "")),
            patch.object(bot, "schedule_telegram_profile", lambda *_a, **_k: None),
            patch.object(bot, "current_maintenance_mode", return_value=False),
            patch.object(bot, "is_admin", return_value=False),
            patch.object(bot, "is_reseller", return_value=False),
            patch.object(bot, "completed_purchase_for_menu", AsyncMock(return_value=False)),
            patch.object(bot, "start_order", AsyncMock(side_effect=AssertionError("must not order"))),
        ):
            await bot.callback_router(SimpleNamespace(callback_query=query), context)
        self.assertNotIn("first_buy_order", context.user_data)
        self.assertIn("غیرفعال", message.edits[-1][0])

    async def test_disabled_features_do_not_discount_or_spend_wallet(self):
        app_settings.set_referral_enabled(False, admin_tg_id=900001)
        app_settings.set_wallet_enabled(False, admin_tg_id=900001)
        with (
            patch.object(bot, "plans_for", return_value={"P1": {"price_toman": 200000}}),
            patch.object(
                bot, "run_blocking", AsyncMock(side_effect=AssertionError("wallet DB read"))
            ),
        ):
            result = await bot.order_price_breakdown(
                7006, "P1", service="openvpn", referral_code="OLD-CODE"
            )
        self.assertEqual(result["referral_discount_toman"], 0)
        self.assertEqual(result["wallet_used_toman"], 0)
        self.assertEqual(result["gateway_toman"], 200000)

    async def test_admin_settings_expose_both_switches(self):
        referral_message = FakeMessage()
        wallet_message = FakeMessage()
        with patch.object(bot, "is_admin", return_value=True):
            await bot.show_admin_referral_settings(referral_message, 900001)
            await bot.show_admin_wallet_settings(wallet_message, 900001)
        self.assertIn(
            "admin_feature_toggle|referral",
            callback_values(referral_message.edits[-1][1]["reply_markup"]),
        )
        self.assertIn(
            "admin_feature_toggle|wallet",
            callback_values(wallet_message.edits[-1][1]["reply_markup"]),
        )


def tearDownModule():
    for lane in bot.BLOCKING_LANES.values():
        lane.shutdown()
    import shutil
    shutil.rmtree(TEST_DATA_DIR, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
