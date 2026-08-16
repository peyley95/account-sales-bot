import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

TEST_DATA_DIR = tempfile.mkdtemp(prefix="vpn-bot-v34-tests-")
os.environ["BOT_TOKEN"] = "123456:V34_TEST_TOKEN"
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


class TemporarySalesDatabase(unittest.TestCase):
    def setUp(self):
        self.original_db = storage.DB_FILE
        self.tempdir = tempfile.TemporaryDirectory(prefix="vpn-v34-")
        storage.DB_FILE = os.path.join(self.tempdir.name, "vpn.sqlite3")
        storage.initialize_storage()
        storage.initialize_app_settings(dict(app_settings.DEFAULTS))
        app_settings.refresh_runtime_settings(root_admin_id=900001)
        storage.initialize_sale_plans([])
        storage.initialize_service_sale_plans()
        bot.refresh_plans(storage.list_service_sale_plans(), service_aware=True)

    def tearDown(self):
        storage.DB_FILE = self.original_db
        app_settings.refresh_runtime_settings(root_admin_id=config.ROOT_ADMIN_ID)
        bot.refresh_plans()
        self.tempdir.cleanup()


class ServicePlanMigrationTests(unittest.TestCase):
    def setUp(self):
        self.original_db = storage.DB_FILE
        self.tempdir = tempfile.TemporaryDirectory(prefix="vpn-v34-migration-")
        storage.DB_FILE = os.path.join(self.tempdir.name, "vpn.sqlite3")
        storage.initialize_storage()
        storage.update_user_profile(44001, first_name="Existing")
        storage.upsert_account(
            44001, "openvpn", "existing-user", username="existing-user",
            password="keep-me", profile="OLD-PROFILE",
        )
        ts = storage.now_iso()
        with storage._tx(immediate=True) as conn:
            conn.execute(
                """INSERT INTO sale_plans(
                       plan_key,gb,months,days,price_toman,openvpn_profile,
                       sort_order,created_at,updated_at
                   ) VALUES(?,?,?,?,?,?,?,?,?)""",
                ("LEGACY", 25, 2, 60, 345000, "EXACT-UM", 10, ts, ts),
            )

    def tearDown(self):
        storage.DB_FILE = self.original_db
        self.tempdir.cleanup()

    def test_one_time_split_is_idempotent_and_preserves_business_rows(self):
        before_accounts = storage.list_accounts(44001, "openvpn")
        result = storage.initialize_service_sale_plans()
        self.assertEqual(result["migrated"], 2)
        self.assertEqual(storage.service_sale_plans_migration_version(), "3.4.0")

        openvpn = storage.get_sale_plan("LEGACY", "openvpn")
        v2ray = storage.get_sale_plan("LEGACY", "v2ray")
        self.assertEqual(openvpn["openvpn_profile"], "EXACT-UM")
        self.assertEqual(v2ray["openvpn_profile"], "")
        self.assertEqual(v2ray["price_toman"], openvpn["price_toman"])
        self.assertEqual(storage.list_accounts(44001, "openvpn"), before_accounts)

        storage.delete_sale_plan("LEGACY", service="v2ray")
        second = storage.initialize_service_sale_plans()
        self.assertEqual(second["migrated"], 0)
        self.assertIsNone(storage.get_sale_plan("LEGACY", "v2ray"))
        self.assertIsNotNone(storage.get_sale_plan("LEGACY", "openvpn"))


class ServiceSalesSettingsTests(TemporarySalesDatabase):
    def test_defaults_are_enabled_and_both_cannot_be_disabled(self):
        self.assertEqual(app_settings.enabled_sales_services(), ("openvpn", "v2ray"))
        app_settings.set_service_sales_enabled("openvpn", False, admin_tg_id=900001)
        self.assertFalse(app_settings.service_sales_enabled("openvpn"))
        self.assertTrue(app_settings.service_sales_enabled("v2ray"))
        with self.assertRaisesRegex(ValueError, "حداقل فروش یکی"):
            app_settings.set_service_sales_enabled("v2ray", False, admin_tg_id=900001)
        self.assertTrue(app_settings.service_sales_enabled("v2ray"))

    def test_sale_switch_persists_and_additive_marker_never_overwrites_it(self):
        app_settings.set_service_sales_enabled("openvpn", False, admin_tg_id=900001)
        state = storage.initialize_v34_sales_settings({
            "openvpn_sales_enabled": True,
            "v2ray_sales_enabled": True,
        })
        self.assertFalse(state["settings"]["openvpn_sales_enabled"])
        app_settings.APP_SETTINGS.replace(
            {"settings": dict(app_settings.DEFAULTS)}, root_admin_id=900001
        )
        app_settings.refresh_runtime_settings(root_admin_id=900001)
        self.assertFalse(app_settings.service_sales_enabled("openvpn"))

    def test_hot_path_reads_only_atomic_snapshot(self):
        with patch.object(storage, "get_app_settings_state", side_effect=AssertionError("DB read")):
            self.assertTrue(app_settings.service_sales_enabled("openvpn"))
            self.assertEqual(app_settings.enabled_sales_services(), ("openvpn", "v2ray"))

    def test_independent_crud_and_atomic_openvpn_copy(self):
        storage.create_sale_plan(
            service="openvpn", plan_key="OV1", gb=50, months=3,
            price_toman=600000, openvpn_profile="EXACT-MT",
            copy_to_v2ray=True, admin_tg_id=900001,
        )
        ovpn = storage.get_sale_plan("OV1", "openvpn")
        v2 = storage.get_sale_plan("OV1", "v2ray")
        self.assertEqual(ovpn["openvpn_profile"], "EXACT-MT")
        self.assertEqual(v2["openvpn_profile"], "")
        self.assertEqual((v2["gb"], v2["months"], v2["price_toman"]), (50, 3, 600000))

        storage.update_sale_plan(
            "OV1", service="openvpn", field="price_toman", value=700000,
            admin_tg_id=900001,
        )
        self.assertEqual(storage.get_sale_plan("OV1", "openvpn")["price_toman"], 700000)
        self.assertEqual(storage.get_sale_plan("OV1", "v2ray")["price_toman"], 600000)
        storage.delete_sale_plan("OV1", service="openvpn", admin_tg_id=900001)
        self.assertIsNone(storage.get_sale_plan("OV1", "openvpn"))
        self.assertIsNotNone(storage.get_sale_plan("OV1", "v2ray"))


class ServiceSalesUiTests(TemporarySalesDatabase, unittest.IsolatedAsyncioTestCase):
    async def test_admin_toggle_is_first_setting_in_each_service(self):
        for show, expected_callback, expected_text in (
            (bot.show_admin_mikrotik_settings, "admin_service_sales|openvpn", "فروش OpenVPN"),
            (bot.show_admin_xui_settings, "admin_service_sales|v2ray", "فروش V2ray"),
        ):
            message = FakeMessage()
            with patch.object(bot, "is_admin", return_value=True):
                await show(message, 900001)
            text, kwargs = message.edits[-1]
            self.assertIn(expected_text, text)
            self.assertEqual(
                kwargs["reply_markup"].inline_keyboard[0][0].callback_data,
                expected_callback,
            )

    async def test_single_service_home_skips_service_selector(self):
        with (
            patch.object(bot, "completed_purchase_for_menu", AsyncMock(return_value=False)),
            patch.object(bot, "is_admin", return_value=False),
        ):
            both = await bot.main_menu_keyboard(55)
            self.assertIn("menu|services", callback_values(both))
            app_settings.set_service_sales_enabled("openvpn", False, admin_tg_id=900001)
            one = await bot.main_menu_keyboard(55)
        callbacks = callback_values(one)
        self.assertNotIn("menu|services", callbacks)
        self.assertIn("act|buy|v2ray", callbacks)
        self.assertIn("act|accounts|v2ray", callbacks)
        self.assertFalse(any("openvpn" in value for value in callbacks))

    async def test_disabled_service_plan_section_is_hidden(self):
        app_settings.set_service_sales_enabled("openvpn", False, admin_tg_id=900001)
        message = FakeMessage()
        with patch.object(bot, "is_admin", return_value=True):
            await bot.show_admin_plans(message, 900001)
        callbacks = callback_values(message.edits[-1][1]["reply_markup"])
        self.assertNotIn("admin_plans_service|openvpn|0", callbacks)
        self.assertIn("admin_plans_service|v2ray|0", callbacks)

    async def test_stale_disabled_buy_callback_cannot_start_order(self):
        app_settings.set_service_sales_enabled("openvpn", False, admin_tg_id=900001)
        message = FakeMessage()
        user = SimpleNamespace(id=551, username="u", first_name="U", last_name="")
        query = SimpleNamespace(
            data="act|buy|openvpn", from_user=user, message=message,
            answer=AsyncMock(return_value=None),
        )
        update = SimpleNamespace(callback_query=query)
        context = SimpleNamespace(user_data={})
        with (
            patch.object(bot.CALLBACK_LIMITER, "allow", return_value=(True, "")),
            patch.object(bot, "schedule_telegram_profile", lambda *_a, **_k: None),
            patch.object(bot, "start_order", AsyncMock(side_effect=AssertionError("must not order"))),
            patch.object(bot, "completed_purchase_for_menu", AsyncMock(return_value=False)),
            patch.object(bot, "is_admin", return_value=False),
        ):
            await bot.callback_router(update, context)
        self.assertIn("غیرفعال", message.edits[-1][0])

    async def test_v2ray_wizard_finishes_without_mikrotik_profile(self):
        context = SimpleNamespace(user_data={
            "awaiting": {"kind": "admin_plan_add", "step": "gb", "service": "v2ray"},
            "admin_plan_draft": {"service": "v2ray"},
        })
        user = SimpleNamespace(id=900001, username="admin", first_name="Admin", last_name="")

        async def send(value):
            message = FakeMessage()
            message.text = value
            update = SimpleNamespace(message=message, effective_user=user)
            with (
                patch.object(bot, "is_admin", return_value=True),
                patch.object(bot, "schedule_telegram_profile", lambda *_a, **_k: None),
            ):
                await bot.text_router(update, context)
            return message

        await send("20")
        await send("2")
        final = await send("250000")
        self.assertNotIn("awaiting", context.user_data)
        self.assertNotIn("MikroTik", final.replies[-1][0])
        self.assertIn("admin_plan_add_confirm|0", callback_values(final.replies[-1][1]["reply_markup"]))

    async def test_openvpn_wizard_offers_copy_only_while_v2ray_enabled(self):
        draft = {
            "service": "openvpn", "gb": 10, "months": 1,
            "price_toman": 100000, "openvpn_profile": "MT-10",
        }
        self.assertIn("OpenVPN و V2Ray", bot._plan_draft_summary(draft))
        app_settings.set_service_sales_enabled("v2ray", False, admin_tg_id=900001)
        self.assertNotIn("OpenVPN و V2Ray", bot._plan_draft_summary(draft))


def tearDownModule():
    for lane in bot.BLOCKING_LANES.values():
        lane.shutdown()
    import shutil
    shutil.rmtree(TEST_DATA_DIR, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
