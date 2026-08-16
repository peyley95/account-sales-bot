import os
import sqlite3
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


os.environ.setdefault("BOT_TOKEN", "123456:V38_TEST_TOKEN")
os.environ.setdefault("ADMIN_IDS", "900001")
os.environ.setdefault("DATA_DIR", tempfile.mkdtemp(prefix="vpn-bot-v38-import-"))

import app_settings
import bot
import config
import storage


def callback_values(markup):
    return [
        button.callback_data
        for row in markup.inline_keyboard
        for button in row
        if button.callback_data
    ]


class FakeMessage:
    def __init__(self, text=""):
        self.text = text
        self.text_html = text
        self.edits = []
        self.replies = []
        self.chat_id = 1
        self.message_id = 1

    async def edit_text(self, text, **kwargs):
        self.text = text
        self.text_html = text
        self.edits.append((text, kwargs))

    async def reply_text(self, text, **kwargs):
        self.replies.append((text, kwargs))


class TemporaryResellerTrialDatabase(unittest.TestCase):
    def setUp(self):
        self.original_db = storage.DB_FILE
        self.original_snapshot = app_settings.APP_SETTINGS.snapshot()
        self.tempdir = tempfile.TemporaryDirectory(prefix="vpn-v38-")
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
                "admins": (),
                "inbounds": (),
                "resellers": (),
            },
            root_admin_id=int(
                self.original_snapshot.get("root_admin_id") or config.ROOT_ADMIN_ID
            ),
        )
        self.tempdir.cleanup()

    def add_reseller(self, *, tg_id=780001, trial_enabled=True):
        app_settings.add_reseller(
            name="V38 Shop",
            tg_id=tg_id,
            price_per_gb_toman=5_000,
            trial_enabled=trial_enabled,
            admin_tg_id=900001,
        )
        return app_settings.reseller_record(tg_id)


class ResellerTrialMigrationTests(TemporaryResellerTrialDatabase):
    def test_v37_reseller_rows_migrate_enabled_without_data_loss(self):
        legacy_db = os.path.join(self.tempdir.name, "legacy-v37.sqlite3")
        storage.DB_FILE = legacy_db
        conn = sqlite3.connect(legacy_db)
        try:
            conn.executescript(
                """
                CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                INSERT INTO meta(key,value) VALUES('schema_version','25');
                CREATE TABLE resellers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tg_id INTEGER NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    price_per_gb_toman INTEGER NOT NULL DEFAULT 0,
                    debt_toman INTEGER NOT NULL DEFAULT 0,
                    created_by INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    deleted_at TEXT NOT NULL DEFAULT ''
                );
                INSERT INTO resellers(
                    tg_id,name,price_per_gb_toman,debt_toman,created_by,
                    created_at,updated_at,deleted_at
                ) VALUES(780010,'Legacy Shop',4321,98765,900001,
                         '2026-01-01T00:00:00+00:00','2026-01-02T00:00:00+00:00','');
                """
            )
            conn.commit()
        finally:
            conn.close()

        storage.initialize_storage()
        migrated = storage.get_reseller_by_tg_id(780010)
        self.assertEqual(migrated["name"], "Legacy Shop")
        self.assertEqual(migrated["price_per_gb_toman"], 4321)
        self.assertEqual(migrated["debt_toman"], 98765)
        self.assertEqual(migrated["trial_enabled"], 1)
        conn = storage._connect()
        try:
            columns = {
                row[1] for row in conn.execute("PRAGMA table_info(resellers)")
            }
            version = conn.execute(
                "SELECT value FROM meta WHERE key='schema_version'"
            ).fetchone()[0]
        finally:
            conn.close()
        self.assertIn("trial_enabled", columns)
        self.assertEqual(version, "26")

    def test_trial_permission_refreshes_atomic_runtime_snapshot_immediately(self):
        reseller = self.add_reseller(tg_id=780011, trial_enabled=False)
        self.assertFalse(app_settings.reseller_record(780011)["trial_enabled"])
        app_settings.edit_reseller(
            reseller["id"], trial_enabled=True, admin_tg_id=900001
        )
        self.assertTrue(app_settings.reseller_record(780011)["trial_enabled"])


class ResellerTrialUiTests(TemporaryResellerTrialDatabase, unittest.IsolatedAsyncioTestCase):
    async def _admin_callback(self, data, context, message):
        user = SimpleNamespace(
            id=900001, username="admin", first_name="Admin", last_name=""
        )
        query = SimpleNamespace(
            data=data,
            from_user=user,
            message=message,
            answer=AsyncMock(return_value=None),
        )
        with (
            patch.object(bot.CALLBACK_LIMITER, "allow", return_value=(True, "")),
            patch.object(bot, "safe_callback_answer", AsyncMock(return_value=True)),
            patch.object(bot, "schedule_telegram_profile", lambda *_a, **_k: None),
        ):
            await bot.callback_router(
                SimpleNamespace(callback_query=query), context
            )

    async def test_add_wizard_requires_yes_or_no_and_persists_selection(self):
        context = SimpleNamespace(
            user_data={
                "awaiting": {"kind": "reseller_add_rate"},
                "reseller_draft": {"name": "New Shop", "tg_id": 780020},
            }
        )
        message = FakeMessage("6000")
        update = SimpleNamespace(
            message=message,
            effective_user=SimpleNamespace(
                id=900001, username="admin", first_name="Admin", last_name=""
            ),
        )
        with patch.object(bot, "schedule_telegram_profile", lambda *_a, **_k: None):
            await bot.text_router(update, context)
        prompt_text, prompt_kwargs = message.replies[-1]
        self.assertIn("امکان دریافت اکانت تست", prompt_text)
        self.assertEqual(
            set(callback_values(prompt_kwargs["reply_markup"])),
            {"rsaddtrial|1", "rsaddtrial|0", "admin_resellers"},
        )

        callback_message = FakeMessage()
        await self._admin_callback("rsaddtrial|0", context, callback_message)
        summary_text, summary_kwargs = callback_message.edits[-1]
        self.assertIn("اکانت تست: <b>غیرفعال", summary_text)
        self.assertIn("rsaddok", callback_values(summary_kwargs["reply_markup"]))
        self.assertFalse(context.user_data["reseller_draft"]["trial_enabled"])

        await self._admin_callback("rsaddok", context, callback_message)
        created = app_settings.reseller_record(780020)
        self.assertTrue(created)
        self.assertFalse(created["trial_enabled"])
        self.assertNotIn("reseller_draft", context.user_data)
        self.assertNotIn("awaiting", context.user_data)

    async def test_admin_detail_can_toggle_old_reseller_permission(self):
        reseller = self.add_reseller(tg_id=780021, trial_enabled=False)
        message = FakeMessage()
        await bot.show_admin_reseller_detail(message, 900001, reseller["id"])
        text, kwargs = message.edits[-1]
        self.assertIn("اکانت تست: <b>غیرفعال", text)
        self.assertIn(
            f"rstrial|{reseller['id']}|1",
            callback_values(kwargs["reply_markup"]),
        )

        await self._admin_callback(
            f"rstrial|{reseller['id']}|1", SimpleNamespace(user_data={}), message
        )
        self.assertTrue(app_settings.reseller_record(780021)["trial_enabled"])
        self.assertIn("اکانت تست: <b>فعال", message.edits[-1][0])

    async def test_disabled_reseller_never_sees_or_uses_trial(self):
        self.add_reseller(tg_id=780022, trial_enabled=False)
        with (
            patch.object(bot, "test_plan_enabled", return_value=True),
            patch.object(storage, "_connect", side_effect=AssertionError("hot path DB read")),
        ):
            for service in ("openvpn", "v2ray"):
                callbacks = callback_values(bot.service_menu_keyboard(service, 780022))
                self.assertNotIn(f"act|test|{service}", callbacks)

        message = FakeMessage()
        query = SimpleNamespace(from_user=SimpleNamespace(id=780022), message=message)
        with (
            patch.object(bot, "service_sales_enabled", return_value=True),
            patch.object(bot, "test_plan_enabled", return_value=True),
            patch.object(bot, "fulfill", AsyncMock(side_effect=AssertionError("provision"))) as fulfill,
        ):
            await bot.create_test(query, SimpleNamespace(), "openvpn")
        fulfill.assert_not_awaited()
        self.assertIn("غیرفعال", message.edits[-1][0])

    async def test_enabled_reseller_can_receive_unlimited_trials(self):
        self.add_reseller(tg_id=780023, trial_enabled=True)
        message = FakeMessage()
        query = SimpleNamespace(from_user=SimpleNamespace(id=780023), message=message)
        order_ids = []

        async def fake_fulfill(*_args, **kwargs):
            order_ids.append(kwargs["order_id"])
            return "delivered", None

        async def fake_blocking(func, *_args, **_kwargs):
            if func is bot.mark_fulfillment_completed:
                return True
            if func in {bot.has_test, bot.mark_test}:
                raise AssertionError("reseller trial must not use one-time marker")
            raise AssertionError(f"unexpected blocking call: {func}")

        with (
            patch.object(bot, "service_sales_enabled", return_value=True),
            patch.object(bot, "test_plan_enabled", return_value=True),
            patch.object(bot, "fulfill", side_effect=fake_fulfill),
            patch.object(bot, "run_blocking", side_effect=fake_blocking),
        ):
            await bot.create_test(query, SimpleNamespace(), "openvpn")
            await bot.create_test(query, SimpleNamespace(), "openvpn")

        self.assertEqual(len(order_ids), 2)
        self.assertEqual(len(set(order_ids)), 2)
        self.assertTrue(all(value.startswith("test-openvpn-780023-") for value in order_ids))

    async def test_regular_user_remains_limited_to_one_trial(self):
        message = FakeMessage()
        query = SimpleNamespace(from_user=SimpleNamespace(id=780024), message=message)

        async def fake_blocking(func, *_args, **_kwargs):
            if func is bot.has_test:
                return True
            raise AssertionError(f"unexpected blocking call: {func}")

        with (
            patch.object(bot, "service_sales_enabled", return_value=True),
            patch.object(bot, "test_plan_enabled", return_value=True),
            patch.object(bot, "run_blocking", side_effect=fake_blocking),
            patch.object(bot, "fulfill", AsyncMock(side_effect=AssertionError("provision"))) as fulfill,
        ):
            await bot.create_test(query, SimpleNamespace(), "v2ray")
        fulfill.assert_not_awaited()
        self.assertIn("قبلاً دریافت کرده‌اید", message.edits[-1][0])


if __name__ == "__main__":
    unittest.main()
