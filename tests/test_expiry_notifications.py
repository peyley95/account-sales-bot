import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


os.environ.setdefault("BOT_TOKEN", "123456:V101_EXPIRY_TEST_TOKEN")
os.environ.setdefault("ADMIN_IDS", "910001")
os.environ.setdefault("DATA_DIR", tempfile.mkdtemp(prefix="account-bot-v101-import-"))

import app_settings
import bot
import storage
from account_notifications import classify_openvpn_status, classify_v2ray_status


GIB = 1024 ** 3


class ExpiryClassifierTests(unittest.TestCase):
    def test_v2ray_warns_at_exact_thresholds(self):
        result = classify_v2ray_status({
            "enabled": True,
            "waiting_first_use": False,
            "total_bytes": 10 * GIB,
            "remaining_bytes": GIB,
            "expiry_ms": 1_900_000_000_000,
            "remaining_days_float": 1.0,
        })
        self.assertEqual(result, {"kind": "warning", "low_volume": True, "low_time": True})

    def test_v2ray_expired_takes_precedence_and_zero_means_unlimited(self):
        expired = classify_v2ray_status({
            "enabled": True,
            "total_bytes": 10 * GIB,
            "remaining_bytes": 0,
            "expiry_ms": 1_900_000_000_000,
            "remaining_days_float": 0.5,
        })
        self.assertEqual(expired["kind"], "expired")
        self.assertIsNone(classify_v2ray_status({
            "enabled": True,
            "total_bytes": 0,
            "remaining_bytes": 0,
            "expiry_ms": 0,
            "remaining_days_float": 0,
        }))

    def test_first_use_accounts_do_not_warn_before_activation(self):
        self.assertIsNone(classify_v2ray_status({
            "enabled": True,
            "waiting_first_use": True,
            "total_bytes": GIB,
            "remaining_bytes": GIB,
            "expiry_ms": -86_400_000,
            "remaining_days_float": 1.0,
        }))
        self.assertIsNone(classify_openvpn_status({
            "found": True,
            "usage_available": True,
            "total_download": 0,
            "total_upload": 0,
            "um_profile_state": 0,
            "um_profile_starts_at": 0,
        }, quota_bytes=GIB))

    def test_openvpn_warning_and_terminal_states(self):
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        warning = classify_openvpn_status({
            "found": True,
            "usage_available": True,
            "total_download": 9 * GIB,
            "total_upload": 0,
            "um_profile_state": 2,
            "expiry": now + timedelta(days=1),
        }, quota_bytes=10 * GIB, now=now)
        self.assertEqual(warning, {"kind": "warning", "low_volume": True, "low_time": True})
        expired = classify_openvpn_status({
            "found": True,
            "usage_available": False,
            "um_profile_state": 3,
        }, now=now)
        self.assertEqual(expired["kind"], "expired")


class ExpiryNotificationStorageTests(unittest.TestCase):
    TG_ID = 910101

    def tearDown(self):
        with storage._tx(immediate=True) as conn:
            conn.execute("DELETE FROM users WHERE tg_id=?", (self.TG_ID,))

    def _account(self, *, is_test=False):
        storage.upsert_account(
            self.TG_ID, "v2ray", "expiry-storage-user",
            plan_key="V2RAY-10", is_test=is_test,
        )
        return next(
            row for row in storage.list_accounts_for_expiry_monitor()
            if row["tg_id"] == self.TG_ID
        )

    def test_monitor_query_excludes_trial_accounts(self):
        storage.upsert_account(
            self.TG_ID, "v2ray", "expiry-trial-user",
            plan_key="__test_v2ray__", is_test=True,
        )
        self.assertFalse(any(
            row["tg_id"] == self.TG_ID
            for row in storage.list_accounts_for_expiry_monitor()
        ))

    def test_warning_and_expired_are_each_claimed_once_per_cycle(self):
        account = self._account()
        account_id = account["account_id"]
        cycle = account["updated_at"]
        self.assertTrue(storage.claim_account_expiry_notification(account_id, "warning", cycle))
        self.assertFalse(storage.claim_account_expiry_notification(account_id, "warning", cycle))
        self.assertTrue(storage.claim_account_expiry_notification(account_id, "expired", cycle))
        self.assertFalse(storage.claim_account_expiry_notification(account_id, "expired", cycle))

    def test_renewal_row_version_starts_a_new_cycle_and_rejects_stale_claim(self):
        old = self._account()
        storage.upsert_account(
            self.TG_ID, "v2ray", "expiry-storage-user",
            plan_key="V2RAY-20", is_test=False,
        )
        current = next(
            row for row in storage.list_accounts_for_expiry_monitor()
            if row["tg_id"] == self.TG_ID
        )
        self.assertNotEqual(old["updated_at"], current["updated_at"])
        self.assertFalse(storage.claim_account_expiry_notification(
            old["account_id"], "warning", old["updated_at"]
        ))
        self.assertTrue(storage.claim_account_expiry_notification(
            current["account_id"], "warning", current["updated_at"]
        ))

    def test_v101_setting_migration_marker_and_defaults_exist(self):
        state = storage.get_app_settings_state()["settings"]
        self.assertIs(state["account_expiry_notifications_enabled"], True)
        self.assertEqual(state["account_expiry_check_interval_minutes"], 30)
        with storage._connect() as conn:
            marker = conn.execute(
                "SELECT value FROM meta WHERE key=?",
                (storage.EXPIRY_NOTIFICATIONS_V101_MIGRATION_KEY,),
            ).fetchone()
        self.assertEqual(marker[0], "1.0.1")


class ExpiryMonitorIntegrationTests(unittest.IsolatedAsyncioTestCase):
    TG_ID = 910201

    async def asyncTearDown(self):
        with storage._tx(immediate=True) as conn:
            conn.execute("DELETE FROM users WHERE tg_id=?", (self.TG_ID,))

    async def test_warning_then_expiry_send_once_each_with_direct_renew_button(self):
        storage.upsert_account(
            self.TG_ID, "v2ray", "expiry-monitor-user",
            plan_key="V2RAY-10", is_test=False,
        )
        account = next(
            row for row in storage.list_accounts_for_expiry_monitor()
            if row["tg_id"] == self.TG_ID
        )
        telegram_bot = SimpleNamespace(send_message=AsyncMock(return_value=True))
        application = SimpleNamespace(bot=telegram_bot)
        statuses = [
            {
                "enabled": True, "waiting_first_use": False,
                "total_bytes": 10 * GIB, "remaining_bytes": GIB,
                "expiry_ms": 1_900_000_000_000, "remaining_days_float": 5.0,
            },
            {
                "enabled": True, "waiting_first_use": False,
                "total_bytes": 10 * GIB, "remaining_bytes": GIB,
                "expiry_ms": 1_900_000_000_000, "remaining_days_float": 5.0,
            },
            {
                "enabled": True, "waiting_first_use": False,
                "total_bytes": 10 * GIB, "remaining_bytes": 0,
                "expiry_ms": 1_900_000_000_000, "remaining_days_float": 0.0,
            },
            {
                "enabled": True, "waiting_first_use": False,
                "total_bytes": 10 * GIB, "remaining_bytes": 0,
                "expiry_ms": 1_900_000_000_000, "remaining_days_float": 0.0,
            },
        ]

        async def fake_remote(*_args, **_kwargs):
            return statuses.pop(0)

        with patch.object(bot, "run_blocking_retry", side_effect=fake_remote):
            results = [
                await bot._scan_one_account_for_expiry(application, account)
                for _ in range(4)
            ]

        self.assertEqual(results, ["warning", "duplicate_or_stale", "expired", "duplicate_or_stale"])
        self.assertEqual(telegram_bot.send_message.await_count, 2)
        first = telegram_bot.send_message.await_args_list[0].kwargs
        second = telegram_bot.send_message.await_args_list[1].kwargs
        self.assertIn("۱ گیگ یا کمتر", first["text"])
        self.assertIn("به پایان رسیده", second["text"])
        callback = first["reply_markup"].inline_keyboard[0][0].callback_data
        self.assertTrue(callback.startswith("myactref|renew|v2ray|"))
        self.assertLessEqual(len(callback.encode("utf-8")), 64)

    async def test_telegram_failure_is_not_retried_every_scan(self):
        storage.upsert_account(
            self.TG_ID, "v2ray", "expiry-send-failure-user",
            plan_key="V2RAY-10", is_test=False,
        )
        account = next(
            row for row in storage.list_accounts_for_expiry_monitor()
            if row["tg_id"] == self.TG_ID
        )
        telegram_bot = SimpleNamespace(
            send_message=AsyncMock(side_effect=RuntimeError("temporary send failure"))
        )
        application = SimpleNamespace(bot=telegram_bot)
        warning = {
            "enabled": True, "waiting_first_use": False,
            "total_bytes": 10 * GIB, "remaining_bytes": GIB,
            "expiry_ms": 1_900_000_000_000, "remaining_days_float": 5.0,
        }

        async def fake_remote(*_args, **_kwargs):
            return dict(warning)

        with patch.object(bot, "run_blocking_retry", side_effect=fake_remote):
            first = await bot._scan_one_account_for_expiry(application, account)
            second = await bot._scan_one_account_for_expiry(application, account)

        self.assertEqual(first, "send_error")
        self.assertEqual(second, "duplicate_or_stale")
        self.assertEqual(telegram_bot.send_message.await_count, 1)

    async def test_disabled_monitor_does_not_contact_remote_services(self):
        remote = AsyncMock()
        with (
            patch.object(bot.APP_SETTINGS, "get", return_value=False),
            patch.object(bot, "run_blocking_retry", remote),
        ):
            result = await bot._scan_one_account_for_expiry(
                SimpleNamespace(bot=SimpleNamespace(send_message=AsyncMock())),
                {"account_id": 1, "tg_id": self.TG_ID, "service": "v2ray",
                 "identifier": "disabled-monitor", "updated_at": "cycle"},
            )
        self.assertEqual(result, "disabled")
        remote.assert_not_awaited()


class ExpirySettingValidationTests(unittest.TestCase):
    def test_interval_is_bounded(self):
        self.assertEqual(
            app_settings.normalize_setting("account_expiry_check_interval_minutes", "30"),
            30,
        )
        for invalid in ("4", "1441", "not-a-number"):
            with self.assertRaises(ValueError):
                app_settings.normalize_setting("account_expiry_check_interval_minutes", invalid)


class ExpiryAdminUiTests(unittest.IsolatedAsyncioTestCase):
    async def test_bot_settings_expose_toggle_and_interval_controls(self):
        message = SimpleNamespace(edit_text=AsyncMock())
        snapshot = dict(bot.APP_SETTINGS.snapshot())
        snapshot.update({
            "account_expiry_notifications_enabled": True,
            "account_expiry_check_interval_minutes": 30,
        })
        with (
            patch.object(bot, "is_admin", return_value=True),
            patch.object(bot.APP_SETTINGS, "snapshot", return_value=snapshot),
        ):
            await bot.show_admin_bot_settings(message, 910001)
        text = message.edit_text.await_args.args[0]
        markup = message.edit_text.await_args.kwargs["reply_markup"]
        callbacks = [
            button.callback_data
            for row in markup.inline_keyboard
            for button in row
            if button.callback_data
        ]
        self.assertIn("اعلان پایان اکانت", text)
        self.assertIn("30 دقیقه", text)
        self.assertIn("admin_account_expiry_toggle", callbacks)
        self.assertIn("admin_cfg_edit|expinterval", callbacks)


if __name__ == "__main__":
    unittest.main()
