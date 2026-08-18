import os
import shutil
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


TEST_DATA_DIR = tempfile.mkdtemp(prefix="account-bot-v110-admin-ui-")
os.environ.setdefault("BOT_TOKEN", "123456:V110_ADMIN_UI_TOKEN")
os.environ.setdefault("ADMIN_IDS", "911000")
os.environ["DATA_DIR"] = TEST_DATA_DIR

import bot  # noqa: E402


class FakeMessage:
    def __init__(self):
        self.edits = []

    async def edit_text(self, text, **kwargs):
        self.edits.append((text, kwargs))

    async def reply_text(self, text, **kwargs):
        self.edits.append((text, kwargs))


def callbacks(markup):
    return [
        button.callback_data
        for row in markup.inline_keyboard
        for button in row
        if button.callback_data
    ]


class AdminUiV110Tests(unittest.IsolatedAsyncioTestCase):
    async def _open_callback(self, data):
        message = FakeMessage()
        query = SimpleNamespace(
            data=data,
            from_user=SimpleNamespace(
                id=911000, username="admin", first_name="Admin", last_name=""
            ),
            message=message,
            answer=AsyncMock(return_value=None),
        )
        update = SimpleNamespace(callback_query=query)
        context = SimpleNamespace(user_data={})
        with (
            patch.object(bot, "is_admin", return_value=True),
            patch.object(bot.CALLBACK_LIMITER, "allow", return_value=(True, "")),
            patch.object(bot, "schedule_telegram_profile", lambda *_a, **_k: None),
        ):
            await bot.callback_router(update, context)
        self.assertTrue(message.edits, data)
        return message.edits[-1]

    def test_root_navigation_is_grouped_and_callback_safe(self):
        markup = bot.admin_tools_keyboard()
        values = callbacks(markup)
        self.assertEqual(values[:5], [
            "admin_payments_menu",
            "admin_users_menu",
            "admin_sales_menu",
            "admin_settings_menu",
            "admin_system_menu",
        ])
        self.assertTrue(all(len(value.encode("utf-8")) <= 64 for value in values))

    async def test_settings_contains_only_five_logical_groups(self):
        message = FakeMessage()
        with patch.object(bot, "is_admin", return_value=True):
            await bot.show_admin_settings_menu(message, 911000)
        values = callbacks(message.edits[-1][1]["reply_markup"])
        self.assertEqual(values, [
            "admin_cfg|bot",
            "admin_connections_menu",
            "admin_gateways",
            "admin_marketing_menu",
            "admin_notification_settings",
            "admin_tools",
        ])
        self.assertNotIn("admin_backup_settings", values)
        self.assertFalse(any(value.startswith("admin_plans") for value in values))

    async def test_system_owns_backup_and_maintenance_controls(self):
        message = FakeMessage()
        with patch.object(bot, "is_admin", return_value=True):
            await bot.show_admin_system_menu(message, 911000)
        text, kwargs = message.edits[-1]
        values = callbacks(kwargs["reply_markup"])
        self.assertIn("admin_backup_settings", values)
        self.assertTrue(any(value.startswith("admin_maintenance_set|") for value in values))
        self.assertIn("ساعت بکاپ", text)

    async def test_user_page_has_one_account_entry_and_paginated_accounts(self):
        accounts = [
            {
                "service": "openvpn" if index % 2 == 0 else "v2ray",
                "identifier": f"user-{index}",
                "is_test": False,
            }
            for index in range(11)
        ]
        summary = {
            "tg_id": 911101,
            "label": "کاربر آزمایشی",
            "profile": {},
            "balance_toman": 0,
            "reserved_toman": 0,
            "purchase_count": 11,
            "transaction_count": 11,
            "accounts": accounts,
            "referral": {},
        }
        detail = FakeMessage()
        with (
            patch.object(bot, "is_admin", return_value=True),
            patch.object(bot, "run_blocking", new=AsyncMock(return_value=summary)),
        ):
            await bot.show_admin_user_detail(detail, 911000, 911101)
        detail_values = callbacks(detail.edits[-1][1]["reply_markup"])
        self.assertEqual(
            len([value for value in detail_values if value.startswith("admin_user_accounts|")]),
            1,
        )
        self.assertFalse(any(value.startswith("admacc_ref|") for value in detail_values))

        listing = FakeMessage()
        with (
            patch.object(bot, "is_admin", return_value=True),
            patch.object(bot, "run_blocking", new=AsyncMock(return_value=summary)),
        ):
            await bot.show_admin_user_accounts(listing, 911000, 911101, 0)
        list_values = callbacks(listing.edits[-1][1]["reply_markup"])
        self.assertEqual(len([value for value in list_values if value.startswith("admacc_ref|")]), 8)
        self.assertIn("admin_user_accounts|911101|1", list_values)
        self.assertTrue(all(len(value.encode("utf-8")) <= 64 for value in list_values))

    async def test_new_admin_text_is_right_to_left_anchored(self):
        message = FakeMessage()
        with patch.object(bot, "is_admin", return_value=True):
            await bot.show_admin_notification_settings(message, 911000)
        text = message.edits[-1][0]
        for line in text.splitlines():
            if line:
                self.assertTrue(line.startswith(bot._RLM))

    async def test_resellers_and_service_details_have_single_clear_parent(self):
        users = bot.admin_users_menu_keyboard()
        self.assertIn("admin_resellers", callbacks(users))

        general = FakeMessage()
        with patch.object(bot, "is_admin", return_value=True):
            await bot.show_admin_bot_settings(general, 911000)
        self.assertNotIn("admin_resellers", callbacks(general.edits[-1][1]["reply_markup"]))

        connections = FakeMessage()
        with patch.object(bot, "is_admin", return_value=True):
            await bot.show_admin_connections_menu(connections, 911000)
        self.assertEqual(callbacks(connections.edits[-1][1]["reply_markup"]), [
            "admin_cfg|mikrotik", "admin_cfg|xui", "admin_settings_menu",
        ])

    async def test_every_new_navigation_callback_opens_its_screen(self):
        expected = {
            "admin_sales_menu": "فروش و بسته‌ها",
            "admin_connections_menu": "اتصال‌ها و سرویس‌ها",
            "admin_marketing_menu": "بازاریابی و کیف پول",
            "admin_notification_settings": "اعلان‌ها",
            "admin_mt_connection": "اتصال روتر",
            "admin_um_settings": "تنظیمات یوزر منیجر",
            "admin_xui_connection": "اتصال پنل ثنایی",
        }
        for callback_data, phrase in expected.items():
            text, _kwargs = await self._open_callback(callback_data)
            self.assertIn(phrase, text, callback_data)


def tearDownModule():
    for lane in bot.BLOCKING_LANES.values():
        lane.shutdown()
    shutil.rmtree(TEST_DATA_DIR, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
