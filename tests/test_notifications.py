import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


os.environ.setdefault("BOT_TOKEN", "123456:V37_TEST_TOKEN")
os.environ.setdefault("ADMIN_IDS", "900001")
os.environ.setdefault("DATA_DIR", tempfile.mkdtemp(prefix="vpn-bot-v37-import-"))

import bot
from services import zarinpal


def only_button(markup):
    return markup.inline_keyboard[0][0]


class FakeMessage:
    def __init__(self, text):
        self.text = text
        self.text_html = text
        self.edits = []

    async def edit_text(self, text, **kwargs):
        self.text = text
        self.text_html = text
        self.edits.append((text, kwargs))


class AdminNotificationTests(unittest.IsolatedAsyncioTestCase):
    async def test_full_account_button_collapses_to_original_notification(self):
        original = (
            "✅ <b>خرید موفق</b>\n"
            "نوع خرید: مستقیم\n"
            "Telegram ID: <code>700001</code>"
        )
        initial_button = only_button(
            bot.admin_account_keyboard("openvpn", 700001, "vpn-user")
        )
        self.assertEqual(initial_button.text, "🔐 مشاهده اطلاعات کامل اکانت")
        self.assertTrue(initial_button.callback_data.startswith("admnot_ref|"))
        self.assertLessEqual(len(initial_button.callback_data.encode("utf-8")), 64)

        message = FakeMessage(original)
        user = SimpleNamespace(
            id=900001, username="admin", first_name="Admin", last_name=""
        )
        query = SimpleNamespace(
            data=initial_button.callback_data,
            from_user=user,
            message=message,
            answer=AsyncMock(return_value=None),
        )
        update = SimpleNamespace(callback_query=query)
        context = SimpleNamespace(user_data={})

        async def fake_blocking(func, *_args, **_kwargs):
            if func is bot._resolve_account_ref:
                return "vpn-user"
            if func is bot._account_record:
                return {
                    "identifier": "vpn-user",
                    "username": "vpn-user",
                    "password": "123456",
                    "profile": "10GB-30D",
                    "plan_key": "OPENVPN-10",
                    "is_test": False,
                }
            raise AssertionError(f"unexpected blocking call: {func}")

        with (
            patch.object(bot, "is_admin", return_value=True),
            patch.object(bot.CALLBACK_LIMITER, "allow", return_value=(True, "")),
            patch.object(bot, "safe_callback_answer", AsyncMock(return_value=True)),
            patch.object(bot, "schedule_telegram_profile", lambda *_a, **_k: None),
            patch.object(bot, "run_blocking", fake_blocking),
        ):
            await bot.callback_router(update, context)
            expanded_text, expanded_kwargs = message.edits[-1]
            expanded_button = only_button(expanded_kwargs["reply_markup"])
            self.assertIn("اطلاعات کامل اکانت", expanded_text)
            self.assertEqual(expanded_button.text, "نمایش کمتر")
            self.assertTrue(expanded_button.callback_data.startswith("admnot_less|"))
            self.assertNotIn("صفحه کاربر", expanded_button.text)
            self.assertLessEqual(len(expanded_button.callback_data.encode("utf-8")), 64)

            query.data = expanded_button.callback_data
            await bot.callback_router(update, context)

        collapsed_text, collapsed_kwargs = message.edits[-1]
        collapsed_button = only_button(collapsed_kwargs["reply_markup"])
        self.assertEqual(collapsed_text, original)
        self.assertNotIn("اطلاعات کامل اکانت", collapsed_text)
        self.assertEqual(collapsed_button.text, "🔐 مشاهده اطلاعات کامل اکانت")
        self.assertEqual(collapsed_button.callback_data, initial_button.callback_data)


class ZarinPalDescriptionTests(unittest.TestCase):
    def test_payment_description_never_discloses_action_service_or_account(self):
        captured = []
        authorities = iter(["A" * 36, "B" * 36])

        def fake_post(_url, payload, **_kwargs):
            captured.append(dict(payload))
            return {"data": {"code": 100, "authority": next(authorities)}}

        gateway_config = (
            "merchant-v37",
            False,
            "https://api.zarinpal.com/pg/v4/payment/request.json",
            "https://api.zarinpal.com/pg/v4/payment/verify.json",
            "https://www.zarinpal.com/pg/StartPay/{authority}",
        )
        with (
            patch.object(zarinpal, "_current_gateway_config", return_value=gateway_config),
            patch.object(zarinpal, "_post_json", side_effect=fake_post),
            patch.object(zarinpal, "add_pending"),
        ):
            zarinpal.create_payment(
                tg_id=700001,
                service="openvpn",
                action="buy",
                plan_key="OPENVPN-10",
                identifier="private-openvpn-user",
                amount_rial=1_000_000,
                order_id="v37-buy",
            )
            zarinpal.create_payment(
                tg_id=700002,
                service="v2ray",
                action="renew",
                plan_key="V2RAY-20",
                identifier="private-v2ray-user",
                amount_rial=2_000_000,
                order_id="v37-renew",
            )

        self.assertEqual([row["description"] for row in captured], ["ربات تلگرام"] * 2)
        joined = " ".join(row["description"] for row in captured).lower()
        for forbidden in (
            "openvpn", "v2ray", "خرید", "تمدید",
            "private-openvpn-user", "private-v2ray-user",
            "openvpn-10", "v2ray-20",
        ):
            self.assertNotIn(forbidden, joined)


if __name__ == "__main__":
    unittest.main()
