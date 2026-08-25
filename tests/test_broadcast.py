import os
import shutil
import sqlite3
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


TEST_DATA_DIR = tempfile.mkdtemp(prefix="account-bot-broadcast-tests-")
os.environ.setdefault("BOT_TOKEN", "123456:BROADCAST_TEST_TOKEN")
os.environ.setdefault("ADMIN_IDS", "913000")
os.environ["DATA_DIR"] = TEST_DATA_DIR

import app_settings  # noqa: E402
import bot  # noqa: E402
import storage  # noqa: E402
from telegram.error import Forbidden  # noqa: E402


class FakeMessage:
    def __init__(self, *, text="", message_id=1, chat_id=913000):
        self.text = text
        self.message_id = message_id
        self.chat_id = chat_id
        self.replies = []
        self.edits = []

    async def reply_text(self, text, **kwargs):
        self.replies.append((text, kwargs))

    async def edit_text(self, text, **kwargs):
        self.edits.append((text, kwargs))


class FakeBroadcastBot:
    def __init__(self, forbidden_ids=()):
        self.forbidden_ids = {int(value) for value in forbidden_ids}
        self.copies = []
        self.messages = []

    async def copy_message(self, **kwargs):
        self.copies.append(dict(kwargs))
        if int(kwargs["chat_id"]) in self.forbidden_ids:
            raise Forbidden("bot was blocked by the user")
        return SimpleNamespace(message_id=len(self.copies))

    async def send_message(self, **kwargs):
        self.messages.append(dict(kwargs))
        return SimpleNamespace(message_id=999)


class FakeCreatedTask:
    def done(self):
        return False


class FakeApplication:
    def __init__(self):
        self.bot_data = {}
        self.created = []

    def create_task(self, coroutine, *, name=""):
        self.created.append(name)
        coroutine.close()
        return FakeCreatedTask()


def callback_values(markup):
    return [
        button.callback_data
        for row in markup.inline_keyboard
        for button in row
        if button.callback_data
    ]


class PublicBroadcastTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory(prefix="account-broadcast-case-")
        storage.DB_FILE = os.path.join(self.tempdir.name, "vpn_bot_v2.sqlite3")
        storage.BACKUP_DIR = os.path.join(self.tempdir.name, "backups")
        storage.initialize_storage()
        app_settings.initialize_runtime_settings(root_admin_id=913000)

    def tearDown(self):
        app_settings.refresh_runtime_settings(root_admin_id=913000)
        self.tempdir.cleanup()

    def test_schema_upgrade_backfills_existing_users_as_historical_starters(self):
        legacy = os.path.join(self.tempdir.name, "legacy-v12.sqlite3")
        conn = sqlite3.connect(legacy)
        try:
            conn.executescript(
                """
                CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
                INSERT INTO meta(key,value) VALUES('schema_version','27');
                CREATE TABLE users(
                    tg_id INTEGER PRIMARY KEY,
                    first_name TEXT NOT NULL DEFAULT '',
                    last_name TEXT NOT NULL DEFAULT '',
                    username TEXT NOT NULL DEFAULT '',
                    language_code TEXT NOT NULL DEFAULT '',
                    phone_number TEXT NOT NULL DEFAULT '',
                    email TEXT NOT NULL DEFAULT '',
                    test_openvpn INTEGER NOT NULL DEFAULT 0,
                    test_v2ray INTEGER NOT NULL DEFAULT 0,
                    legacy_purchase_qualified INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                INSERT INTO users(tg_id,created_at,updated_at)
                VALUES(7001,'2026-01-01T00:00:00+00:00','2026-01-01T00:00:00+00:00');
                """
            )
            conn.commit()
        finally:
            conn.close()
        storage.DB_FILE = legacy
        storage.initialize_storage()
        conn = storage._connect()
        try:
            row = conn.execute(
                "SELECT bot_started_at FROM users WHERE tg_id=7001"
            ).fetchone()
            version = conn.execute(
                "SELECT value FROM meta WHERE key='schema_version'"
            ).fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(row[0], "2026-01-01T00:00:00+00:00")
        self.assertEqual(version, "28")
        self.assertEqual(storage.list_broadcast_recipient_ids(), [7001])

    def test_only_users_marked_by_start_are_new_broadcast_recipients(self):
        storage.update_user_profile(7101, first_name="Not Started")
        self.assertEqual(storage.list_broadcast_recipient_ids(), [])
        started_at = storage.mark_user_started(7101)
        self.assertTrue(started_at)
        self.assertEqual(storage.list_broadcast_recipient_ids(), [7101])
        self.assertEqual(
            storage.broadcast_recipient_count(exclude_tg_ids=(7101,)), 0
        )
        with patch.object(
            storage, "_tx", side_effect=AssertionError("repeat /start must stay read-only")
        ):
            self.assertEqual(storage.mark_user_started(7101), started_at)

    def test_admin_settings_contains_requested_broadcast_button(self):
        markup = bot.admin_settings_menu_keyboard()
        values = callback_values(markup)
        self.assertIn("admin_broadcast_begin", values)
        button = next(
            button
            for row in markup.inline_keyboard
            for button in row
            if button.callback_data == "admin_broadcast_begin"
        )
        self.assertIn("ارسال پیام عمومی در ربات", button.text)
        self.assertTrue(all(len(value.encode("utf-8")) <= 64 for value in values))

    async def test_admin_callback_opens_flow_and_confirmation_starts_background_task(self):
        application = FakeApplication()
        context = SimpleNamespace(user_data={}, application=application)
        message = FakeMessage()
        query = SimpleNamespace(
            data="admin_broadcast_begin",
            from_user=SimpleNamespace(
                id=913000, first_name="Admin", last_name="", username="admin"
            ),
            message=message,
            answer=AsyncMock(return_value=None),
        )
        update = SimpleNamespace(callback_query=query)
        with (
            patch.object(bot, "is_admin", return_value=True),
            patch.object(bot.CALLBACK_LIMITER, "allow", return_value=(True, "")),
            patch.object(bot, "schedule_telegram_profile", lambda *_a, **_k: None),
        ):
            await bot.callback_router(update, context)
        self.assertEqual(
            context.user_data["awaiting"]["kind"], "admin_broadcast_message"
        )
        self.assertIn("ارسال پیام عمومی", message.edits[-1][0])

        token = "abcdef123456"
        context.user_data = {
            "public_broadcast_draft": {
                "token": token,
                "source_chat_id": 913000,
                "source_message_id": 987,
                "recipient_count": 3,
                "text_length": 10,
                "text_sha256": "b" * 64,
                "created_monotonic": bot.time.monotonic(),
            }
        }
        query.data = f"admin_broadcast_confirm|{token}"
        with (
            patch.object(bot, "is_admin", return_value=True),
            patch.object(bot.CALLBACK_LIMITER, "allow", return_value=(True, "")),
            patch.object(bot, "schedule_telegram_profile", lambda *_a, **_k: None),
        ):
            await bot.callback_router(update, context)
        self.assertEqual(len(application.created), 1)
        self.assertIn("public_broadcast_task", application.bot_data)
        self.assertIn("ارسال پیام عمومی شروع شد", message.edits[-1][0])

    async def test_admin_text_is_captured_by_message_id_without_rewriting_content(self):
        storage.mark_user_started(7201)
        message_text = "سلام <کاربر>\nhttps://example.com"
        message = FakeMessage(text=message_text, message_id=345, chat_id=913000)
        update = SimpleNamespace(
            message=message,
            effective_message=message,
            effective_user=SimpleNamespace(id=913000),
            effective_chat=SimpleNamespace(id=913000),
        )
        context = SimpleNamespace(
            user_data={"awaiting": {"kind": "admin_broadcast_message"}},
            application=SimpleNamespace(bot_data={}),
        )
        with (
            patch.object(bot, "is_admin", return_value=True),
            patch.object(bot, "schedule_telegram_profile", lambda *_a, **_k: None),
        ):
            await bot.text_router(update, context)
        draft = context.user_data.get("public_broadcast_draft") or {}
        self.assertEqual(draft["source_chat_id"], 913000)
        self.assertEqual(draft["source_message_id"], 345)
        self.assertEqual(draft["text_length"], len(message_text))
        self.assertNotIn(message_text, str(draft))
        callbacks = callback_values(message.replies[-1][1]["reply_markup"])
        self.assertTrue(any(value.startswith("admin_broadcast_confirm|") for value in callbacks))

    async def test_broadcast_copies_exact_source_message_and_reports_failures(self):
        for tg_id in (913000, 7301, 7302, 7303):
            storage.mark_user_started(tg_id)
        telegram_bot = FakeBroadcastBot(forbidden_ids=(7302,))
        application = SimpleNamespace(bot=telegram_bot, bot_data={})
        with patch.object(bot, "_BROADCAST_SEND_INTERVAL_SECONDS", 0):
            await bot._run_public_broadcast(
                application,
                admin_tg_id=913000,
                source_chat_id=913000,
                source_message_id=456,
                text_length=25,
                text_sha256="a" * 64,
            )
        self.assertEqual([item["chat_id"] for item in telegram_bot.copies], [7301, 7302, 7303])
        self.assertTrue(all(item["from_chat_id"] == 913000 for item in telegram_bot.copies))
        self.assertTrue(all(item["message_id"] == 456 for item in telegram_bot.copies))
        self.assertEqual(len(telegram_bot.messages), 1)
        report = telegram_bot.messages[0]["text"]
        self.assertIn("2", report)
        self.assertIn("1", report)
        audits, _ = storage.list_admin_audit(limit=10)
        completed = next(row for row in audits if row["action"] == "public_broadcast_completed")
        self.assertEqual(completed["meta"]["sent"], 2)
        self.assertEqual(completed["meta"]["failed"], 1)
        self.assertNotIn("a" * 64, str(completed))


def tearDownModule():
    for lane in bot.BLOCKING_LANES.values():
        lane.shutdown()
    shutil.rmtree(TEST_DATA_DIR, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
