import os
import shutil
import sqlite3
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


TEST_DATA_DIR = tempfile.mkdtemp(prefix="account-bot-restore-tests-")
os.environ.setdefault("BOT_TOKEN", "123456:RESTORE_TEST_TOKEN")
os.environ.setdefault("ADMIN_IDS", "911200")
os.environ["DATA_DIR"] = TEST_DATA_DIR

import app_settings  # noqa: E402
import bot  # noqa: E402
import restore_manager  # noqa: E402
import storage  # noqa: E402


class FakeMessage:
    def __init__(self, document=None, caption=""):
        self.document = document
        self.caption = caption
        self.replies = []

    async def reply_text(self, text, **kwargs):
        self.replies.append((text, kwargs))


class FakeTelegramFile:
    def __init__(self, source):
        self.source = source

    async def download_to_drive(self, custom_path):
        shutil.copyfile(self.source, custom_path)


class DatabaseRestoreTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory(prefix="account-bot-restore-case-")
        storage.DB_FILE = os.path.join(self.tempdir.name, "vpn_bot_v2.sqlite3")
        storage.BACKUP_DIR = os.path.join(self.tempdir.name, "backups")
        storage.initialize_storage()
        app_settings.initialize_runtime_settings(root_admin_id=911200)
        self._insert_user(111)

    def tearDown(self):
        app_settings.refresh_runtime_settings(root_admin_id=911200)
        self.tempdir.cleanup()

    def _insert_user(self, tg_id, *, with_business_rows=False):
        now = storage.now_iso()
        with storage._tx(immediate=True) as conn:
            storage._ensure_user(conn, tg_id)
            if with_business_rows:
                conn.execute(
                    "INSERT INTO accounts(tg_id,service,identifier,data_json,created_at,updated_at) "
                    "VALUES(?,?,?,?,?,?)",
                    (tg_id, "openvpn", f"user-{tg_id}", "{}", now, now),
                )
                conn.execute(
                    "INSERT INTO transactions(order_id,tg_id,service,action,created_at) "
                    "VALUES(?,?,?,?,?)",
                    (f"order-{tg_id}", tg_id, "openvpn", "buy", now),
                )

    def _candidate_with_restored_data(self):
        exported = storage.export_database_snapshot()
        candidate_dir = tempfile.mkdtemp(dir=self.tempdir.name, prefix="candidate-")
        candidate = os.path.join(candidate_dir, "forwarded.sqlite3")
        shutil.copyfile(exported["path"], candidate)
        shutil.rmtree(exported["temp_dir"], ignore_errors=True)
        conn = sqlite3.connect(candidate)
        try:
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("DELETE FROM transactions")
            conn.execute("DELETE FROM accounts")
            conn.execute("DELETE FROM users")
            now = storage.now_iso()
            conn.execute(
                "INSERT INTO users(tg_id,created_at,updated_at) VALUES(?,?,?)",
                (222, now, now),
            )
            conn.execute("INSERT INTO wallets(tg_id,balance_toman) VALUES(222,0)")
            conn.execute("INSERT INTO referrals(tg_id) VALUES(222)")
            conn.execute(
                "INSERT INTO accounts(tg_id,service,identifier,data_json,created_at,updated_at) "
                "VALUES(222,'v2ray','restored-user','{}',?,?)",
                (now, now),
            )
            conn.execute(
                "INSERT INTO transactions(order_id,tg_id,service,action,created_at) "
                "VALUES('restored-order',222,'v2ray','buy',?)",
                (now,),
            )
            conn.execute(
                "INSERT OR REPLACE INTO app_settings(key,value_json,updated_at) "
                "VALUES('bot_brand_name','\"Restored Brand\"',?)",
                (now,),
            )
            conn.commit()
        finally:
            conn.close()
        return candidate

    def test_preview_checks_integrity_version_and_counts(self):
        candidate = self._candidate_with_restored_data()
        preview = restore_manager.inspect_database_backup(candidate)
        self.assertEqual(preview["quick_check"], "ok")
        self.assertEqual(preview["schema_version"], storage.SCHEMA_VERSION)
        self.assertEqual(preview["counts"], {
            "users": 1, "accounts": 1, "transactions": 1,
        })
        self.assertTrue(preview["backup_created_at"])
        self.assertEqual(len(preview["sha256"]), 64)

    def test_newer_database_is_rejected_before_restore(self):
        candidate = self._candidate_with_restored_data()
        conn = sqlite3.connect(candidate)
        try:
            conn.execute(
                "UPDATE meta SET value=? WHERE key='schema_version'",
                (str(storage.SCHEMA_VERSION + 1),),
            )
            conn.commit()
        finally:
            conn.close()
        with self.assertRaisesRegex(
            restore_manager.RestoreValidationError, "ابتدا ربات را آپدیت کنید"
        ):
            restore_manager.inspect_database_backup(candidate)

    def test_changed_file_is_rejected_and_current_database_stays_active(self):
        candidate = self._candidate_with_restored_data()
        preview = restore_manager.inspect_database_backup(candidate)
        conn = sqlite3.connect(candidate)
        try:
            conn.execute(
                "UPDATE meta SET value='tampered' WHERE key='backup_kind'"
            )
            conn.commit()
        finally:
            conn.close()
        with self.assertRaisesRegex(
            restore_manager.RestoreValidationError, "تغییر کرده است"
        ):
            restore_manager.restore_database_backup(
                candidate, expected_sha256=preview["sha256"]
            )
        conn = storage._connect()
        try:
            ids = [int(row[0]) for row in conn.execute("SELECT tg_id FROM users")]
        finally:
            conn.close()
        self.assertEqual(ids, [111])

    def test_restore_is_atomic_keeps_safety_copy_and_reloads_without_restart(self):
        candidate = self._candidate_with_restored_data()
        preview = restore_manager.inspect_database_backup(candidate)
        result = restore_manager.restore_database_backup(
            candidate, expected_sha256=preview["sha256"]
        )
        bot._reload_runtime_after_database_restore()

        conn = storage._connect()
        try:
            ids = [int(row[0]) for row in conn.execute("SELECT tg_id FROM users")]
        finally:
            conn.close()
        self.assertEqual(ids, [222])
        self.assertEqual(app_settings.get_setting("bot_brand_name"), "Restored Brand")
        self.assertTrue(os.path.isfile(result["safety_backup_path"]))
        safety = restore_manager.inspect_database_backup(result["safety_backup_path"])
        self.assertEqual(safety["counts"]["users"], 1)

    async def test_forwarded_document_caption_is_ignored_and_only_file_is_used(self):
        candidate = self._candidate_with_restored_data()
        document = SimpleNamespace(
            file_name="vpn_bot_v2.sqlite3",
            file_size=os.path.getsize(candidate),
            file_id="telegram-file-id",
        )
        message = FakeMessage(
            document=document,
            caption="این متن فوروارد شده باید کاملاً نادیده گرفته شود",
        )
        update = SimpleNamespace(
            effective_user=SimpleNamespace(id=911200),
            effective_message=message,
        )
        context = SimpleNamespace(
            user_data={"awaiting": {"kind": "database_restore_upload"}},
            bot=SimpleNamespace(
                get_file=AsyncMock(return_value=FakeTelegramFile(candidate))
            ),
        )

        async def immediate(func, /, *args, _lane=None, **kwargs):
            return func(*args, **kwargs)

        with (
            patch.object(bot, "_is_root_admin", return_value=True),
            patch.object(bot, "run_blocking", side_effect=immediate),
        ):
            await bot.database_restore_document_router(update, context)

        state = context.user_data.get("database_restore_preview") or {}
        self.assertTrue(state)
        self.assertNotIn("caption", state)
        self.assertNotIn(message.caption, str(state))
        self.assertIn("پیش‌نمایش بکاپ", message.replies[-1][0])
        shutil.rmtree(str(state.get("temp_dir") or ""), ignore_errors=True)

    async def test_backup_menu_exposes_restore_action(self):
        message = SimpleNamespace(edit_text=AsyncMock())
        with (
            patch.object(bot, "is_admin", return_value=True),
            patch.object(bot, "run_blocking", new=AsyncMock(return_value={
                "enabled": True,
                "hour": 6,
                "last_backup": {},
                "last_delivery": {},
            })),
        ):
            await bot.show_admin_backup_settings(message, 911200)
        markup = message.edit_text.await_args.kwargs["reply_markup"]
        callbacks = [
            button.callback_data
            for row in markup.inline_keyboard
            for button in row
            if button.callback_data
        ]
        self.assertIn("admin_restore_begin", callbacks)
        self.assertTrue(all(len(value.encode("utf-8")) <= 64 for value in callbacks))

    async def test_final_confirm_restores_immediately_in_same_bot_process(self):
        candidate = self._candidate_with_restored_data()
        preview = restore_manager.inspect_database_backup(candidate)
        token = "abcdef123456"
        user_data = {
            "database_restore_preview": {
                "token": token,
                "temp_dir": os.path.dirname(candidate),
                "path": candidate,
                "filename": "vpn_bot_v2.sqlite3",
                "preview": preview,
                "uploaded_monotonic": bot.time.monotonic(),
            }
        }
        message = SimpleNamespace(edit_text=AsyncMock())
        query = SimpleNamespace(
            data=f"admin_restore_execute|{token}",
            from_user=SimpleNamespace(
                id=911200, username="admin", first_name="Admin", last_name=""
            ),
            message=message,
            answer=AsyncMock(return_value=None),
        )
        update = SimpleNamespace(callback_query=query)
        application = SimpleNamespace(user_data={911200: user_data})
        context = SimpleNamespace(user_data=user_data, application=application)
        with (
            patch.object(bot.CALLBACK_LIMITER, "allow", return_value=(True, "")),
            patch.object(bot, "schedule_telegram_profile", lambda *_a, **_k: None),
            patch.object(bot, "_is_root_admin", return_value=True),
            patch.object(bot, "_wait_for_restore_quiescence", new=AsyncMock()),
        ):
            await bot.callback_router(update, context)

        conn = storage._connect()
        try:
            ids = [int(row[0]) for row in conn.execute("SELECT tg_id FROM users")]
        finally:
            conn.close()
        self.assertEqual(ids, [222])
        self.assertEqual(app_settings.get_setting("bot_brand_name"), "Restored Brand")
        self.assertFalse(bot._DATABASE_RESTORE_IN_PROGRESS.is_set())
        final_text = message.edit_text.await_args_list[-1].args[0]
        self.assertIn("نیازی به ری‌استارت", final_text)


def tearDownModule():
    for lane in bot.BLOCKING_LANES.values():
        lane.shutdown()
    shutil.rmtree(TEST_DATA_DIR, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
