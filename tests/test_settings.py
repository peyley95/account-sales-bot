import asyncio
import json
import os
import shutil
import tempfile
import threading
import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch


TEST_DATA_DIR = tempfile.mkdtemp(prefix="vpn-bot-v32-tests-")
os.environ["BOT_TOKEN"] = "123456:V32_TEST_TOKEN"
os.environ["ADMIN_IDS"] = "1001,invalid,1002"
os.environ["DATA_DIR"] = TEST_DATA_DIR
os.environ["BOT_BRAND_NAME"] = "ENV Brand"
os.environ["ACCOUNT_USERNAME_PREFIX"] = "envuser"
os.environ["REFERRAL_CODE_PREFIX"] = "ENV"
os.environ["OPENVPN_CONNECTIONS_URL"] = "https://example.com/openvpn"
os.environ["API_IP"] = "10.0.0.1"
os.environ["API_PORT"] = "8729"
os.environ["API_USER"] = "digi"
os.environ["API_PASS"] = "migration-secret-pass"
os.environ["UM_SCHEME"] = "https"
os.environ["UM_HOST"] = "10.0.0.9"
os.environ["UM_PORT"] = "8443"
os.environ["UM_PATH"] = "/legacy//um/"
os.environ["XUI_API_TOKEN"] = "migration-xui-token"
os.environ["XUI_SCHEME"] = "http"
os.environ["XUI_HOST"] = "10.0.0.4"
os.environ["XUI_PORT"] = "6220"
os.environ["XUI_BASE_PATH"] = "panel"
os.environ["XUI_VERIFY_TLS"] = "true"
os.environ["XUI_INBOUND_REMARKS"] = "inbound1|inbound2|inbound1|"
os.environ["XUI_SUB_PUBLIC_BASE"] = "https://old.example/"
os.environ["ZARINPAL_MERCHANT_ID"] = "old-merchant-id"
os.environ["ZARINPAL_SANDBOX"] = "false"
os.environ.setdefault("PLAN_TEST", "10|30|150000|1M-10G")

import app_settings  # noqa: E402
import bot  # noqa: E402
import config  # noqa: E402
import storage  # noqa: E402
from services import mikrotik, xui, zarinpal  # noqa: E402


class FakeMessage:
    def __init__(self):
        self.edits = []
        self.replies = []

    async def edit_text(self, text, **kwargs):
        self.edits.append((text, kwargs))

    async def reply_text(self, text, **kwargs):
        self.replies.append((text, kwargs))


class SettingsStateCase(unittest.TestCase):
    def setUp(self):
        self._state = storage.get_app_settings_state()
        self._root = app_settings.root_admin_id()

    def tearDown(self):
        state = self._state
        with storage._tx(immediate=True) as conn:
            conn.execute("DELETE FROM app_settings")
            for key, value in state["settings"].items():
                conn.execute(
                    "INSERT INTO app_settings(key,value_json,updated_at) VALUES(?,?,?)",
                    (key, json.dumps(value, ensure_ascii=False), storage.now_iso()),
                )
            conn.execute("DELETE FROM bot_admins")
            for tg_id in state["admins"]:
                conn.execute(
                    "INSERT INTO bot_admins(tg_id,created_at,created_by) VALUES(?,?,0)",
                    (tg_id, storage.now_iso()),
                )
            conn.execute("DELETE FROM reseller_debt_entries")
            conn.execute("DELETE FROM resellers")
            for reseller in state.get("resellers", ()):
                reseller_id, tg_id, name, rate, debt, created_at, updated_at = reseller[:7]
                trial_enabled = bool(reseller[7]) if len(reseller) >= 8 else True
                conn.execute(
                    """INSERT INTO resellers(
                           id,tg_id,name,price_per_gb_toman,debt_toman,trial_enabled,created_by,
                           created_at,updated_at,deleted_at
                       ) VALUES(?,?,?,?,?,?,0,?,?,'')""",
                    (
                        reseller_id, tg_id, name, rate, debt,
                        int(trial_enabled), created_at, updated_at,
                    ),
                )
            conn.execute("DELETE FROM xui_inbounds")
            for order, (inbound_id, remark) in enumerate(state["inbounds"]):
                conn.execute(
                    "INSERT INTO xui_inbounds(id,remark,sort_order,created_at,updated_at) VALUES(?,?,?,?,?)",
                    (inbound_id, remark, order * 10, storage.now_iso(), storage.now_iso()),
                )
        app_settings.APP_SETTINGS.replace(state, root_admin_id=self._root)


class SettingsMigrationTests(SettingsStateCase):
    def test_legacy_empty_urls_migrate_to_disabled_zero(self):
        seed = app_settings.build_migration_seed({
            "OPENVPN_CONNECTIONS_URL": "",
            "XUI_SUB_PUBLIC_BASE": "   ",
        })
        self.assertEqual(seed["openvpn_connections_url"], "0")
        self.assertEqual(seed["xui_sub_public_base"], "0")

    def test_fresh_defaults_match_release_contract(self):
        expected = {
            "bot_brand_name": "Account Sales Bot",
            "account_username_prefix": "accountbot",
            "referral_code_prefix": "ASB",
            "openvpn_connections_url": "0",
            "api_ip": "127.0.0.1",
            "api_port": 8728,
            "um_scheme": "http",
            "um_path": "um",
            "xui_scheme": "https",
            "xui_host": "127.0.0.1",
            "xui_port": 2053,
            "xui_base_path": "/admin/",
            "xui_verify_tls": False,
            "xui_sub_public_base": "0",
            "zarinpal_sandbox": False,
            "zarinpal_merchant_id": "xxxx-xxx-xxx-xxx-xxxx",
        }
        for key, value in expected.items():
            self.assertEqual(app_settings.DEFAULTS[key], value, key)

    def test_first_env_migration_and_marker(self):
        snap = app_settings.settings_snapshot()
        self.assertEqual(snap["migration_version"], "3.2.1")
        self.assertEqual(storage.app_settings_migration_version(), "3.2.1")
        self.assertEqual(snap["bot_brand_name"], "ENV Brand")
        self.assertEqual(snap["account_username_prefix"], "envuser")
        self.assertEqual(snap["referral_code_prefix"], "ENV")
        self.assertEqual(snap["api_ip"], "10.0.0.1")
        self.assertEqual(snap["api_port"], 8729)
        self.assertEqual(snap["api_user"], "digi")
        self.assertEqual(snap["um_host_legacy"], "10.0.0.9")
        self.assertEqual(snap["um_port_legacy"], "8443")
        self.assertEqual(snap["um_path"], "legacy/um")
        self.assertEqual(snap["xui_base_path"], "/panel/")
        self.assertEqual(snap["xui_inbound_remarks"], ("inbound1", "inbound2"))

    def test_second_restart_never_overwrites_database_with_stale_env(self):
        app_settings.update_setting("bot_brand_name", "Admin Brand", admin_tg_id=1001)
        app_settings.update_setting("api_port", 9443, admin_tg_id=1001)
        stale = {
            "BOT_BRAND_NAME": "STALE ENV",
            "API_PORT": "not-a-port",
            "API_IP": "invalid host/that-would-fail",
            "API_USER": "stale",
            "XUI_INBOUND_REMARKS": "stale-inbound",
        }
        restarted = app_settings.initialize_runtime_settings(
            source=stale,
            root_admin_id=1001,
            env_admin_ids=(1001, 9090),
            inbound_remarks=("stale-inbound",),
        )
        self.assertEqual(restarted["bot_brand_name"], "Admin Brand")
        self.assertEqual(restarted["api_port"], 9443)
        self.assertNotIn(9090, restarted["dynamic_admin_ids"])
        self.assertNotIn("stale-inbound", restarted["xui_inbound_remarks"])

    def test_existing_v320_marker_remains_authoritative(self):
        original_marker = storage.app_settings_migration_version()
        try:
            app_settings.update_setting("bot_brand_name", "Kept from 3.2.0", admin_tg_id=1001)
            with storage._tx(immediate=True) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)",
                    (storage.APP_SETTINGS_MIGRATION_KEY, "3.2.0"),
                )
            restarted = app_settings.initialize_runtime_settings(
                source={"BOT_BRAND_NAME": "stale v3.1 ENV"},
                root_admin_id=1001,
                env_admin_ids=(1001,),
            )
            self.assertEqual(restarted["bot_brand_name"], "Kept from 3.2.0")
            self.assertEqual(restarted["migration_version"], "3.2.0")
        finally:
            with storage._tx(immediate=True) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)",
                    (storage.APP_SETTINGS_MIGRATION_KEY, original_marker),
                )
            app_settings.refresh_runtime_settings(root_admin_id=self._root)

    def test_bot_token_is_env_only_and_never_in_sqlite_settings(self):
        self.assertEqual(config.BOT_TOKEN, "123456:V32_TEST_TOKEN")
        self.assertNotIn("bot_token", app_settings.settings_snapshot())
        conn = storage._connect()
        try:
            rows = conn.execute(
                "SELECT key FROM app_settings WHERE LOWER(key) LIKE '%token%'"
            ).fetchall()
        finally:
            conn.close()
        self.assertEqual([row[0] for row in rows], ["xui_api_token"])

    def test_realistic_upgrade_restart_preserves_all_existing_data(self):
        original_db = storage.DB_FILE
        temp_dir = tempfile.mkdtemp(prefix="v31-upgrade-copy-")
        copied_db = os.path.join(temp_dir, "vpn_bot_v2.sqlite3")
        try:
            storage.DB_FILE = copied_db
            storage.initialize_storage()
            storage.upsert_account(7001, "openvpn", "existing-user", password="123456")
            storage.record_purchase(
                7001, "existing-order", service="openvpn", plan_key="TEST",
                base_price_toman=150000,
            )
            storage.admin_adjust_wallet(7001, 50000, admin_tg_id=1001, operation_id="seed")
            storage.add_pending("A" * 36, {
                "tg_id": 7001, "ts": 1, "order_id": "pending-order",
                "service": "v2ray", "action": "buy", "plan_key": "TEST",
            })
            storage.create_sale_plan(
                plan_key="UPGRADE", gb=12, months=1, price_toman=123000,
                openvpn_profile="EXACT-UPGRADE", admin_tg_id=1001,
            )
            # Turn the populated copy into a realistic pre-v3.2 database: all
            # business tables/data remain, while the three new v3.2 tables and
            # their migration marker do not exist yet.
            with storage._tx(immediate=True) as conn:
                conn.execute("DROP TABLE app_settings")
                conn.execute("DROP TABLE bot_admins")
                conn.execute("DROP TABLE xui_inbounds")
                conn.execute("DELETE FROM meta WHERE key LIKE 'app_settings_v32_%'")
                conn.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('schema_version','21')")
            storage.initialize_storage()
            before = storage.database_stats(check_integrity=True)["counts"]
            first = app_settings.initialize_runtime_settings(
                source={
                    "BOT_BRAND_NAME": "First Seed", "API_IP": "10.1.1.1",
                    "API_USER": "upgrade", "XUI_INBOUND_REMARKS": "a|b",
                },
                root_admin_id=1001, env_admin_ids=(1001, 1002),
                inbound_remarks=("a", "b"),
            )
            self.assertEqual(first["bot_brand_name"], "First Seed")
            app_settings.update_setting("bot_brand_name", "DB Wins", admin_tg_id=1001)
            app_settings.update_setting("xui_port", 2443, admin_tg_id=1001)
            second = app_settings.initialize_runtime_settings(
                source={
                    "BOT_BRAND_NAME": "Old ENV", "API_IP": "203.0.113.8",
                    "API_USER": "old", "XUI_PORT": "9999",
                },
                root_admin_id=1001, env_admin_ids=(1001,), inbound_remarks=("old",),
            )
            after = storage.database_stats(check_integrity=True)["counts"]
            self.assertEqual(second["bot_brand_name"], "DB Wins")
            self.assertEqual(second["xui_port"], 2443)
            self.assertTrue(app_settings.is_admin(1001))
            for table, count in before.items():
                if table not in {"admin_audit", "resellers", "reseller_debt_entries"}:
                    self.assertEqual(count, after[table], table)
            self.assertEqual(after["admin_audit"], before["admin_audit"] + 2)
            self.assertEqual(storage.wallet_balance(7001), 50000)
            self.assertEqual(storage.get_pending("A" * 36)["order_id"], "pending-order")
            self.assertEqual(storage.list_accounts(7001, "openvpn")[0]["identifier"], "existing-user")
            self.assertIsNotNone(storage.get_sale_plan("UPGRADE"))
        finally:
            storage.DB_FILE = original_db
            app_settings.refresh_runtime_settings(root_admin_id=self._root)
            shutil.rmtree(temp_dir, ignore_errors=True)


class RootAdminAndResellerMigrationTests(SettingsStateCase):
    def test_numeric_admin_parser_rejects_internal_spaces_and_overflow(self):
        parsed = app_settings.parse_admin_ids(
            "bad,\t123,12 34,456,999999999999999999999999999"
        )
        self.assertEqual(parsed, (123, 456))

    def test_root_and_migrated_admin_authorization(self):
        self.assertEqual(app_settings.root_admin_id(), 1001)
        self.assertTrue(app_settings.is_admin(1001))
        self.assertFalse(app_settings.is_admin(1002))
        self.assertTrue(app_settings.is_reseller(1002))
        self.assertEqual(app_settings.reseller_record(1002)["price_per_gb_toman"], 0)
        with self.assertRaises(ValueError):
            app_settings.add_reseller(
                name="Root", tg_id=1001, price_per_gb_toman=1000,
                admin_tg_id=1001,
            )
        self.assertTrue(app_settings.is_admin(1001))

    def test_dynamic_admin_add_remove_is_immediate(self):
        app_settings.add_reseller(
            name="Shop", tg_id=2002, price_per_gb_toman=5000,
            admin_tg_id=1001,
        )
        self.assertFalse(bot.is_admin(2002))
        self.assertTrue(app_settings.is_reseller(2002))
        reseller_id = app_settings.reseller_record(2002)["id"]
        app_settings.remove_reseller(reseller_id, admin_tg_id=1001)
        self.assertFalse(app_settings.is_reseller(2002))
        with self.assertRaises(ValueError):
            app_settings.add_reseller(
                name="Overflow", tg_id=10**30, price_per_gb_toman=1,
                admin_tg_id=1001,
            )

    def test_admin_keyboard_lists_ids_individually_and_callbacks_are_short(self):
        markup = asyncio.run(self._render_admins())
        callbacks = [b.callback_data for row in markup.inline_keyboard for b in row if b.callback_data]
        self.assertTrue(any(c.startswith("rs|") for c in callbacks))
        self.assertTrue(all(len(c.encode("utf-8")) <= 64 for c in callbacks))

    def test_users_menu_has_no_redundant_wallet_management_button(self):
        menu_callbacks = [
            button.callback_data
            for row in bot.admin_users_menu_keyboard().inline_keyboard
            for button in row
            if button.callback_data
        ]
        self.assertNotIn("admin_wallet_manage", menu_callbacks)
        detail_callbacks = [
            button.callback_data
            for row in bot.admin_user_detail_keyboard({"tg_id": 4321}).inline_keyboard
            for button in row
            if button.callback_data
        ]
        self.assertIn("admin_wallet_user|4321", detail_callbacks)

    def test_admin_summaries_hide_bootstrap_values_and_xui_token(self):
        rendered = asyncio.run(self._render_setting_groups())
        combined_text = "\n".join(item[0] for item in rendered)
        self.assertNotIn(config.BOT_TOKEN, combined_text)
        self.assertNotIn(config.DATA_DIR, combined_text)
        self.assertNotIn("Asia/Tehran", combined_text)
        self.assertNotIn("migration-xui-token", combined_text)
        callbacks = [
            button.callback_data
            for _, kwargs in rendered
            for row in kwargs["reply_markup"].inline_keyboard
            for button in row
            if button.callback_data
        ]
        self.assertTrue(all(len(value.encode("utf-8")) <= 64 for value in callbacks))

    async def _render_admins(self):
        message = FakeMessage()
        await bot.show_admin_resellers(message, 1001)
        return message.edits[-1][1]["reply_markup"]

    async def _render_setting_groups(self):
        message = FakeMessage()
        await bot.show_admin_bot_settings(message, 1001)
        await bot.show_admin_mikrotik_settings(message, 1001)
        await bot.show_admin_xui_settings(message, 1001)
        await bot.show_admin_zarinpal_settings(message, 1001)
        return message.edits


class DynamicBusinessSettingsTests(SettingsStateCase):
    def test_invalid_admin_value_never_replaces_working_snapshot_or_db(self):
        before = app_settings.settings_snapshot()
        stored_before = storage.get_app_settings_state()["settings"]
        with self.assertRaises(ValueError):
            app_settings.update_setting("api_port", 65536, admin_tg_id=1001)
        with self.assertRaises(ValueError):
            app_settings.update_setting("xui_verify_tls", "maybe", admin_tg_id=1001)
        self.assertIs(app_settings.settings_snapshot(), before)
        self.assertEqual(storage.get_app_settings_state()["settings"], stored_before)

    def test_setting_change_is_immediate_without_restart(self):
        old_snapshot = app_settings.settings_snapshot()
        app_settings.update_setting("bot_brand_name", "Immediate Brand", admin_tg_id=1001)
        self.assertIn("Immediate Brand", bot.welcome_text())
        self.assertEqual(old_snapshot["bot_brand_name"], "ENV Brand")

    def test_username_prefix_changes_only_future_generated_names(self):
        app_settings.update_setting("account_username_prefix", "before", admin_tg_id=1001)
        old_name = bot.generate_username()
        storage.upsert_account(3001, "openvpn", old_name, username=old_name)
        app_settings.update_setting("account_username_prefix", "after", admin_tg_id=1001)
        new_name = bot.generate_username()
        self.assertTrue(old_name.startswith("before"))
        self.assertTrue(new_name.startswith("after"))
        self.assertEqual(storage.list_accounts(3001, "openvpn")[0]["identifier"], old_name)

    def test_referral_prefix_changes_only_new_codes_and_old_code_resolves(self):
        storage.record_purchase(3101, "ref-old-purchase", service="openvpn", plan_key="TEST", base_price_toman=1)
        app_settings.update_setting("referral_code_prefix", "OLD", admin_tg_id=1001)
        old_code = storage.get_or_create_referral_code(3101)
        app_settings.update_setting("referral_code_prefix", "NEW", admin_tg_id=1001)
        storage.record_purchase(3102, "ref-new-purchase", service="v2ray", plan_key="TEST", base_price_toman=1)
        new_code = storage.get_or_create_referral_code(3102)
        self.assertTrue(old_code.startswith("OLD"))
        self.assertTrue(new_code.startswith("NEW"))
        self.assertEqual(storage.find_referrer_by_code(old_code), 3101)

    def test_openvpn_zero_hides_button(self):
        app_settings.update_setting("openvpn_connections_url", "0", admin_tg_id=1001)
        labels = [b.text for row in bot.service_menu_keyboard("openvpn").inline_keyboard for b in row]
        self.assertFalse(any("کانکشن‌های OpenVPN" in label for label in labels))

    def test_stale_openvpn_callback_is_safe_when_disabled(self):
        app_settings.update_setting("openvpn_connections_url", "0", admin_tg_id=1001)
        message = FakeMessage()
        query = SimpleNamespace(
            data="openvpn_connections", from_user=SimpleNamespace(id=1001),
            message=message, answer=AsyncMock(return_value=None),
        )
        update = SimpleNamespace(callback_query=query)
        context = SimpleNamespace(user_data={})
        with (
            patch.object(bot.CALLBACK_LIMITER, "allow", return_value=(True, "")),
            patch.object(bot, "schedule_telegram_profile", lambda *_a, **_k: None),
        ):
            asyncio.run(bot.callback_router(update, context))
        self.assertIn("غیرفعال", message.edits[-1][0])
        markup = message.edits[-1][1]["reply_markup"]
        self.assertFalse(any(getattr(b, "url", None) == "0" for row in markup.inline_keyboard for b in row))


class MikroTikSettingsTests(SettingsStateCase):
    def test_username_is_stored_exactly_without_automatic_suffix(self):
        username = "myuser_RouterOS_API"
        normalized_once = app_settings.normalize_setting("api_user", username)
        normalized_twice = app_settings.normalize_setting("api_user", normalized_once)
        self.assertEqual(normalized_once, username)
        self.assertEqual(normalized_twice, username)
        app_settings.update_setting("api_user", normalized_twice, admin_tg_id=1001)
        self.assertEqual(app_settings.get_setting("api_user"), username)
        message = FakeMessage()
        asyncio.run(bot.show_admin_mikrotik_connection(message, 1001))
        self.assertIn(f"<code>{username}</code>", message.edits[-1][0])

        restarted = app_settings.initialize_runtime_settings(
            source={"API_USER": "stale-env-user"},
            root_admin_id=1001,
            env_admin_ids=(1001,),
        )
        self.assertEqual(restarted["api_user"], username)

    def test_bare_username_remains_bare_and_service_uses_exact_value(self):
        username = "mybase"
        app_settings.update_setting("api_user", username, admin_tg_id=1001)
        pool = MagicMock()
        pool.get_api.return_value = MagicMock()
        with patch.object(mikrotik.routeros_api, "RouterOsApiPool", return_value=pool) as factory:
            mikrotik.connect_mikrotik()
        self.assertEqual(factory.call_args.kwargs["username"], username)

    def test_password_visible_to_admin_but_absent_from_audit_payloads(self):
        secret = "visible-only-to-admin"
        app_settings.update_setting("api_pass", secret, admin_tg_id=1001)
        message = FakeMessage()
        asyncio.run(bot.show_admin_mikrotik_connection(message, 1001))
        self.assertIn(secret, message.edits[-1][0])
        rows, _ = storage.list_admin_audit(offset=0, limit=100)
        row = next(r for r in rows if r["action"] == "API_PASS updated")
        serialized = json.dumps(row, ensure_ascii=False)
        self.assertNotIn(secret, serialized)

    def test_password_cannot_escape_through_connection_exception(self):
        secret = "never-log-this-router-password"
        app_settings.update_setting("api_pass", secret, admin_tg_id=1001)
        with patch.object(
            mikrotik.routeros_api,
            "RouterOsApiPool",
            side_effect=RuntimeError(f"bad credentials: {secret}"),
        ):
            with self.assertRaises(RuntimeError) as raised:
                mikrotik.connect_mikrotik()
        self.assertNotIn(secret, str(raised.exception))

    def test_um_choices_and_path_normalization(self):
        app_settings.update_setting("um_scheme", "HTTPS", admin_tg_id=1001)
        app_settings.update_setting("um_path", "//new//um///", admin_tg_id=1001)
        snap = app_settings.settings_snapshot()
        self.assertEqual(snap["um_scheme"], "https")
        self.assertEqual(snap["um_path"], "new/um")
        self.assertNotIn("//", mikrotik._build_um_base(snap).split("://", 1)[1])

    def test_legacy_um_host_and_port_are_preserved_internally(self):
        snap = app_settings.settings_snapshot()
        self.assertEqual(snap["um_host_legacy"], "10.0.0.9")
        self.assertEqual(snap["um_port_legacy"], "8443")
        self.assertTrue(mikrotik._build_um_base(snap).startswith("https://10.0.0.9:8443/"))


class XUISettingsTests(SettingsStateCase):
    def test_scheme_path_tls_and_port_normalization(self):
        app_settings.update_setting("xui_scheme", "HTTP", admin_tg_id=1001)
        app_settings.update_setting("xui_base_path", "admin", admin_tg_id=1001)
        app_settings.update_setting("xui_verify_tls", "Disabled", admin_tg_id=1001)
        app_settings.update_setting("xui_port", 2053, admin_tg_id=1001)
        snap = app_settings.settings_snapshot()
        self.assertEqual(snap["xui_scheme"], "http")
        self.assertEqual(snap["xui_base_path"], "/admin/")
        self.assertFalse(snap["xui_verify_tls"])
        client = xui.XUIClient()
        self.assertEqual(client.base, "http://10.0.0.4:2053/admin")
        self.assertNotIn("//admin", client.base)

    def test_inbound_migration_and_crud_are_individual_and_immediate(self):
        records = app_settings.inbound_records()
        self.assertEqual([r[1] for r in records], ["inbound1", "inbound2"])
        app_settings.add_inbound("inbound3", admin_tg_id=1001)
        added = next(r for r in app_settings.inbound_records() if r[1] == "inbound3")
        self.assertIn("inbound3", xui.XUIClient().inbound_remarks)
        app_settings.rename_inbound(added[0], "renamed", admin_tg_id=1001)
        self.assertIn("renamed", xui.XUIClient().inbound_remarks)
        self.assertNotIn("inbound3", xui.XUIClient().inbound_remarks)
        app_settings.delete_inbound(added[0], admin_tg_id=1001)
        self.assertNotIn("renamed", xui.XUIClient().inbound_remarks)

    def test_duplicate_and_empty_inbounds_are_rejected(self):
        with self.assertRaises(ValueError):
            app_settings.add_inbound("", admin_tg_id=1001)
        with self.assertRaises(ValueError):
            app_settings.add_inbound("INBOUND1", admin_tg_id=1001)

    def test_new_provisioning_resolves_current_inbound_list(self):
        app_settings.add_inbound("live-new", admin_tg_id=1001)
        client = xui.XUIClient()
        options = [
            {"id": index + 1, "remark": remark, "protocol": "vless"}
            for index, remark in enumerate(client.inbound_remarks)
        ]
        with patch.object(client, "get", return_value={"obj": options}):
            ids = client.inbound_ids()
        self.assertEqual(len(ids), len(client.inbound_remarks))
        self.assertEqual(options[-1]["remark"], "live-new")

    def test_subscription_zero_returns_original_panel_url(self):
        app_settings.update_setting("xui_sub_public_base", "0", admin_tg_id=1001)
        client = xui.XUIClient()
        with patch.object(client, "_sub_uri_from_panel", return_value="https://panel.example/sub/"):
            self.assertEqual(client.subscription_url("abc"), "https://panel.example/sub/abc")
        with patch.object(client, "_sub_uri_from_panel", return_value="/sub/"):
            fallback_url = client.subscription_url("abc")
            self.assertEqual(fallback_url, f"{client.origin}/sub/abc")
            self.assertNotIn("/0/", fallback_url)

    def test_subscription_base_change_affects_only_new_accounts(self):
        app_settings.update_setting("xui_sub_public_base", "https://old-public.example/", admin_tg_id=1001)
        old_client = xui.XUIClient()
        with patch.object(old_client, "_sub_uri_from_panel", return_value="https://panel/sub/"):
            old_url = old_client.subscription_url("oldsub")
        storage.upsert_account(
            4201, "v2ray", "existing-v2", sub_id="oldsub",
            sub_url=old_url, links=["vless://old"],
        )
        app_settings.update_setting("xui_sub_public_base", "https://new-public.example/", admin_tg_id=1001)
        new_client = xui.XUIClient()
        with patch.object(new_client, "_sub_uri_from_panel", return_value="https://panel/sub/"):
            new_url = new_client.subscription_url("newsub")
        self.assertEqual(old_url, "https://old-public.example/sub/oldsub")
        self.assertEqual(new_url, "https://new-public.example/sub/newsub")
        message = FakeMessage()
        asyncio.run(bot.show_my_account_detail(message, 4201, "v2ray", "existing-v2"))
        self.assertIn(old_url, message.edits[-1][0])
        self.assertNotIn("new-public.example", message.edits[-1][0])
        self.assertEqual(storage.list_accounts(4201, "v2ray")[0]["sub_url"], old_url)

    def test_renewal_preserves_saved_subscription_without_recalculation(self):
        saved_url = "https://byte.example/exact?x=1"
        storage.upsert_account(4202, "v2ray", "renew-existing", sub_id="sid", sub_url=saved_url, links=["vless://old"])

        class FakeXUI:
            def renew(self, *_args, **_kwargs):
                return None

            def get_client(self, _email):
                return {"client": {"email": "renew-existing", "subId": "sid"}}

            def links(self, _email):
                return ["vless://fresh"]

            def subscription_url(self, _sub_id):
                raise AssertionError("existing subscription URL must not be recalculated")

        with (
            patch.object(bot, "XUIClient", return_value=FakeXUI()),
            patch.object(bot, "notify_admins", new=AsyncMock()),
        ):
            asyncio.run(bot.fulfill("v2ray", "renew", "TEST", 4202, "renew-existing", SimpleNamespace()))
        self.assertEqual(storage.list_accounts(4202, "v2ray")[0]["sub_url"], saved_url)


class ZarinPalDynamicSettingsTests(SettingsStateCase):
    def test_merchant_and_sandbox_switch_take_effect_without_restart(self):
        captured = []
        authorities = iter(["A" * 36, "B" * 36])

        def fake_post(url, payload, **_kwargs):
            captured.append((url, dict(payload)))
            return {"data": {"code": 100, "authority": next(authorities)}}

        with patch.object(zarinpal, "_post_json", side_effect=fake_post):
            app_settings.update_setting("zarinpal_merchant_id", "merchant-one", admin_tg_id=1001)
            app_settings.update_setting("zarinpal_sandbox", False, admin_tg_id=1001)
            _, auth1 = zarinpal.create_payment(
                tg_id=5001, service="openvpn", action="buy", plan_key="TEST",
                amount_rial=10000, order_id="zp-one",
            )
            app_settings.update_setting("zarinpal_merchant_id", "merchant-two", admin_tg_id=1001)
            app_settings.update_setting("zarinpal_sandbox", True, admin_tg_id=1001)
            _, auth2 = zarinpal.create_payment(
                tg_id=5002, service="v2ray", action="buy", plan_key="TEST",
                amount_rial=20000, order_id="zp-two",
            )
        self.assertEqual(captured[0][1]["merchant_id"], "merchant-one")
        self.assertEqual(captured[1][1]["merchant_id"], "merchant-two")
        self.assertIn("api.zarinpal.com", captured[0][0])
        self.assertIn("sandbox.zarinpal.com", captured[1][0])
        storage.pop_pending(auth1)
        storage.pop_pending(auth2)

    def test_verify_and_cancel_keep_same_proven_verify_flow(self):
        app_settings.update_setting("zarinpal_merchant_id", "merchant-verify", admin_tg_id=1001)
        calls = []
        with patch.object(zarinpal, "_post_json", side_effect=lambda url, payload, **kw: calls.append((url, payload, kw)) or {"errors": {"code": -51}}):
            zarinpal.verify_payment("C" * 36, 10000)
            zarinpal.verify_payment_for_cancel("D" * 36, 20000)
        self.assertEqual(len(calls), 2)
        self.assertTrue(all("verify.json" in call[0] for call in calls))
        self.assertTrue(all(call[2].get("accept_client_error") for call in calls))

    def test_zarinpal_connection_test_creates_no_pending_or_payment(self):
        app_settings.update_setting("zarinpal_merchant_id", "merchant-health", admin_tg_id=1001)
        before = storage.database_stats(check_integrity=False)["counts"]

        class Response:
            status_code = 200
            content = b""

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

        class Session:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def get(self, *_args, **_kwargs):
                return Response()

            def post(self, *_args, **_kwargs):
                raise AssertionError("health test must not POST")

        with patch.object(zarinpal.requests, "Session", return_value=Session()):
            result = zarinpal.test_connection()
        after = storage.database_stats(check_integrity=False)["counts"]
        self.assertTrue(result["reachable"])
        self.assertEqual(before["pending_payments"], after["pending_payments"])
        self.assertEqual(before["transactions"], after["transactions"])


class RuntimeAndConnectionTests(SettingsStateCase):
    def test_hot_path_snapshot_reads_do_not_open_sqlite(self):
        with patch.object(storage, "_connect", side_effect=AssertionError("hot path DB read")):
            self.assertEqual(app_settings.get_setting("bot_brand_name"), "ENV Brand")
            self.assertTrue(bot.generate_username().startswith("envuser"))
            bot.service_menu_keyboard("openvpn")
            xui.XUIClient()

    def test_routine_callback_does_not_read_maintenance_from_sqlite(self):
        async def exercise():
            async def one(tg_id):
                message = FakeMessage()
                query = SimpleNamespace(
                    data="menu|services",
                    from_user=SimpleNamespace(id=tg_id),
                    message=message,
                    answer=AsyncMock(return_value=None),
                )
                update = SimpleNamespace(callback_query=query)
                context = SimpleNamespace(user_data={})
                await bot.callback_router(update, context)
                return message

            return await asyncio.gather(*(one(tg_id) for tg_id in range(8100, 8108)))

        with (
            patch.object(bot.CALLBACK_LIMITER, "allow", return_value=(True, "")),
            patch.object(bot, "schedule_telegram_profile", lambda *_a, **_k: None),
            patch.object(
                bot, "maintenance_mode",
                side_effect=AssertionError("per-click maintenance SQLite read"),
            ),
        ):
            messages = asyncio.run(exercise())
        self.assertTrue(all(message.edits for message in messages))

    def test_callback_preamble_exception_is_contained(self):
        message = FakeMessage()
        query = SimpleNamespace(
            data="menu|services", from_user=SimpleNamespace(id=8200),
            message=message, answer=AsyncMock(return_value=None),
        )
        update = SimpleNamespace(callback_query=query)
        context = SimpleNamespace(user_data={})
        with (
            patch.object(bot.CALLBACK_LIMITER, "allow", side_effect=RuntimeError("boom")),
            patch.object(bot, "callback_failure_reply", new=AsyncMock()) as failure,
        ):
            asyncio.run(bot.callback_router(update, context))
        failure.assert_awaited_once_with(query, 8200)

    def test_service_lanes_cover_normal_multi_user_concurrency(self):
        required = config.BOT_CONCURRENT_UPDATES + 2
        for name in ("mikrotik", "xui", "zarinpal"):
            self.assertGreaterEqual(bot.BLOCKING_LANES[name].capacity, required, name)

    def test_maintenance_runtime_flag_changes_only_after_commit(self):
        before = bot.current_maintenance_mode()
        try:
            _old, committed = storage.set_maintenance_mode(not before, admin_tg_id=1001)
            bot._apply_maintenance_mode(committed)
            self.assertEqual(bot.current_maintenance_mode(), (not before))
            with patch.object(
                storage, "_connect", side_effect=AssertionError("runtime flag DB read")
            ):
                self.assertEqual(bot.current_maintenance_mode(), (not before))
        finally:
            storage.set_maintenance_mode(before, admin_tg_id=1001)
            bot._apply_maintenance_mode(before)

    def test_concurrent_refresh_never_exposes_partial_snapshot(self):
        base_state = storage.get_app_settings_state()
        state_a = {**base_state, "settings": {**base_state["settings"], "bot_brand_name": "A", "account_username_prefix": "AA"}}
        state_b = {**base_state, "settings": {**base_state["settings"], "bot_brand_name": "B", "account_username_prefix": "BB"}}
        failures = []
        stop = threading.Event()

        def writer():
            for index in range(3000):
                app_settings.APP_SETTINGS.replace(state_a if index % 2 else state_b, root_admin_id=1001)
            stop.set()

        def reader():
            while not stop.is_set():
                snap = app_settings.settings_snapshot()
                pair = (snap["bot_brand_name"], snap["account_username_prefix"])
                if pair not in {("A", "AA"), ("B", "BB"), ("ENV Brand", "envuser")}:
                    failures.append(pair)

        threads = [threading.Thread(target=reader) for _ in range(4)]
        for thread in threads:
            thread.start()
        writer_thread = threading.Thread(target=writer)
        writer_thread.start()
        writer_thread.join()
        for thread in threads:
            thread.join()
        self.assertEqual(failures, [])

    def test_concurrent_commits_leave_snapshot_equal_to_sqlite(self):
        def writer(prefix):
            for index in range(20):
                app_settings.update_setting(
                    "bot_brand_name", f"{prefix}-{index}", admin_tg_id=1001
                )

        threads = [threading.Thread(target=writer, args=(prefix,)) for prefix in ("A", "B", "C")]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        stored = storage.get_app_settings_state()["settings"]["bot_brand_name"]
        self.assertEqual(app_settings.get_setting("bot_brand_name"), stored)

    def test_connection_operations_are_non_destructive(self):
        before = storage.database_stats(check_integrity=False)["counts"]

        class ApiResource:
            def get(self):
                return [{"uptime": "1d"}]

        class Api:
            def get_resource(self, _path):
                return ApiResource()

        class Pool:
            def disconnect(self):
                pass

        with (
            patch.object(mikrotik, "connect_mikrotik", return_value=(Pool(), Api())),
            patch.object(
                mikrotik.requests, "Session",
                side_effect=AssertionError("MikroTik test must not probe User Manager"),
            ),
        ):
            mt = mikrotik.test_connection()
        client = xui.XUIClient()
        with (
            patch.object(client, "healthcheck", return_value={"obj": []}),
            patch.object(client, "inbound_ids", return_value=[]),
        ):
            xu = client.test_connection()
        after = storage.database_stats(check_integrity=False)["counts"]
        self.assertTrue(mt["routeros_ok"])
        self.assertNotIn("user_manager_ok", mt)
        self.assertNotIn("user_manager_detail", mt)
        self.assertTrue(xu["connectivity_ok"])
        self.assertEqual(before["pending_payments"], after["pending_payments"])
        self.assertEqual(before["accounts"], after["accounts"])

    def test_mikrotik_admin_test_displays_only_routeros_result(self):
        message = FakeMessage()
        query = SimpleNamespace(
            data="admin_cfg_test|mikrotik",
            from_user=SimpleNamespace(id=1001),
            message=message,
            answer=AsyncMock(return_value=None),
        )
        update = SimpleNamespace(callback_query=query)
        context = SimpleNamespace(user_data={})
        result = {
            "routeros_ok": True,
            "routeros_detail": "RouterOS API connectivity/authentication succeeded",
        }
        with (
            patch.object(bot.CALLBACK_LIMITER, "allow", return_value=(True, "")),
            patch.object(bot, "schedule_telegram_profile", lambda *_a, **_k: None),
            patch.object(bot, "run_blocking", new=AsyncMock(return_value=result)),
        ):
            asyncio.run(bot.callback_router(update, context))
        rendered = message.edits[-1][0]
        self.assertIn("RouterOS API: ✅", rendered)
        self.assertIn("connectivity/authentication succeeded", rendered)
        self.assertNotIn("User Manager", rendered)

    def test_tehran_timezone_is_fixed_and_backup_defaults_to_six(self):
        self.assertEqual(app_settings.APP_TIMEZONE, "Asia/Tehran")
        self.assertEqual(app_settings.APP_BACKUP_HOUR, 6)
        tz = bot._backup_timezone()
        before = datetime(2026, 8, 12, 5, 59, tzinfo=tz)
        self.assertAlmostEqual(bot._seconds_until_next_backup(before, hour=6), 60.0, delta=0.1)
        self.assertEqual(bot._format_tx_time("2026-08-12T00:00:00+00:00"), "2026-08-12 03:30")


def tearDownModule():
    for lane in bot.BLOCKING_LANES.values():
        lane.shutdown()
    shutil.rmtree(TEST_DATA_DIR, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
