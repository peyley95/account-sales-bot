import json
import os
import secrets
import shutil
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from config import APP_VERSION, DATA_DIR, REFERRAL_CODE_PREFIX, AUTO_BACKUP_ENABLED

os.makedirs(DATA_DIR, exist_ok=True)
DB_FILE = os.path.join(DATA_DIR, "vpn_bot_v2.sqlite3")
STORE_FILE = os.path.join(DATA_DIR, "store.json")
PENDING_FILE = os.path.join(DATA_DIR, "pending_payments.json")
TRANSACTIONS_FILE = os.path.join(DATA_DIR, "transactions.json")
WALLET_ADMIN_AUDIT_FILE = os.path.join(DATA_DIR, "wallet_admin_audit.json")
BACKUP_DIR = os.path.join(DATA_DIR, "backups")
_INIT_LOCK = threading.RLock()
_BACKUP_LOCK = threading.Lock()
SCHEMA_VERSION = 28
_REFERRAL_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
MAX_WALLET_BALANCE_TOMAN = 9_000_000_000_000_000
MAX_RESELLER_DEBT_TOMAN = 9_000_000_000_000_000


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_dumps(value) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=False, separators=(",", ":"))


def _json_loads(value, default):
    if value in (None, ""):
        return default
    try:
        parsed = json.loads(value)
        return parsed
    except Exception:
        return default


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE, timeout=10.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA temp_store=MEMORY")
    return conn


@contextmanager
def _tx(immediate: bool = False):
    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _ensure_user(conn: sqlite3.Connection, tg_id: int):
    ts = now_iso()
    conn.execute(
        "INSERT OR IGNORE INTO users(tg_id, created_at, updated_at) VALUES(?,?,?)",
        (int(tg_id), ts, ts),
    )
    conn.execute("INSERT OR IGNORE INTO wallets(tg_id, balance_toman) VALUES(?,0)", (int(tg_id),))
    conn.execute("INSERT OR IGNORE INTO referrals(tg_id) VALUES(?)", (int(tg_id),))


def _schema(conn: sqlite3.Connection):
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS users (
            tg_id INTEGER PRIMARY KEY,
            first_name TEXT NOT NULL DEFAULT '',
            last_name TEXT NOT NULL DEFAULT '',
            username TEXT NOT NULL DEFAULT '',
            language_code TEXT NOT NULL DEFAULT '',
            phone_number TEXT NOT NULL DEFAULT '',
            email TEXT NOT NULL DEFAULT '',
            bot_started_at TEXT NOT NULL DEFAULT '',
            test_openvpn INTEGER NOT NULL DEFAULT 0,
            test_v2ray INTEGER NOT NULL DEFAULT 0,
            legacy_purchase_qualified INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_users_username ON users(username COLLATE NOCASE);
        CREATE INDEX IF NOT EXISTS idx_users_name ON users(first_name COLLATE NOCASE, last_name COLLATE NOCASE);

        CREATE TABLE IF NOT EXISTS accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_id INTEGER NOT NULL REFERENCES users(tg_id) ON DELETE CASCADE,
            service TEXT NOT NULL,
            identifier TEXT NOT NULL,
            data_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(tg_id, service, identifier)
        );
        CREATE INDEX IF NOT EXISTS idx_accounts_owner_service ON accounts(tg_id, service);
        CREATE INDEX IF NOT EXISTS idx_accounts_identifier ON accounts(identifier COLLATE NOCASE);

        CREATE TABLE IF NOT EXISTS account_expiry_notifications (
            account_id INTEGER PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
            warning_cycle_key TEXT NOT NULL DEFAULT '',
            warning_claimed_at TEXT NOT NULL DEFAULT '',
            warning_sent_at TEXT NOT NULL DEFAULT '',
            expired_cycle_key TEXT NOT NULL DEFAULT '',
            expired_claimed_at TEXT NOT NULL DEFAULT '',
            expired_sent_at TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS purchases (
            order_id TEXT PRIMARY KEY,
            tg_id INTEGER NOT NULL REFERENCES users(tg_id) ON DELETE CASCADE,
            service TEXT NOT NULL,
            plan_key TEXT NOT NULL DEFAULT '',
            base_price_toman INTEGER NOT NULL DEFAULT 0,
            referral_code TEXT NOT NULL DEFAULT '',
            referrer_tg_id INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_purchases_user ON purchases(tg_id, created_at DESC);

        CREATE TABLE IF NOT EXISTS referrals (
            tg_id INTEGER PRIMARY KEY REFERENCES users(tg_id) ON DELETE CASCADE,
            code TEXT UNIQUE,
            used_code TEXT NOT NULL DEFAULT '',
            referrer_tg_id INTEGER NOT NULL DEFAULT 0,
            used_at TEXT NOT NULL DEFAULT '',
            used_order_id TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_referrals_code ON referrals(code COLLATE NOCASE);

        CREATE TABLE IF NOT EXISTS wallets (
            tg_id INTEGER PRIMARY KEY REFERENCES users(tg_id) ON DELETE CASCADE,
            balance_toman INTEGER NOT NULL DEFAULT 0 CHECK(balance_toman >= 0)
        );

        CREATE TABLE IF NOT EXISTS wallet_transactions (
            tx_id TEXT PRIMARY KEY,
            tg_id INTEGER NOT NULL REFERENCES users(tg_id) ON DELETE CASCADE,
            kind TEXT NOT NULL,
            delta_toman INTEGER NOT NULL,
            balance_after_toman INTEGER NOT NULL,
            note TEXT NOT NULL DEFAULT '',
            meta_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_wallet_tx_user ON wallet_transactions(tg_id, created_at DESC);

        CREATE TABLE IF NOT EXISTS pending_payments (
            authority TEXT PRIMARY KEY,
            tg_id INTEGER NOT NULL,
            ts INTEGER NOT NULL DEFAULT 0,
            first_purchase INTEGER NOT NULL DEFAULT 0,
            wallet_used_toman INTEGER NOT NULL DEFAULT 0,
            wallet_committed INTEGER NOT NULL DEFAULT 0,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_pending_user_ts ON pending_payments(tg_id, ts DESC);

        CREATE TABLE IF NOT EXISTS card_transfer_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            authority TEXT NOT NULL UNIQUE,
            order_id TEXT NOT NULL UNIQUE,
            tg_id INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'awaiting_receipt'
                CHECK(status IN ('awaiting_receipt','submitted','processing','approved','rejected','cancelled')),
            receipt_kind TEXT NOT NULL DEFAULT '',
            receipt_text TEXT NOT NULL DEFAULT '',
            receipt_file_id TEXT NOT NULL DEFAULT '',
            receipt_file_unique_id TEXT NOT NULL DEFAULT '',
            submitted_at TEXT NOT NULL DEFAULT '',
            decided_at TEXT NOT NULL DEFAULT '',
            decided_by INTEGER NOT NULL DEFAULT 0,
            rejection_reason TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_card_transfer_user_status
            ON card_transfer_requests(tg_id,status,updated_at DESC,id DESC);
        CREATE INDEX IF NOT EXISTS idx_card_transfer_status_time
            ON card_transfer_requests(status,submitted_at DESC,id DESC);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_card_transfer_one_active_user
            ON card_transfer_requests(tg_id)
            WHERE status IN ('awaiting_receipt','submitted','processing');

        CREATE TABLE IF NOT EXISTS transactions (
            order_id TEXT PRIMARY KEY,
            tg_id INTEGER NOT NULL,
            service TEXT NOT NULL,
            action TEXT NOT NULL,
            plan_key TEXT NOT NULL DEFAULT '',
            base_price_toman INTEGER NOT NULL DEFAULT 0,
            referral_discount_toman INTEGER NOT NULL DEFAULT 0,
            wallet_used_toman INTEGER NOT NULL DEFAULT 0,
            gateway_toman INTEGER NOT NULL DEFAULT 0,
            payment_kind TEXT NOT NULL DEFAULT '',
            payment_method TEXT NOT NULL DEFAULT '',
            reseller_id INTEGER NOT NULL DEFAULT 0,
            reseller_charge_toman INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            legacy INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_transactions_time ON transactions(created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_transactions_user_time ON transactions(tg_id, created_at DESC);

        CREATE TABLE IF NOT EXISTS fulfillments (
            order_id TEXT PRIMARY KEY,
            tg_id INTEGER NOT NULL,
            service TEXT NOT NULL,
            action TEXT NOT NULL,
            requested_identifier TEXT NOT NULL DEFAULT '',
            delivery_identifier TEXT NOT NULL DEFAULT '',
            secret_json TEXT NOT NULL DEFAULT '{}',
            state TEXT NOT NULL DEFAULT 'prepared',
            result_json TEXT NOT NULL DEFAULT '{}',
            meta_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_fulfillments_user_state ON fulfillments(tg_id,state,updated_at DESC);

        CREATE TABLE IF NOT EXISTS admin_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_tg_id INTEGER NOT NULL,
            target_tg_id INTEGER NOT NULL DEFAULT 0,
            action TEXT NOT NULL,
            before_json TEXT NOT NULL DEFAULT '{}',
            after_json TEXT NOT NULL DEFAULT '{}',
            meta_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_admin_audit_time ON admin_audit(created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_admin_audit_target ON admin_audit(target_tg_id, created_at DESC);

        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS app_settings (
            key TEXT PRIMARY KEY,
            value_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS bot_admins (
            tg_id INTEGER PRIMARY KEY,
            created_at TEXT NOT NULL,
            created_by INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS resellers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_id INTEGER NOT NULL UNIQUE,
            name TEXT NOT NULL,
            price_per_gb_toman INTEGER NOT NULL DEFAULT 0
                CHECK(price_per_gb_toman >= 0),
            debt_toman INTEGER NOT NULL DEFAULT 0 CHECK(debt_toman >= 0),
            trial_enabled INTEGER NOT NULL DEFAULT 1
                CHECK(trial_enabled IN (0,1)),
            created_by INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            deleted_at TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_resellers_active
            ON resellers(deleted_at,created_at,id);

        CREATE TABLE IF NOT EXISTS reseller_debt_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reseller_id INTEGER NOT NULL REFERENCES resellers(id),
            operation_id TEXT NOT NULL UNIQUE,
            order_id TEXT NOT NULL DEFAULT '',
            kind TEXT NOT NULL,
            delta_toman INTEGER NOT NULL,
            before_toman INTEGER NOT NULL,
            after_toman INTEGER NOT NULL,
            service TEXT NOT NULL DEFAULT '',
            plan_key TEXT NOT NULL DEFAULT '',
            gb INTEGER NOT NULL DEFAULT 0,
            price_per_gb_toman INTEGER NOT NULL DEFAULT 0,
            admin_tg_id INTEGER NOT NULL DEFAULT 0,
            meta_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_reseller_debt_entries_reseller
            ON reseller_debt_entries(reseller_id,id DESC);

        CREATE TABLE IF NOT EXISTS xui_inbounds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            remark TEXT NOT NULL COLLATE NOCASE UNIQUE,
            sort_order INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_xui_inbounds_sort ON xui_inbounds(sort_order,id);

        CREATE TABLE IF NOT EXISTS sale_plans (
            plan_key TEXT PRIMARY KEY,
            gb INTEGER NOT NULL CHECK(gb > 0),
            months INTEGER NOT NULL DEFAULT 0 CHECK(months >= 0),
            days INTEGER NOT NULL CHECK(days > 0),
            price_toman INTEGER NOT NULL CHECK(price_toman > 0),
            openvpn_profile TEXT NOT NULL,
            sort_order INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_sale_plans_sort ON sale_plans(sort_order, days, gb, price_toman);

        CREATE TABLE IF NOT EXISTS service_sale_plans (
            service TEXT NOT NULL CHECK(service IN ('openvpn','v2ray')),
            plan_key TEXT NOT NULL,
            gb INTEGER NOT NULL CHECK(gb > 0),
            months INTEGER NOT NULL DEFAULT 0 CHECK(months >= 0),
            days INTEGER NOT NULL CHECK(days > 0),
            price_toman INTEGER NOT NULL CHECK(price_toman > 0),
            openvpn_profile TEXT NOT NULL DEFAULT '',
            sort_order INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(service, plan_key)
        );
        CREATE INDEX IF NOT EXISTS idx_service_sale_plans_sort
            ON service_sale_plans(service, sort_order, days, gb, price_toman);
        """
    )
    # Existing v3.5 databases already have the transactions table. Additive
    # ALTERs preserve every historical row and can safely run on every start.
    transaction_columns = {
        str(row[1]) for row in conn.execute("PRAGMA table_info(transactions)")
    }
    for name, declaration in (
        ("payment_kind", "TEXT NOT NULL DEFAULT ''"),
        ("payment_method", "TEXT NOT NULL DEFAULT ''"),
        ("reseller_id", "INTEGER NOT NULL DEFAULT 0"),
        ("reseller_charge_toman", "INTEGER NOT NULL DEFAULT 0"),
    ):
        if name not in transaction_columns:
            conn.execute(f"ALTER TABLE transactions ADD COLUMN {name} {declaration}")
    # v3.8 adds one reversible permission to each reseller. Existing v3.6/v3.7
    # records default to enabled, preserving their previous access to Trial.
    reseller_columns = {
        str(row[1]) for row in conn.execute("PRAGMA table_info(resellers)")
    }
    if "trial_enabled" not in reseller_columns:
        conn.execute(
            "ALTER TABLE resellers ADD COLUMN trial_enabled INTEGER NOT NULL DEFAULT 1 "
            "CHECK(trial_enabled IN (0,1))"
        )
    # v1.3 records explicit /start recipients. Existing installations cannot
    # distinguish historical starters from other known bot users, so the
    # one-time additive migration preserves expected broadcast reach by marking
    # every existing user with their original creation time. Users created by
    # admin-only operations after this migration remain excluded until /start.
    user_columns = {
        str(row[1]) for row in conn.execute("PRAGMA table_info(users)")
    }
    if "bot_started_at" not in user_columns:
        conn.execute(
            "ALTER TABLE users ADD COLUMN bot_started_at TEXT NOT NULL DEFAULT ''"
        )
        conn.execute(
            "UPDATE users SET bot_started_at=created_at WHERE bot_started_at=''"
        )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_users_bot_started "
        "ON users(bot_started_at,tg_id)"
    )
    conn.execute(
        "INSERT OR REPLACE INTO meta(key,value) VALUES('schema_version',?)",
        (str(SCHEMA_VERSION),),
    )


def _read_json_file(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _legacy_normalize_record(raw: dict) -> dict:
    source = raw if isinstance(raw, dict) else {}
    accounts = []
    for item in source.get("accounts", []) if isinstance(source.get("accounts"), list) else []:
        if not isinstance(item, dict):
            continue
        item = dict(item)
        item.setdefault("service", "openvpn")
        item.setdefault("identifier", item.get("username") or item.get("email") or "")
        if item.get("identifier"):
            accounts.append(item)
    tests = dict(source.get("tests")) if isinstance(source.get("tests"), dict) else {}
    old_test = source.get("test") if isinstance(source.get("test"), dict) else {}
    tests.setdefault("openvpn", bool(old_test.get("got", False)))
    tests.setdefault("v2ray", False)
    wallet = dict(source.get("wallet")) if isinstance(source.get("wallet"), dict) else {}
    wallet.setdefault("balance_toman", 0)
    wallet.setdefault("transactions", [])
    referral = dict(source.get("referral")) if isinstance(source.get("referral"), dict) else {}
    profile = dict(source.get("profile")) if isinstance(source.get("profile"), dict) else {}
    purchases = [p for p in source.get("purchases", []) if isinstance(p, dict)] if isinstance(source.get("purchases"), list) else []
    return {
        "accounts": accounts,
        "tests": tests,
        "wallet": wallet,
        "referral": referral,
        "profile": profile,
        "purchases": purchases,
        "legacy_purchase_qualified": bool(source.get("legacy_purchase_qualified", False)),
    }


def _copy_legacy_files_once():
    files = [p for p in (STORE_FILE, PENDING_FILE, TRANSACTIONS_FILE, WALLET_ADMIN_AUDIT_FILE) if os.path.isfile(p)]
    if not files:
        return ""
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    dest = os.path.join(DATA_DIR, f"legacy-json-backup-{stamp}")
    os.makedirs(dest, exist_ok=True)
    for src in files:
        try:
            shutil.copy2(src, os.path.join(dest, os.path.basename(src)))
        except Exception:
            pass
    return dest


def _migrate_legacy_json(conn: sqlite3.Connection):
    marker = conn.execute("SELECT value FROM meta WHERE key='legacy_json_migrated'").fetchone()
    if marker and marker[0] == "1":
        return

    backup_dir = _copy_legacy_files_once()
    store = _read_json_file(STORE_FILE)
    pending = _read_json_file(PENDING_FILE)
    global_txs = _read_json_file(TRANSACTIONS_FILE)
    wallet_audit = _read_json_file(WALLET_ADMIN_AUDIT_FILE)
    ts_now = now_iso()

    for raw_uid, raw in store.items():
        try:
            tg_id = int(raw_uid)
        except Exception:
            continue
        rec = _legacy_normalize_record(raw)
        profile = rec["profile"]
        _ensure_user(conn, tg_id)
        legacy_qualified = bool(rec.get("legacy_purchase_qualified"))
        if not legacy_qualified:
            legacy_qualified = any(a.get("is_test") is not True and str(a.get("plan_key") or "").strip() for a in rec["accounts"])
        conn.execute(
            """UPDATE users SET first_name=?,last_name=?,username=?,language_code=?,phone_number=?,email=?,
               test_openvpn=?,test_v2ray=?,legacy_purchase_qualified=?,updated_at=? WHERE tg_id=?""",
            (
                str(profile.get("first_name") or ""), str(profile.get("last_name") or ""),
                str(profile.get("username") or ""), str(profile.get("language_code") or ""),
                str(profile.get("phone_number") or ""), str(profile.get("email") or ""),
                int(bool(rec["tests"].get("openvpn"))), int(bool(rec["tests"].get("v2ray"))),
                int(legacy_qualified), ts_now, tg_id,
            ),
        )
        for a in rec["accounts"]:
            ident = str(a.get("identifier") or "").strip()
            if not ident:
                continue
            created = str(a.get("created_at") or ts_now)
            updated = str(a.get("updated_at") or created)
            conn.execute(
                "INSERT OR REPLACE INTO accounts(tg_id,service,identifier,data_json,created_at,updated_at) VALUES(?,?,?,?,?,?)",
                (tg_id, str(a.get("service") or "openvpn"), ident, _json_dumps(a), created, updated),
            )
        ref = rec["referral"]
        code = str(ref.get("code") or "").strip() or None
        conn.execute(
            """UPDATE referrals SET code=?,used_code=?,referrer_tg_id=?,used_at=?,used_order_id=? WHERE tg_id=?""",
            (code, str(ref.get("used_code") or ""), int(ref.get("referrer_tg_id") or 0),
             str(ref.get("used_at") or ""), str(ref.get("used_order_id") or ""), tg_id),
        )
        balance = max(int(rec["wallet"].get("balance_toman") or 0), 0)
        conn.execute("UPDATE wallets SET balance_toman=? WHERE tg_id=?", (balance, tg_id))
        for wtx in rec["wallet"].get("transactions", []) if isinstance(rec["wallet"].get("transactions"), list) else []:
            if not isinstance(wtx, dict):
                continue
            tx_id = str(wtx.get("tx_id") or "").strip()
            if not tx_id:
                continue
            conn.execute(
                """INSERT OR IGNORE INTO wallet_transactions(tx_id,tg_id,kind,delta_toman,balance_after_toman,note,meta_json,created_at)
                   VALUES(?,?,?,?,?,?,?,?)""",
                (tx_id, tg_id, str(wtx.get("kind") or "legacy"), int(wtx.get("delta_toman") or 0),
                 int(wtx.get("balance_after_toman") or 0), str(wtx.get("note") or ""),
                 _json_dumps(wtx.get("meta") if isinstance(wtx.get("meta"), dict) else {}), str(wtx.get("created_at") or ts_now)),
            )
        for p in rec["purchases"]:
            order_id = str(p.get("order_id") or "").strip()
            if not order_id:
                continue
            created = str(p.get("created_at") or ts_now)
            conn.execute(
                """INSERT OR IGNORE INTO purchases(order_id,tg_id,service,plan_key,base_price_toman,referral_code,referrer_tg_id,created_at)
                   VALUES(?,?,?,?,?,?,?,?)""",
                (order_id, tg_id, str(p.get("service") or ""), str(p.get("plan_key") or ""), int(p.get("base_price_toman") or 0),
                 str(p.get("referral_code") or ""), int(p.get("referrer_tg_id") or 0), created),
            )

    tx_rows = global_txs.get("transactions") if isinstance(global_txs.get("transactions"), list) else []
    for t in tx_rows:
        if not isinstance(t, dict):
            continue
        order_id = str(t.get("order_id") or "").strip()
        if not order_id:
            continue
        tx_tg_id = int(t.get("tg_id") or 0)
        if tx_tg_id:
            _ensure_user(conn, tx_tg_id)
        else:
            continue
        conn.execute(
            """INSERT OR IGNORE INTO transactions(order_id,tg_id,service,action,plan_key,base_price_toman,referral_discount_toman,wallet_used_toman,gateway_toman,created_at,legacy)
               VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            (order_id, tx_tg_id, str(t.get("service") or ""), str(t.get("action") or "buy"),
             str(t.get("plan_key") or ""), int(t.get("base_price_toman") or 0), int(t.get("referral_discount_toman") or 0),
             int(t.get("wallet_used_toman") or 0), int(t.get("gateway_toman") or 0), str(t.get("created_at") or ts_now), int(bool(t.get("legacy")))),
        )

    # Backfill old successful purchases not already in the v1.8+ transaction ledger.
    rows = conn.execute("SELECT * FROM purchases").fetchall()
    for p in rows:
        conn.execute(
            """INSERT OR IGNORE INTO transactions(order_id,tg_id,service,action,plan_key,base_price_toman,referral_discount_toman,wallet_used_toman,gateway_toman,created_at,legacy)
               VALUES(?,?,?,?,?,?,?,?,?,?,1)""",
            (p["order_id"], p["tg_id"], p["service"], "buy", p["plan_key"], p["base_price_toman"], 0, 0,
             p["base_price_toman"], p["created_at"]),
        )

    for authority, p in pending.items():
        if not isinstance(p, dict):
            continue
        pending_tg_id = int(p.get("tg_id") or 0)
        if not pending_tg_id:
            continue
        _ensure_user(conn, pending_tg_id)
        conn.execute(
            """INSERT OR REPLACE INTO pending_payments(authority,tg_id,ts,first_purchase,wallet_used_toman,wallet_committed,payload_json,created_at,updated_at)
               VALUES(?,?,?,?,?,?,?,?,?)""",
            (str(authority), pending_tg_id, int(p.get("ts") or 0), int(bool(p.get("first_purchase"))),
             int(p.get("wallet_used_toman") or 0), int(bool(p.get("wallet_committed"))), _json_dumps(p), ts_now, ts_now),
        )

    audit_rows = wallet_audit.get("entries") if isinstance(wallet_audit.get("entries"), list) else []
    for row in audit_rows:
        if not isinstance(row, dict):
            continue
        before = {"wallet_balance_toman": int(row.get("before_toman") or 0)}
        after = {"wallet_balance_toman": int(row.get("after_toman") or 0)}
        meta = {"amount_toman": int(row.get("amount_toman") or 0), "legacy_wallet_audit": True}
        target_tg_id = int(row.get("user_tg_id") or 0)
        if target_tg_id:
            _ensure_user(conn, target_tg_id)
        conn.execute(
            "INSERT INTO admin_audit(admin_tg_id,target_tg_id,action,before_json,after_json,meta_json,created_at) VALUES(?,?,?,?,?,?,?)",
            (int(row.get("admin_tg_id") or 0), target_tg_id,
             f"wallet_{str(row.get('action') or 'adjust')}", _json_dumps(before), _json_dumps(after), _json_dumps(meta),
             str(row.get("created_at") or ts_now)),
        )

    conn.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('legacy_json_migrated','1')")
    conn.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('legacy_json_backup_dir',?)", (backup_dir,))
    conn.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('legacy_json_migrated_at',?)", (ts_now,))


def initialize_storage():
    with _INIT_LOCK:
        os.makedirs(DATA_DIR, exist_ok=True)
        conn = _connect()
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("BEGIN IMMEDIATE")
            _schema(conn)
            _migrate_legacy_json(conn)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


initialize_storage()


# -------------------- User/profile/account compatibility --------------------


def mark_user_started(tg_id: int) -> str:
    """Durably register one Telegram user as a broadcast recipient."""
    uid = int(tg_id)
    if uid <= 0:
        raise ValueError("Telegram ID نامعتبر است")
    conn = _connect()
    try:
        row = conn.execute(
            "SELECT bot_started_at FROM users WHERE tg_id=?", (uid,)
        ).fetchone()
        existing = str(row[0] or "") if row else ""
        if existing:
            return existing
    finally:
        conn.close()
    with _tx(immediate=True) as conn:
        _ensure_user(conn, uid)
        row = conn.execute(
            "SELECT bot_started_at FROM users WHERE tg_id=?", (uid,)
        ).fetchone()
        existing = str(row[0] or "") if row else ""
        if existing:
            return existing
        started_at = now_iso()
        conn.execute(
            "UPDATE users SET bot_started_at=? WHERE tg_id=?",
            (started_at, uid),
        )
        return started_at


def list_broadcast_recipient_ids(*, exclude_tg_ids=()) -> list[int]:
    excluded = sorted({
        int(value)
        for value in (exclude_tg_ids or ())
        if str(value or "").strip().lstrip("-").isdigit() and int(value) > 0
    })
    where = "WHERE bot_started_at!=''"
    params: tuple = ()
    if excluded:
        placeholders = ",".join("?" for _ in excluded)
        where += f" AND tg_id NOT IN ({placeholders})"
        params = tuple(excluded)
    conn = _connect()
    try:
        return [
            int(row[0])
            for row in conn.execute(
                f"SELECT tg_id FROM users {where} ORDER BY tg_id", params
            )
        ]
    finally:
        conn.close()


def broadcast_recipient_count(*, exclude_tg_ids=()) -> int:
    excluded = sorted({
        int(value)
        for value in (exclude_tg_ids or ())
        if str(value or "").strip().lstrip("-").isdigit() and int(value) > 0
    })
    where = "WHERE bot_started_at!=''"
    params: tuple = ()
    if excluded:
        placeholders = ",".join("?" for _ in excluded)
        where += f" AND tg_id NOT IN ({placeholders})"
        params = tuple(excluded)
    conn = _connect()
    try:
        return int(
            conn.execute(f"SELECT COUNT(*) FROM users {where}", params).fetchone()[0]
        )
    finally:
        conn.close()

def get_user_profile(tg_id: int) -> dict:
    conn = _connect()
    try:
        row = conn.execute("SELECT first_name,last_name,username,language_code,phone_number,email FROM users WHERE tg_id=?", (int(tg_id),)).fetchone()
        if not row:
            return {"first_name": "", "last_name": "", "username": "", "language_code": "", "phone_number": "", "email": ""}
        return dict(row)
    finally:
        conn.close()


def update_user_profile(tg_id: int, **fields):
    """Update Telegram profile without taking a SQLite write lock on every update.

    v2.0-v2.0.1 used BEGIN IMMEDIATE before checking whether anything changed.
    With concurrent Telegram users that turned every button press and /start
    into a writer and could make the asyncio thread wait behind another writer.
    The common no-change path is now a WAL-friendly read only.
    """
    allowed = {"first_name", "last_name", "username", "language_code", "phone_number", "email"}
    updates = {k: str(v) for k, v in fields.items() if k in allowed and v is not None}
    if not updates:
        return False

    uid = int(tg_id)
    conn = _connect()
    try:
        current = conn.execute("SELECT * FROM users WHERE tg_id=?", (uid,)).fetchone()
        if current is not None:
            changed = {k: v for k, v in updates.items() if str(current[k] or "") != v}
            if not changed:
                return False
    finally:
        conn.close()

    # Re-check after obtaining the writer lock because another concurrent update
    # may have written the same profile between the read above and this point.
    with _tx(immediate=True) as conn:
        _ensure_user(conn, uid)
        current = conn.execute("SELECT * FROM users WHERE tg_id=?", (uid,)).fetchone()
        changed = {k: v for k, v in updates.items() if str(current[k] or "") != v}
        if not changed:
            return False
        sets = ",".join(f"{k}=?" for k in changed)
        conn.execute(f"UPDATE users SET {sets},updated_at=? WHERE tg_id=?", (*changed.values(), now_iso(), uid))
        return True


def list_accounts(tg_id: int, service: str) -> list[dict]:
    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT identifier,data_json,created_at,updated_at FROM accounts WHERE tg_id=? AND service=? ORDER BY id",
            (int(tg_id), str(service)),
        ).fetchall()
        result = []
        for r in rows:
            item = _json_loads(r["data_json"], {})
            item = dict(item) if isinstance(item, dict) else {}
            item.update({"service": str(service), "identifier": r["identifier"], "created_at": item.get("created_at") or r["created_at"], "updated_at": r["updated_at"]})
            if isinstance(item.get("links"), list):
                item["links"] = list(item["links"])
            result.append(item)
        return result
    finally:
        conn.close()


def list_accounts_for_expiry_monitor() -> list[dict]:
    """Load monitor candidates in one SQLite read, excluding free Trial rows.

    The background scanner must not reopen SQLite once per account. Returning
    the database row version also lets the notification claim reject a stale
    result if a renewal updates the account during the remote status check.
    """
    conn = _connect()
    try:
        rows = conn.execute(
            """SELECT id,tg_id,service,identifier,data_json,created_at,updated_at
               FROM accounts
               WHERE service IN ('openvpn','v2ray')
               ORDER BY service,id"""
        ).fetchall()
        result = []
        for row in rows:
            data = _json_loads(row["data_json"], {})
            item = dict(data) if isinstance(data, dict) else {}
            if bool(item.get("is_test", False)):
                continue
            item.update({
                "account_id": int(row["id"]),
                "tg_id": int(row["tg_id"]),
                "service": str(row["service"]),
                "identifier": str(row["identifier"]),
                "created_at": str(item.get("created_at") or row["created_at"]),
                "updated_at": str(row["updated_at"]),
            })
            if isinstance(item.get("links"), list):
                item["links"] = list(item["links"])
            result.append(item)
        return result
    finally:
        conn.close()


def claim_account_expiry_notification(
    account_id: int, kind: str, cycle_key: str
) -> bool:
    """Atomically reserve one warning/expiry message for one account cycle.

    Reservation happens before the Telegram request, giving at-most-once
    behavior even if the process is interrupted during a send. A later renewal
    changes accounts.updated_at and starts a fresh notification cycle.
    """
    notification_kind = str(kind or "").strip().lower()
    if notification_kind not in {"warning", "expired"}:
        raise ValueError("Notification kind must be warning or expired")
    expected_cycle = str(cycle_key or "")
    if not expected_cycle:
        raise ValueError("Notification cycle key is required")
    cycle_column = f"{notification_kind}_cycle_key"
    claimed_column = f"{notification_kind}_claimed_at"
    sent_column = f"{notification_kind}_sent_at"
    with _tx(immediate=True) as conn:
        account = conn.execute(
            "SELECT updated_at FROM accounts WHERE id=?", (int(account_id),)
        ).fetchone()
        if not account or str(account["updated_at"]) != expected_cycle:
            return False
        state = conn.execute(
            "SELECT warning_cycle_key,expired_cycle_key FROM account_expiry_notifications WHERE account_id=?",
            (int(account_id),),
        ).fetchone()
        if state and str(state[cycle_column] or "") == expected_cycle:
            return False
        ts = now_iso()
        conn.execute(
            """INSERT OR IGNORE INTO account_expiry_notifications(account_id,updated_at)
               VALUES(?,?)""",
            (int(account_id), ts),
        )
        conn.execute(
            f"""UPDATE account_expiry_notifications
                SET {cycle_column}=?,{claimed_column}=?,{sent_column}='',updated_at=?
                WHERE account_id=?""",
            (expected_cycle, ts, ts, int(account_id)),
        )
        return True


def mark_account_expiry_notification_sent(
    account_id: int, kind: str, cycle_key: str
) -> bool:
    notification_kind = str(kind or "").strip().lower()
    if notification_kind not in {"warning", "expired"}:
        raise ValueError("Notification kind must be warning or expired")
    cycle_column = f"{notification_kind}_cycle_key"
    sent_column = f"{notification_kind}_sent_at"
    ts = now_iso()
    with _tx(immediate=True) as conn:
        cur = conn.execute(
            f"""UPDATE account_expiry_notifications
                SET {sent_column}=?,updated_at=?
                WHERE account_id=? AND {cycle_column}=?""",
            (ts, ts, int(account_id), str(cycle_key or "")),
        )
        return bool(cur.rowcount)


def get_account_expiry_notification_state(account_id: int) -> dict:
    """Read one notification row for diagnostics and regression tests."""
    conn = _connect()
    try:
        row = conn.execute(
            "SELECT * FROM account_expiry_notifications WHERE account_id=?",
            (int(account_id),),
        ).fetchone()
        return dict(row) if row else {}
    finally:
        conn.close()


def upsert_account(tg_id: int, service: str, identifier: str, **fields):
    identifier = str(identifier or "").strip()
    if not identifier:
        raise ValueError("Account identifier is required")
    with _tx(immediate=True) as conn:
        _ensure_user(conn, tg_id)
        row = conn.execute(
            "SELECT data_json,created_at,updated_at FROM accounts WHERE tg_id=? AND service=? AND identifier=?",
            (int(tg_id), str(service), identifier),
        ).fetchone()
        previous = _json_loads(row["data_json"], {}) if row else {}
        item = dict(previous) if isinstance(previous, dict) else {}
        item.update(fields)
        updated_at = now_iso()
        if row and str(row["updated_at"] or "") >= updated_at:
            # Some platforms can return the same wall-clock timestamp for two
            # immediate calls. Account notifications use this value as their
            # renewal-cycle key, so every committed account update must advance.
            try:
                previous_dt = datetime.fromisoformat(
                    str(row["updated_at"]).replace("Z", "+00:00")
                )
                if previous_dt.tzinfo is None:
                    previous_dt = previous_dt.replace(tzinfo=timezone.utc)
                updated_at = (previous_dt + timedelta(microseconds=1)).isoformat()
            except Exception:
                updated_at = (datetime.now(timezone.utc) + timedelta(microseconds=1)).isoformat()
        item.update({"service": str(service), "identifier": identifier, "updated_at": updated_at})
        created = str(item.get("created_at") or (row["created_at"] if row else now_iso()))
        item["created_at"] = created
        conn.execute(
            """INSERT INTO accounts(tg_id,service,identifier,data_json,created_at,updated_at) VALUES(?,?,?,?,?,?)
               ON CONFLICT(tg_id,service,identifier) DO UPDATE SET data_json=excluded.data_json,updated_at=excluded.updated_at""",
            (int(tg_id), str(service), identifier, _json_dumps(item), created, item["updated_at"]),
        )
        return item


def has_test(tg_id: int, service: str) -> bool:
    column = "test_openvpn" if service == "openvpn" else "test_v2ray"
    conn = _connect()
    try:
        row = conn.execute(f"SELECT {column} FROM users WHERE tg_id=?", (int(tg_id),)).fetchone()
        return bool(row[0]) if row else False
    finally:
        conn.close()


def mark_test(tg_id: int, service: str, value: bool = True):
    column = "test_openvpn" if service == "openvpn" else "test_v2ray"
    with _tx(immediate=True) as conn:
        _ensure_user(conn, tg_id)
        conn.execute(f"UPDATE users SET {column}=?,updated_at=? WHERE tg_id=?", (int(bool(value)), now_iso(), int(tg_id)))


# -------------------- Purchase / referral --------------------

def has_completed_purchase(tg_id: int) -> bool:
    conn = _connect()
    try:
        if conn.execute("SELECT 1 FROM purchases WHERE tg_id=? LIMIT 1", (int(tg_id),)).fetchone():
            return True
        row = conn.execute("SELECT legacy_purchase_qualified FROM users WHERE tg_id=?", (int(tg_id),)).fetchone()
        return bool(row[0]) if row else False
    finally:
        conn.close()


def record_purchase(tg_id: int, order_id: str, *, service: str, plan_key: str, base_price_toman: int,
                    referral_code: str = "", referrer_tg_id: int = 0) -> bool:
    with _tx(immediate=True) as conn:
        _ensure_user(conn, tg_id)
        cur = conn.execute(
            "INSERT OR IGNORE INTO purchases(order_id,tg_id,service,plan_key,base_price_toman,referral_code,referrer_tg_id,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (str(order_id), int(tg_id), str(service), str(plan_key), int(base_price_toman), str(referral_code or ""), int(referrer_tg_id or 0), now_iso()),
        )
        return cur.rowcount > 0


def _normalize_referral_code(code: str) -> str:
    return "".join(ch for ch in str(code or "").upper().strip() if ch.isalnum())


def get_or_create_referral_code(tg_id: int) -> str:
    if not has_completed_purchase(tg_id):
        raise ValueError("برای دریافت کد معرف باید حداقل یک خرید موفق داشته باشید.")
    with _tx(immediate=True) as conn:
        _ensure_user(conn, tg_id)
        row = conn.execute("SELECT code FROM referrals WHERE tg_id=?", (int(tg_id),)).fetchone()
        existing = _normalize_referral_code(row[0] if row else "")
        if existing:
            return existing
        # Lazy import avoids a storage/app_settings import cycle during schema
        # initialization. Existing codes are looked up directly and therefore
        # never depend on the currently selected prefix.
        try:
            from app_settings import get_setting as get_runtime_setting
            prefix = str(get_runtime_setting("referral_code_prefix", REFERRAL_CODE_PREFIX))
        except Exception:
            prefix = str(REFERRAL_CODE_PREFIX)
        for _ in range(100):
            candidate = prefix + "".join(secrets.choice(_REFERRAL_ALPHABET) for _ in range(8))
            try:
                conn.execute("UPDATE referrals SET code=? WHERE tg_id=?", (candidate, int(tg_id)))
                return candidate
            except sqlite3.IntegrityError:
                continue
    raise RuntimeError("ساخت کد معرف یکتا ناموفق بود")


def find_referrer_by_code(code: str):
    normalized = _normalize_referral_code(code)
    if not normalized:
        return None
    conn = _connect()
    try:
        row = conn.execute("SELECT tg_id FROM referrals WHERE UPPER(code)=? LIMIT 1", (normalized,)).fetchone()
        if not row:
            return None
        tg_id = int(row[0])
        return tg_id if has_completed_purchase(tg_id) else None
    finally:
        conn.close()


def referral_already_used(tg_id: int) -> bool:
    conn = _connect()
    try:
        row = conn.execute("SELECT used_code,used_order_id FROM referrals WHERE tg_id=?", (int(tg_id),)).fetchone()
        return bool(row and (row[0] or row[1]))
    finally:
        conn.close()


def mark_referral_used(tg_id: int, *, order_id: str, code: str, referrer_tg_id: int):
    with _tx(immediate=True) as conn:
        _ensure_user(conn, tg_id)
        row = conn.execute("SELECT used_order_id FROM referrals WHERE tg_id=?", (int(tg_id),)).fetchone()
        used_order = str(row[0] or "") if row else ""
        if used_order:
            if used_order == str(order_id):
                return
            raise ValueError("کد معرف قبلاً برای این کاربر استفاده شده است.")
        conn.execute(
            "UPDATE referrals SET used_code=?,referrer_tg_id=?,used_at=?,used_order_id=? WHERE tg_id=?",
            (_normalize_referral_code(code), int(referrer_tg_id), now_iso(), str(order_id), int(tg_id)),
        )


# -------------------- Wallet --------------------

def wallet_balance(tg_id: int) -> int:
    conn = _connect()
    try:
        row = conn.execute("SELECT balance_toman FROM wallets WHERE tg_id=?", (int(tg_id),)).fetchone()
        return max(int(row[0] if row else 0), 0)
    finally:
        conn.close()


def _wallet_tx(tg_id: int, delta_toman: int, *, tx_id: str, kind: str, note: str = "", meta: dict | None = None) -> int:
    with _tx(immediate=True) as conn:
        _ensure_user(conn, tg_id)
        old_tx = conn.execute(
            "SELECT tg_id,delta_toman,kind,balance_after_toman FROM wallet_transactions WHERE tx_id=?",
            (str(tx_id),),
        ).fetchone()
        if old_tx:
            if (
                int(old_tx["tg_id"]) != int(tg_id)
                or int(old_tx["delta_toman"]) != int(delta_toman)
                or str(old_tx["kind"]) != str(kind)
            ):
                raise ValueError("شناسه تراکنش کیف پول با عملیات دیگری استفاده شده است.")
            return int(old_tx["balance_after_toman"])
        row = conn.execute("SELECT balance_toman FROM wallets WHERE tg_id=?", (int(tg_id),)).fetchone()
        before = int(row[0] if row else 0)
        after = before + int(delta_toman)
        if after < 0:
            raise ValueError("موجودی کیف پول کافی نیست.")
        if after > MAX_WALLET_BALANCE_TOMAN:
            raise ValueError("موجودی جدید از سقف امن قابل ذخیره‌سازی بیشتر است.")
        conn.execute("UPDATE wallets SET balance_toman=? WHERE tg_id=?", (after, int(tg_id)))
        conn.execute(
            "INSERT INTO wallet_transactions(tx_id,tg_id,kind,delta_toman,balance_after_toman,note,meta_json,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (str(tx_id), int(tg_id), str(kind), int(delta_toman), after, str(note or ""), _json_dumps(meta or {}), now_iso()),
        )
        return after


def debit_wallet(tg_id: int, amount_toman: int, *, order_id: str) -> int:
    amount = max(int(amount_toman or 0), 0)
    return wallet_balance(tg_id) if not amount else _wallet_tx(tg_id, -amount, tx_id=f"order:{order_id}:wallet-debit", kind="order_debit", note="پرداخت سفارش با کیف پول", meta={"order_id": str(order_id)})


def refund_wallet(tg_id: int, amount_toman: int, *, order_id: str) -> int:
    amount = max(int(amount_toman or 0), 0)
    return wallet_balance(tg_id) if not amount else _wallet_tx(tg_id, amount, tx_id=f"order:{order_id}:wallet-refund", kind="order_refund", note="بازگشت اعتبار سفارش", meta={"order_id": str(order_id)})


def wallet_order_debited(order_id: str) -> bool:
    """Whether the idempotent wallet debit for an order was committed.

    This is intentionally checked from the ledger instead of trusting the
    pending-payment flag. A process can die after SQLite commits the debit but
    before the pending row is updated.
    """
    order_id = str(order_id or "").strip()
    if not order_id:
        return False
    conn = _connect()
    try:
        row = conn.execute(
            "SELECT 1 FROM wallet_transactions WHERE tx_id=? LIMIT 1",
            (f"order:{order_id}:wallet-debit",),
        ).fetchone()
        return bool(row)
    finally:
        conn.close()


def credit_referral_reward(referrer_tg_id: int, amount_toman: int, *, order_id: str, buyer_tg_id: int) -> int:
    amount = max(int(amount_toman or 0), 0)
    return wallet_balance(referrer_tg_id) if not amount else _wallet_tx(referrer_tg_id, amount, tx_id=f"referral:{order_id}:reward", kind="referral_reward", note="پاداش معرفی دوست", meta={"order_id": str(order_id), "buyer_tg_id": int(buyer_tg_id)})


# -------------------- Pending payments / reservations --------------------

def add_pending(authority: str, payload: dict):
    payload = dict(payload or {})
    ts = now_iso()
    with _tx(immediate=True) as conn:
        tg_id = int(payload.get("tg_id") or 0)
        if tg_id:
            _ensure_user(conn, tg_id)
        conn.execute(
            """INSERT OR REPLACE INTO pending_payments(authority,tg_id,ts,first_purchase,wallet_used_toman,wallet_committed,payload_json,created_at,updated_at)
               VALUES(?,?,?,?,?,?,?,?,?)""",
            (str(authority), tg_id, int(payload.get("ts") or 0), int(bool(payload.get("first_purchase"))),
             int(payload.get("wallet_used_toman") or 0), int(bool(payload.get("wallet_committed"))), _json_dumps(payload), ts, ts),
        )


def replace_pending_authority(old_authority: str, new_authority: str, payload: dict):
    """Atomically replace a local gateway reservation with its real authority."""
    old_authority = str(old_authority or "")
    new_authority = str(new_authority or "")
    payload = dict(payload or {})
    if not old_authority or not new_authority:
        raise ValueError("شناسه سفارش درگاه ناقص است")
    ts = now_iso()
    with _tx(immediate=True) as conn:
        old = conn.execute(
            "SELECT tg_id,payload_json,created_at FROM pending_payments WHERE authority=?",
            (old_authority,),
        ).fetchone()
        if not old:
            raise RuntimeError("رزرو موقت سفارش درگاه پیدا نشد")
        old_payload = _json_loads(old["payload_json"], {})
        if (
            int(old["tg_id"] or 0) != int(payload.get("tg_id") or 0)
            or str((old_payload or {}).get("order_id") or "") != str(payload.get("order_id") or "")
        ):
            raise RuntimeError("رزرو موقت با پاسخ درگاه مطابقت ندارد")
        collision = conn.execute(
            "SELECT 1 FROM pending_payments WHERE authority=? AND authority!=?",
            (new_authority, old_authority),
        ).fetchone()
        if collision:
            raise RuntimeError("Authority درگاه قبلاً ثبت شده است")
        conn.execute("DELETE FROM pending_payments WHERE authority=?", (old_authority,))
        conn.execute(
            """INSERT INTO pending_payments(authority,tg_id,ts,first_purchase,wallet_used_toman,wallet_committed,payload_json,created_at,updated_at)
               VALUES(?,?,?,?,?,?,?,?,?)""",
            (
                new_authority,
                int(payload.get("tg_id") or 0),
                int(payload.get("ts") or 0),
                int(bool(payload.get("first_purchase"))),
                int(payload.get("wallet_used_toman") or 0),
                int(bool(payload.get("wallet_committed"))),
                _json_dumps(payload),
                str(old["created_at"] or ts),
                ts,
            ),
        )
    return payload


def update_pending(authority: str, **fields):
    with _tx(immediate=True) as conn:
        row = conn.execute("SELECT payload_json FROM pending_payments WHERE authority=?", (str(authority),)).fetchone()
        if not row:
            return None
        payload = _json_loads(row[0], {})
        payload = dict(payload) if isinstance(payload, dict) else {}
        payload.update(fields)
        conn.execute(
            """UPDATE pending_payments SET tg_id=?,ts=?,first_purchase=?,wallet_used_toman=?,wallet_committed=?,payload_json=?,updated_at=? WHERE authority=?""",
            (int(payload.get("tg_id") or 0), int(payload.get("ts") or 0), int(bool(payload.get("first_purchase"))),
             int(payload.get("wallet_used_toman") or 0), int(bool(payload.get("wallet_committed"))), _json_dumps(payload), now_iso(), str(authority)),
        )
        return payload


def authorize_pending_payment(
    authority: str, *, method: str, verification_code: int = 0,
    admin_tg_id: int = 0,
) -> dict:
    """Durably authorize a pending order before any remote provisioning.

    This is the financial boundary between payment verification and account
    delivery. The marker lives inside the pending payload so a crash/restart
    cannot lose it, and validation/update is one SQLite transaction.
    """
    authority = str(authority or "").strip()
    normalized_method = str(method or "").strip().lower()
    if not authority or normalized_method not in {"zarinpal", "card_transfer"}:
        raise ValueError("روش تأیید پرداخت نامعتبر است")

    with _tx(immediate=True) as conn:
        row = conn.execute(
            "SELECT payload_json FROM pending_payments WHERE authority=?",
            (authority,),
        ).fetchone()
        if not row:
            raise RuntimeError("سفارش در انتظار برای تأیید پرداخت پیدا نشد")
        payload = _json_loads(row[0], {})
        payload = dict(payload) if isinstance(payload, dict) else {}
        payment_kind = str(payload.get("payment_kind") or "gateway").strip().lower()

        if payload.get("payment_authorized"):
            recorded_method = str(
                payload.get("payment_authorization_method") or ""
            ).strip().lower()
            if recorded_method != normalized_method:
                raise RuntimeError("روش تأیید ثبت‌شده با سفارش مطابقت ندارد")
            return payload

        amount_rial = int(payload.get("amount_rial") or 0)
        gateway_toman = int(payload.get("gateway_toman") or 0)
        if amount_rial <= 0 and gateway_toman <= 0:
            raise RuntimeError("مبلغ پرداخت سفارش نامعتبر است")
        if amount_rial > 0 and gateway_toman > 0 and amount_rial != gateway_toman * 10:
            raise RuntimeError("مبلغ ریالی پرداخت با سفارش مطابقت ندارد")

        if normalized_method == "zarinpal":
            if payment_kind in {"wallet", "admin", "preflight", "card_transfer"}:
                raise RuntimeError("این سفارش قابل تأیید از طریق زرین‌پال نیست")
            code = int(verification_code or 0)
            if code not in (100, 101):
                raise RuntimeError("پاسخ موفق زرین‌پال برای این سفارش ثبت نشده است")
            payload.update({
                "payment_authorized": True,
                "payment_authorization_method": "zarinpal",
                "payment_authorization_code": code,
                "payment_authorized_at": now_iso(),
            })
        else:
            if payment_kind != "card_transfer":
                raise RuntimeError("این سفارش کارت به کارت نیست")
            request = conn.execute(
                """SELECT id,tg_id,order_id,status FROM card_transfer_requests
                   WHERE authority=?""",
                (authority,),
            ).fetchone()
            if not request or str(request["status"] or "") != "processing":
                raise RuntimeError("رسید کارت به کارت هنوز توسط ادمین تأیید نشده است")
            if (
                int(request["tg_id"] or 0) != int(payload.get("tg_id") or 0)
                or str(request["order_id"] or "") != str(payload.get("order_id") or "")
                or int(request["id"] or 0) != int(payload.get("card_request_id") or 0)
            ):
                raise RuntimeError("درخواست کارت به کارت با سفارش در انتظار مطابقت ندارد")
            approver = int(admin_tg_id or 0)
            if approver <= 0:
                raise RuntimeError("شناسه ادمین تأییدکننده نامعتبر است")
            payload.update({
                "payment_authorized": True,
                "payment_authorization_method": "card_transfer",
                "payment_authorized_by": approver,
                "payment_authorized_at": now_iso(),
            })

        conn.execute(
            """UPDATE pending_payments
               SET payload_json=?,updated_at=? WHERE authority=?""",
            (_json_dumps(payload), now_iso(), authority),
        )
        return payload


def get_pending(authority: str):
    conn = _connect()
    try:
        row = conn.execute("SELECT payload_json FROM pending_payments WHERE authority=?", (str(authority),)).fetchone()
        return _json_loads(row[0], {}) if row else None
    finally:
        conn.close()


def pop_pending(authority: str):
    with _tx(immediate=True) as conn:
        row = conn.execute("SELECT payload_json FROM pending_payments WHERE authority=?", (str(authority),)).fetchone()
        if not row:
            return None
        payload = _json_loads(row[0], {})
        conn.execute("DELETE FROM pending_payments WHERE authority=?", (str(authority),))
        return payload


def latest_pending_for_user(tg_id: int):
    conn = _connect()
    try:
        row = conn.execute(
            """SELECT authority,payload_json FROM pending_payments
               WHERE tg_id=? ORDER BY ts DESC,updated_at DESC,rowid DESC LIMIT 1""",
            (int(tg_id),),
        ).fetchone()
        return (str(row["authority"]), _json_loads(row["payload_json"], {})) if row else (None, None)
    finally:
        conn.close()


def list_pending_for_user(tg_id: int):
    """Return all pending rows for a user, newest first.

    Used by payment migration cleanup; returning decoded payloads keeps gateway
    policy in bot.py rather than deleting rows blindly in storage.
    """
    conn = _connect()
    try:
        rows = conn.execute(
            """SELECT authority,payload_json FROM pending_payments
               WHERE tg_id=? ORDER BY ts DESC,updated_at DESC,rowid DESC""",
            (int(tg_id),),
        ).fetchall()
        return [
            (str(row["authority"]), _json_loads(row["payload_json"], {}))
            for row in rows
        ]
    finally:
        conn.close()


def _card_transfer_item(row) -> dict:
    if not row:
        return {}
    item = dict(row)
    payload_json = item.pop("payload_json", None)
    if payload_json is not None:
        item["payload"] = _json_loads(payload_json, {})
    if "first_name" in item:
        profile = {
            key: str(item.get(key) or "")
            for key in ("first_name", "last_name", "username")
        }
        item["label"] = _label_from_profile(int(item.get("tg_id") or 0), profile)[0]
    return item


def create_card_transfer_request(payload: dict) -> dict:
    """Persist a manual-payment order without provisioning any remote account."""
    payload = dict(payload or {})
    tg_id = int(payload.get("tg_id") or 0)
    order_id = str(payload.get("order_id") or "").strip()
    if tg_id <= 0 or not order_id:
        raise ValueError("اطلاعات سفارش کارت به کارت ناقص است")
    authority = str(payload.get("authority") or f"card-{order_id}").strip()
    if not authority:
        raise ValueError("شناسه سفارش کارت به کارت ناقص است")
    ts = now_iso()
    with _tx(immediate=True) as conn:
        _ensure_user(conn, tg_id)
        existing_order = conn.execute(
            "SELECT * FROM card_transfer_requests WHERE order_id=?", (order_id,)
        ).fetchone()
        if existing_order:
            item = _card_transfer_item(existing_order)
            if int(item.get("tg_id") or 0) != tg_id:
                raise RuntimeError("شناسه سفارش قبلاً استفاده شده است")
            pending = conn.execute(
                "SELECT payload_json FROM pending_payments WHERE authority=?", (authority,)
            ).fetchone()
            if pending:
                item["payload"] = _json_loads(pending[0], {})
            return item
        active = conn.execute(
            """SELECT id FROM card_transfer_requests
               WHERE tg_id=? AND status IN ('awaiting_receipt','submitted','processing')
               ORDER BY id DESC LIMIT 1""",
            (tg_id,),
        ).fetchone()
        if active:
            raise ValueError("یک درخواست کارت به کارت فعال برای شما وجود دارد")
        cur = conn.execute(
            """INSERT INTO card_transfer_requests(
                   authority,order_id,tg_id,status,created_at,updated_at
               ) VALUES(?,?,?,'awaiting_receipt',?,?)""",
            (authority, order_id, tg_id, ts, ts),
        )
        request_id = int(cur.lastrowid)
        payload.update({
            "authority": authority,
            "payment_kind": "card_transfer",
            "card_request_id": request_id,
        })
        conn.execute(
            """INSERT INTO pending_payments(
                   authority,tg_id,ts,first_purchase,wallet_used_toman,wallet_committed,
                   payload_json,created_at,updated_at
               ) VALUES(?,?,?,?,?,?,?,?,?)""",
            (
                authority, tg_id, int(payload.get("ts") or 0),
                int(bool(payload.get("first_purchase"))),
                int(payload.get("wallet_used_toman") or 0),
                int(bool(payload.get("wallet_committed"))),
                _json_dumps(payload), ts, ts,
            ),
        )
        row = conn.execute(
            "SELECT * FROM card_transfer_requests WHERE id=?", (request_id,)
        ).fetchone()
        item = _card_transfer_item(row)
        item["payload"] = payload
        return item


def get_card_transfer_request(request_id: int) -> dict:
    conn = _connect()
    try:
        row = conn.execute(
            """SELECT c.*,p.payload_json,
                      COALESCE(u.first_name,'') first_name,
                      COALESCE(u.last_name,'') last_name,
                      COALESCE(u.username,'') username
               FROM card_transfer_requests c
               LEFT JOIN pending_payments p ON p.authority=c.authority
               LEFT JOIN users u ON u.tg_id=c.tg_id
               WHERE c.id=?""",
            (int(request_id),),
        ).fetchone()
        return _card_transfer_item(row)
    finally:
        conn.close()


def get_card_transfer_request_by_authority(authority: str) -> dict:
    conn = _connect()
    try:
        row = conn.execute(
            """SELECT c.*,p.payload_json FROM card_transfer_requests c
               LEFT JOIN pending_payments p ON p.authority=c.authority
               WHERE c.authority=?""",
            (str(authority or ""),),
        ).fetchone()
        return _card_transfer_item(row)
    finally:
        conn.close()


def active_card_transfer_request_for_user(tg_id: int) -> dict:
    conn = _connect()
    try:
        row = conn.execute(
            """SELECT c.*,p.payload_json FROM card_transfer_requests c
               LEFT JOIN pending_payments p ON p.authority=c.authority
               WHERE c.tg_id=? AND c.status IN ('awaiting_receipt','submitted','processing')
               ORDER BY c.id DESC LIMIT 1""",
            (int(tg_id),),
        ).fetchone()
        return _card_transfer_item(row)
    finally:
        conn.close()


def submit_card_transfer_receipt(
    request_id: int,
    tg_id: int,
    *,
    receipt_kind: str,
    receipt_text: str = "",
    receipt_file_id: str = "",
    receipt_file_unique_id: str = "",
) -> dict:
    kind = str(receipt_kind or "").strip().lower()
    text = str(receipt_text or "").strip()
    file_id = str(receipt_file_id or "").strip()
    file_unique_id = str(receipt_file_unique_id or "").strip()
    if kind not in {"text", "photo", "document"}:
        raise ValueError("نوع رسید معتبر نیست")
    if kind == "text" and not text:
        raise ValueError("متن رسید نمی‌تواند خالی باشد")
    if kind in {"photo", "document"} and not file_id:
        raise ValueError("فایل رسید پیدا نشد")
    if len(text) > 4000:
        raise ValueError("متن رسید بیش از حد طولانی است")
    ts = now_iso()
    with _tx(immediate=True) as conn:
        row = conn.execute(
            "SELECT * FROM card_transfer_requests WHERE id=? AND tg_id=?",
            (int(request_id), int(tg_id)),
        ).fetchone()
        if not row:
            raise ValueError("درخواست کارت به کارت پیدا نشد")
        status = str(row["status"] or "")
        if status != "awaiting_receipt":
            return _card_transfer_item(row)
        cur = conn.execute(
            """UPDATE card_transfer_requests
               SET status='submitted',receipt_kind=?,receipt_text=?,receipt_file_id=?,
                   receipt_file_unique_id=?,submitted_at=?,updated_at=?
               WHERE id=? AND tg_id=? AND status='awaiting_receipt'""",
            (kind, text, file_id, file_unique_id, ts, ts, int(request_id), int(tg_id)),
        )
        if cur.rowcount != 1:
            raise RuntimeError("رسید هم‌زمان ثبت شده است")
        fresh = conn.execute(
            """SELECT c.*,p.payload_json,
                      COALESCE(u.first_name,'') first_name,
                      COALESCE(u.last_name,'') last_name,
                      COALESCE(u.username,'') username
               FROM card_transfer_requests c
               LEFT JOIN pending_payments p ON p.authority=c.authority
               LEFT JOIN users u ON u.tg_id=c.tg_id WHERE c.id=?""",
            (int(request_id),),
        ).fetchone()
        return _card_transfer_item(fresh)


def list_card_transfer_requests(
    *, statuses=("submitted", "processing"), offset: int = 0, limit: int = 10
) -> tuple[list[dict], int]:
    allowed = tuple(
        status for status in dict.fromkeys(str(x or "") for x in statuses)
        if status in {"awaiting_receipt", "submitted", "processing", "approved", "rejected", "cancelled"}
    )
    if not allowed:
        return [], 0
    placeholders = ",".join("?" for _ in allowed)
    conn = _connect()
    try:
        total = int(conn.execute(
            f"SELECT COUNT(*) FROM card_transfer_requests WHERE status IN ({placeholders})",
            allowed,
        ).fetchone()[0])
        rows = conn.execute(
            f"""SELECT c.*,p.payload_json,
                       COALESCE(u.first_name,'') first_name,
                       COALESCE(u.last_name,'') last_name,
                       COALESCE(u.username,'') username
                FROM card_transfer_requests c
                LEFT JOIN pending_payments p ON p.authority=c.authority
                LEFT JOIN users u ON u.tg_id=c.tg_id
                WHERE c.status IN ({placeholders})
                ORDER BY CASE c.status WHEN 'submitted' THEN 0 ELSE 1 END,
                         c.submitted_at ASC,c.id ASC LIMIT ? OFFSET ?""",
            (*allowed, max(int(limit), 1), max(int(offset), 0)),
        ).fetchall()
        return [_card_transfer_item(row) for row in rows], total
    finally:
        conn.close()


def claim_card_transfer_request(request_id: int, *, admin_tg_id: int) -> dict:
    ts = now_iso()
    with _tx(immediate=True) as conn:
        row = conn.execute(
            "SELECT * FROM card_transfer_requests WHERE id=?", (int(request_id),)
        ).fetchone()
        if not row:
            return {}
        if str(row["status"] or "") == "submitted":
            conn.execute(
                """UPDATE card_transfer_requests
                   SET status='processing',decided_by=?,updated_at=?
                   WHERE id=? AND status='submitted'""",
                (int(admin_tg_id), ts, int(request_id)),
            )
        fresh = conn.execute(
            """SELECT c.*,p.payload_json FROM card_transfer_requests c
               LEFT JOIN pending_payments p ON p.authority=c.authority WHERE c.id=?""",
            (int(request_id),),
        ).fetchone()
        return _card_transfer_item(fresh)


def reject_card_transfer_request(
    request_id: int, *, admin_tg_id: int, reason: str = ""
) -> dict:
    reason = str(reason or "").strip()
    if len(reason) > 2000:
        raise ValueError("علت رد بیش از حد طولانی است")
    ts = now_iso()
    with _tx(immediate=True) as conn:
        row = conn.execute(
            "SELECT * FROM card_transfer_requests WHERE id=?", (int(request_id),)
        ).fetchone()
        if not row:
            return {}
        status = str(row["status"] or "")
        if status == "rejected":
            return _card_transfer_item(row)
        if status != "submitted":
            raise ValueError("این درخواست دیگر قابل رد کردن نیست")
        conn.execute(
            """UPDATE card_transfer_requests
               SET status='rejected',decided_at=?,decided_by=?,rejection_reason=?,updated_at=?
               WHERE id=? AND status='submitted'""",
            (ts, int(admin_tg_id), reason, ts, int(request_id)),
        )
        conn.execute("DELETE FROM pending_payments WHERE authority=?", (str(row["authority"]),))
        conn.execute(
            """INSERT INTO admin_audit(
                   admin_tg_id,target_tg_id,action,before_json,after_json,meta_json,created_at
               ) VALUES(?,?,?,?,?,?,?)""",
            (
                int(admin_tg_id), int(row["tg_id"]), "card_transfer_rejected",
                _json_dumps({"status": status}), _json_dumps({"status": "rejected"}),
                _json_dumps({"request_id": int(request_id), "reason_supplied": bool(reason)}), ts,
            ),
        )
        return _card_transfer_item(conn.execute(
            "SELECT * FROM card_transfer_requests WHERE id=?", (int(request_id),)
        ).fetchone())


def cancel_card_transfer_request(request_id: int, *, tg_id: int) -> dict:
    ts = now_iso()
    with _tx(immediate=True) as conn:
        row = conn.execute(
            "SELECT * FROM card_transfer_requests WHERE id=? AND tg_id=?",
            (int(request_id), int(tg_id)),
        ).fetchone()
        if not row:
            return {}
        status = str(row["status"] or "")
        if status == "cancelled":
            return _card_transfer_item(row)
        if status != "awaiting_receipt":
            raise ValueError("پس از ارسال رسید، لغو فقط توسط بررسی ادمین انجام می‌شود")
        conn.execute(
            """UPDATE card_transfer_requests SET status='cancelled',updated_at=?
               WHERE id=? AND status='awaiting_receipt'""",
            (ts, int(request_id)),
        )
        conn.execute("DELETE FROM pending_payments WHERE authority=?", (str(row["authority"]),))
        return _card_transfer_item(conn.execute(
            "SELECT * FROM card_transfer_requests WHERE id=?", (int(request_id),)
        ).fetchone())


def complete_card_transfer_request(request_id: int, *, admin_tg_id: int) -> dict:
    ts = now_iso()
    with _tx(immediate=True) as conn:
        row = conn.execute(
            """SELECT c.*,p.payload_json FROM card_transfer_requests c
               LEFT JOIN pending_payments p ON p.authority=c.authority
               WHERE c.id=?""",
            (int(request_id),),
        ).fetchone()
        if not row:
            return {}
        status = str(row["status"] or "")
        if status == "approved":
            return _card_transfer_item(row)
        if status != "processing":
            raise ValueError("این درخواست در وضعیت قابل تأیید نیست")
        pending = _json_loads(row["payload_json"], {})
        if (
            not isinstance(pending, dict)
            or pending.get("payment_authorized") is not True
            or str(pending.get("payment_authorization_method") or "")
            != "card_transfer"
        ):
            raise RuntimeError("مجوز مالی کارت به کارت برای ثبت نهایی پیدا نشد")
        conn.execute(
            """UPDATE card_transfer_requests
               SET status='approved',decided_at=?,decided_by=?,updated_at=?
               WHERE id=? AND status='processing'""",
            (ts, int(admin_tg_id), ts, int(request_id)),
        )
        conn.execute("DELETE FROM pending_payments WHERE authority=?", (str(row["authority"]),))
        conn.execute(
            """INSERT INTO admin_audit(
                   admin_tg_id,target_tg_id,action,before_json,after_json,meta_json,created_at
               ) VALUES(?,?,?,?,?,?,?)""",
            (
                int(admin_tg_id), int(row["tg_id"]), "card_transfer_approved",
                _json_dumps({"status": status}), _json_dumps({"status": "approved"}),
                _json_dumps({"request_id": int(request_id)}), ts,
            ),
        )
        return _card_transfer_item(conn.execute(
            "SELECT * FROM card_transfer_requests WHERE id=?", (int(request_id),)
        ).fetchone())


def pending_first_purchase_for_user(tg_id: int):
    conn = _connect()
    try:
        row = conn.execute(
            """SELECT authority,payload_json FROM pending_payments
               WHERE tg_id=? AND first_purchase=1
               ORDER BY ts DESC,updated_at DESC,rowid DESC LIMIT 1""",
            (int(tg_id),),
        ).fetchone()
        return (str(row["authority"]), _json_loads(row["payload_json"], {})) if row else (None, None)
    finally:
        conn.close()


def reserved_wallet_for_user(tg_id: int) -> int:
    conn = _connect()
    try:
        row = conn.execute("SELECT COALESCE(SUM(wallet_used_toman),0) FROM pending_payments WHERE tg_id=? AND wallet_committed=0", (int(tg_id),)).fetchone()
        return max(int(row[0] or 0), 0)
    finally:
        conn.close()


def wallet_available(tg_id: int) -> int:
    return max(wallet_balance(tg_id) - reserved_wallet_for_user(tg_id), 0)


# -------------------- Successful transaction ledger --------------------

def record_transaction(payload: dict) -> bool:
    action = str(payload.get("action") or "")
    if action not in {"buy", "renew"}:
        return False
    tg_id = int(payload.get("tg_id") or 0)
    order_id = str(payload.get("order_id") or "").strip()
    if not tg_id or not order_id:
        return False
    base = int(payload.get("base_price_toman") or 0)
    discount = int(payload.get("referral_discount_toman") or 0)
    wallet_used = int(payload.get("wallet_used_toman") or 0)
    gateway = int(payload.get("gateway_toman") or 0)
    payment_kind = str(payload.get("payment_kind") or "gateway").strip().lower()
    payment_method = str(
        payload.get("payment_authorization_method")
        or ("zarinpal" if payment_kind == "gateway" else payment_kind)
        or ""
    ).strip().lower()
    reseller_id = int(payload.get("reseller_id") or 0)
    reseller_charge = int(payload.get("reseller_charge_toman") or 0)
    if gateway <= 0 and base > 0 and payment_kind not in {"owner", "reseller_debt"}:
        gateway = max(base - discount - wallet_used, 0)
    with _tx(immediate=True) as conn:
        cur = conn.execute(
            """INSERT OR IGNORE INTO transactions(
                   order_id,tg_id,service,action,plan_key,base_price_toman,
                   referral_discount_toman,wallet_used_toman,gateway_toman,
                   payment_kind,payment_method,reseller_id,reseller_charge_toman,
                   created_at,legacy
               ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)""",
            (
                order_id, tg_id, str(payload.get("service") or ""), action,
                str(payload.get("plan_key") or ""), base, discount, wallet_used,
                gateway, payment_kind, payment_method, reseller_id,
                reseller_charge, now_iso(),
            ),
        )
        return cur.rowcount > 0


def _transaction_rows(where: str = "", params=(), *, offset: int = 0, limit: int = 5):
    conn = _connect()
    try:
        clause = f" WHERE {where}" if where else ""
        total = int(conn.execute(f"SELECT COUNT(*) FROM transactions{clause}", params).fetchone()[0])
        rows = [dict(r) for r in conn.execute(
            f"SELECT * FROM transactions{clause} ORDER BY created_at DESC,rowid DESC LIMIT ? OFFSET ?",
            (*params, max(int(limit), 1), max(int(offset), 0)),
        ).fetchall()]
        return rows, total
    finally:
        conn.close()


def list_transactions(offset: int = 0, limit: int = 5) -> tuple[list[dict], int]:
    return _transaction_rows(offset=offset, limit=limit)


def list_user_transactions(tg_id: int, offset: int = 0, limit: int = 5) -> tuple[list[dict], int]:
    return _transaction_rows("tg_id=?", (int(tg_id),), offset=offset, limit=limit)


# -------------------- Idempotent service fulfillment journal --------------------

def get_fulfillment(order_id: str):
    if not str(order_id or ""):
        return None
    conn = _connect()
    try:
        row = conn.execute("SELECT * FROM fulfillments WHERE order_id=?", (str(order_id),)).fetchone()
        if not row:
            return None
        item = dict(row)
        item["secret"] = _json_loads(item.pop("secret_json"), {})
        item["result"] = _json_loads(item.pop("result_json"), {})
        item["meta"] = _json_loads(item.pop("meta_json"), {})
        return item
    finally:
        conn.close()


def prepare_fulfillment(order_id: str, *, tg_id: int, service: str, action: str, requested_identifier: str = "",
                        delivery_identifier: str = "", secret: dict | None = None, meta: dict | None = None):
    order_id = str(order_id or "").strip()
    if not order_id:
        return None
    ts = now_iso()
    with _tx(immediate=True) as conn:
        conn.execute(
            """INSERT OR IGNORE INTO fulfillments(order_id,tg_id,service,action,requested_identifier,delivery_identifier,secret_json,state,result_json,meta_json,created_at,updated_at)
               VALUES(?,?,?,?,?,?,?,'prepared','{}',?,?,?)""",
            (order_id, int(tg_id), str(service), str(action), str(requested_identifier or ""), str(delivery_identifier or ""),
             _json_dumps(secret or {}), _json_dumps(meta or {}), ts, ts),
        )
        row = conn.execute("SELECT delivery_identifier,secret_json,meta_json FROM fulfillments WHERE order_id=?", (order_id,)).fetchone()
        updates = []
        params = []
        if delivery_identifier and not str(row["delivery_identifier"] or ""):
            updates.append("delivery_identifier=?"); params.append(str(delivery_identifier))
        old_secret = _json_loads(row["secret_json"], {})
        if secret and not old_secret:
            updates.append("secret_json=?"); params.append(_json_dumps(secret))
        old_meta = _json_loads(row["meta_json"], {})
        if meta:
            merged = dict(old_meta) if isinstance(old_meta, dict) else {}
            merged.update(dict(meta))
            updates.append("meta_json=?"); params.append(_json_dumps(merged))
        if updates:
            updates.append("updated_at=?"); params.append(ts); params.append(order_id)
            conn.execute(f"UPDATE fulfillments SET {','.join(updates)} WHERE order_id=?", tuple(params))
    return get_fulfillment(order_id)


def mark_fulfillment_executing(order_id: str, meta: dict | None = None):
    """Mark an external service write as started before touching RouterOS/3x-ui.

    An 'executing' row is intentionally not auto-retried after a process crash,
    because we cannot always prove whether a remote write committed. This avoids
    double quota/day application on renewals.
    """
    if not str(order_id or ""):
        return False
    ts = now_iso()
    with _tx(immediate=True) as conn:
        row = conn.execute("SELECT state,meta_json FROM fulfillments WHERE order_id=?", (str(order_id),)).fetchone()
        if not row:
            return False
        if str(row["state"] or "") != "prepared":
            return False
        old = _json_loads(row["meta_json"], {})
        merged = dict(old) if isinstance(old, dict) else {}
        if meta:
            merged.update(dict(meta))
        cur = conn.execute(
            "UPDATE fulfillments SET state='executing',meta_json=?,updated_at=? WHERE order_id=? AND state='prepared'",
            (_json_dumps(merged), ts, str(order_id)),
        )
        return cur.rowcount > 0


def mark_fulfillment_prepared(order_id: str, meta: dict | None = None):
    """Return a journal row to prepared only when the caller knows no remote write happened."""
    if not str(order_id or ""):
        return False
    ts = now_iso()
    with _tx(immediate=True) as conn:
        row = conn.execute("SELECT meta_json FROM fulfillments WHERE order_id=?", (str(order_id),)).fetchone()
        if not row:
            return False
        old = _json_loads(row["meta_json"], {})
        merged = dict(old) if isinstance(old, dict) else {}
        if meta:
            merged.update(dict(meta))
        cur = conn.execute(
            "UPDATE fulfillments SET state='prepared',meta_json=?,updated_at=? WHERE order_id=? AND state='executing'",
            (_json_dumps(merged), ts, str(order_id)),
        )
        return cur.rowcount > 0


def mark_fulfillment_remote_done(order_id: str, meta: dict | None = None):
    """The remote service write returned successfully; remaining work is local/read-only."""
    if not str(order_id or ""):
        return False
    ts = now_iso()
    with _tx(immediate=True) as conn:
        row = conn.execute("SELECT state,meta_json FROM fulfillments WHERE order_id=?", (str(order_id),)).fetchone()
        if not row:
            return False
        # A late concurrent handler must never move a locally provisioned or
        # completed order backwards to remote_done.
        if str(row["state"] or "") not in {"prepared", "executing", "remote_done"}:
            return False
        old = _json_loads(row["meta_json"], {})
        merged = dict(old) if isinstance(old, dict) else {}
        if meta:
            merged.update(dict(meta))
        cur = conn.execute(
            """UPDATE fulfillments SET state='remote_done',meta_json=?,updated_at=?
               WHERE order_id=? AND state IN ('prepared','executing','remote_done')""",
            (_json_dumps(merged), ts, str(order_id)),
        )
        return cur.rowcount > 0


def mark_fulfillment_provisioned(order_id: str, result: dict):
    if not str(order_id or ""):
        return False
    with _tx(immediate=True) as conn:
        cur = conn.execute(
            """UPDATE fulfillments SET state='provisioned',result_json=?,updated_at=?
               WHERE order_id=? AND state IN ('remote_done','provisioned')""",
            (_json_dumps(result or {}), now_iso(), str(order_id)),
        )
        return cur.rowcount > 0


def mark_fulfillment_completed(order_id: str):
    if not str(order_id or ""):
        return False
    with _tx(immediate=True) as conn:
        cur = conn.execute(
            """UPDATE fulfillments SET state='completed',updated_at=?
               WHERE order_id=? AND state IN ('provisioned','completed')""",
            (now_iso(), str(order_id)),
        )
        return cur.rowcount > 0


def list_incomplete_fulfillments(tg_id: int | None = None, limit: int = 50) -> list[dict]:
    conn = _connect()
    try:
        if tg_id is None:
            rows = conn.execute("SELECT * FROM fulfillments WHERE state!='completed' ORDER BY updated_at DESC LIMIT ?", (max(int(limit),1),)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM fulfillments WHERE tg_id=? AND state!='completed' ORDER BY updated_at DESC LIMIT ?", (int(tg_id),max(int(limit),1))).fetchall()
        result=[]
        for row in rows:
            item=dict(row); item["secret"]=_json_loads(item.pop("secret_json"),{}); item["result"]=_json_loads(item.pop("result_json"),{}); item["meta"]=_json_loads(item.pop("meta_json"),{}); result.append(item)
        return result
    finally:
        conn.close()


# -------------------- Admin user / wallet / audit --------------------

def _label_from_profile(tg_id: int, profile: dict) -> tuple[str, str, str]:
    first = str(profile.get("first_name") or "").strip()
    last = str(profile.get("last_name") or "").strip()
    username = str(profile.get("username") or "").strip().lstrip("@")
    name = " ".join(x for x in (first, last) if x).strip()
    label = f"{name} @{username}" if name and username else (name or (f"@{username}" if username else str(int(tg_id))))
    return label, name, username


def list_known_users(*, positive_wallet_only: bool = False, offset: int = 0, limit: int = 10) -> tuple[list[dict], int]:
    conn = _connect()
    try:
        where = "WHERE COALESCE(w.balance_toman,0)>0" if positive_wallet_only else ""
        total = int(conn.execute(f"SELECT COUNT(*) FROM users u LEFT JOIN wallets w ON w.tg_id=u.tg_id {where}").fetchone()[0])
        rows = conn.execute(
            f"""SELECT u.tg_id,u.first_name,u.last_name,u.username,u.language_code,u.phone_number,u.email,COALESCE(w.balance_toman,0) balance_toman
                FROM users u LEFT JOIN wallets w ON w.tg_id=u.tg_id {where}
                ORDER BY u.tg_id DESC LIMIT ? OFFSET ?""",
            (max(int(limit), 1), max(int(offset), 0)),
        ).fetchall()
        result = []
        for r in rows:
            profile = {k: str(r[k] or "") for k in ("first_name", "last_name", "username", "language_code", "phone_number", "email")}
            label, name, username = _label_from_profile(int(r["tg_id"]), profile)
            result.append({"tg_id": int(r["tg_id"]), "label": label, "name": name, "username": username, "balance_toman": int(r["balance_toman"] or 0), "profile": profile})
        return result, total
    finally:
        conn.close()


def search_known_users(query: str, limit: int = 20) -> list[dict]:
    q = str(query or "").strip()
    if not q:
        return []
    q_no_at = q.lstrip("@").lower()
    like = f"%{q_no_at}%"
    conn = _connect()
    try:
        rows = conn.execute(
            """SELECT DISTINCT u.tg_id,u.first_name,u.last_name,u.username,u.language_code,u.phone_number,u.email,
                      COALESCE(w.balance_toman,0) balance_toman
               FROM users u
               LEFT JOIN wallets w ON w.tg_id=u.tg_id
               LEFT JOIN accounts a ON a.tg_id=u.tg_id
               WHERE CAST(u.tg_id AS TEXT)=? OR LOWER(u.username)=? OR LOWER(u.username) LIKE ?
                  OR LOWER(u.first_name || ' ' || u.last_name) LIKE ? OR LOWER(u.first_name) LIKE ? OR LOWER(u.last_name) LIKE ?
                  OR LOWER(u.email) LIKE ? OR LOWER(u.phone_number) LIKE ? OR LOWER(a.identifier) LIKE ?
               LIMIT 100""",
            (q, q_no_at, like, like, like, like, like, like, like),
        ).fetchall()
        scored = []
        for r in rows:
            tg_id = int(r["tg_id"])
            profile = {k: str(r[k] or "") for k in ("first_name", "last_name", "username", "language_code", "phone_number", "email")}
            label, name, username = _label_from_profile(tg_id, profile)
            identifiers = [str(x[0]) for x in conn.execute("SELECT identifier FROM accounts WHERE tg_id=?", (tg_id,)).fetchall()]
            low_ids = [x.lower() for x in identifiers]
            score = 9
            if q == str(tg_id): score = 0
            elif q_no_at == username.lower() and username: score = 1
            elif q_no_at in low_ids: score = 2
            elif username.lower().startswith(q_no_at) and username: score = 3
            elif any(x.startswith(q_no_at) for x in low_ids): score = 4
            elif name.lower().startswith(q.lower()) and name: score = 5
            elif q_no_at in username.lower() or any(q_no_at in x for x in low_ids): score = 6
            elif q.lower() in name.lower(): score = 7
            matched_account = next((x for x in identifiers if q_no_at in x.lower()), "")
            scored.append((score, {"tg_id": tg_id, "label": label, "name": name, "username": username, "balance_toman": int(r["balance_toman"] or 0), "profile": profile, "matched_account": matched_account}))
        scored.sort(key=lambda x: (x[0], -x[1]["tg_id"]))
        return [x[1] for x in scored[:max(int(limit), 1)]]
    finally:
        conn.close()


def admin_dashboard_stats(*, day_start_utc: str, month_start_utc: str) -> dict:
    """Return the admin dashboard in one short SQLite session.

    Keeping this as one storage call avoids N+1 reads when the admin panel is
    opened and keeps the Telegram event loop completely non-blocking.
    """
    conn = _connect()
    try:
        counts = {
            "users": int(conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]),
            "accounts": int(conn.execute("SELECT COUNT(*) FROM accounts").fetchone()[0]),
            "pending": int(conn.execute("SELECT COUNT(*) FROM pending_payments").fetchone()[0]),
            "incomplete": int(conn.execute("SELECT COUNT(*) FROM fulfillments WHERE state!='completed'").fetchone()[0]),
            "new_users_today": int(conn.execute("SELECT COUNT(*) FROM users WHERE created_at>=?", (str(day_start_utc),)).fetchone()[0]),
        }
        day = conn.execute(
            """SELECT COUNT(*) tx_count,
                      COALESCE(SUM(gateway_toman + wallet_used_toman),0) revenue_toman,
                      COALESCE(SUM(CASE WHEN action='buy' THEN 1 ELSE 0 END),0) buys,
                      COALESCE(SUM(CASE WHEN action='renew' THEN 1 ELSE 0 END),0) renews,
                      COALESCE(SUM(CASE WHEN service='openvpn' THEN 1 ELSE 0 END),0) openvpn,
                      COALESCE(SUM(CASE WHEN service='v2ray' THEN 1 ELSE 0 END),0) v2ray
               FROM transactions WHERE created_at>=?""",
            (str(day_start_utc),),
        ).fetchone()
        month = conn.execute(
            """SELECT COUNT(*) tx_count,
                      COALESCE(SUM(gateway_toman + wallet_used_toman),0) revenue_toman
               FROM transactions WHERE created_at>=?""",
            (str(month_start_utc),),
        ).fetchone()
        return {
            "counts": counts,
            "today": {k: int(day[k] or 0) for k in day.keys()},
            "month": {k: int(month[k] or 0) for k in month.keys()},
        }
    finally:
        conn.close()


def admin_referral_stats() -> dict:
    conn = _connect()
    try:
        codes = int(conn.execute("SELECT COUNT(*) FROM referrals WHERE code!=''").fetchone()[0])
        used = int(conn.execute("SELECT COUNT(*) FROM referrals WHERE used_order_id!=''").fetchone()[0])
        rewards = int(conn.execute(
            "SELECT COALESCE(SUM(delta_toman),0) FROM wallet_transactions WHERE kind='referral_reward' AND delta_toman>0"
        ).fetchone()[0])
        wallet_total = int(conn.execute("SELECT COALESCE(SUM(balance_toman),0) FROM wallets").fetchone()[0])
        wallet_users = int(conn.execute("SELECT COUNT(*) FROM wallets WHERE balance_toman>0").fetchone()[0])
        return {
            "codes": codes,
            "used": used,
            "reward_toman": rewards,
            "wallet_total_toman": wallet_total,
            "wallet_users": wallet_users,
        }
    finally:
        conn.close()


def list_admin_pending_payments(offset: int = 0, limit: int = 10) -> tuple[list[dict], int]:
    conn = _connect()
    try:
        total = int(conn.execute("SELECT COUNT(*) FROM pending_payments").fetchone()[0])
        rows = conn.execute(
            """SELECT p.rowid pending_id,p.authority,p.tg_id,p.ts,p.first_purchase,p.wallet_used_toman,p.wallet_committed,
                      p.payload_json,p.created_at,p.updated_at,
                      COALESCE(u.first_name,'') first_name,COALESCE(u.last_name,'') last_name,COALESCE(u.username,'') username
               FROM pending_payments p LEFT JOIN users u ON u.tg_id=p.tg_id
               ORDER BY p.ts DESC,p.updated_at DESC,p.rowid DESC LIMIT ? OFFSET ?""",
            (max(int(limit), 1), max(int(offset), 0)),
        ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["payload"] = _json_loads(item.pop("payload_json"), {})
            profile = {k: str(item.get(k) or "") for k in ("first_name", "last_name", "username")}
            item["label"] = _label_from_profile(int(item.get("tg_id") or 0), profile)[0]
            result.append(item)
        return result, total
    finally:
        conn.close()


def get_admin_pending_payment_by_id(pending_id: int) -> dict:
    conn = _connect()
    try:
        row = conn.execute(
            """SELECT p.rowid pending_id,p.authority,p.tg_id,p.ts,p.first_purchase,p.wallet_used_toman,p.wallet_committed,
                      p.payload_json,p.created_at,p.updated_at,
                      COALESCE(u.first_name,'') first_name,COALESCE(u.last_name,'') last_name,COALESCE(u.username,'') username
               FROM pending_payments p LEFT JOIN users u ON u.tg_id=p.tg_id WHERE p.rowid=?""",
            (int(pending_id),),
        ).fetchone()
        if not row:
            return {}
        item = dict(row)
        item["payload"] = _json_loads(item.pop("payload_json"), {})
        profile = {k: str(item.get(k) or "") for k in ("first_name", "last_name", "username")}
        item["label"] = _label_from_profile(int(item.get("tg_id") or 0), profile)[0]
        return item
    finally:
        conn.close()


def get_user_admin_summary(tg_id: int) -> dict:
    conn = _connect()
    try:
        u = conn.execute("SELECT * FROM users WHERE tg_id=?", (int(tg_id),)).fetchone()
        if not u:
            return {}
        profile = {k: str(u[k] or "") for k in ("first_name", "last_name", "username", "language_code", "phone_number", "email")}
        label, name, username = _label_from_profile(int(tg_id), profile)
        wallet = conn.execute("SELECT balance_toman FROM wallets WHERE tg_id=?", (int(tg_id),)).fetchone()
        ref = conn.execute("SELECT code,used_code,referrer_tg_id,used_at,used_order_id FROM referrals WHERE tg_id=?", (int(tg_id),)).fetchone()
        accounts = []
        for r in conn.execute("SELECT service,identifier,data_json FROM accounts WHERE tg_id=? ORDER BY service,id", (int(tg_id),)):
            data = _json_loads(r["data_json"], {})
            accounts.append({"service": r["service"], "identifier": r["identifier"], "is_test": bool(data.get("is_test")) if isinstance(data, dict) else False, "plan_key": str(data.get("plan_key") or "") if isinstance(data, dict) else ""})
        purchase_count = int(conn.execute("SELECT COUNT(*) FROM purchases WHERE tg_id=?", (int(tg_id),)).fetchone()[0])
        tx_count = int(conn.execute("SELECT COUNT(*) FROM transactions WHERE tg_id=?", (int(tg_id),)).fetchone()[0])
        return {
            "tg_id": int(tg_id), "label": label, "name": name, "username": username, "profile": profile,
            "balance_toman": int(wallet[0] if wallet else 0), "reserved_toman": reserved_wallet_for_user(tg_id),
            "referral": {"code": str(ref[0] or "") if ref else "", "used_code": str(ref[1] or "") if ref else "", "referrer_tg_id": int(ref[2] or 0) if ref else 0, "used_at": str(ref[3] or "") if ref else "", "used_order_id": str(ref[4] or "") if ref else ""},
            "accounts": accounts, "purchase_count": purchase_count, "transaction_count": tx_count,
            "tests": {"openvpn": bool(u["test_openvpn"]), "v2ray": bool(u["test_v2ray"])},
            "created_at": str(u["created_at"]), "updated_at": str(u["updated_at"]),
        }
    finally:
        conn.close()


def record_admin_audit(*, admin_tg_id: int, action: str, target_tg_id: int = 0, before=None, after=None, meta=None):
    conn = _connect()
    try:
        conn.execute(
            "INSERT INTO admin_audit(admin_tg_id,target_tg_id,action,before_json,after_json,meta_json,created_at) VALUES(?,?,?,?,?,?,?)",
            (int(admin_tg_id), int(target_tg_id or 0), str(action), _json_dumps(before or {}), _json_dumps(after or {}), _json_dumps(meta or {}), now_iso()),
        )
    finally:
        conn.close()


def list_admin_audit(offset: int = 0, limit: int = 10) -> tuple[list[dict], int]:
    conn = _connect()
    try:
        total = int(conn.execute("SELECT COUNT(*) FROM admin_audit").fetchone()[0])
        rows = []
        for r in conn.execute("SELECT * FROM admin_audit ORDER BY id DESC LIMIT ? OFFSET ?", (max(int(limit), 1), max(int(offset), 0))):
            item = dict(r)
            item["before"] = _json_loads(item.pop("before_json"), {})
            item["after"] = _json_loads(item.pop("after_json"), {})
            item["meta"] = _json_loads(item.pop("meta_json"), {})
            rows.append(item)
        return rows, total
    finally:
        conn.close()


def admin_adjust_wallet(user_tg_id: int, delta_toman: int, *, admin_tg_id: int,
                        operation_id: str = "") -> tuple[int, int]:
    delta = int(delta_toman or 0)
    if not delta:
        raise ValueError("مبلغ تغییر باید بیشتر از صفر باشد.")
    user_tg_id = int(user_tg_id)
    admin_tg_id = int(admin_tg_id)
    operation_id = "".join(ch for ch in str(operation_id or "") if ch.isalnum() or ch in "_-")[:64]
    tx_id = (
        f"admin:{admin_tg_id}:{user_tg_id}:{operation_id}"
        if operation_id
        else f"admin:{admin_tg_id}:{user_tg_id}:{secrets.token_hex(12)}"
    )
    with _tx(immediate=True) as conn:
        _ensure_user(conn, user_tg_id)
        existing = conn.execute(
            "SELECT tg_id,delta_toman,balance_after_toman FROM wallet_transactions WHERE tx_id=?",
            (tx_id,),
        ).fetchone()
        if existing:
            if int(existing["tg_id"]) != user_tg_id or int(existing["delta_toman"]) != delta:
                raise ValueError("شناسه عملیات کیف پول با درخواست دیگری استفاده شده است.")
            after = int(existing["balance_after_toman"])
            return after - delta, after
        before = int(conn.execute("SELECT balance_toman FROM wallets WHERE tg_id=?", (user_tg_id,)).fetchone()[0])
        reserved = int(conn.execute(
            "SELECT COALESCE(SUM(wallet_used_toman),0) FROM pending_payments WHERE tg_id=? AND wallet_committed=0",
            (user_tg_id,),
        ).fetchone()[0])
        after = before + delta
        if after < 0:
            raise ValueError("موجودی کیف پول کافی نیست.")
        if after > MAX_WALLET_BALANCE_TOMAN:
            raise ValueError("موجودی جدید از سقف امن قابل ذخیره‌سازی بیشتر است.")
        if delta < 0 and after < reserved:
            raise ValueError(
                f"حداکثر مبلغ قابل کاهش {max(before-reserved,0):,} تومان است؛ "
                "بخشی از موجودی برای سفارش در انتظار رزرو شده است."
            )
        conn.execute("UPDATE wallets SET balance_toman=? WHERE tg_id=?", (after, user_tg_id))
        conn.execute(
            "INSERT INTO wallet_transactions(tx_id,tg_id,kind,delta_toman,balance_after_toman,note,meta_json,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (tx_id, user_tg_id, "admin_credit" if delta > 0 else "admin_debit", delta, after,
             "تغییر دستی موجودی توسط مدیر", _json_dumps({"admin_tg_id": admin_tg_id}), now_iso()),
        )
        conn.execute(
            "INSERT INTO admin_audit(admin_tg_id,target_tg_id,action,before_json,after_json,meta_json,created_at) VALUES(?,?,?,?,?,?,?)",
            (admin_tg_id, user_tg_id, "wallet_increase" if delta > 0 else "wallet_decrease",
             _json_dumps({"balance_toman": before}), _json_dumps({"balance_toman": after}),
             _json_dumps({"amount_toman": abs(delta), "reserved_toman": reserved}), now_iso()),
        )
    return before, after


# -------------------- v3.2 runtime application settings --------------------

APP_SETTINGS_MIGRATION_KEY = "app_settings_v32_migrated"
APP_SETTINGS_MIGRATION_VERSION = "3.2.1"
_SALES_SETTINGS_V34_MIGRATION_KEY = "app_sales_settings_v34_initialized"
_PAYMENT_SETTINGS_V35_MIGRATION_KEY = "app_payment_settings_v35_initialized"
RESELLERS_V36_MIGRATION_KEY = "resellers_v36_migrated"
FEATURE_TOGGLES_V100_MIGRATION_KEY = "feature_toggles_v100_initialized"
EXPIRY_NOTIFICATIONS_V101_MIGRATION_KEY = "expiry_notifications_v101_initialized"
_APP_SETTING_KEYS = {
    "bot_brand_name",
    "account_username_prefix",
    "referral_code_prefix",
    "openvpn_connections_url",
    "api_ip",
    "api_port",
    "api_user",
    "api_pass",
    "um_scheme",
    "um_path",
    "um_host_legacy",
    "um_port_legacy",
    "xui_api_token",
    "xui_scheme",
    "xui_host",
    "xui_port",
    "xui_base_path",
    "xui_verify_tls",
    "xui_sub_public_base",
    "zarinpal_sandbox",
    "zarinpal_merchant_id",
    "zarinpal_enabled",
    "card_transfer_enabled",
    "card_transfer_card_number",
    "card_transfer_card_holder",
    "openvpn_sales_enabled",
    "v2ray_sales_enabled",
    "referral_enabled",
    "wallet_enabled",
    "account_expiry_notifications_enabled",
    "account_expiry_check_interval_minutes",
}
_SECRET_APP_SETTING_KEYS = {"api_pass", "xui_api_token", "card_transfer_card_number"}


def _app_settings_state_from_conn(conn: sqlite3.Connection) -> dict:
    settings = {}
    for row in conn.execute("SELECT key,value_json FROM app_settings"):
        settings[str(row["key"])] = _json_loads(row["value_json"], None)
    admins = tuple(
        int(row["tg_id"])
        for row in conn.execute("SELECT tg_id FROM bot_admins ORDER BY created_at,tg_id")
    )
    inbounds = tuple(
        (int(row["id"]), str(row["remark"]))
        for row in conn.execute(
            "SELECT id,remark FROM xui_inbounds ORDER BY sort_order,id"
        )
    )
    resellers = tuple(
        (
            int(row["id"]), int(row["tg_id"]), str(row["name"]),
            int(row["price_per_gb_toman"]), int(row["debt_toman"]),
            str(row["created_at"]), str(row["updated_at"]),
            bool(row["trial_enabled"]),
        )
        for row in conn.execute(
            """SELECT id,tg_id,name,price_per_gb_toman,debt_toman,created_at,updated_at,
                      trial_enabled
               FROM resellers WHERE deleted_at='' ORDER BY created_at,id"""
        )
    )
    marker = conn.execute(
        "SELECT value FROM meta WHERE key=?", (APP_SETTINGS_MIGRATION_KEY,)
    ).fetchone()
    return {
        "settings": settings,
        "admins": admins,
        "inbounds": inbounds,
        "resellers": resellers,
        "migration_version": str(marker[0]) if marker else "",
    }


def initialize_app_settings(
    seed_settings: dict,
    *,
    extra_admin_ids=(),
    inbound_remarks=(),
) -> dict:
    """Atomically perform the one-time v3.2 ENV-to-SQLite migration.

    The marker, scalar settings, additional admins and individual inbound rows
    commit together. Once the marker exists, every supplied ENV seed is ignored.
    Existing business/user tables are never touched by this migration.
    """
    clean_seed = {
        str(key): value
        for key, value in dict(seed_settings or {}).items()
        if str(key) in _APP_SETTING_KEYS
    }
    with _tx(immediate=True) as conn:
        marker = conn.execute(
            "SELECT value FROM meta WHERE key=?", (APP_SETTINGS_MIGRATION_KEY,)
        ).fetchone()
        if marker:
            return _app_settings_state_from_conn(conn)

        ts = now_iso()
        for key, value in clean_seed.items():
            conn.execute(
                "INSERT OR IGNORE INTO app_settings(key,value_json,updated_at) VALUES(?,?,?)",
                (key, _json_dumps(value), ts),
            )

        for raw_tg_id in extra_admin_ids or ():
            try:
                tg_id = int(raw_tg_id)
            except Exception:
                continue
            if tg_id > 0:
                conn.execute(
                    "INSERT OR IGNORE INTO bot_admins(tg_id,created_at,created_by) VALUES(?,?,0)",
                    (tg_id, ts),
                )

        if int(conn.execute("SELECT COUNT(*) FROM xui_inbounds").fetchone()[0]) == 0:
            seen = set()
            for index, raw_remark in enumerate(inbound_remarks or ()):
                remark = str(raw_remark or "").strip()
                folded = remark.casefold()
                if not remark or folded in seen:
                    continue
                seen.add(folded)
                conn.execute(
                    "INSERT OR IGNORE INTO xui_inbounds(remark,sort_order,created_at,updated_at) VALUES(?,?,?,?)",
                    (remark, index * 10, ts, ts),
                )

        conn.execute(
            "INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)",
            (APP_SETTINGS_MIGRATION_KEY, APP_SETTINGS_MIGRATION_VERSION),
        )
        conn.execute(
            "INSERT OR REPLACE INTO meta(key,value) VALUES('app_settings_v32_migrated_at',?)",
            (ts,),
        )
        return _app_settings_state_from_conn(conn)


def get_app_settings_state() -> dict:
    conn = _connect()
    try:
        return _app_settings_state_from_conn(conn)
    finally:
        conn.close()


def initialize_v34_sales_settings(defaults: dict | None = None) -> dict:
    """Add v3.4 sale switches to an already-migrated v3.2/v3.3 database.

    Only missing rows are inserted. Existing Admin choices can therefore never
    be overwritten, including when startup is retried after a partial failure.
    """
    supplied = dict(defaults or {})
    values = {
        "openvpn_sales_enabled": bool(supplied.get("openvpn_sales_enabled", True)),
        "v2ray_sales_enabled": bool(supplied.get("v2ray_sales_enabled", True)),
    }
    with _tx(immediate=True) as conn:
        marker = conn.execute(
            "SELECT value FROM meta WHERE key=?",
            (_SALES_SETTINGS_V34_MIGRATION_KEY,),
        ).fetchone()
        if not marker:
            ts = now_iso()
            for key, value in values.items():
                conn.execute(
                    "INSERT OR IGNORE INTO app_settings(key,value_json,updated_at) VALUES(?,?,?)",
                    (key, _json_dumps(value), ts),
                )
            conn.execute(
                "INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)",
                (_SALES_SETTINGS_V34_MIGRATION_KEY, "3.4.0"),
            )
            conn.execute(
                "INSERT OR REPLACE INTO meta(key,value) VALUES('app_sales_settings_v34_initialized_at',?)",
                (ts,),
            )
        return _app_settings_state_from_conn(conn)


def initialize_v35_payment_settings(defaults: dict | None = None) -> dict:
    """Add v3.5 gateway switches/details without overwriting Admin choices."""
    supplied = dict(defaults or {})
    values = {
        "zarinpal_enabled": bool(supplied.get("zarinpal_enabled", True)),
        "card_transfer_enabled": bool(supplied.get("card_transfer_enabled", False)),
        "card_transfer_card_number": str(supplied.get("card_transfer_card_number", "") or ""),
        "card_transfer_card_holder": str(supplied.get("card_transfer_card_holder", "") or ""),
    }
    with _tx(immediate=True) as conn:
        marker = conn.execute(
            "SELECT value FROM meta WHERE key=?",
            (_PAYMENT_SETTINGS_V35_MIGRATION_KEY,),
        ).fetchone()
        if not marker:
            ts = now_iso()
            for key, value in values.items():
                conn.execute(
                    "INSERT OR IGNORE INTO app_settings(key,value_json,updated_at) VALUES(?,?,?)",
                    (key, _json_dumps(value), ts),
                )
            conn.execute(
                "INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)",
                (_PAYMENT_SETTINGS_V35_MIGRATION_KEY, "3.5.0"),
            )
            conn.execute(
                "INSERT OR REPLACE INTO meta(key,value) VALUES('app_payment_settings_v35_initialized_at',?)",
                (ts,),
            )
        return _app_settings_state_from_conn(conn)


def initialize_feature_toggles(defaults: dict | None = None) -> dict:
    """Persist referral/wallet switches without changing existing data.

    Both features default to enabled so upgrading an existing installation
    preserves its established behavior. INSERT OR IGNORE also makes the
    migration safe to repeat and keeps every prior Admin choice authoritative.
    """
    supplied = dict(defaults or {})
    values = {
        "referral_enabled": bool(supplied.get("referral_enabled", True)),
        "wallet_enabled": bool(supplied.get("wallet_enabled", True)),
    }
    with _tx(immediate=True) as conn:
        marker = conn.execute(
            "SELECT value FROM meta WHERE key=?",
            (FEATURE_TOGGLES_V100_MIGRATION_KEY,),
        ).fetchone()
        if not marker:
            ts = now_iso()
            for key, value in values.items():
                conn.execute(
                    "INSERT OR IGNORE INTO app_settings(key,value_json,updated_at) VALUES(?,?,?)",
                    (key, _json_dumps(value), ts),
                )
            conn.execute(
                "INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)",
                (FEATURE_TOGGLES_V100_MIGRATION_KEY, "1.0.0"),
            )
            conn.execute(
                "INSERT OR REPLACE INTO meta(key,value) VALUES('feature_toggles_v100_initialized_at',?)",
                (ts,),
            )
        return _app_settings_state_from_conn(conn)


def initialize_v101_expiry_notifications(defaults: dict | None = None) -> dict:
    """Add v1.0.1 monitor settings without overwriting later Admin choices."""
    supplied = dict(defaults or {})
    values = {
        "account_expiry_notifications_enabled": bool(
            supplied.get("account_expiry_notifications_enabled", True)
        ),
        "account_expiry_check_interval_minutes": int(
            supplied.get("account_expiry_check_interval_minutes", 30)
        ),
    }
    with _tx(immediate=True) as conn:
        marker = conn.execute(
            "SELECT value FROM meta WHERE key=?",
            (EXPIRY_NOTIFICATIONS_V101_MIGRATION_KEY,),
        ).fetchone()
        if not marker:
            ts = now_iso()
            for key, value in values.items():
                conn.execute(
                    "INSERT OR IGNORE INTO app_settings(key,value_json,updated_at) VALUES(?,?,?)",
                    (key, _json_dumps(value), ts),
                )
            conn.execute(
                "INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)",
                (EXPIRY_NOTIFICATIONS_V101_MIGRATION_KEY, "1.0.1"),
            )
            conn.execute(
                "INSERT OR REPLACE INTO meta(key,value) VALUES('expiry_notifications_v101_initialized_at',?)",
                (ts,),
            )
        return _app_settings_state_from_conn(conn)


def initialize_v36_resellers(*, root_admin_id: int = 0, env_admin_ids=()) -> dict:
    """One-time conversion of former dynamic Admin IDs into resellers.

    A migrated record deliberately starts with a zero per-GB rate. The runtime
    blocks purchases at that rate until the sole ENV Admin sets a positive
    value, preventing accidental zero-debt provisioning after an upgrade.
    """
    root = int(root_admin_id or 0)
    with _tx(immediate=True) as conn:
        marker = conn.execute(
            "SELECT value FROM meta WHERE key=?", (RESELLERS_V36_MIGRATION_KEY,)
        ).fetchone()
        if not marker:
            legacy_ids = [
                int(row[0]) for row in conn.execute(
                    "SELECT tg_id FROM bot_admins ORDER BY created_at,tg_id"
                )
            ]
            for raw in env_admin_ids or ():
                try:
                    legacy_ids.append(int(raw))
                except Exception:
                    continue
            ts = now_iso()
            seen = set()
            for tg_id in legacy_ids:
                if tg_id <= 0 or tg_id == root or tg_id in seen:
                    continue
                seen.add(tg_id)
                conn.execute(
                    """INSERT OR IGNORE INTO resellers(
                           tg_id,name,price_per_gb_toman,debt_toman,created_by,
                           created_at,updated_at,deleted_at
                       ) VALUES(?,?,0,0,0,?,?,'')""",
                    (tg_id, f"Reseller {tg_id}", ts, ts),
                )
            # Dynamic Admin authorization must disappear in the same commit as
            # reseller creation. Only the first valid ENV ID remains Admin.
            conn.execute("DELETE FROM bot_admins")
            conn.execute(
                "INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)",
                (RESELLERS_V36_MIGRATION_KEY, "3.6.0"),
            )
            conn.execute(
                "INSERT OR REPLACE INTO meta(key,value) VALUES('resellers_v36_migrated_at',?)",
                (ts,),
            )
        return _app_settings_state_from_conn(conn)


def app_settings_migration_version() -> str:
    conn = _connect()
    try:
        row = conn.execute(
            "SELECT value FROM meta WHERE key=?", (APP_SETTINGS_MIGRATION_KEY,)
        ).fetchone()
        return str(row[0]) if row else ""
    finally:
        conn.close()


def set_app_settings(values: dict, *, admin_tg_id: int = 0) -> dict:
    """Commit one or more settings and their audit rows atomically."""
    clean = {str(key or "").strip(): value for key, value in dict(values or {}).items()}
    if not clean or any(key not in _APP_SETTING_KEYS for key in clean):
        raise ValueError("تنظیم درخواستی قابل ذخیره نیست")
    with _tx(immediate=True) as conn:
        ts = now_iso()
        for key, value in clean.items():
            before_row = conn.execute(
                "SELECT value_json FROM app_settings WHERE key=?", (key,)
            ).fetchone()
            before = _json_loads(before_row[0], None) if before_row else None
            conn.execute(
                """INSERT INTO app_settings(key,value_json,updated_at) VALUES(?,?,?)
                   ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at""",
                (key, _json_dumps(value), ts),
            )
            if admin_tg_id and before != value:
                if key in _SECRET_APP_SETTING_KEYS:
                    before_json = after_json = _json_dumps({})
                else:
                    before_json = _json_dumps({"value": before})
                    after_json = _json_dumps({"value": value})
                conn.execute(
                    """INSERT INTO admin_audit(admin_tg_id,target_tg_id,action,before_json,after_json,meta_json,created_at)
                       VALUES(?,?,?,?,?,?,?)""",
                    (
                        int(admin_tg_id), 0, f"{key.upper()} updated",
                        before_json, after_json, _json_dumps({"setting": key}), ts,
                    ),
                )
        return _app_settings_state_from_conn(conn)


def set_app_setting(key: str, value, *, admin_tg_id: int = 0) -> dict:
    return set_app_settings({key: value}, admin_tg_id=admin_tg_id)


# -------------------- v3.6 resellers and debt ledger --------------------

def _reseller_name(value) -> str:
    result = str(value or "").strip()
    if not result or len(result) > 100:
        raise ValueError("نام ریسلر باید بین 1 تا 100 کاراکتر باشد")
    return result


def _reseller_tg_id(value) -> int:
    try:
        result = int(str(value).strip())
    except Exception as exc:
        raise ValueError("Telegram ID باید فقط یک عدد مثبت باشد") from exc
    if result <= 0 or result > 9_223_372_036_854_775_807:
        raise ValueError("Telegram ID باید فقط یک عدد مثبت باشد")
    return result


def _reseller_rate(value, *, allow_zero: bool = False) -> int:
    try:
        result = int(str(value).replace(",", "").replace("٬", "").strip())
    except Exception as exc:
        raise ValueError("هزینه هر گیگ باید یک عدد صحیح به تومان باشد") from exc
    minimum = 0 if allow_zero else 1
    if result < minimum or result > MAX_RESELLER_DEBT_TOMAN:
        raise ValueError("هزینه هر گیگ باید یک عدد مثبت و معتبر به تومان باشد")
    return result


def _reseller_item(row) -> dict:
    return dict(row) if row else {}


def get_reseller_by_tg_id(tg_id: int, *, include_deleted: bool = False) -> dict:
    conn = _connect()
    try:
        suffix = "" if include_deleted else " AND deleted_at=''"
        return _reseller_item(conn.execute(
            f"SELECT * FROM resellers WHERE tg_id=?{suffix}", (int(tg_id),)
        ).fetchone())
    finally:
        conn.close()


def add_reseller(
    *, name: str, tg_id: int, price_per_gb_toman: int,
    admin_tg_id: int, protected_tg_id: int = 0, trial_enabled: bool = True,
) -> dict:
    clean_name = _reseller_name(name)
    clean_tg_id = _reseller_tg_id(tg_id)
    rate = _reseller_rate(price_per_gb_toman)
    trial_access = int(bool(trial_enabled))
    if protected_tg_id and clean_tg_id == int(protected_tg_id):
        raise ValueError("مدیر اصلی نمی‌تواند ریسلر باشد")
    with _tx(immediate=True) as conn:
        old = conn.execute(
            "SELECT * FROM resellers WHERE tg_id=?", (clean_tg_id,)
        ).fetchone()
        if old and not str(old["deleted_at"] or ""):
            raise ValueError("این Telegram ID از قبل ریسلر است")
        ts = now_iso()
        if old:
            conn.execute(
                """UPDATE resellers SET name=?,price_per_gb_toman=?,trial_enabled=?,deleted_at='',
                       updated_at=?,created_by=? WHERE id=?""",
                (
                    clean_name, rate, trial_access, ts,
                    int(admin_tg_id or 0), int(old["id"]),
                ),
            )
            reseller_id = int(old["id"])
            before = dict(old)
        else:
            cur = conn.execute(
                """INSERT INTO resellers(
                       tg_id,name,price_per_gb_toman,debt_toman,trial_enabled,created_by,
                       created_at,updated_at,deleted_at
                   ) VALUES(?,?,?,0,?,?,?,?,'')""",
                (
                    clean_tg_id, clean_name, rate, trial_access,
                    int(admin_tg_id or 0), ts, ts,
                ),
            )
            reseller_id = int(cur.lastrowid)
            before = {}
        after = dict(conn.execute(
            "SELECT * FROM resellers WHERE id=?", (reseller_id,)
        ).fetchone())
        conn.execute(
            """INSERT INTO admin_audit(
                   admin_tg_id,target_tg_id,action,before_json,after_json,meta_json,created_at
               ) VALUES(?,?,?,?,?,?,?)""",
            (
                int(admin_tg_id or 0), clean_tg_id, "RESELLER added",
                _json_dumps(before), _json_dumps(after),
                _json_dumps({"reseller_id": reseller_id}), ts,
            ),
        )
        return _app_settings_state_from_conn(conn)


def update_reseller(
    reseller_id: int, *, admin_tg_id: int, protected_tg_id: int = 0,
    name=None, tg_id=None, price_per_gb_toman=None, trial_enabled=None,
) -> dict:
    reseller_id = int(reseller_id)
    with _tx(immediate=True) as conn:
        row = conn.execute(
            "SELECT * FROM resellers WHERE id=? AND deleted_at=''", (reseller_id,)
        ).fetchone()
        if not row:
            raise ValueError("ریسلر پیدا نشد")
        new_name = _reseller_name(row["name"] if name is None else name)
        new_tg_id = _reseller_tg_id(row["tg_id"] if tg_id is None else tg_id)
        new_rate = _reseller_rate(
            row["price_per_gb_toman"] if price_per_gb_toman is None else price_per_gb_toman
        )
        new_trial_enabled = int(
            bool(row["trial_enabled"])
            if trial_enabled is None else bool(trial_enabled)
        )
        if protected_tg_id and new_tg_id == int(protected_tg_id):
            raise ValueError("مدیر اصلی نمی‌تواند ریسلر باشد")
        duplicate = conn.execute(
            "SELECT 1 FROM resellers WHERE tg_id=? AND id!=?",
            (new_tg_id, reseller_id),
        ).fetchone()
        if duplicate:
            raise ValueError("این Telegram ID قبلاً ثبت شده است")
        before = dict(row)
        ts = now_iso()
        conn.execute(
            """UPDATE resellers SET name=?,tg_id=?,price_per_gb_toman=?,trial_enabled=?,updated_at=?
               WHERE id=?""",
            (new_name, new_tg_id, new_rate, new_trial_enabled, ts, reseller_id),
        )
        after = dict(conn.execute(
            "SELECT * FROM resellers WHERE id=?", (reseller_id,)
        ).fetchone())
        if before != after:
            conn.execute(
                """INSERT INTO admin_audit(
                       admin_tg_id,target_tg_id,action,before_json,after_json,meta_json,created_at
                   ) VALUES(?,?,?,?,?,?,?)""",
                (
                    int(admin_tg_id or 0), new_tg_id, "RESELLER updated",
                    _json_dumps(before), _json_dumps(after),
                    _json_dumps({"reseller_id": reseller_id}), ts,
                ),
            )
        return _app_settings_state_from_conn(conn)


def delete_reseller(
    reseller_id: int, *, admin_tg_id: int, protected_tg_id: int = 0,
) -> dict:
    reseller_id = int(reseller_id)
    with _tx(immediate=True) as conn:
        row = conn.execute(
            "SELECT * FROM resellers WHERE id=? AND deleted_at=''", (reseller_id,)
        ).fetchone()
        if not row:
            raise ValueError("ریسلر پیدا نشد")
        if protected_tg_id and int(row["tg_id"]) == int(protected_tg_id):
            raise ValueError("مدیر اصلی قابل حذف نیست")
        ts = now_iso()
        conn.execute(
            "UPDATE resellers SET deleted_at=?,updated_at=? WHERE id=?",
            (ts, ts, reseller_id),
        )
        conn.execute(
            """INSERT INTO admin_audit(
                   admin_tg_id,target_tg_id,action,before_json,after_json,meta_json,created_at
               ) VALUES(?,?,?,?,?,?,?)""",
            (
                int(admin_tg_id or 0), int(row["tg_id"]), "RESELLER removed",
                _json_dumps(dict(row)), _json_dumps({"deleted_at": ts}),
                _json_dumps({"reseller_id": reseller_id}), ts,
            ),
        )
        return _app_settings_state_from_conn(conn)


def set_reseller_debt(
    reseller_id: int, debt_toman: int, *, admin_tg_id: int,
    operation_id: str = "",
) -> tuple[int, int]:
    reseller_id = int(reseller_id)
    target = _reseller_rate(debt_toman, allow_zero=True)
    operation = str(operation_id or f"reseller-debt-{secrets.token_hex(12)}")
    with _tx(immediate=True) as conn:
        old_entry = conn.execute(
            """SELECT reseller_id,before_toman,after_toman FROM reseller_debt_entries
               WHERE operation_id=?""", (operation,),
        ).fetchone()
        if old_entry:
            if int(old_entry["reseller_id"]) != reseller_id:
                raise ValueError("شناسه عملیات بدهی قبلاً استفاده شده است")
            return int(old_entry["before_toman"]), int(old_entry["after_toman"])
        row = conn.execute(
            "SELECT * FROM resellers WHERE id=? AND deleted_at=''", (reseller_id,)
        ).fetchone()
        if not row:
            raise ValueError("ریسلر پیدا نشد")
        before = int(row["debt_toman"] or 0)
        delta = target - before
        ts = now_iso()
        conn.execute(
            "UPDATE resellers SET debt_toman=?,updated_at=? WHERE id=?",
            (target, ts, reseller_id),
        )
        conn.execute(
            """INSERT INTO reseller_debt_entries(
                   reseller_id,operation_id,kind,delta_toman,before_toman,after_toman,
                   admin_tg_id,meta_json,created_at
               ) VALUES(?,?,'adjustment',?,?,?,?,?,?)""",
            (
                reseller_id, operation, delta, before, target,
                int(admin_tg_id or 0), _json_dumps({}), ts,
            ),
        )
        conn.execute(
            """INSERT INTO admin_audit(
                   admin_tg_id,target_tg_id,action,before_json,after_json,meta_json,created_at
               ) VALUES(?,?,?,?,?,?,?)""",
            (
                int(admin_tg_id or 0), int(row["tg_id"]), "RESELLER debt updated",
                _json_dumps({"debt_toman": before}),
                _json_dumps({"debt_toman": target}),
                _json_dumps({"reseller_id": reseller_id, "operation_id": operation}), ts,
            ),
        )
        return before, target


def create_reseller_pending(authority: str, payload: dict) -> dict:
    """Validate the active reseller and persist an authorized local order."""
    authority = str(authority or "").strip()
    payload = dict(payload or {})
    if not authority:
        raise ValueError("شناسه سفارش ریسلر ناقص است")
    tg_id = _reseller_tg_id(payload.get("tg_id"))
    plan = payload.get("plan_snapshot") if isinstance(payload.get("plan_snapshot"), dict) else {}
    gb = int(plan.get("gb") or 0)
    if gb <= 0:
        raise ValueError("حجم بسته ریسلر معتبر نیست")
    with _tx(immediate=True) as conn:
        reseller = conn.execute(
            "SELECT * FROM resellers WHERE tg_id=? AND deleted_at=''", (tg_id,)
        ).fetchone()
        if not reseller:
            raise ValueError("دسترسی ریسلر فعال نیست")
        rate = int(reseller["price_per_gb_toman"] or 0)
        if rate <= 0:
            raise ValueError("هزینه هر گیگ این ریسلر هنوز توسط مدیر تنظیم نشده است")
        charge = gb * rate
        if charge <= 0 or charge > MAX_RESELLER_DEBT_TOMAN:
            raise ValueError("مبلغ بدهی این سفارش معتبر نیست")
        payload.update({
            "payment_kind": "reseller_debt",
            "payment_authorized": True,
            "payment_authorization_method": "reseller_debt",
            "payment_authorized_at": now_iso(),
            "wallet_used_toman": 0,
            "gateway_toman": 0,
            "amount_rial": 0,
            "wallet_committed": False,
            "referral_code": "",
            "referrer_tg_id": 0,
            "referral_discount_toman": 0,
            "reseller_id": int(reseller["id"]),
            "reseller_name": str(reseller["name"]),
            "reseller_tg_id": tg_id,
            "reseller_price_per_gb_toman": rate,
            "reseller_gb": gb,
            "reseller_charge_toman": charge,
        })
        ts = now_iso()
        _ensure_user(conn, tg_id)
        conn.execute(
            """INSERT OR REPLACE INTO pending_payments(
                   authority,tg_id,ts,first_purchase,wallet_used_toman,wallet_committed,
                   payload_json,created_at,updated_at
               ) VALUES(?,?,?,?,0,0,?,?,?)""",
            (
                authority, tg_id, int(payload.get("ts") or 0),
                int(bool(payload.get("first_purchase"))), _json_dumps(payload), ts, ts,
            ),
        )
        return payload


def record_reseller_debt_charge(payload: dict) -> dict:
    """Add a fulfilled reseller order to debt exactly once."""
    payload = dict(payload or {})
    order_id = str(payload.get("order_id") or "").strip()
    reseller_id = int(payload.get("reseller_id") or 0)
    gb = int(payload.get("reseller_gb") or 0)
    rate = int(payload.get("reseller_price_per_gb_toman") or 0)
    charge = int(payload.get("reseller_charge_toman") or 0)
    if not order_id or reseller_id <= 0 or gb <= 0 or rate <= 0 or charge != gb * rate:
        raise ValueError("اطلاعات بدهی سفارش ریسلر ناقص یا نامعتبر است")
    operation = f"reseller-order:{order_id}"
    with _tx(immediate=True) as conn:
        old = conn.execute(
            """SELECT before_toman,after_toman,delta_toman FROM reseller_debt_entries
               WHERE operation_id=?""", (operation,),
        ).fetchone()
        if old:
            return {
                "added_toman": int(old["delta_toman"]),
                "before_toman": int(old["before_toman"]),
                "after_toman": int(old["after_toman"]),
            }
        reseller = conn.execute(
            "SELECT * FROM resellers WHERE id=?", (reseller_id,)
        ).fetchone()
        if not reseller:
            raise ValueError("ریسلر این سفارش پیدا نشد")
        before = int(reseller["debt_toman"] or 0)
        after = before + charge
        if after > MAX_RESELLER_DEBT_TOMAN:
            raise ValueError("بدهی ریسلر از سقف مجاز عبور می‌کند")
        ts = now_iso()
        conn.execute(
            "UPDATE resellers SET debt_toman=?,updated_at=? WHERE id=?",
            (after, ts, reseller_id),
        )
        conn.execute(
            """INSERT INTO reseller_debt_entries(
                   reseller_id,operation_id,order_id,kind,delta_toman,before_toman,
                   after_toman,service,plan_key,gb,price_per_gb_toman,meta_json,created_at
               ) VALUES(?,?,?,'purchase',?,?,?,?,?,?,?,?,?)""",
            (
                reseller_id, operation, order_id, charge, before, after,
                str(payload.get("service") or ""), str(payload.get("plan_key") or ""),
                gb, rate, _json_dumps({"action": str(payload.get("action") or "")}), ts,
            ),
        )
        return {"added_toman": charge, "before_toman": before, "after_toman": after}


def add_xui_inbound(remark: str, *, admin_tg_id: int = 0) -> dict:
    remark = str(remark or "").strip()
    if not remark:
        raise ValueError("نام Inbound نمی‌تواند خالی باشد")
    if len(remark) > 128:
        raise ValueError("نام Inbound نباید بیشتر از 128 کاراکتر باشد")
    with _tx(immediate=True) as conn:
        if conn.execute(
            "SELECT 1 FROM xui_inbounds WHERE remark=? COLLATE NOCASE", (remark,)
        ).fetchone():
            raise ValueError("این Inbound قبلاً ثبت شده است")
        sort_order = int(
            conn.execute("SELECT COALESCE(MAX(sort_order),-10)+10 FROM xui_inbounds").fetchone()[0]
        )
        ts = now_iso()
        cur = conn.execute(
            "INSERT INTO xui_inbounds(remark,sort_order,created_at,updated_at) VALUES(?,?,?,?)",
            (remark, sort_order, ts, ts),
        )
        if admin_tg_id:
            conn.execute(
                """INSERT INTO admin_audit(admin_tg_id,target_tg_id,action,before_json,after_json,meta_json,created_at)
                   VALUES(?,?,?,?,?,?,?)""",
                (
                    int(admin_tg_id), 0, "XUI_INBOUND added", _json_dumps({}),
                    _json_dumps({"id": int(cur.lastrowid), "remark": remark}), _json_dumps({}), ts,
                ),
            )
        return _app_settings_state_from_conn(conn)


def rename_xui_inbound(inbound_id: int, remark: str, *, admin_tg_id: int = 0) -> dict:
    inbound_id = int(inbound_id)
    remark = str(remark or "").strip()
    if not remark:
        raise ValueError("نام Inbound نمی‌تواند خالی باشد")
    if len(remark) > 128:
        raise ValueError("نام Inbound نباید بیشتر از 128 کاراکتر باشد")
    with _tx(immediate=True) as conn:
        row = conn.execute("SELECT remark FROM xui_inbounds WHERE id=?", (inbound_id,)).fetchone()
        if not row:
            raise ValueError("Inbound پیدا نشد")
        duplicate = conn.execute(
            "SELECT 1 FROM xui_inbounds WHERE remark=? COLLATE NOCASE AND id!=?",
            (remark, inbound_id),
        ).fetchone()
        if duplicate:
            raise ValueError("این Inbound قبلاً ثبت شده است")
        old = str(row[0])
        ts = now_iso()
        conn.execute(
            "UPDATE xui_inbounds SET remark=?,updated_at=? WHERE id=?",
            (remark, ts, inbound_id),
        )
        if admin_tg_id and old != remark:
            conn.execute(
                """INSERT INTO admin_audit(admin_tg_id,target_tg_id,action,before_json,after_json,meta_json,created_at)
                   VALUES(?,?,?,?,?,?,?)""",
                (
                    int(admin_tg_id), 0, "XUI_INBOUND renamed",
                    _json_dumps({"id": inbound_id, "remark": old}),
                    _json_dumps({"id": inbound_id, "remark": remark}), _json_dumps({}), ts,
                ),
            )
        return _app_settings_state_from_conn(conn)


def delete_xui_inbound(inbound_id: int, *, admin_tg_id: int = 0) -> dict:
    inbound_id = int(inbound_id)
    with _tx(immediate=True) as conn:
        row = conn.execute("SELECT remark FROM xui_inbounds WHERE id=?", (inbound_id,)).fetchone()
        if not row:
            raise ValueError("Inbound پیدا نشد")
        remark = str(row[0])
        conn.execute("DELETE FROM xui_inbounds WHERE id=?", (inbound_id,))
        ts = now_iso()
        if admin_tg_id:
            conn.execute(
                """INSERT INTO admin_audit(admin_tg_id,target_tg_id,action,before_json,after_json,meta_json,created_at)
                   VALUES(?,?,?,?,?,?,?)""",
                (
                    int(admin_tg_id), 0, "XUI_INBOUND deleted",
                    _json_dumps({"id": inbound_id, "remark": remark}),
                    _json_dumps({}), _json_dumps({}), ts,
                ),
            )
        return _app_settings_state_from_conn(conn)


# -------------------- Dynamic sale plans / financial settings --------------------

_MAX_PLAN_GB = 1_000_000
_MAX_PLAN_MONTHS = 1_200
_MAX_PLAN_PRICE_TOMAN = 1_000_000_000_000
_MAX_PLAN_PROFILE_CHARS = 128
_MAX_PLAN_COUNT = 90


_SALE_PLAN_SERVICES = frozenset({"openvpn", "v2ray"})
_SERVICE_SALE_PLANS_MIGRATION_KEY = "service_sale_plans_v34_migrated"


def _sale_plan_service(value: str) -> str:
    service = str(value or "").strip().lower()
    if service not in _SALE_PLAN_SERVICES:
        raise ValueError("سرویس بسته نامعتبر است")
    return service


def _validated_sale_plan(*, service: str = "openvpn", gb: int, months: int,
                         price_toman: int, openvpn_profile: str = "",
                         days: int | None = None) -> dict:
    service = _sale_plan_service(service)
    gb = int(gb)
    months = int(months)
    price_toman = int(price_toman)
    profile = str(openvpn_profile or "").strip()
    if gb <= 0 or gb > _MAX_PLAN_GB:
        raise ValueError(f"حجم بسته باید بین 1 و {_MAX_PLAN_GB:,} گیگ باشد")
    if months < 0 or months > _MAX_PLAN_MONTHS:
        raise ValueError(f"مدت بسته نامعتبر است")
    if days is None:
        if months <= 0:
            raise ValueError("مدت بسته باید حداقل یک ماه باشد")
        days = months * 30
    days = int(days)
    if days <= 0 or days > 36_500:
        raise ValueError("تعداد روز بسته نامعتبر است")
    if price_toman <= 0 or price_toman > _MAX_PLAN_PRICE_TOMAN:
        raise ValueError(f"قیمت بسته باید بین 1 و {_MAX_PLAN_PRICE_TOMAN:,} تومان باشد")
    if service == "openvpn" and not profile:
        raise ValueError("نام دقیق پکیج MikroTik نمی‌تواند خالی باشد")
    if len(profile) > _MAX_PLAN_PROFILE_CHARS:
        raise ValueError(f"نام پکیج MikroTik نباید بیشتر از {_MAX_PLAN_PROFILE_CHARS} کاراکتر باشد")
    return {
        "gb": gb,
        "months": months,
        "days": days,
        "price_toman": price_toman,
        "openvpn_profile": profile if service == "openvpn" else "",
    }


def initialize_sale_plans(seed_plans: list[dict] | None = None) -> dict:
    """One-time v3 migration from legacy PLAN_* ENV values into SQLite.

    Once the marker is written, ENV is never used to repopulate the table. This
    is important because deleting all packages in the admin panel must remain a
    deliberate persistent state after a service restart.
    """
    seed_plans = list(seed_plans or [])
    with _tx(immediate=True) as conn:
        marker = conn.execute("SELECT value FROM meta WHERE key='sale_plans_v3_initialized'").fetchone()
        if marker:
            return {"initialized": True, "migrated": 0}
        existing = int(conn.execute("SELECT COUNT(*) FROM sale_plans").fetchone()[0])
        migrated = 0
        if existing == 0:
            for idx, raw in enumerate(seed_plans):
                key = str(raw.get("plan_key") or "").strip()
                if not key:
                    continue
                data = _validated_sale_plan(
                    service="openvpn",
                    gb=int(raw.get("gb") or 0),
                    months=int(raw.get("months") or 0),
                    days=int(raw.get("days") or 0),
                    price_toman=int(raw.get("price_toman") or 0),
                    openvpn_profile=str(raw.get("openvpn_profile") or ""),
                )
                conn.execute(
                    """INSERT OR IGNORE INTO sale_plans(plan_key,gb,months,days,price_toman,openvpn_profile,sort_order,created_at,updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?)""",
                    (key, data["gb"], data["months"], data["days"], data["price_toman"], data["openvpn_profile"], idx * 10, now_iso(), now_iso()),
                )
                migrated += int(conn.execute("SELECT changes()").fetchone()[0] or 0)
        conn.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('sale_plans_v3_initialized','1')")
        conn.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('sale_plans_v3_initialized_at',?)", (now_iso(),))
        return {"initialized": True, "migrated": migrated}


def initialize_service_sale_plans() -> dict:
    """Split every historical shared package into two independent v3.4 rows.

    The old ``sale_plans`` table is deliberately retained.  That keeps the
    migration non-destructive and means a v3.3 database can be restored without
    losing its original package definitions.  The durable marker makes the copy
    idempotent, so later restarts never recreate packages deleted by an admin.
    """
    with _tx(immediate=True) as conn:
        marker = conn.execute(
            "SELECT value FROM meta WHERE key=?",
            (_SERVICE_SALE_PLANS_MIGRATION_KEY,),
        ).fetchone()
        if marker:
            return {"initialized": True, "migrated": 0}
        rows = conn.execute(
            """SELECT plan_key,gb,months,days,price_toman,openvpn_profile,
                      sort_order,created_at,updated_at FROM sale_plans
               ORDER BY sort_order,days,gb,price_toman,plan_key"""
        ).fetchall()
        migrated = 0
        for row in rows:
            for service in ("openvpn", "v2ray"):
                conn.execute(
                    """INSERT OR IGNORE INTO service_sale_plans(
                           service,plan_key,gb,months,days,price_toman,
                           openvpn_profile,sort_order,created_at,updated_at
                       ) VALUES(?,?,?,?,?,?,?,?,?,?)""",
                    (
                        service, str(row["plan_key"]), int(row["gb"]),
                        int(row["months"]), int(row["days"]),
                        int(row["price_toman"]),
                        str(row["openvpn_profile"]) if service == "openvpn" else "",
                        int(row["sort_order"]), str(row["created_at"]),
                        str(row["updated_at"]),
                    ),
                )
                migrated += int(conn.execute("SELECT changes()").fetchone()[0] or 0)
        ts = now_iso()
        conn.execute(
            "INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)",
            (_SERVICE_SALE_PLANS_MIGRATION_KEY, "3.4.0"),
        )
        conn.execute(
            "INSERT OR REPLACE INTO meta(key,value) VALUES('service_sale_plans_v34_migrated_at',?)",
            (ts,),
        )
        return {"initialized": True, "migrated": migrated}


def service_sale_plans_migration_version() -> str:
    conn = _connect()
    try:
        row = conn.execute(
            "SELECT value FROM meta WHERE key=?",
            (_SERVICE_SALE_PLANS_MIGRATION_KEY,),
        ).fetchone()
        return str(row[0]) if row else ""
    finally:
        conn.close()


def list_service_sale_plans(service: str | None = None) -> list[dict]:
    requested = _sale_plan_service(service) if service is not None else None
    conn = _connect()
    try:
        if requested:
            rows = conn.execute(
                """SELECT service,plan_key,gb,months,days,price_toman,
                          openvpn_profile,sort_order,created_at,updated_at
                   FROM service_sale_plans WHERE service=?
                   ORDER BY sort_order,days,gb,price_toman,plan_key""",
                (requested,),
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT service,plan_key,gb,months,days,price_toman,
                          openvpn_profile,sort_order,created_at,updated_at
                   FROM service_sale_plans
                   ORDER BY service,sort_order,days,gb,price_toman,plan_key"""
            ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def list_sale_plans(service: str = "openvpn") -> list[dict]:
    """Compatibility API: historical callers see the OpenVPN package list."""
    return list_service_sale_plans(service)


def get_sale_plan(plan_key: str, service: str = "openvpn") -> dict | None:
    service = _sale_plan_service(service)
    conn = _connect()
    try:
        row = conn.execute(
            """SELECT service,plan_key,gb,months,days,price_toman,
                      openvpn_profile,sort_order,created_at,updated_at
               FROM service_sale_plans WHERE service=? AND plan_key=?""",
            (service, str(plan_key)),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def create_sale_plan(*, plan_key: str, gb: int, months: int, price_toman: int,
                     openvpn_profile: str = "", service: str = "openvpn",
                     copy_to_v2ray: bool = False, admin_tg_id: int = 0) -> dict:
    service = _sale_plan_service(service)
    if copy_to_v2ray and service != "openvpn":
        raise ValueError("کپی هم‌زمان فقط هنگام ساخت بسته OpenVPN مجاز است")
    key = str(plan_key or "").strip()
    if not key or len(key) > 32:
        raise ValueError("شناسه داخلی بسته نامعتبر است")
    data = _validated_sale_plan(
        service=service, gb=gb, months=months, price_toman=price_toman,
        openvpn_profile=openvpn_profile,
    )
    with _tx(immediate=True) as conn:
        count = int(conn.execute(
            "SELECT COUNT(*) FROM service_sale_plans WHERE service=?", (service,)
        ).fetchone()[0])
        if count >= _MAX_PLAN_COUNT:
            raise ValueError(f"حداکثر {_MAX_PLAN_COUNT} بسته قابل تعریف است")
        if conn.execute(
            "SELECT 1 FROM service_sale_plans WHERE service=? AND plan_key=?",
            (service, key),
        ).fetchone():
            raise ValueError("شناسه داخلی بسته تکراری است")
        if copy_to_v2ray:
            v2_count = int(conn.execute(
                "SELECT COUNT(*) FROM service_sale_plans WHERE service='v2ray'"
            ).fetchone()[0])
            if v2_count >= _MAX_PLAN_COUNT:
                raise ValueError(f"حداکثر {_MAX_PLAN_COUNT} بسته V2Ray قابل تعریف است")
            if conn.execute(
                "SELECT 1 FROM service_sale_plans WHERE service='v2ray' AND plan_key=?",
                (key,),
            ).fetchone():
                raise ValueError("شناسه این بسته از قبل در V2Ray وجود دارد")
        max_sort = int(conn.execute(
            "SELECT COALESCE(MAX(sort_order),-10) FROM service_sale_plans WHERE service=?",
            (service,),
        ).fetchone()[0])
        ts = now_iso()
        conn.execute(
            """INSERT INTO service_sale_plans(service,plan_key,gb,months,days,
                      price_toman,openvpn_profile,sort_order,created_at,updated_at)
               VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (service, key, data["gb"], data["months"], data["days"],
             data["price_toman"], data["openvpn_profile"], max_sort + 10, ts, ts),
        )
        if copy_to_v2ray:
            v2_sort = int(conn.execute(
                "SELECT COALESCE(MAX(sort_order),-10) FROM service_sale_plans WHERE service='v2ray'"
            ).fetchone()[0])
            conn.execute(
                """INSERT INTO service_sale_plans(service,plan_key,gb,months,days,
                          price_toman,openvpn_profile,sort_order,created_at,updated_at)
                   VALUES('v2ray',?,?,?,?,?,?,?,?,?)""",
                (key, data["gb"], data["months"], data["days"],
                 data["price_toman"], "", v2_sort + 10, ts, ts),
            )
        if admin_tg_id:
            conn.execute(
                """INSERT INTO admin_audit(admin_tg_id,target_tg_id,action,before_json,after_json,meta_json,created_at)
                   VALUES(?,?,?,?,?,?,?)""",
                (int(admin_tg_id), 0, "plan_create", _json_dumps({}),
                 _json_dumps({"service": service, "plan_key": key, **data}),
                 _json_dumps({"service": service, "copy_to_v2ray": bool(copy_to_v2ray)}), ts),
            )
    return {"service": service, "plan_key": key, **data}


def update_sale_plan(plan_key: str, *, field: str, value,
                     service: str = "openvpn", admin_tg_id: int = 0) -> dict:
    service = _sale_plan_service(service)
    key = str(plan_key or "").strip()
    field = str(field or "").strip()
    allowed = {"gb", "months", "price_toman", "openvpn_profile"}
    if field not in allowed:
        raise ValueError("فیلد قابل ویرایش نامعتبر است")
    if service == "v2ray" and field == "openvpn_profile":
        raise ValueError("بسته V2Ray نام پکیج MikroTik ندارد")
    with _tx(immediate=True) as conn:
        row = conn.execute(
            "SELECT * FROM service_sale_plans WHERE service=? AND plan_key=?",
            (service, key),
        ).fetchone()
        if not row:
            raise ValueError("بسته پیدا نشد")
        before = dict(row)
        values = {
            "gb": int(row["gb"]),
            "months": int(row["months"]),
            "days": int(row["days"]),
            "price_toman": int(row["price_toman"]),
            "openvpn_profile": str(row["openvpn_profile"]),
        }
        if field == "openvpn_profile":
            values[field] = str(value or "").strip()
        else:
            values[field] = int(value)
        if field == "months":
            values["days"] = int(values["months"]) * 30
        data = _validated_sale_plan(service=service, **values)
        ts = now_iso()
        conn.execute(
            """UPDATE service_sale_plans SET gb=?,months=?,days=?,price_toman=?,
                      openvpn_profile=?,updated_at=? WHERE service=? AND plan_key=?""",
            (data["gb"], data["months"], data["days"], data["price_toman"],
             data["openvpn_profile"], ts, service, key),
        )
        if admin_tg_id:
            conn.execute(
                """INSERT INTO admin_audit(admin_tg_id,target_tg_id,action,before_json,after_json,meta_json,created_at)
                   VALUES(?,?,?,?,?,?,?)""",
                (int(admin_tg_id), 0, "plan_update", _json_dumps(before),
                 _json_dumps({"service": service, "plan_key": key, **data}),
                 _json_dumps({"service": service, "field": field}), ts),
            )
    return {"service": service, "plan_key": key, **data}


def delete_sale_plan(plan_key: str, *, service: str = "openvpn", admin_tg_id: int = 0) -> dict:
    service = _sale_plan_service(service)
    key = str(plan_key or "").strip()
    with _tx(immediate=True) as conn:
        row = conn.execute(
            "SELECT * FROM service_sale_plans WHERE service=? AND plan_key=?",
            (service, key),
        ).fetchone()
        if not row:
            raise ValueError("بسته پیدا نشد")
        before = dict(row)
        conn.execute(
            "DELETE FROM service_sale_plans WHERE service=? AND plan_key=?",
            (service, key),
        )
        if admin_tg_id:
            conn.execute(
                """INSERT INTO admin_audit(admin_tg_id,target_tg_id,action,before_json,after_json,meta_json,created_at)
                   VALUES(?,?,?,?,?,?,?)""",
                (int(admin_tg_id), 0, "plan_delete", _json_dumps(before),
                 _json_dumps({}), _json_dumps({"service": service, "plan_key": key}), now_iso()),
            )
        return before


# -------------------- Managed trial plan (v3.1) --------------------

_MAX_TRIAL_DAYS = 36_500
_TRIAL_SETTING_KEY = "trial_plan_v31"


def _validated_trial_plan(*, gb: int, days: int, openvpn_profile: str, enabled: bool = True) -> dict:
    gb = int(gb)
    days = int(days)
    profile = str(openvpn_profile or "").strip()
    if gb <= 0 or gb > _MAX_PLAN_GB:
        raise ValueError(f"حجم تست باید بین 1 و {_MAX_PLAN_GB:,} گیگ باشد")
    if days <= 0 or days > _MAX_TRIAL_DAYS:
        raise ValueError(f"مدت تست باید بین 1 و {_MAX_TRIAL_DAYS:,} روز باشد")
    if not profile:
        raise ValueError("نام دقیق پکیج تست MikroTik نمی‌تواند خالی باشد")
    if len(profile) > _MAX_PLAN_PROFILE_CHARS:
        raise ValueError(f"نام پکیج MikroTik نباید بیشتر از {_MAX_PLAN_PROFILE_CHARS} کاراکتر باشد")
    return {
        "gb": gb,
        "months": 0,
        "days": days,
        "price_toman": 0,
        "openvpn_profile": profile,
        "enabled": bool(enabled),
    }


def initialize_trial_plan(seed: dict | None = None) -> dict:
    """Persist the legacy TEST_PLAN exactly once, then make SQLite authoritative."""
    raw_seed = dict(seed or {})
    data = _validated_trial_plan(
        gb=int(raw_seed.get("gb") or 1),
        days=int(raw_seed.get("days") or 1),
        openvpn_profile=str(raw_seed.get("openvpn_profile") or "1D-1G-Test"),
        enabled=bool(raw_seed.get("enabled", True)),
    )
    with _tx(immediate=True) as conn:
        row = conn.execute("SELECT value_json FROM settings WHERE key=?", (_TRIAL_SETTING_KEY,)).fetchone()
        if row:
            stored = _json_loads(row[0], {})
            if isinstance(stored, dict):
                try:
                    return _validated_trial_plan(
                        gb=int(stored.get("gb") or data["gb"]),
                        days=int(stored.get("days") or data["days"]),
                        openvpn_profile=str(stored.get("openvpn_profile") or data["openvpn_profile"]),
                        enabled=bool(stored.get("enabled", True)),
                    )
                except Exception:
                    pass
        conn.execute(
            "INSERT OR REPLACE INTO settings(key,value_json,updated_at) VALUES(?,?,?)",
            (_TRIAL_SETTING_KEY, _json_dumps(data), now_iso()),
        )
        return data


def get_trial_plan(default: dict | None = None) -> dict:
    fallback = dict(default or {"gb": 1, "days": 1, "openvpn_profile": "1D-1G-Test", "enabled": True})
    conn = _connect()
    try:
        row = conn.execute("SELECT value_json FROM settings WHERE key=?", (_TRIAL_SETTING_KEY,)).fetchone()
        raw = _json_loads(row[0], fallback) if row else fallback
    finally:
        conn.close()
    if not isinstance(raw, dict):
        raw = fallback
    return _validated_trial_plan(
        gb=int(raw.get("gb") or fallback.get("gb") or 1),
        days=int(raw.get("days") or fallback.get("days") or 1),
        openvpn_profile=str(raw.get("openvpn_profile") or fallback.get("openvpn_profile") or "1D-1G-Test"),
        enabled=bool(raw.get("enabled", fallback.get("enabled", True))),
    )


def update_trial_plan(*, field: str, value, admin_tg_id: int = 0) -> dict:
    field = str(field or "").strip()
    if field not in {"gb", "days", "openvpn_profile"}:
        raise ValueError("فیلد قابل ویرایش تست نامعتبر است")
    with _tx(immediate=True) as conn:
        row = conn.execute("SELECT value_json FROM settings WHERE key=?", (_TRIAL_SETTING_KEY,)).fetchone()
        if not row:
            raise ValueError("تنظیمات اکانت تست پیدا نشد")
        before_raw = _json_loads(row[0], {})
        if not isinstance(before_raw, dict):
            raise ValueError("تنظیمات اکانت تست نامعتبر است")
        before = _validated_trial_plan(
            gb=int(before_raw.get("gb") or 0),
            days=int(before_raw.get("days") or 0),
            openvpn_profile=str(before_raw.get("openvpn_profile") or ""),
            enabled=bool(before_raw.get("enabled", True)),
        )
        values = {
            "gb": int(before["gb"]),
            "days": int(before["days"]),
            "openvpn_profile": str(before["openvpn_profile"]),
            "enabled": bool(before["enabled"]),
        }
        values[field] = str(value or "").strip() if field == "openvpn_profile" else int(value)
        after = _validated_trial_plan(**values)
        ts = now_iso()
        conn.execute(
            "UPDATE settings SET value_json=?,updated_at=? WHERE key=?",
            (_json_dumps(after), ts, _TRIAL_SETTING_KEY),
        )
        if admin_tg_id:
            conn.execute(
                """INSERT INTO admin_audit(admin_tg_id,target_tg_id,action,before_json,after_json,meta_json,created_at)
                   VALUES(?,?,?,?,?,?,?)""",
                (int(admin_tg_id), 0, "trial_plan_update", _json_dumps(before), _json_dumps(after), _json_dumps({"field": field}), ts),
            )
        return after


def set_trial_plan_enabled(enabled: bool, *, admin_tg_id: int = 0) -> dict:
    with _tx(immediate=True) as conn:
        row = conn.execute("SELECT value_json FROM settings WHERE key=?", (_TRIAL_SETTING_KEY,)).fetchone()
        if not row:
            raise ValueError("تنظیمات اکانت تست پیدا نشد")
        before_raw = _json_loads(row[0], {})
        if not isinstance(before_raw, dict):
            raise ValueError("تنظیمات اکانت تست نامعتبر است")
        before = _validated_trial_plan(
            gb=int(before_raw.get("gb") or 0),
            days=int(before_raw.get("days") or 0),
            openvpn_profile=str(before_raw.get("openvpn_profile") or ""),
            enabled=bool(before_raw.get("enabled", True)),
        )
        after = dict(before)
        after["enabled"] = bool(enabled)
        ts = now_iso()
        conn.execute(
            "UPDATE settings SET value_json=?,updated_at=? WHERE key=?",
            (_json_dumps(after), ts, _TRIAL_SETTING_KEY),
        )
        if admin_tg_id and before["enabled"] != after["enabled"]:
            conn.execute(
                """INSERT INTO admin_audit(admin_tg_id,target_tg_id,action,before_json,after_json,meta_json,created_at)
                   VALUES(?,?,?,?,?,?,?)""",
                (int(admin_tg_id), 0, "trial_plan_toggle", _json_dumps(before), _json_dumps(after), _json_dumps({}), ts),
            )
        return after


def get_referral_settings(*, default_discount_percent: int, default_reward_percent: int) -> dict:
    discount = get_setting("referral_discount_percent", int(default_discount_percent))
    reward = get_setting("referral_reward_percent", int(default_reward_percent))
    try:
        discount = int(discount)
    except Exception:
        discount = int(default_discount_percent)
    try:
        reward = int(reward)
    except Exception:
        reward = int(default_reward_percent)
    return {
        "discount_percent": max(0, min(discount, 100)),
        "reward_percent": max(0, min(reward, 10_000)),
    }


def set_referral_percent(kind: str, percent: int, *, admin_tg_id: int,
                         default_discount_percent: int, default_reward_percent: int) -> dict:
    kind = str(kind or "").strip().lower()
    if kind not in {"discount", "reward"}:
        raise ValueError("نوع درصد Referral نامعتبر است")
    value = int(percent)
    maximum = 100 if kind == "discount" else 10_000
    if value < 0 or value > maximum:
        raise ValueError(f"درصد باید بین 0 و {maximum} باشد")
    before = get_referral_settings(
        default_discount_percent=default_discount_percent,
        default_reward_percent=default_reward_percent,
    )
    key = "referral_discount_percent" if kind == "discount" else "referral_reward_percent"
    set_setting(key, value)
    after = get_referral_settings(
        default_discount_percent=default_discount_percent,
        default_reward_percent=default_reward_percent,
    )
    if before != after:
        record_admin_audit(
            admin_tg_id=int(admin_tg_id), action="referral_settings_update",
            before=before, after=after, meta={"field": kind},
        )
    return after


# -------------------- Persistent settings / maintenance --------------------

def get_setting(key: str, default=None):
    conn = _connect()
    try:
        row = conn.execute("SELECT value_json FROM settings WHERE key=?", (str(key),)).fetchone()
        return _json_loads(row[0], default) if row else default
    finally:
        conn.close()


def set_setting(key: str, value):
    conn = _connect()
    try:
        conn.execute(
            """INSERT INTO settings(key,value_json,updated_at) VALUES(?,?,?)
               ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at""",
            (str(key), _json_dumps(value), now_iso()),
        )
    finally:
        conn.close()


def maintenance_mode() -> bool:
    return bool(get_setting("maintenance_mode", False))


def set_maintenance_mode(enabled: bool, *, admin_tg_id: int = 0):
    before = maintenance_mode()
    enabled = bool(enabled)
    if before != enabled:
        set_setting("maintenance_mode", enabled)
    if admin_tg_id and before != enabled:
        record_admin_audit(admin_tg_id=admin_tg_id, action="maintenance_toggle", before={"enabled": before}, after={"enabled": bool(enabled)})
    return before, enabled


def auto_backup_enabled() -> bool:
    return bool(get_setting("auto_backup_enabled", bool(AUTO_BACKUP_ENABLED)))


def set_auto_backup_enabled(enabled: bool, *, admin_tg_id: int = 0):
    before = auto_backup_enabled()
    enabled = bool(enabled)
    if before != enabled:
        set_setting("auto_backup_enabled", enabled)
    if admin_tg_id and before != enabled:
        record_admin_audit(
            admin_tg_id=admin_tg_id,
            action="auto_backup_toggle",
            before={"enabled": before},
            after={"enabled": enabled},
        )
    return before, enabled


def auto_backup_hour() -> int:
    try:
        value = int(get_setting("auto_backup_hour", 6))
    except Exception:
        value = 6
    return value if 0 <= value <= 24 else 6


def set_auto_backup_hour(hour: int, *, admin_tg_id: int = 0):
    try:
        normalized = int(hour)
    except Exception as exc:
        raise ValueError("ساعت بکاپ باید یک عدد صحیح باشد") from exc
    if normalized < 0 or normalized > 24:
        raise ValueError("ساعت بکاپ باید بین 0 و 24 باشد")
    before = auto_backup_hour()
    if before != normalized:
        set_setting("auto_backup_hour", normalized)
    if admin_tg_id and before != normalized:
        record_admin_audit(
            admin_tg_id=admin_tg_id,
            action="auto_backup_hour_update",
            before={"hour": before},
            after={"hour": normalized},
        )
    return before, normalized


def record_auto_backup_delivery(
    *, filename: str, size_bytes: int, delivered_admin_ids=(), failed_admin_ids=(),
) -> dict:
    result = {
        "created_at": now_iso(),
        "filename": str(filename or ""),
        "size_bytes": max(int(size_bytes or 0), 0),
        "delivered_admin_ids": [int(value) for value in delivered_admin_ids or ()],
        "failed_admin_ids": [int(value) for value in failed_admin_ids or ()],
    }
    set_setting("last_auto_backup_delivery", result)
    return result


def auto_backup_status() -> dict:
    last = get_setting("last_backup", {}) or {}
    delivery = get_setting("last_auto_backup_delivery", {}) or {}
    return {
        "enabled": auto_backup_enabled(),
        "hour": auto_backup_hour(),
        "last_backup": dict(last) if isinstance(last, dict) else {},
        "last_delivery": dict(delivery) if isinstance(delivery, dict) else {},
    }


# -------------------- Backup / health --------------------


def _annotate_database_backup(
    conn: sqlite3.Connection, *, created_at: str, backup_kind: str
):
    """Store portable preview metadata inside a completed backup copy."""
    values = {
        "backup_created_at": str(created_at),
        "backup_app_version": str(APP_VERSION),
        "backup_schema_version": str(SCHEMA_VERSION),
        "backup_kind": str(backup_kind),
    }
    for key, value in values.items():
        conn.execute(
            "INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)",
            (key, value),
        )
    conn.commit()


def export_database_snapshot() -> dict:
    """Create a consistent temporary SQLite snapshot for Telegram delivery.

    The exported file keeps the exact database basename and is created outside
    DATA_DIR/backups so the manual admin action does not leave a persistent
    backup behind on the router. The caller must remove ``temp_dir`` after send.
    """
    import tempfile

    temp_dir = tempfile.mkdtemp(prefix="vpn-bot-db-export-")
    filename = os.path.basename(DB_FILE)
    path = os.path.join(temp_dir, filename)
    created_at = now_iso()
    try:
        with _BACKUP_LOCK:
            src = None
            dst = None
            try:
                src = _connect()
                dst = sqlite3.connect(path)
                src.backup(dst)
                _annotate_database_backup(
                    dst, created_at=created_at, backup_kind="manual_export"
                )
            finally:
                if dst is not None:
                    dst.close()
                if src is not None:
                    src.close()
    except Exception:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise
    return {
        "path": path,
        "temp_dir": temp_dir,
        "filename": filename,
        "size_bytes": os.path.getsize(path),
        "created_at": created_at,
    }

def _backup_database_unlocked(*, force: bool = False, keep: int = 14) -> dict:
    os.makedirs(BACKUP_DIR, exist_ok=True)
    last = get_setting("last_backup", {}) or {}
    now = datetime.now(timezone.utc)
    if not force and isinstance(last, dict) and last.get("created_at"):
        try:
            prev = datetime.fromisoformat(str(last["created_at"]))
            if prev.tzinfo is None:
                prev = prev.replace(tzinfo=timezone.utc)
            if (now - prev).total_seconds() < 23 * 3600 and os.path.isfile(str(last.get("path") or "")):
                return {**last, "created": False}
        except Exception:
            pass
    stamp = now.strftime("%Y%m%d-%H%M%S-%f")
    path = os.path.join(BACKUP_DIR, f"vpn-bot-{stamp}.sqlite3")
    created_at = now_iso()
    src = _connect()
    dst = sqlite3.connect(path)
    try:
        src.backup(dst)
        _annotate_database_backup(
            dst, created_at=created_at, backup_kind="automatic"
        )
    finally:
        dst.close()
        src.close()
    files = sorted(Path(BACKUP_DIR).glob("vpn-bot-*.sqlite3"), key=lambda p: p.stat().st_mtime, reverse=True)
    for old in files[max(int(keep), 1):]:
        try:
            old.unlink()
        except Exception:
            pass
    result = {"path": path, "created_at": created_at, "size_bytes": os.path.getsize(path), "created": True}
    set_setting("last_backup", result)
    return result


def backup_database(*, force: bool = False, keep: int = 14) -> dict:
    # Automatic and manual exports must not operate on the same source/destination
    # at once. This also prevents two same-second backups from sharing a filename.
    with _BACKUP_LOCK:
        return _backup_database_unlocked(force=force, keep=keep)


def database_stats(*, check_integrity: bool = True) -> dict:
    conn = _connect()
    try:
        quick = str(conn.execute("PRAGMA quick_check").fetchone()[0]) if check_integrity else "not_checked"
        counts = {}
        for table in (
            "users", "accounts", "purchases", "transactions", "pending_payments",
            "card_transfer_requests", "wallet_transactions", "fulfillments",
            "admin_audit", "resellers", "reseller_debt_entries",
        ):
            counts[table] = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        marker = conn.execute("SELECT value FROM meta WHERE key='legacy_json_migrated_at'").fetchone()
        incomplete = int(conn.execute("SELECT COUNT(*) FROM fulfillments WHERE state!='completed'").fetchone()[0])
        uncertain = int(conn.execute("SELECT COUNT(*) FROM fulfillments WHERE state='executing'").fetchone()[0])
        card_waiting = int(conn.execute(
            "SELECT COUNT(*) FROM card_transfer_requests WHERE status IN ('submitted','processing')"
        ).fetchone()[0])
        return {
            "quick_check": quick,
            "size_bytes": os.path.getsize(DB_FILE) if os.path.isfile(DB_FILE) else 0,
            "db_file": DB_FILE,
            "counts": counts,
            "incomplete_fulfillments": incomplete,
            "uncertain_fulfillments": uncertain,
            "card_transfer_waiting": card_waiting,
            "legacy_migrated_at": str(marker[0]) if marker else "",
        }
    finally:
        conn.close()
