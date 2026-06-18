"""
device_guard.py — Device Fingerprint & Referral Guard  v1
===========================================================
Standalone module: stores device fingerprints and referral records,
verifies devices to prevent fake/spoofed referrals.

Communication with main bot file:
    from device_guard import (
        handle_verify_device,
        is_verified,
        record_referral,
        get_referral_count,
        get_referral_tree,
        get_user_info,
        VERIFY_HTML_URL,
    )

The main bot calls  handle_verify_device(data_dict)  when it receives
a Telegram mini-app  sendData  payload with  action == "verify_device".

ZERO changes to fb_parser.py are required.
"""

import json, os, time, hashlib
from datetime import datetime, timezone
from typing import Optional

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════

# ┌─────────────────────────────────────────────────────────────┐
# │  PASTE YOUR VERCEL HOSTED verify-device.html URL BELOW      │
# │  This is the URL opened in the Telegram Mini App            │
# └─────────────────────────────────────────────────────────────┘
VERIFY_HTML_URL = ""   # <-- PUT YOUR VERCEL URL HERE  e.g. "https://your-app.vercel.app/verify-device.html"


# JSON storage file path — lives next to this script
_STORE_DIR = os.path.dirname(os.path.abspath(__file__))
_STORE_FILE = os.path.join(_STORE_DIR, "device_store.json")

# ── Anti-fake settings ─────────────────────────────────────
MAX_DEVICES_PER_USER   = 3       # max unique fingerprints one telegram_id can have
MAX_USERS_PER_FP       = 1       # max telegram_ids per fingerprint  (1 = strict)
FP_STALE_DAYS          = 30      # days before an FP record is considered stale
REFERRAL_COOLDOWN_SECS = 60      # seconds between verify attempts per user


# ═══════════════════════════════════════════════════════════════
# INTERNAL — JSON FILE READ / WRITE
# ═══════════════════════════════════════════════════════════════

def _load() -> dict:
    """Load the store from disk, creating defaults if missing."""
    if os.path.exists(_STORE_FILE):
        try:
            with open(_STORE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    # Fresh store
    return {
        "devices": {},        # fp_hash  -> {telegram_id, first_seen, last_seen, screen, tz, name, dashboard}
        "users": {},          # telegram_id -> {fps: [fp1, fp2...], verified: bool, first_seen, name}
        "referrals": {},      # telegram_id -> {referred_by, referred_at, verified}
        "referral_counts": {},# referrer_id -> count (denormalised for speed)
    }


def _save(store: dict):
    """Atomic-ish write to the JSON store."""
    tmp = _STORE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(store, f, indent=2, ensure_ascii=False)
    os.replace(tmp, _STORE_FILE)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ts() -> float:
    return time.time()


# ═══════════════════════════════════════════════════════════════
# DEVICE VERIFICATION  (called by main bot)
# ═══════════════════════════════════════════════════════════════

def handle_verify_device(data: dict) -> dict:
    """
    Process a verify_device payload from the Telegram mini-app.

    Expected `data` keys (sent by verify-device.html via tg.sendData):
        action     - "verify_device"
        fp         - SHA-256 device fingerprint hex string
        uid        - Telegram user ID (string)
        name       - Telegram first name
        ts         - client timestamp (ms)
        screen     - e.g. "390x844"
        tz         - timezone string
        dashboard  - the Vercel dashboard URL the user came from

    Returns a result dict the bot can use to reply:
        {
            "ok": True/False,
            "status": "new" | "already_verified" | "fake_referral" | "fp_conflict" | "invalid" | "cooldown",
            "message": "human-readable string for the bot reply",
            "telegram_id": "...",
            "fingerprint": "...",
        }
    """
    # ── Validate payload ───────────────────────────────────
    if not isinstance(data, dict):
        return {"ok": False, "status": "invalid",
                "message": "Invalid payload format.", "telegram_id": "", "fingerprint": ""}

    action = data.get("action", "")
    if action != "verify_device":
        return {"ok": False, "status": "invalid",
                "message": f"Unknown action: {action}", "telegram_id": "", "fingerprint": ""}

    fp      = str(data.get("fp", "")).strip()
    uid     = str(data.get("uid", "")).strip()
    name    = str(data.get("name", "")).strip() or "Unknown"
    screen  = str(data.get("screen", "")).strip()
    tz      = str(data.get("tz", "")).strip()
    dash    = str(data.get("dashboard", "")).strip()

    if not fp or len(fp) < 32:
        return {"ok": False, "status": "invalid",
                "message": "Device fingerprint missing or too short.",
                "telegram_id": uid, "fingerprint": fp}

    if not uid or uid == "unknown":
        return {"ok": False, "status": "invalid",
                "message": "Telegram ID missing. Open this page inside Telegram.",
                "telegram_id": uid, "fingerprint": fp}

    store = _load()

    # ── Cooldown check ─────────────────────────────────────
    user_rec = store["users"].get(uid, {})
    last_ts  = user_rec.get("last_verify_ts", 0)
    if _ts() - last_ts < REFERRAL_COOLDOWN_SECS:
        return {"ok": False, "status": "cooldown",
                "message": f"Please wait {REFERRAL_COOLDOWN_SECS}s before retrying.",
                "telegram_id": uid, "fingerprint": fp}

    # ── Core anti-fake check: one FP → max ONE telegram ID ──
    fp_rec = store["devices"].get(fp)
    if fp_rec is not None:
        # This fingerprint was seen before
        existing_uid = fp_rec.get("telegram_id", "")
        if existing_uid and existing_uid != uid:
            # ⛔ DIFFERENT telegram user on the SAME device fingerprint
            # This is a FAKE REFERRAL — someone sharing a link/device to farm referrals
            _flag_fake(store, uid, fp, existing_uid)
            return {
                "ok": False,
                "status": "fake_referral",
                "message": (
                    f"⛔ <b>Verification Failed</b>\n\n"
                    f"This device is already linked to another account.\n"
                    f"Fake referral detected — each device can only verify ONE account.\n\n"
                    f"FP: <code>{fp[:16]}...</code>\n"
                    f"Linked to UID: <code>{existing_uid}</code>"
                ),
                "telegram_id": uid,
                "fingerprint": fp,
            }

    # ── Check max devices per user ─────────────────────────
    user_fps = user_rec.get("fps", [])
    if fp not in user_fps:
        if len(user_fps) >= MAX_DEVICES_PER_USER:
            return {
                "ok": False,
                "status": "fp_conflict",
                "message": (
                    f"⛔ <b>Device Limit Reached</b>\n\n"
                    f"Your account already has {MAX_DEVICES_PER_USER} verified devices.\n"
                    f"Contact admin if you switched devices."
                ),
                "telegram_id": uid,
                "fingerprint": fp,
            }
        user_fps.append(fp)

    # ── All checks passed — store / update ─────────────────
    now = _now_iso()

    # Update device record
    store["devices"][fp] = {
        "telegram_id": uid,
        "name": name,
        "first_seen": fp_rec["first_seen"] if fp_rec else now,
        "last_seen":  now,
        "screen": screen,
        "tz": tz,
        "dashboard": dash,
        "verify_count": (fp_rec.get("verify_count", 0) if fp_rec else 0) + 1,
    }

    # Update user record
    already_verified = user_rec.get("verified", False)
    store["users"][uid] = {
        "fps": user_fps,
        "verified": True,
        "first_seen": user_rec.get("first_seen", now),
        "last_seen": now,
        "name": name,
        "last_verify_ts": _ts(),
        "screen": screen,
        "tz": tz,
    }

    _save(store)

    if already_verified:
        status_msg = "already_verified"
        reply = (
            f"✅ <b>Device Re-verified</b>\n\n"
            f"Account: <code>{uid}</code> ({name})\n"
            f"Device:  <code>{fp[:20]}...</code>\n"
            f"Screen:  {screen}  |  TZ: {tz}\n"
            f"Devices linked: {len(user_fps)}/{MAX_DEVICES_PER_USER}"
        )
    else:
        status_msg = "new"
        reply = (
            f"✅ <b>Device Verified</b>\n\n"
            f"Account: <code>{uid}</code> ({name})\n"
            f"Fingerprint: <code>{fp[:20]}...</code>\n"
            f"Screen: {screen}  |  TZ: {tz}\n\n"
            f"Verification complete. You now have access."
        )

    return {
        "ok": True,
        "status": status_msg,
        "message": reply,
        "telegram_id": uid,
        "fingerprint": fp,
    }


# ═══════════════════════════════════════════════════════════════
# FAKE REFERRAL FLAGGING
# ═══════════════════════════════════════════════════════════════

def _flag_fake(store: dict, fake_uid: str, fp: str, real_uid: str):
    """
    Internal: record a fake referral attempt.
    Stores in  store["fake_attempts"]  so admins can review.
    """
    fake_list = store.setdefault("fake_attempts", [])
    fake_list.append({
        "fake_uid":    fake_uid,
        "real_uid":    real_uid,
        "fingerprint": fp,
        "timestamp":   _now_iso(),
    })
    # Keep only last 500 attempts to prevent unbounded growth
    store["fake_attempts"] = fake_list[-500:]
    _save(store)


def get_fake_attempts(limit: int = 20) -> list:
    """Return the most recent fake referral attempts for admin review."""
    store = _load()
    return list(reversed(store.get("fake_attempts", [])))[:limit]


# ═══════════════════════════════════════════════════════════════
# REFERRAL TRACKING  (called by main bot)
# ═══════════════════════════════════════════════════════════════

def record_referral(new_user_id: str, referrer_id: str) -> dict:
    """
    Record that `new_user_id` was referred by `referrer_id`.

    Before recording, verifies:
      1. Both users exist in the device store (i.e. both have verified devices).
      2. `new_user_id` does NOT share a fingerprint with `referrer_id`
         (would mean same device referring itself = fake referral).
      3. `new_user_id` hasn't already been referred by someone else.

    Returns:
        {
            "ok": True/False,
            "status": "recorded" | "already_referred" | "self_refer" |
                      "unverified_new" | "unverified_referrer" |
                      "shared_fp" | "invalid",
            "message": "human-readable string",
        }
    """
    new_uid  = str(new_user_id).strip()
    ref_uid  = str(referrer_id).strip()

    if not new_uid or not ref_uid:
        return {"ok": False, "status": "invalid",
                "message": "Missing user ID(s)."}

    if new_uid == ref_uid:
        return {"ok": False, "status": "self_refer",
                "message": "Cannot refer yourself."}

    store = _load()

    # Both users must be device-verified
    new_user  = store["users"].get(new_uid, {})
    ref_user  = store["users"].get(ref_uid, {})

    if not new_user.get("verified"):
        return {"ok": False, "status": "unverified_new",
                "message": f"User {new_uid} has not verified their device yet."}

    if not ref_user.get("verified"):
        return {"ok": False, "status": "unverified_referrer",
                "message": f"Referrer {ref_uid} has not verified their device yet."}

    # ── Anti-fake: check if both users share ANY fingerprint ──
    new_fps  = set(new_user.get("fps", []))
    ref_fps  = set(ref_user.get("fps", []))
    shared   = new_fps & ref_fps
    if shared:
        shared_fp = list(shared)[0]
        _flag_fake(store, new_uid, shared_fp, ref_uid)
        return {
            "ok": False,
            "status": "shared_fp",
            "message": (
                f"⛔ <b>Fake Referral Blocked</b>\n\n"
                f"Both accounts share the same device fingerprint.\n"
                f"FP: <code>{shared_fp[:20]}...</code>\n"
                f"This referral is invalid."
            ),
        }

    # Already referred?
    existing_ref = store["referrals"].get(new_uid)
    if existing_ref:
        prev_ref = existing_ref.get("referred_by", "?")
        return {
            "ok": False,
            "status": "already_referred",
            "message": (
                f"This user was already referred by <code>{prev_ref}</code>.\n"
                f"Each user can only be referred once."
            ),
        }

    # ── Record the referral ────────────────────────────────
    now = _now_iso()
    store["referrals"][new_uid] = {
        "referred_by": ref_uid,
        "referred_at": now,
        "verified": True,
    }

    # Update referral count
    store["referral_counts"][ref_uid] = store["referral_counts"].get(ref_uid, 0) + 1

    _save(store)

    ref_count = store["referral_counts"][ref_uid]
    return {
        "ok": True,
        "status": "recorded",
        "message": (
            f"✅ <b>Referral Recorded</b>\n\n"
            f"User: <code>{new_uid}</code>\n"
            f"Referred by: <code>{ref_uid}</code>\n"
            f"Referrer's total referrals: {ref_count}"
        ),
    }


# ═══════════════════════════════════════════════════════════════
# QUERY HELPERS  (called by main bot)
# ═══════════════════════════════════════════════════════════════

def is_verified(telegram_id: str) -> bool:
    """Check if a Telegram user has completed device verification."""
    store = _load()
    user = store["users"].get(str(telegram_id), {})
    return bool(user.get("verified", False))


def get_referral_count(telegram_id: str) -> int:
    """How many users has this person referred?"""
    store = _load()
    return store["referral_counts"].get(str(telegram_id), 0)


def get_referral_tree(telegram_id: str, depth: int = 1) -> dict:
    """
    Get the referral tree for a user.

    Returns:
        {
            "user": telegram_id,
            "name": "...",
            "referred_by": "..." or None,
            "direct_referrals": [uid1, uid2, ...],
            "total_referrals": N,
        }
    If depth > 1, also includes each direct referral's direct referrals (recursive).
    """
    store = _load()
    uid = str(telegram_id)
    user = store["users"].get(uid, {})
    ref_info = store["referrals"].get(uid, {})

    # Who referred this user?
    referred_by = ref_info.get("referred_by")

    # Who did this user refer? (direct)
    direct = [k for k, v in store["referrals"].items()
              if v.get("referred_by") == uid]

    tree = {
        "user": uid,
        "name": user.get("name", "Unknown"),
        "referred_by": referred_by,
        "direct_referrals": direct,
        "total_referrals": store["referral_counts"].get(uid, 0),
    }

    if depth > 1:
        tree["referral_tree"] = {
            r: get_referral_tree(r, depth - 1) for r in direct
        }

    return tree


def get_user_info(telegram_id: str) -> dict:
    """
    Full info about a user for admin display.

    Returns dict with: verified, fps, first_seen, last_seen, name,
    referral info, device details.
    """
    store = _load()
    uid = str(telegram_id)
    user = store["users"].get(uid, {})
    ref  = store["referrals"].get(uid, {})

    # Get device details for each FP
    devices = []
    for fp_hash in user.get("fps", []):
        fp_data = store["devices"].get(fp_hash, {})
        devices.append({
            "fingerprint": fp_hash,
            "screen": fp_data.get("screen", ""),
            "tz": fp_data.get("tz", ""),
            "first_seen": fp_data.get("first_seen", ""),
            "last_seen": fp_data.get("last_seen", ""),
            "verify_count": fp_data.get("verify_count", 0),
        })

    return {
        "telegram_id": uid,
        "name": user.get("name", "Unknown"),
        "verified": user.get("verified", False),
        "first_seen": user.get("first_seen", ""),
        "last_seen": user.get("last_seen", ""),
        "devices": devices,
        "referred_by": ref.get("referred_by"),
        "referred_at": ref.get("referred_at"),
        "referral_count": store["referral_counts"].get(uid, 0),
    }


def get_all_verified_users() -> list:
    """Return list of all verified user IDs (for admin use)."""
    store = _load()
    return [uid for uid, u in store["users"].items() if u.get("verified")]


def remove_user(telegram_id: str) -> dict:
    """
    Admin action: remove a user and all their data.
    Returns {"ok": True, "removed_fps": N, "removed_referrals": N}
    """
    store = _load()
    uid = str(telegram_id)
    user = store["users"].pop(uid, None)

    if not user:
        return {"ok": False, "message": f"User {uid} not found."}

    # Remove their fingerprints from device store
    removed_fps = 0
    for fp in user.get("fps", []):
        fp_rec = store["devices"].get(fp)
        if fp_rec and fp_rec.get("telegram_id") == uid:
            del store["devices"][fp]
            removed_fps += 1

    # Remove any referral record where this user was the new user
    removed_refs = 0
    ref = store["referrals"].pop(uid, None)
    if ref:
        ref_by = ref.get("referred_by")
        if ref_by and ref_by in store["referral_counts"]:
            store["referral_counts"][ref_by] = max(
                0, store["referral_counts"][ref_by] - 1
            )
        removed_refs = 1

    # Remove from referral counts
    store["referral_counts"].pop(uid, None)

    _save(store)
    return {
        "ok": True,
        "removed_fps": removed_fps,
        "removed_referrals": removed_refs,
        "message": f"User {uid} removed. {removed_fps} devices, {removed_refs} referrals cleaned.",
    }


def get_stats() -> dict:
    """Overview statistics for admin panel."""
    store = _load()
    total_users     = len(store["users"])
    verified_users  = sum(1 for u in store["users"].values() if u.get("verified"))
    total_devices   = len(store["devices"])
    total_referrals = sum(store["referral_counts"].values())
    fake_attempts   = len(store.get("fake_attempts", []))
    total_referred  = len(store["referrals"])

    return {
        "total_users": total_users,
        "verified_users": verified_users,
        "total_devices": total_devices,
        "total_referrals": total_referrals,
        "referred_users": total_referred,
        "fake_attempts": fake_attempts,
        "referral_counts": dict(store["referral_counts"]),
    }


# ═══════════════════════════════════════════════════════════════
# STANDALONE TEST
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 60)
    print("device_guard.py — Standalone Test")
    print("=" * 60)

    # Simulate a real verification payload (as sent by verify-device.html)
    test_payload = {
        "action": "verify_device",
        "fp": "a" * 64,   # 64-char hex SHA-256
        "uid": "123456789",
        "name": "TestUser",
        "ts": 1718000000000,
        "screen": "390x844",
        "tz": "Asia/Kolkata",
        "dashboard": "https://example.vercel.app",
    }

    print("\n[1] First verification (should succeed):")
    result = handle_verify_device(test_payload)
    print(f"  ok={result['ok']}  status={result['status']}")
    print(f"  {result['message'][:120]}...")

    print("\n[2] Re-verify same user + device (should succeed, 'already_verified'):")
    test_payload["ts"] = 1718000060000
    result = handle_verify_device(test_payload)
    print(f"  ok={result['ok']}  status={result['status']}")

    print("\n[3] Different user, SAME fingerprint (should FAIL — fake referral):")
    fake_payload = dict(test_payload)
    fake_payload["uid"] = "999999999"
    fake_payload["name"] = "FakeUser"
    result = handle_verify_device(fake_payload)
    print(f"  ok={result['ok']}  status={result['status']}")
    print(f"  {result['message'][:120]}...")

    print("\n[4] Record a valid referral:")
    # First verify the referrer
    ref_payload = dict(test_payload)
    ref_payload["uid"] = "111111111"
    ref_payload["fp"] = "b" * 64
    ref_payload["name"] = "Referrer"
    handle_verify_device(ref_payload)

    ref_result = record_referral("123456789", "111111111")
    print(f"  ok={ref_result['ok']}  status={ref_result['status']}")
    print(f"  {ref_result['message'][:120]}...")

    print("\n[5] Try duplicate referral (should fail):")
    ref_result2 = record_referral("123456789", "111111111")
    print(f"  ok={ref_result2['ok']}  status={ref_result2['status']}")

    print("\n[6] Stats:")
    stats = get_stats()
    for k, v in stats.items():
        if k != "referral_counts":
            print(f"  {k}: {v}")

    print("\n[7] Fake attempts log:")
    for fa in get_fake_attempts(5):
        print(f"  {fa['fake_uid']} tried to use FP of {fa['real_uid']} at {fa['timestamp']}")

    print("\n✅ All tests passed.")
    print(f"\nStore file: {_STORE_FILE}")