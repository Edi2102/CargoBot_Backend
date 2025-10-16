from __future__ import annotations
import json
import re
import threading
from typing import Tuple, Dict, Any
from datetime import datetime
from typing import TypedDict, List
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.utils import timezone as djtz
from google.api_core.exceptions import GoogleAPICallError
from app.firestore_client import db
from app.firestore_utils import doc_id_from_username

# ==========================================
# In-memory Greenlight store (process-local)
# ==========================================

_GL_LOCK = threading.RLock()
# key: f"{user_name}__{cargo_key}"  -> dict(state)
_GL_STORE: Dict[str, Dict[str, Any]] = {}


def _gl_key(user_name: str, cargo_key: str) -> str:
    return f"{user_name}__{cargo_key}"


def _gl_get(user_name: str, cargo_key: str) -> Dict[str, Any]:
    with _GL_LOCK:
        return (_GL_STORE.get(_gl_key(user_name, cargo_key)) or {}).copy()


def _gl_set(user_name: str, cargo_key: str, updates: Dict[str, Any], merge: bool = True) -> None:
    with _GL_LOCK:
        k = _gl_key(user_name, cargo_key)
        cur = _GL_STORE.get(k) if merge and isinstance(_GL_STORE.get(k), dict) else {}
        cur = {**cur, **updates}
        _GL_STORE[k] = cur


def _gl_delete(user_name: str, cargo_key: str) -> None:
    with _GL_LOCK:
        _GL_STORE.pop(_gl_key(user_name, cargo_key), None)


def _gl_find_recent_pending_for_user(
    user_name: str,
    max_age_sec: int = 12,
    require_ready: bool = True,
):
    """
    Look in process memory for this user's pending 'ready' item
    within a short time window.
    Returns (cargo_key, doc_dict) or (None, None).
    """
    now = djtz.now()
    best: tuple[str, Dict[str, Any], datetime] | None = None

    with _GL_LOCK:
        for k, d in _GL_STORE.items():
            if not isinstance(d, dict):
                continue
            if (d.get("user") or "") != user_name:
                continue
            if not d.get("armed", False):
                continue
            if d.get("pressed_once", False):
                continue
            if require_ready and not d.get("ready_for_auto_finalize", False):
                continue

            ps_raw = d.get("pending_since")
            if not isinstance(ps_raw, str):  # avoid .strip() on non-str
                continue
            ps = ps_raw.strip()
            if not ps:
                continue

            try:
                t = datetime.fromisoformat(ps)
            except (ValueError, TypeError):
                # Bad/unknown timestamp format -> skip
                continue

            if djtz.is_naive(t):
                t = djtz.make_aware(t, djtz.get_current_timezone())

            age = (now - t).total_seconds()
            if age <= max_age_sec and (best is None or t > best[2]):
                best = (k, d, t)

    if best:
        k, d, _ = best
        try:
            _, cargo_key = k.split("__", 1)
        except ValueError:
            return None, None
        return cargo_key, d

    return None, None


def _gl_finalize_success(user_name: str, cargo_key: str, gl_doc: Dict[str, Any], reason: str):
    """Mark success in memory and print logs."""
    user = gl_doc.get("user", user_name) or user_name
    cargo_key_eff = cargo_key or gl_doc.get("cargo_key", "") or gl_doc.get("cargo_id", "")
    bm_id = gl_doc.get("bm_id", "")
    timp_de = gl_doc.get("press_timpde_final") or gl_doc.get("press_timpde_before") or gl_doc.get("last_timp_de") or 1

    print(
        f"[PRESS] Final 'Timp de' used: {int(timp_de)} for cargo {cargo_key_eff or '(unknown)'}"
        + (f" (BM={bm_id})" if bm_id else "")
        + f" (user: {user})"
    )
    print(
        f"[PRESS] after_click: Incarcare='(empty)' for cargo {cargo_key_eff or '(unknown)'}"
        + (f" (BM={bm_id})" if bm_id else "")
        + f" (user: {user}) — {reason}"
    )
    print(
        f"[RESULT] Publish Successful! (cargo {cargo_key_eff or '(unknown)'}"
        + (f", BM={bm_id}" if bm_id else "")
        + f", user: {user})"
    )

    _gl_set(user, cargo_key, {
        "pressed_once": True,
        "armed": False,
        "press_incarcare_final": "",
        "press_success": True,
        "press_message": "Publish Successful!",
        "pending_since": None,
        "last_after_click": djtz.now().isoformat(),
        "ready_for_auto_finalize": False,
    }, merge=True)


# =========================
# Utilities
# =========================

def _dedupe_keep_order(items: List[str]) -> List[str]:
    seen = set()
    out = []
    for x in items:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _load_cargo_doc(user_name: str) -> Dict[str, Any]:
    if not user_name:
        return {}
    doc_id = doc_id_from_username(user_name)
    snap = db.collection("Cargo").document(doc_id).get()
    return (snap.to_dict() or {}) if snap.exists else {}


def _resolve_bm_for_cargo(user_name: str, cargo_id_digits: str) -> str:
    if not user_name or not cargo_id_digits:
        return ""

    try:
        data = _load_cargo_doc(user_name)
    except (KeyError, AttributeError, TypeError):
        return ""

    ids = data.get("ids") if isinstance(data, dict) else None
    if not isinstance(ids, list):
        return ""

    for x in ids:
        if isinstance(x, str):
            match = re.search(r"(\d+)$", x)
            if match and match.group(1) == cargo_id_digits:
                return x

    return ""


def _lookup_meta_for_id(user_name: str, qid: str) -> Dict[str, Any]:
    res = {"id": "", "start_date": None, "for_days": None}
    if not user_name or not qid:
        return res
    data = _load_cargo_doc(user_name)
    ids_meta = data.get("ids_meta") or []
    for row in ids_meta:
        if isinstance(row, dict) and (row.get("id") or "") == qid:
            return {"id": row.get("id") or "", "start_date": row.get("start_date"), "for_days": row.get("for_days")}
    m_q = re.search(r"(\d+)$", qid)
    if m_q:
        suf = m_q.group(1)
        for row in ids_meta:
            rid = (row.get("id") or "")
            m_r = re.search(r"(\d+)$", rid)
            if m_r and m_r.group(1) == suf:
                return {"id": rid, "start_date": row.get("start_date"), "for_days": row.get("for_days")}
    return res


def _digits(s: str) -> str:
    m = re.search(r"(\d+)$", s or "")
    return m.group(1) if m else ""


def _pick_cargo_key(cargo_id: str, bm_id: str) -> str:
    cid = (cargo_id or "").strip()
    bid = (bm_id or "").strip()
    if bid:
        return bid
    if cid.upper().startswith("BM-"):
        return cid
    return cid


def _standard_cargo_key(user_name: str, cargo_id: str, bm_id_in: str) -> Tuple[str, str]:
    """
    Compute a *consistent* cargo_key for ALL endpoints:
    - If cargo_id starts with BM-, use cargo_id.
    - Else, try to resolve BM- by suffix or use cargo_id as fallback.
    Returns (cargo_key, bm_id)
    """
    cid = (cargo_id or "").strip()
    if cid.upper().startswith("BM-"):
        return cid, cid
    suf = _digits(cid)
    bm = (bm_id_in or "").strip() or _resolve_bm_for_cargo(user_name, suf)
    return _pick_cargo_key(cid, bm), bm


def _now_aware():
    return djtz.now()


def _parse_iso_aware(s: str) -> datetime | None:
    """
    Parse an ISO 8601 datetime string into a timezone-aware datetime.
    Returns None if parsing fails.
    """
    if not isinstance(s, str):
        return None

    s = s.strip()
    if not s:
        return None

    try:
        dt = datetime.fromisoformat(s)
    except (ValueError, TypeError):
        return None

    if djtz.is_naive(dt):
        dt = djtz.make_aware(dt, djtz.get_current_timezone())

    return dt


# ==================================
# Views
# ==================================

@csrf_exempt
def active_products(request):
    if request.method != "POST":
        return JsonResponse({"error": "POST required"}, status=405)

    try:
        data = json.loads(request.body or "{}")
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)

    user_name = (data.get("user_name") or "").strip()
    if not user_name:
        return JsonResponse({"error": "Missing user_name"}, status=400)

    n = int(data.get("active_products", 0))
    rows = data.get("rows", []) or []

    own_rows = [r for r in rows if (r.get("owner") or "").strip() == user_name]
    incoming_ids = _dedupe_keep_order([r.get("id") for r in own_rows if r.get("id")])

    print(f"[BACKEND] User: {user_name}")
    print(f"[BACKEND] Active products (header): {n}")
    print(f"[BACKEND] Own rows in payload: {len(own_rows)}")
    for i, row in enumerate(own_rows, 1):
        rid = row.get("id")
        owner = row.get("owner")
        start_date = row.get("start_date")
        for_days = row.get("for_days")
        print(
            f"--- Own Row {i} ---  "
            f"ID: {rid}  Owner: {owner}  "
            f"start_date: {start_date if start_date is not None else '(n/a)'}  "
            f"for_days: {for_days if for_days is not None else '(n/a)'}"
        )

    users_col = db.collection("Cargo")
    doc_id = doc_id_from_username(user_name)
    user_doc = users_col.document(doc_id)

    snap = user_doc.get()
    existing_ids = []
    if snap.exists:
        d = snap.to_dict() or {}
        existing_ids = (d.get("ids") or [])[:]

    added = [x for x in incoming_ids if x not in existing_ids]
    removed = [x for x in existing_ids if x not in incoming_ids]
    kept = [x for x in incoming_ids if x in existing_ids]

    print(f"[FIRESTORE] REPLACING ids for '{user_name}':")
    print(f"  - previous: {len(existing_ids)}")
    print(f"  - incoming: {len(incoming_ids)}")
    print(f"  - added:    {len(added)}  {', '.join(added) if added else ''}")
    print(f"  - removed:  {len(removed)} {', '.join(removed) if removed else ''}")
    print(f"  - kept:     {len(kept)}    {', '.join(kept) if kept else ''}")

    own_by_id = {}
    for r in own_rows:
        rid = r.get("id")
        if rid and rid not in own_by_id:
            own_by_id[rid] = {
                "id": rid,
                "owner": r.get("owner"),
                "start_date": r.get("start_date"),
                "for_days": r.get("for_days"),
            }

    ids_meta = [{"id": rid,
                 "start_date": (own_by_id.get(rid) or {}).get("start_date"),
                 "for_days": (own_by_id.get(rid) or {}).get("for_days")}
                for rid in incoming_ids]

    items = [{"id": rid,
              "owner": (own_by_id.get(rid) or {}).get("owner") or user_name,
              "start_date": (own_by_id.get(rid) or {}).get("start_date") or "",
              "for_days": (own_by_id.get(rid) or {}).get("for_days")}
             for rid in incoming_ids]

    try:
        user_doc.set(
            {
                "user": user_name,
                "ids": incoming_ids,
                "ids_meta": ids_meta,
            },
            merge=True,
        )

        return JsonResponse({
            "ok": True,
            "mode": "replace_all",
            "received_rows": len(rows),
            "own_rows": len(own_rows),
            "user_name": user_name,
            "ids_now": incoming_ids,
            "ids_meta": ids_meta,
            "items": items,
            "diff": {
                "added": added,
                "removed": removed,
                "kept": kept,
            },
        })
    except GoogleAPICallError as e:
        print("[FIRESTORE] Error:", e)
        return JsonResponse({"error": "Firestore error", "details": str(e)}, status=502)


class DeletedRow(TypedDict):
    id: str
    owner: str


@csrf_exempt
def deleted_products(request):
    if request.method != "POST":
        return JsonResponse({"error": "POST required"}, status=405)

    try:
        data = json.loads(request.body or "{}")
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)

    user_name = (data.get("user_name") or "").strip()
    if not user_name:
        return JsonResponse({"error": "Missing user_name"}, status=400)

    raw_summary = data.get("summary_text") or ""
    summary_compact = re.sub(r"\s+", " ", raw_summary).strip()
    m = re.search(r"\b\d+\s+marfuri\s+sterse?\s+in\s+ultimele\s+\d+\s+ore\b", summary_compact, flags=re.I)
    summary = m.group(0) if m else summary_compact

    rows = data.get("rows", []) or []

    own_rows: List[DeletedRow] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        rid = (r.get("id") or "").strip()
        owner = (r.get("owner") or "").strip()
        if rid and owner == user_name:
            own_rows.append(DeletedRow(id=rid, owner=owner))

    print(f"[DELETED] User: {user_name}")
    print(f"[DELETED] Summary: {summary or '(not found)'}")
    print(f"[DELETED] Own rows on page: {len(own_rows)}")
    for i, row in enumerate(own_rows, 1):
        print(f"--- Own Row {i} ---  ID: {row['id']}  Owner: {row['owner']}")

    return JsonResponse({
        "ok": True,
        "user_name": user_name,
        "summary_text": summary,
        "own_rows": len(own_rows),
        "received_rows": len(rows),
    })


@csrf_exempt
def get_user_ids(request):
    user_name = (request.GET.get("user") or "").strip()
    if not user_name:
        return JsonResponse({"error": "Missing user"}, status=400)

    data = _load_cargo_doc(user_name)
    return JsonResponse({
        "user": user_name,
        "ids": data.get("ids", []),
        "ids_meta": data.get("ids_meta", []),
    })


@csrf_exempt
def get_cargo_meta(request):
    if request.method != "GET":
        return JsonResponse({"error": "GET required"}, status=405)

    user_name = (request.GET.get("user") or "").strip()
    qid = (request.GET.get("id") or "").strip()

    if not user_name or not qid:
        return JsonResponse({"error": "Missing user or id"}, status=400)

    meta = _lookup_meta_for_id(user_name, qid)
    return JsonResponse(meta)


@csrf_exempt
def delete_greenlight(request):
    if request.method != "POST":
        return JsonResponse({"error": "POST required"}, status=405)
    try:
        data = json.loads(request.body or "{}")
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)
    user_name = (data.get("user_name") or "").strip()
    print(f"[GREENLIGHT] After-DA ping from: {user_name or '(unknown)'}")
    return JsonResponse({"ok": True, "go": True})


@csrf_exempt
def ping_active(request):
    if request.method != "POST":
        return JsonResponse({"error": "POST required"}, status=405)
    print("[BACKEND] We are in the Active Cargo page")
    print("Active ping")
    return JsonResponse({"ok": True})


@csrf_exempt
def ping_deleted(request):
    if request.method != "POST":
        return JsonResponse({"error": "POST required"}, status=405)
    print("[BACKEND] We are in the deleted Cargo page")
    return JsonResponse({"ok": True})


@csrf_exempt
def page_ping(request):
    if request.method != "POST":
        return JsonResponse({"error": "POST required"}, status=405)

    try:
        data = json.loads(request.body or "{}")
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)

    page = (data.get("page") or "").strip().lower()
    user_name = (data.get("user_name") or "").strip()
    cargo_id = (data.get("cargo_id") or "").strip()
    bm_id_in = (data.get("bm_id") or "").strip()
    page_state = (data.get("page_state") or "").strip().lower()
    url_in = (data.get("url") or request.META.get("HTTP_REFERER") or "").strip()

    if page == "active":
        print(f"[BACKEND] We are in the Active Cargo page (user: {user_name}) | url={url_in}")
        return JsonResponse({"ok": True, "go": False})

    if page == "deleted":
        print(f"[BACKEND] We are in the Deleted Cargo page (user: {user_name}) | url={url_in}")
        return JsonResponse({"ok": True, "go": False})

    if page != "addload":
        print(f"[BACKEND] We are in an Unknown page (user: {user_name}) | url={url_in}")
        return JsonResponse({"ok": True, "go": False})

    # No cargo_id → this is the empty addload page after navigation (success/fail echo)
    if not cargo_id:
        print(f"[BACKEND] We are in the Addload page (user: {user_name}) | page_state={page_state} | url={url_in}")
        result_message = "Publish successful" if url_in.strip() == "https://www.bursatransport.com/freightexchange/addload" else "Publish failed"
        print(result_message)

        # Auto-finalize if we recently saw "before_click"
        try:
            if user_name and page_state == "empty":
                cargo_key, gl_doc = _gl_find_recent_pending_for_user(
                    user_name, max_age_sec=12, require_ready=True
                )
                if cargo_key and gl_doc:
                    _gl_finalize_success(
                        user_name, cargo_key, gl_doc,
                        reason="auto-finalize via addload ping with page_state=empty"
                    )
        except Exception as e:
            print(f"[BACKEND] Auto-finalize check failed for user {user_name}: {e}")

        return JsonResponse({"ok": True, "go": False, "message": result_message})

    # cargo present
    cargo_key, bm_id = _standard_cargo_key(user_name, cargo_id, bm_id_in)
    pretty = cargo_key if cargo_key.upper().startswith("BM-") else f"{cargo_key}" + (f" (BM={bm_id})" if bm_id else "")
    print(f"[BACKEND] We entered the publica page for the cargo with the id {pretty} | url={url_in}")

    # Arm immediately when we reach the cargo publish page.
    d = _gl_get(user_name, cargo_key)
    armed = bool(d.get("armed"))
    pressed_once = bool(d.get("pressed_once"))
    if not pressed_once and not armed:
        _gl_set(user_name, cargo_key, {
            "user": user_name,
            "cargo_id": cargo_id,
            "bm_id": bm_id or "",
            "cargo_key": cargo_key,
            "armed": True,
            "pressed_once": False,
        }, merge=False)
        print(f"[GREENLIGHT] Armed for cargo {cargo_key} (BM={bm_id}) (user: {user_name})")
        go_now = True
    else:
        state = "already pressed" if pressed_once else "already armed"
        print(f"[GREENLIGHT] Skipping arm ({state}) for cargo {cargo_key} (user: {user_name})")
        go_now = armed and not pressed_once

    # Optional meta — not used to decide pressing anymore
    meta = _lookup_meta_for_id(user_name, cargo_id)
    for_days = int(meta.get("for_days") or 1)
    if for_days <= 0:
        for_days = 1

    return JsonResponse({
        "ok": True,
        "go": go_now,
        "for_days": for_days,
        "cargo_key": cargo_key
    })


@csrf_exempt
def greenlight_check(request):
    if request.method != "POST":
        return JsonResponse({"error": "POST required"}, status=405)

    try:
        data = json.loads(request.body or "{}")
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)

    user_name = (data.get("user_name") or "").strip()
    cargo_id = (data.get("cargo_id") or "").strip()

    if not user_name:
        return JsonResponse({"error": "Missing user_name"}, status=400)
    if not cargo_id:
        return JsonResponse({"error": "Missing cargo_id"}, status=400)

    cargo_key, bm_id = _standard_cargo_key(user_name, cargo_id, data.get("bm_id") or "")

    d = _gl_get(user_name, cargo_key)
    armed = bool(d.get("armed"))
    pressed_once = bool(d.get("pressed_once"))
    bm_id = (d.get("bm_id") or bm_id or "").strip()

    if armed and not pressed_once:
        print(f"[GREENLIGHT] PRESS THE BUTTON for cargo {cargo_key}"
              + (f" (BM={bm_id})" if bm_id else "")
              + f" (user: {user_name})")
        _gl_set(user_name, cargo_key, {
            "armed": False,
            "pressed_once": True
        })
        return JsonResponse({"ok": True, "go": True, "cargo_key": cargo_key})

    print(f"[GREENLIGHT] No press for cargo {cargo_key}"
          + (f" (BM={bm_id})" if bm_id else "")
          + f" (user: {user_name}) (armed={armed}, pressed_once={pressed_once})")
    return JsonResponse({"ok": True, "go": False, "cargo_key": cargo_key})


@csrf_exempt
def press_ack(request):
    if request.method != "POST":
        return JsonResponse({"error": "POST required"}, status=405)

    try:
        data = json.loads(request.body or "{}")
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)

    user_name = (data.get("user_name") or "").strip()
    cargo_id = (data.get("cargo_id") or "").strip()
    bm_id_in = (data.get("bm_id") or "").strip()
    incar_raw = (data.get("incarcare") or "").strip()
    when = (data.get("when") or "").strip().lower()
    timp_de_raw = data.get("timp_de", None)
    try:
        timp_de = int(timp_de_raw) if timp_de_raw is not None else None
    except (TypeError, ValueError):
        timp_de = None

    def norm_incarcare(s: str) -> str:
        s = (s or "").strip()
        if s in {"", "-", "–", "—"}:
            return ""
        return s

    incarcare = norm_incarcare(incar_raw)
    phase_for_log = when or "unknown"
    print(f"We enter the press_ack function [{phase_for_log}]")

    # If missing data, just echo result for logging/UX
    if not user_name or not cargo_id:
        success = (when in {"after_click", "post_flow"}) and (incarcare == "")
        message = "Publish Successful!" if success else (
            "Publish Failed!" if when in {"after_click", "post_flow"} else "Recorded.")
        if timp_de is not None:
            print(
                f"[PRESS:{phase_for_log}] 'Timp de'={timp_de} for cargo {cargo_id or '(unknown)'} (user: {user_name or '(unknown)'})")
        if when in {"after_click", "post_flow"}:
            print(
                f"[PRESS:{phase_for_log}] Incarcare='{incarcare or '(empty)'}' cargo {cargo_id or '(unknown)'} (user: {user_name or '(unknown)'})")
            print(f"[RESULT] {message} (repeat) (cargo {cargo_id or '(unknown)'}"
                  f", user: {user_name or '(unknown)'})")
        return JsonResponse({"ok": True, "success": success, "message": message})

    cargo_key, bm_id = _standard_cargo_key(user_name, cargo_id, bm_id_in)
    d = _gl_get(user_name, cargo_key)

    bm_id = (d.get("bm_id") or bm_id or "").strip()
    press_phases = list(d.get("press_phases") or [])
    incar_before = (d.get("press_incarcare_before") or "").strip()
    press_success = d.get("press_success")
    press_message = d.get("press_message")
    timpde_before = d.get("press_timpde_before")
    timpde_final = d.get("press_timpde_final")

    if when and when not in press_phases:
        press_phases.append(when)

    if when == "prepared":
        # Remember a candidate "timp_de" and arm (idempotent).
        if timp_de is not None:
            timpde_before = int(timp_de)
            print(f"[PRESS:{phase_for_log}] 'Timp de'={timpde_before} for cargo {cargo_id}"
                  + (f" (BM={bm_id})" if bm_id else "") + f" (user: {user_name})")

        _gl_set(user_name, cargo_key, {
            "user": user_name,
            "cargo_id": cargo_id,
            "bm_id": bm_id or "",
            "cargo_key": cargo_key,
            "armed": True,
            "pressed_once": False,
            "last_timp_de": int(timpde_before) if timpde_before is not None else None,
            "press_phases": press_phases,
        }, merge=True)

        return JsonResponse({
            "ok": True,
            "success": None,
            "message": "Recorded.",
            "cargo_id": cargo_id,
            "user_name": user_name
        })

    if when == "before_click":
        if not incar_before:
            incar_before = incarcare
        print(f"[PRESS:{phase_for_log}] Incarcare(before)='{incarcare or '(empty)'}' for cargo {cargo_id}"
              + (f" (BM={bm_id})" if bm_id else "") + f" (user: {user_name})")

        if timp_de is not None:
            timpde_before = int(timp_de)
            print(f"[PRESS:{phase_for_log}] 'Timp de'={timpde_before} cargo {cargo_id}"
                  + (f" (BM={bm_id})" if bm_id else "") + f" (user: {user_name})")

        _gl_set(user_name, cargo_key, {
            "user": user_name,
            "cargo_id": cargo_id,
            "bm_id": bm_id or "",
            "cargo_key": cargo_key,

            "armed": True,
            "pressed_once": False,
            "pending_since": djtz.now().isoformat(),

            "last_timp_de": int(timpde_before) if timpde_before is not None else None,
            "last_incarcare": incar_before,

            "press_phases": press_phases,
            "press_incarcare_before": incar_before,
            "press_timpde_before": timpde_before if timpde_before is not None else None,

            "ready_for_auto_finalize": True,
            "ready_since": djtz.now().isoformat(),
        }, merge=True)

        return JsonResponse({
            "ok": True,
            "success": None,
            "message": "Recorded. Checking publish result…",
            "cargo_id": cargo_id,
            "user_name": user_name
        })

    if when in {"after_click", "post_flow"}:
        if press_success is None:
            incar_final = incarcare
            if timp_de is not None:
                timpde_final = int(timp_de)
            elif timpde_final is None and timpde_before is not None:
                timpde_final = int(timpde_before)

            success = (incar_final == "")
            message = "Publish Successful!" if success else "Publish Failed!"

            used_days = timpde_final if timpde_final is not None else timpde_before
            if used_days is not None:
                print(f"[PRESS] Final 'Timp de' used: {used_days} for cargo {cargo_id}"
                      + (f" (BM={bm_id})" if bm_id else "") + f" (user: {user_name})")

            print(f"[PRESS:{phase_for_log}] Incarcare='{incar_final or '(empty)'}' for cargo {cargo_id}"
                  + (f" (BM={bm_id})" if bm_id else "") + f" (user: {user_name})")
            print(f"[RESULT] {message} (cargo {cargo_id}"
                  + (f", BM={bm_id}" if bm_id else "") + f", user: {user_name})")

            _gl_set(user_name, cargo_key, {
                "user": user_name,
                "cargo_id": cargo_id,
                "bm_id": bm_id or "",
                "cargo_key": cargo_key,

                "armed": False,
                "pressed_once": True,

                "press_phases": press_phases,
                "press_incarcare_final": incar_final,
                "press_success": bool(success),
                "press_message": message,

                "press_timpde_before": timpde_before if timpde_before is not None else None,
                "press_timpde_final": timpde_final if timpde_final is not None else None,

                "pending_since": None,
                "ready_for_auto_finalize": False,
                "last_after_click": djtz.now().isoformat(),
            }, merge=True)

            return JsonResponse({
                "ok": True,
                "success": bool(success),
                "message": message,
                "cargo_id": cargo_id,
                "user_name": user_name
            })

        # late duplicate finalize → just echo existing result
        print(f"[PRESS:{phase_for_log}] (late) Incarcare='{(incarcare or '(empty)')}' cargo {cargo_id}"
              + (f" (BM={bm_id})" if bm_id else "") + f" (user: {user_name})")
        final_msg = press_message or ("Publish Successful!" if press_success else "Publish Failed!")
        print(f"[RESULT] {final_msg} (repeat) (cargo {cargo_id}"
              + (f", BM={bm_id}" if bm_id else "") + f", user: {user_name})")

        return JsonResponse({
            "ok": True,
            "success": bool(press_success),
            "message": final_msg,
            "cargo_id": cargo_id,
            "user_name": user_name
        })

    # generic phase record
    payload = {
        "user": user_name,
        "cargo_id": cargo_id,
        "bm_id": bm_id or "",
        "cargo_key": cargo_key,
        "press_phases": press_phases,
    }
    if timp_de is not None:
        payload["press_timpde_before"] = int(timp_de)
        print(f"[PRESS:{phase_for_log}] 'Timp de'={int(timp_de)} for cargo {cargo_id}"
              + (f" (BM={bm_id})" if bm_id else "") + f" (user: {user_name})")

    _gl_set(user_name, cargo_key, payload, merge=True)

    return JsonResponse({
        "ok": True,
        "success": None,
        "message": "Recorded.",
        "cargo_id": cargo_id,
        "user_name": user_name
    })


@csrf_exempt
def set_greenlight(request):
    if request.method != "POST":
        return JsonResponse({"error": "POST required"}, status=405)

    try:
        data = json.loads(request.body or "{}")
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)

    user_name = (data.get("user_name") or "").strip()
    cargo_id = (data.get("cargo_id") or "").strip()
    press = bool(data.get("press", True))

    if not user_name or not cargo_id:
        return JsonResponse({"error": "Missing user_name or cargo_id"}, status=400)

    cargo_key, bm_id = _standard_cargo_key(user_name, cargo_id, data.get("bm_id") or "")

    _gl_set(user_name, cargo_key, {
        "armed": bool(press),
        "pressed_once": False if press else True,
        "user": user_name,
        "cargo_id": cargo_id,
        "bm_id": bm_id or "",
        "cargo_key": cargo_key,
    }, merge=True)

    print(f"[GREENLIGHT] Set armed={bool(press)} for cargo {cargo_key}"
          + (f" (BM={bm_id})" if bm_id else "") + f" (user: {user_name})")
    return JsonResponse({"ok": True, "armed": bool(press), "cargo_key": cargo_key})
