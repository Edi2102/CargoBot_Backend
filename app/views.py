import json
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from google.cloud import firestore
from app.firestore_client import db
from app.firestore_utils import doc_id_from_username


@csrf_exempt
def active_products(request):
    if request.method != "POST":
        return JsonResponse({"error": "POST required"}, status=405)

    data = json.loads(request.body or "{}")
    n = int(data.get("active_products", 0))
    user_name = (data.get("user_name") or "").strip()
    rows = data.get("rows", [])

    own_rows = [row for row in rows if (row.get("owner") or "").strip() == user_name]
    incoming_ids = [r.get("id") for r in own_rows if r.get("id")]

    print(f"[BACKEND] User: {user_name or 'Unknown'}")
    print(f"[BACKEND] Active products: {n}")
    print(f"[BACKEND] Own products: {len(own_rows)}")
    for i, row in enumerate(own_rows, 1):
        print(f"--- Own Row {i} ---  ID: {row.get('id')}  Owner: {row.get('owner')}")

    users_col = db.collection("Cargo")
    doc_id = doc_id_from_username(user_name)
    user_doc = users_col.document(doc_id)

    snap = user_doc.get()
    if not snap.exists:
        user_doc.set({
            "user": user_name,
            "ids": incoming_ids,
        })
        current_ids = incoming_ids[:]
        print(f"[FIRESTORE] Created new doc for '{user_name}' with {len(incoming_ids)} ids.")
    else:
        existing = snap.to_dict() or {}
        existing_ids = existing.get("ids", []) or []

        missing = [x for x in incoming_ids if x not in existing_ids]

        if missing:
            user_doc.update({
                "ids": firestore.ArrayUnion(missing)
            })
            print(f"[FIRESTORE] Added {len(missing)} new ids for '{user_name}': {', '.join(missing)}")
        else:
            print(f"[FIRESTORE] No new ids to add for '{user_name}'.")

        snap = user_doc.get()
        current_ids = (snap.to_dict() or {}).get("ids", []) or []

    print(f"[FIRESTORE] Saved IDs for {user_name}: {', '.join(current_ids) if current_ids else '(none)'}")

    return JsonResponse({
        "ok": True,
        "received_rows": len(rows),
        "own_rows": len(own_rows),
        "user_name": user_name,
        "ids_now": current_ids,
    })
