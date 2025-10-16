import os
import firebase_admin
from firebase_admin import credentials, firestore

KEY_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "core",
    "cargobot-473913-0d5ca84b1c39.json",
)

if not os.path.isfile(KEY_PATH):
    raise FileNotFoundError(
        f"Firestore key not found at: {KEY_PATH}\n"
        "Check the filename & location, and that it's not accidentally renamed."
    )

if not firebase_admin._apps:
    cred = credentials.Certificate(KEY_PATH)
    firebase_admin.initialize_app(cred)

db = firestore.client()
