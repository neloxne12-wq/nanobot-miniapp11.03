"""Add 1 generation to user"""
import sys
sys.path.insert(0, ".")
from database import db

USERNAME = "Alexandrsowwme"

conn = db.get_connection()
cur = conn.cursor()
cur.execute(
    "SELECT user_id FROM users WHERE LOWER(TRIM(REPLACE(COALESCE(username,''), '@', ''))) = ?",
    (USERNAME.lower(),)
)
row = cur.fetchone()
conn.close()

if row:
    user_id = row[0]
    success = db.add_generations(user_id, 1)
    print(f"User {user_id}: added 1 generation" if success else f"User {user_id}: no active sub to add to")
else:
    print("User not found")
