"""Reset free WELCOME generations for user"""
import sqlite3

USERNAME = "Alexandrsowwme"

conn = sqlite3.connect("bot_database.db")
cur = conn.cursor()
cur.execute(
    "SELECT user_id FROM users WHERE LOWER(TRIM(REPLACE(COALESCE(username,''), '@', ''))) = ?",
    (USERNAME.lower(),)
)
row = cur.fetchone()
if row:
    user_id = row[0]
    cur.execute(
        "UPDATE subscriptions SET generations_used = generations_limit WHERE user_id = ? AND plan_type = ?",
        (user_id, "WELCOME")
    )
    print(f"User {user_id}: WELCOME generations exhausted ({cur.rowcount} rows)")
else:
    print("User not found")
conn.commit()
conn.close()
