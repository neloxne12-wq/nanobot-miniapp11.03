"""Remove user by username - as if they never used the bot"""
import sqlite3

USERNAME = "Alexandrsowwme"  # without @

conn = sqlite3.connect("bot_database.db")
cur = conn.cursor()

# Find user (username may have @ or not)
cur.execute(
    "SELECT user_id, username FROM users WHERE LOWER(TRIM(REPLACE(COALESCE(username,''), '@', ''))) = ?",
    (USERNAME.lower(),)
)
row = cur.fetchone()

if not row:
    print("User not found")
    conn.close()
    exit(1)

user_id, username = row
print("Removing user_id:", user_id)

# Delete in correct order (child tables first)
tables = [
    "generations", "subscriptions", "referrals", 
    "promocode_usage", "channel_subscriptions", "payments", "users"
]

for table in tables:
    if table == "referrals":
        cur.execute("DELETE FROM referrals WHERE referrer_id = ? OR referred_id = ?", (user_id, user_id))
    else:
        cur.execute(f"DELETE FROM {table} WHERE user_id = ?", (user_id,))
    print(f"  {table}: {cur.rowcount} rows deleted")

conn.commit()
conn.close()
print("Done. User removed completely.")
