import sqlite3
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict

logger = logging.getLogger(__name__)

# Admin user IDs - unlimited access
ADMIN_IDS = [1066928889]

class Database:
    def __init__(self, db_path: str = "bot_database.db"):
        self.db_path = db_path
        self.init_database()
    
    def get_connection(self):
        """Get database connection"""
        return sqlite3.connect(self.db_path)
    
    def init_database(self):
        """Initialize database tables"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Users table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                preferred_resolution TEXT DEFAULT '16:9',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Subscriptions table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                plan_type TEXT,
                generations_limit INTEGER,
                generations_used INTEGER DEFAULT 0,
                start_date TIMESTAMP,
                end_date TIMESTAMP,
                is_active BOOLEAN DEFAULT 1,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        """)
        
        # Generations history table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS generations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                prompt TEXT,
                generation_type TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        """)
        
        # Referrals table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS referrals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                referrer_id INTEGER,
                referred_id INTEGER,
                reward_claimed BOOLEAN DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (referrer_id) REFERENCES users (user_id),
                FOREIGN KEY (referred_id) REFERENCES users (user_id),
                UNIQUE(referred_id)
            )
        """)
        
        # Promocodes table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS promocodes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT UNIQUE,
                reward_type TEXT,
                reward_value INTEGER,
                max_uses INTEGER DEFAULT 0,
                current_uses INTEGER DEFAULT 0,
                expires_at TIMESTAMP,
                is_active BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Promocode usage tracking
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS promocode_usage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                promocode_id INTEGER,
                used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (user_id),
                FOREIGN KEY (promocode_id) REFERENCES promocodes (id),
                UNIQUE(user_id, promocode_id)
            )
        """)
        
        # Payments table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                telegram_payment_charge_id TEXT UNIQUE,
                provider_payment_charge_id TEXT,
                plan_type TEXT,
                amount INTEGER,
                currency TEXT DEFAULT 'RUB',
                generations_added INTEGER,
                status TEXT DEFAULT 'completed',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        """)
        
        # Add preferred_resolution column if it doesn't exist (migration)
        try:
            cursor.execute("SELECT preferred_resolution FROM users LIMIT 1")
        except sqlite3.OperationalError:
            cursor.execute("ALTER TABLE users ADD COLUMN preferred_resolution TEXT DEFAULT '16:9'")
            logger.info("Added preferred_resolution column to users table")
        
        # Add generations_since_channel_notify for paid users (show CTA every 4 gens)
        try:
            cursor.execute("SELECT generations_since_channel_notify FROM users LIMIT 1")
        except sqlite3.OperationalError:
            cursor.execute("ALTER TABLE users ADD COLUMN generations_since_channel_notify INTEGER DEFAULT 0")
            conn.commit()
            logger.info("Added generations_since_channel_notify column to users table")
        
        # Channel subscriptions table (for +2 gens reward)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS channel_subscriptions (
                user_id INTEGER PRIMARY KEY,
                channel_username TEXT DEFAULT '@AIARTpromp',
                subscribed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                reward_claimed INTEGER DEFAULT 0
            )
        """)
        
        # Templates table for mini app
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                description TEXT DEFAULT '',
                type TEXT DEFAULT 'single',
                lbl1 TEXT DEFAULT 'Загрузите ваше фото',
                lbl2 TEXT DEFAULT '',
                lbl3 TEXT DEFAULT '',
                lbl4 TEXT DEFAULT '',
                prompt TEXT DEFAULT '',
                category TEXT DEFAULT 'all',
                cost INTEGER DEFAULT 10,
                default_ratio TEXT DEFAULT '9:16',
                active INTEGER DEFAULT 1,
                preview TEXT DEFAULT '',
                preview_type TEXT DEFAULT 'image',
                uses INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Generation history table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS generation_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                name TEXT DEFAULT 'Генерация',
                prompt TEXT DEFAULT '',
                ratio TEXT DEFAULT '9:16',
                image_data TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Template categories table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS template_categories (
                id TEXT PRIMARY KEY,
                label TEXT NOT NULL,
                emoji TEXT DEFAULT '',
                sort_order INTEGER DEFAULT 0
            )
        """)
        
        # Insert default categories if empty
        cursor.execute("SELECT COUNT(*) FROM template_categories")
        if cursor.fetchone()[0] == 0:
            default_cats = [
                ('all', 'ВСЕ', '', 0),
                ('hairstyle', 'ПРИЧЕСКИ', '💇', 1),
                ('outfit', 'ОДЕЖДА', '👗', 2),
                ('postcard', 'ОТКРЫТКИ', '🎖', 3),
                ('portrait', 'ПОРТРЕТ', '📸', 4),
                ('anime', 'АНИМЕ', '🌸', 5),
                ('fantasy', 'ФЭНТЕЗИ', '⚔️', 6),
            ]
            cursor.executemany(
                "INSERT INTO template_categories (id, label, emoji, sort_order) VALUES (?, ?, ?, ?)",
                default_cats
            )
        
        conn.commit()
        conn.close()
        logger.info("Database initialized successfully")
    
    def get_or_create_user(self, user_id: int, username: str = None, 
                          first_name: str = None, last_name: str = None) -> Dict:
        """Get user or create if not exists"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Try to get user
        cursor.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        user = cursor.fetchone()
        
        if not user:
            # Create new user
            cursor.execute("""
                INSERT INTO users (user_id, username, first_name, last_name)
                VALUES (?, ?, ?, ?)
            """, (user_id, username, first_name, last_name))
            
            # New users get 3 free generations (welcome bonus)
            cursor.execute("""
                INSERT INTO subscriptions (user_id, plan_type, generations_limit, generations_used, start_date, end_date, is_active)
                VALUES (?, 'WELCOME', 2, 0, datetime('now'), datetime('now', '+30 days'), 1)
            """, (user_id,))
            
            conn.commit()
            logger.info(f"Created new user: {user_id} with 2 free generations")
        else:
            # Update last active
            cursor.execute("""
                UPDATE users SET last_active = CURRENT_TIMESTAMP
                WHERE user_id = ?
            """, (user_id,))
            conn.commit()
        
        conn.close()
        return self.get_user_info(user_id)
    
    def get_user_info(self, user_id: int) -> Dict:
        """Get complete user information with subscription"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Check if admin
        is_admin = user_id in ADMIN_IDS
        
        if is_admin:
            conn.close()
            return {
                "user_id": user_id,
                "is_admin": True,
                "status": "ADMIN",
                "generations_limit": 999999,
                "generations_used": 0,
                "generations_left": 999999
            }
        
        # Get active subscription
        cursor.execute("""
            SELECT plan_type, generations_limit, generations_used, end_date
            FROM subscriptions
            WHERE user_id = ? AND is_active = 1 AND end_date > datetime('now')
            ORDER BY end_date DESC
            LIMIT 1
        """, (user_id,))
        
        sub = cursor.fetchone()
        conn.close()
        
        if sub:
            plan_type, limit, used, end_date = sub
            return {
                "user_id": user_id,
                "is_admin": False,
                "status": plan_type,
                "generations_limit": limit,
                "generations_used": used,
                "generations_left": max(0, limit - used),
                "end_date": end_date
            }
        else:
            return {
                "user_id": user_id,
                "is_admin": False,
                "status": "FREE",
                "generations_limit": 0,
                "generations_used": 0,
                "generations_left": 0
            }
    
    def can_generate(self, user_id: int) -> tuple[bool, str]:
        """Check if user can generate image"""
        info = self.get_user_info(user_id)
        
        if info["is_admin"]:
            return True, "Admin - unlimited access"
        
        if info["generations_left"] > 0:
            return True, f"{info['generations_left']} generations left"
        
        return False, "Недостаточно генераций. Купите подписку в магазине."
    
    def use_generation(self, user_id: int, prompt: str, generation_type: str = "generate", cost: float = 1.0):
        """Record a generation usage with custom cost"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Record generation
        cursor.execute("""
            INSERT INTO generations (user_id, prompt, generation_type)
            VALUES (?, ?, ?)
        """, (user_id, prompt, generation_type))
        
        # Update subscription usage (only for non-admins)
        if user_id not in ADMIN_IDS:
            cursor.execute("""
                UPDATE subscriptions
                SET generations_used = generations_used + ?
                WHERE user_id = ? AND is_active = 1 AND end_date > datetime('now')
            """, (cost, user_id))
        
        conn.commit()
        conn.close()
        logger.info(f"User {user_id} used {cost} generation(s): {generation_type}")
    
    def add_subscription(self, user_id: int, plan_type: str, 
                        generations_limit: int, duration_days: int = 30):
        """Add subscription for user"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        start_date = datetime.now()
        end_date = start_date + timedelta(days=duration_days)
        
        # Deactivate old subscriptions
        cursor.execute("""
            UPDATE subscriptions SET is_active = 0
            WHERE user_id = ?
        """, (user_id,))
        
        # Add new subscription
        cursor.execute("""
            INSERT INTO subscriptions 
            (user_id, plan_type, generations_limit, start_date, end_date)
            VALUES (?, ?, ?, ?, ?)
        """, (user_id, plan_type, generations_limit, start_date, end_date))
        
        conn.commit()
        conn.close()
        logger.info(f"Added subscription {plan_type} for user {user_id}")
    
    def get_stats(self) -> Dict:
        """Get bot statistics"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM users")
        total_users = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM generations")
        total_generations = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT COUNT(*) FROM subscriptions 
            WHERE is_active = 1 AND end_date > datetime('now')
        """)
        active_subs = cursor.fetchone()[0]
        
        # Get generations today
        cursor.execute("""
            SELECT COUNT(*) FROM generations 
            WHERE DATE(created_at) = DATE('now')
        """)
        today_generations = cursor.fetchone()[0]
        
        # Get new users today
        cursor.execute("""
            SELECT COUNT(*) FROM users 
            WHERE DATE(created_at) = DATE('now')
        """)
        today_users = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            "total_users": total_users,
            "total_generations": total_generations,
            "active_subscriptions": active_subs,
            "today_generations": today_generations,
            "today_users": today_users
        }
    
    def search_user(self, query: str) -> list:
        """Search users by ID or username"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Remove @ symbol if present
        clean_query = query.lstrip('@')
        
        # Try to search by user_id if query is numeric
        if clean_query.isdigit():
            cursor.execute("""
                SELECT user_id, username, first_name, last_name, created_at, last_active
                FROM users WHERE user_id = ?
            """, (int(clean_query),))
        else:
            # Search by username or first_name (case-insensitive)
            cursor.execute("""
                SELECT user_id, username, first_name, last_name, created_at, last_active
                FROM users 
                WHERE LOWER(username) LIKE LOWER(?) 
                   OR LOWER(first_name) LIKE LOWER(?)
                   OR LOWER(last_name) LIKE LOWER(?)
            """, (f"%{clean_query}%", f"%{clean_query}%", f"%{clean_query}%"))
        
        results = cursor.fetchall()
        conn.close()
        
        return results
    
    def get_user_full_info(self, user_id: int) -> Dict:
        """Get full user information including all subscriptions and generations"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Get user basic info
        cursor.execute("""
            SELECT user_id, username, first_name, last_name, created_at, last_active
            FROM users WHERE user_id = ?
        """, (user_id,))
        
        user = cursor.fetchone()
        if not user:
            conn.close()
            return None
        
        # Get all subscriptions
        cursor.execute("""
            SELECT plan_type, generations_limit, generations_used, 
                   start_date, end_date, is_active
            FROM subscriptions
            WHERE user_id = ?
            ORDER BY start_date DESC
        """, (user_id,))
        
        subscriptions = cursor.fetchall()
        
        # Get generation count
        cursor.execute("""
            SELECT COUNT(*), generation_type
            FROM generations
            WHERE user_id = ?
            GROUP BY generation_type
        """, (user_id,))
        
        gen_stats = cursor.fetchall()
        
        conn.close()
        
        return {
            "user_id": user[0],
            "username": user[1],
            "first_name": user[2],
            "last_name": user[3],
            "created_at": user[4],
            "last_active": user[5],
            "subscriptions": subscriptions,
            "generation_stats": gen_stats,
            "is_admin": user_id in ADMIN_IDS
        }
    
    def cancel_subscription(self, user_id: int) -> bool:
        """Cancel active subscription for user"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE subscriptions SET is_active = 0
            WHERE user_id = ? AND is_active = 1
        """, (user_id,))
        
        conn.commit()
        affected = cursor.rowcount
        conn.close()
        
        logger.info(f"Cancelled subscription for user {user_id}")
        return affected > 0
    
    def add_generations(self, user_id: int, amount: int) -> bool:
        """Add generations to active subscription"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE subscriptions
            SET generations_limit = generations_limit + ?
            WHERE user_id = ? AND is_active = 1 AND end_date > datetime('now')
        """, (amount, user_id))
        
        conn.commit()
        affected = cursor.rowcount
        conn.close()
        
        if affected > 0:
            logger.info(f"Added {amount} generations to user {user_id}")
        return affected > 0
    
    def check_channel_reward_claimed(self, user_id: int) -> bool:
        """Check if user already claimed channel subscription reward"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT reward_claimed FROM channel_subscriptions WHERE user_id = ?",
            (user_id,)
        )
        result = cursor.fetchone()
        conn.close()
        return result[0] == 1 if result else False
    
    def claim_channel_reward(self, user_id: int) -> tuple[bool, str]:
        """Give +2 generations for channel subscription. Returns (success, reason)."""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Anti-abuse: already claimed
        cursor.execute(
            "SELECT reward_claimed FROM channel_subscriptions WHERE user_id = ?",
            (user_id,)
        )
        result = cursor.fetchone()
        if result and result[0] == 1:
            conn.close()
            return False, "already_claimed"
        
        # Add 2 generations to active subscription
        cursor.execute("""
            UPDATE subscriptions
            SET generations_limit = generations_limit + 1
            WHERE user_id = ? AND is_active = 1 AND end_date > datetime('now')
        """, (user_id,))
        
        if cursor.rowcount == 0:
            # No active subscription - create CHANNEL_BONUS
            end_date = datetime.now() + timedelta(days=30)
            cursor.execute("""
                INSERT INTO subscriptions
                (user_id, plan_type, generations_limit, generations_used, start_date, end_date, is_active)
                VALUES (?, 'CHANNEL_BONUS', 1, 0, datetime('now'), ?, 1)
            """, (user_id, end_date))
        
        # Mark reward as claimed
        cursor.execute("""
            INSERT OR REPLACE INTO channel_subscriptions (user_id, reward_claimed)
            VALUES (?, 1)
        """, (user_id,))
        
        conn.commit()
        conn.close()
        logger.info(f"Channel reward claimed: user {user_id} got +2 generations")
        return True, "ok"
    
    def get_generations_since_channel_notify(self, user_id: int) -> int:
        """Get count of generations since last channel notification (for paid users)"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT generations_since_channel_notify FROM users WHERE user_id = ?", (user_id,))
            row = cursor.fetchone()
            conn.close()
            return row[0] if row and row[0] is not None else 0
        except sqlite3.OperationalError:
            conn.close()
            return 0
    
    def increment_generations_since_channel_notify(self, user_id: int) -> int:
        """Increment counter, return new value. For paid users only."""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                UPDATE users SET generations_since_channel_notify = COALESCE(generations_since_channel_notify, 0) + 1
                WHERE user_id = ?
            """, (user_id,))
            conn.commit()
            cursor.execute("SELECT generations_since_channel_notify FROM users WHERE user_id = ?", (user_id,))
            row = cursor.fetchone()
            conn.close()
            return row[0] if row and row[0] is not None else 1
        except sqlite3.OperationalError:
            conn.close()
            return 1
    
    def reset_generations_since_channel_notify(self, user_id: int):
        """Reset counter after showing channel notification"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("UPDATE users SET generations_since_channel_notify = 0 WHERE user_id = ?", (user_id,))
            conn.commit()
        except sqlite3.OperationalError:
            pass
        conn.close()
    
    def has_any_paid_subscription(self, user_id: int) -> bool:
        """Check if user has any paid subscription (not WELCOME, REFERRAL_BONUS, etc.)"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT plan_type FROM subscriptions
            WHERE user_id = ? AND is_active = 1
            AND plan_type NOT IN ('WELCOME', 'REFERRAL_BONUS', 'CHANNEL_BONUS', 'PROMO_BONUS')
        """, (user_id,))
        result = cursor.fetchone()
        conn.close()
        return result is not None
    
    def get_all_users(self, limit: int = 50, offset: int = 0) -> list:
        """Get all users with pagination"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT user_id, username, first_name, created_at, last_active
            FROM users
            ORDER BY last_active DESC
            LIMIT ? OFFSET ?
        """, (limit, offset))
        
        users = cursor.fetchall()
        conn.close()
        
        return users
    
    def get_recent_generations(self, limit: int = 20) -> list:
        """Get recent generations"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT g.user_id, u.username, u.first_name, g.prompt, 
                   g.generation_type, g.created_at
            FROM generations g
            LEFT JOIN users u ON g.user_id = u.user_id
            ORDER BY g.created_at DESC
            LIMIT ?
        """, (limit,))
        
        generations = cursor.fetchall()
        conn.close()
        
        return generations
    
    def get_user_preferred_resolution(self, user_id: int) -> str:
        """Get user's preferred resolution"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT preferred_resolution FROM users WHERE user_id = ?
        """, (user_id,))
        
        result = cursor.fetchone()
        conn.close()
        
        return result[0] if result and result[0] else "16:9"
    
    def set_user_preferred_resolution(self, user_id: int, resolution: str) -> bool:
        """Set user's preferred resolution"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE users SET preferred_resolution = ? WHERE user_id = ?
        """, (resolution, user_id))
        
        conn.commit()
        affected = cursor.rowcount
        conn.close()
        
        if affected > 0:
            logger.info(f"Updated preferred resolution for user {user_id}: {resolution}")
        return affected > 0
    
    # ===== REFERRAL SYSTEM =====
    
    def add_referral(self, referrer_id: int, referred_id: int) -> tuple[bool, str]:
        """Add referral relationship with anti-abuse checks"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Anti-abuse: referred user already has a referrer
        cursor.execute("SELECT referrer_id FROM referrals WHERE referred_id = ?", (referred_id,))
        if cursor.fetchone():
            conn.close()
            return False, "Пользователь уже использовал реферальную ссылку"
        
        # Anti-abuse: self-referral
        if referrer_id == referred_id:
            conn.close()
            return False, "Нельзя использовать свою реферальную ссылку"
        
        # Anti-abuse: referrer exists
        cursor.execute("SELECT user_id FROM users WHERE user_id = ?", (referrer_id,))
        if not cursor.fetchone():
            conn.close()
            return False, "Реферер не найден"
        
        # Anti-abuse: max 15 referrals per 24h per referrer
        cursor.execute("""
            SELECT COUNT(*) FROM referrals 
            WHERE referrer_id = ? AND created_at > datetime('now', '-1 day')
        """, (referrer_id,))
        recent_count = cursor.fetchone()[0]
        if recent_count >= 15:
            conn.close()
            return False, "Достигнут лимит приглашений за сутки (15). Попробуйте завтра."
        
        # Add referral
        try:
            cursor.execute("""
                INSERT INTO referrals (referrer_id, referred_id)
                VALUES (?, ?)
            """, (referrer_id, referred_id))
            conn.commit()
            logger.info(f"Referral added: {referrer_id} -> {referred_id}")
            conn.close()
            return True, "Реферальная связь создана"
        except sqlite3.IntegrityError:
            conn.close()
            return False, "Ошибка создания реферальной связи"
    
    def claim_referral_reward(self, referrer_id: int, referred_id: int, reward_generations: int = 2) -> bool:
        """Claim referral reward (called after referred user makes first generation). Anti-abuse: one claim per referral."""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Anti-abuse: reward not yet claimed
        cursor.execute("""
            SELECT id, reward_claimed FROM referrals 
            WHERE referrer_id = ? AND referred_id = ? AND reward_claimed = 0
        """, (referrer_id, referred_id))
        
        referral = cursor.fetchone()
        if not referral:
            conn.close()
            return False
        
        # Mark reward as claimed
        cursor.execute("""
            UPDATE referrals SET reward_claimed = 1 
            WHERE id = ?
        """, (referral[0],))
        
        # Add generations to referrer's active subscription
        cursor.execute("""
            UPDATE subscriptions
            SET generations_limit = generations_limit + ?
            WHERE user_id = ? AND is_active = 1 AND end_date > datetime('now')
        """, (reward_generations, referrer_id))
        
        # If no active subscription, create a temporary one with reward
        if cursor.rowcount == 0:
            end_date = datetime.now() + timedelta(days=30)
            cursor.execute("""
                INSERT INTO subscriptions 
                (user_id, plan_type, generations_limit, generations_used, start_date, end_date, is_active)
                VALUES (?, 'REFERRAL_BONUS', ?, 0, datetime('now'), ?, 1)
            """, (referrer_id, reward_generations, end_date))
        
        conn.commit()
        conn.close()
        logger.info(f"Referral reward claimed: {referrer_id} got {reward_generations} generations")
        return True
    
    def get_referral_stats(self, user_id: int) -> Dict:
        """Get user's referral statistics"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Total referrals
        cursor.execute("""
            SELECT COUNT(*) FROM referrals WHERE referrer_id = ?
        """, (user_id,))
        total_referrals = cursor.fetchone()[0]
        
        # Claimed rewards
        cursor.execute("""
            SELECT COUNT(*) FROM referrals 
            WHERE referrer_id = ? AND reward_claimed = 1
        """, (user_id,))
        claimed_rewards = cursor.fetchone()[0]
        
        # Pending rewards
        cursor.execute("""
            SELECT COUNT(*) FROM referrals 
            WHERE referrer_id = ? AND reward_claimed = 0
        """, (user_id,))
        pending_rewards = cursor.fetchone()[0]
        
        # Recent referrals
        cursor.execute("""
            SELECT r.referred_id, u.first_name, u.username, r.reward_claimed, r.created_at
            FROM referrals r
            LEFT JOIN users u ON r.referred_id = u.user_id
            WHERE r.referrer_id = ?
            ORDER BY r.created_at DESC
            LIMIT 10
        """, (user_id,))
        recent_referrals = cursor.fetchall()
        
        conn.close()
        
        return {
            "total_referrals": total_referrals,
            "claimed_rewards": claimed_rewards,
            "pending_rewards": pending_rewards,
            "recent_referrals": recent_referrals
        }
    
    def get_referrer_id(self, referred_id: int) -> Optional[int]:
        """Get referrer ID for a referred user"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT referrer_id FROM referrals WHERE referred_id = ?
        """, (referred_id,))
        
        result = cursor.fetchone()
        conn.close()
        
        return result[0] if result else None
    
    # ===== PROMOCODE SYSTEM =====
    
    def create_promocode(self, code: str, reward_type: str, reward_value: int, 
                        max_uses: int = 0, expires_days: int = 30) -> tuple[bool, str]:
        """Create new promocode"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        code = code.upper().strip()
        
        # Check if code already exists
        cursor.execute("SELECT id FROM promocodes WHERE code = ?", (code,))
        if cursor.fetchone():
            conn.close()
            return False, "Промокод уже существует"
        
        expires_at = datetime.now() + timedelta(days=expires_days) if expires_days > 0 else None
        
        try:
            cursor.execute("""
                INSERT INTO promocodes (code, reward_type, reward_value, max_uses, expires_at)
                VALUES (?, ?, ?, ?, ?)
            """, (code, reward_type, reward_value, max_uses, expires_at))
            conn.commit()
            logger.info(f"Promocode created: {code} ({reward_type}: {reward_value})")
            conn.close()
            return True, f"Промокод {code} создан"
        except Exception as e:
            conn.close()
            logger.error(f"Error creating promocode: {e}")
            return False, "Ошибка создания промокода"
    
    def use_promocode(self, user_id: int, code: str) -> tuple[bool, str, Optional[Dict]]:
        """Use promocode with validation"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        code_normalized = code.upper().strip()
        
        # Get promocode (case-insensitive: SowWme, SOWWME, sowwme — all work)
        cursor.execute("""
            SELECT id, reward_type, reward_value, max_uses, current_uses, expires_at, is_active
            FROM promocodes WHERE UPPER(TRIM(code)) = ?
        """, (code_normalized,))
        
        promo = cursor.fetchone()
        if not promo:
            conn.close()
            return False, "Промокод не найден", None
        
        promo_id, reward_type, reward_value, max_uses, current_uses, expires_at, is_active = promo
        
        # Check if active
        if not is_active:
            conn.close()
            return False, "Промокод деактивирован", None
        
        # Check expiration
        if expires_at:
            if datetime.now() > datetime.fromisoformat(expires_at):
                conn.close()
                return False, "Промокод истек", None
        
        # Check max uses
        if max_uses > 0 and current_uses >= max_uses:
            conn.close()
            return False, "Лимит использований исчерпан", None
        
        # Check if user already used this promocode
        cursor.execute("""
            SELECT id FROM promocode_usage WHERE user_id = ? AND promocode_id = ?
        """, (user_id, promo_id))
        
        if cursor.fetchone():
            conn.close()
            return False, "Вы уже использовали этот промокод", None
        
        # Add usage record
        cursor.execute("""
            INSERT INTO promocode_usage (user_id, promocode_id)
            VALUES (?, ?)
        """, (user_id, promo_id))
        
        # Update current uses
        cursor.execute("""
            UPDATE promocodes SET current_uses = current_uses + 1
            WHERE id = ?
        """, (promo_id,))
        
        # Apply reward based on type
        if reward_type == "generations":
            # Add generations to active subscription or create new one
            cursor.execute("""
                UPDATE subscriptions
                SET generations_limit = generations_limit + ?
                WHERE user_id = ? AND is_active = 1 AND end_date > datetime('now')
            """, (reward_value, user_id))
            
            if cursor.rowcount == 0:
                # No active subscription, create temporary one
                end_date = datetime.now() + timedelta(days=30)
                cursor.execute("""
                    INSERT INTO subscriptions 
                    (user_id, plan_type, generations_limit, generations_used, start_date, end_date, is_active)
                    VALUES (?, 'PROMO_BONUS', ?, 0, datetime('now'), ?, 1)
                """, (user_id, reward_value, end_date))
        
        elif reward_type == "subscription":
            # reward_value is plan type ID (1=MINI, 2=STARTER, 3=PRO, 4=UNLIMITED)
            plans = {
                1: ("MINI", 5),
                2: ("STARTER", 10),
                3: ("PRO", 30),
                4: ("UNLIMITED", 90)
            }
            plan_name, gens = plans.get(reward_value, ("STARTER", 10))
            promo_tier = reward_value

            # Get user's active subscription (if any)
            cursor.execute("""
                SELECT id, plan_type FROM subscriptions
                WHERE user_id = ? AND is_active = 1 AND end_date > datetime('now')
            """, (user_id,))
            active_sub = cursor.fetchone()

            tier_map = {"MINI": 1, "STARTER": 2, "PRO": 3, "UNLIMITED": 4}

            promo_applied = "new"
            if not active_sub:
                # No subscription — add new
                end_date = datetime.now() + timedelta(days=30)
                cursor.execute("""
                    INSERT INTO subscriptions 
                    (user_id, plan_type, generations_limit, generations_used, start_date, end_date, is_active)
                    VALUES (?, ?, ?, 0, datetime('now'), ?, 1)
                """, (user_id, plan_name, gens, end_date))
                logger.info(f"Promocode {code}: new subscription {plan_name} for user {user_id}")
            else:
                current_plan = active_sub[1]
                current_tier = tier_map.get(current_plan, 0)
                if promo_tier > current_tier:
                    # Promo higher — replace subscription
                    promo_applied = "replaced"
                    cursor.execute("""
                        UPDATE subscriptions SET is_active = 0 WHERE user_id = ?
                    """, (user_id,))
                    end_date = datetime.now() + timedelta(days=30)
                    cursor.execute("""
                        INSERT INTO subscriptions 
                        (user_id, plan_type, generations_limit, generations_used, start_date, end_date, is_active)
                        VALUES (?, ?, ?, 0, datetime('now'), ?, 1)
                    """, (user_id, plan_name, gens, end_date))
                    logger.info(f"Promocode {code}: upgraded {current_plan} -> {plan_name} for user {user_id}")
                else:
                    # Promo same or lower — add generations to current subscription
                    promo_applied = "added"
                    cursor.execute("""
                        UPDATE subscriptions
                        SET generations_limit = generations_limit + ?
                        WHERE user_id = ? AND is_active = 1 AND end_date > datetime('now')
                    """, (gens, user_id))
                    logger.info(f"Promocode {code}: +{gens} gens to {current_plan} for user {user_id}")
        
        conn.commit()
        conn.close()
        
        logger.info(f"Promocode used: {code} by user {user_id}")
        result_reward = {"reward_type": reward_type, "reward_value": reward_value}
        if reward_type == "subscription":
            result_reward["promo_applied"] = promo_applied
            result_reward["plan_name"] = plan_name
            result_reward["gens"] = gens
        return True, "Промокод активирован!", result_reward
    
    def get_promocode_info(self, code: str) -> Optional[Dict]:
        """Get promocode information"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        code = code.upper().strip()
        
        cursor.execute("""
            SELECT id, code, reward_type, reward_value, max_uses, current_uses, 
                   expires_at, is_active, created_at
            FROM promocodes WHERE code = ?
        """, (code,))
        
        promo = cursor.fetchone()
        conn.close()
        
        if not promo:
            return None
        
        return {
            "id": promo[0],
            "code": promo[1],
            "reward_type": promo[2],
            "reward_value": promo[3],
            "max_uses": promo[4],
            "current_uses": promo[5],
            "expires_at": promo[6],
            "is_active": promo[7],
            "created_at": promo[8]
        }
    
    def get_all_promocodes(self) -> list:
        """Get all promocodes for admin"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, code, reward_type, reward_value, max_uses, current_uses, 
                   expires_at, is_active, created_at
            FROM promocodes
            ORDER BY created_at DESC
        """)
        
        promos = cursor.fetchall()
        conn.close()
        
        return promos
    
    def delete_promocode(self, code: str) -> bool:
        """Delete promocode"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        code = code.upper().strip()
        
        cursor.execute("DELETE FROM promocodes WHERE code = ?", (code,))
        conn.commit()
        affected = cursor.rowcount
        conn.close()
        
        if affected > 0:
            logger.info(f"Promocode deleted: {code}")
        return affected > 0
    
    def toggle_promocode(self, code: str) -> tuple[bool, bool]:
        """Toggle promocode active status"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        code = code.upper().strip()
        
        cursor.execute("SELECT is_active FROM promocodes WHERE code = ?", (code,))
        result = cursor.fetchone()
        
        if not result:
            conn.close()
            return False, False
        
        new_status = not result[0]
        cursor.execute("""
            UPDATE promocodes SET is_active = ? WHERE code = ?
        """, (new_status, code))
        
        conn.commit()
        conn.close()
        
        logger.info(f"Promocode {code} toggled to {new_status}")
        return True, new_status
    
    def get_user_generation_history(self, user_id: int, limit: int = 20) -> list:
        """Get user's generation history"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT prompt, generation_type, created_at
            FROM generations
            WHERE user_id = ?
            ORDER BY created_at DESC
            LIMIT ?
        """, (user_id, limit))
        
        history = cursor.fetchall()
        conn.close()
        
        return history
    
    def save_last_generated_image(self, user_id: int, file_id: str):
        """Save user's last generated image file_id"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Add column if doesn't exist
        try:
            cursor.execute("SELECT last_image_file_id FROM users LIMIT 1")
        except sqlite3.OperationalError:
            cursor.execute("ALTER TABLE users ADD COLUMN last_image_file_id TEXT")
            conn.commit()
            logger.info("Added last_image_file_id column to users table")
        
        cursor.execute("""
            UPDATE users SET last_image_file_id = ? WHERE user_id = ?
        """, (file_id, user_id))
        
        conn.commit()
        conn.close()
        logger.info(f"Saved last image for user {user_id}")
    
    def get_last_generated_image(self, user_id: int) -> Optional[str]:
        """Get user's last generated image file_id"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT last_image_file_id FROM users WHERE user_id = ?
            """, (user_id,))
            
            result = cursor.fetchone()
            conn.close()
            
            return result[0] if result and result[0] else None
        except sqlite3.OperationalError:
            # Column doesn't exist yet
            conn.close()
            return None
    
    # ===== PAYMENT SYSTEM =====
    
    def log_payment(self, user_id: int, telegram_charge_id: str, 
                    provider_charge_id: str, plan_type: str, 
                    amount: int, generations_added: int) -> bool:
        """Log successful payment"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO payments 
                (user_id, telegram_payment_charge_id, provider_payment_charge_id, 
                 plan_type, amount, generations_added)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (user_id, telegram_charge_id, provider_charge_id, 
                  plan_type, amount, generations_added))
            
            conn.commit()
            conn.close()
            logger.info(f"Payment logged: {plan_type} for user {user_id}, amount: {amount/100:.2f} RUB")
            return True
        except Exception as e:
            logger.error(f"Error logging payment: {e}")
            conn.close()
            return False
    
    def payment_exists(self, telegram_charge_id: str) -> bool:
        """Проверка: был ли платёж уже обработан"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM payments WHERE telegram_payment_charge_id = ?",
            (telegram_charge_id,)
        )
        count = cursor.fetchone()[0]
        conn.close()
        return count > 0
    
    def get_payment_stats(self) -> Dict:
        """Get payment statistics"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Total revenue
        cursor.execute("SELECT SUM(amount) FROM payments WHERE status = 'completed'")
        total_revenue = cursor.fetchone()[0] or 0
        
        # Total payments
        cursor.execute("SELECT COUNT(*) FROM payments WHERE status = 'completed'")
        total_payments = cursor.fetchone()[0]
        
        # Payments today
        cursor.execute("""
            SELECT COUNT(*), SUM(amount) 
            FROM payments 
            WHERE DATE(created_at) = DATE('now') AND status = 'completed'
        """)
        today_stats = cursor.fetchone()
        
        conn.close()
        
        return {
            "total_revenue": total_revenue / 100,  # В рублях
            "total_payments": total_payments,
            "today_payments": today_stats[0] or 0,
            "today_revenue": (today_stats[1] or 0) / 100
        }
    
    def get_recent_payments(self, limit: int = 20) -> list:
        """Get recent payments"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT p.user_id, u.username, u.first_name, 
                   p.plan_type, p.amount, p.generations_added, p.created_at
            FROM payments p
            LEFT JOIN users u ON p.user_id = u.user_id
            ORDER BY p.created_at DESC
            LIMIT ?
        """, (limit,))
        
        payments = cursor.fetchall()
        conn.close()
        
        return payments

    # ===== TEMPLATES SYSTEM (for mini app) =====
    
    def get_categories(self) -> list:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id, label, emoji FROM template_categories ORDER BY sort_order")
        rows = cursor.fetchall()
        conn.close()
        return [{"id": r[0], "label": r[1], "emoji": r[2]} for r in rows]
    
    def add_category(self, cat_id: str, label: str, emoji: str = '') -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT MAX(sort_order) FROM template_categories")
            max_order = cursor.fetchone()[0] or 0
            cursor.execute(
                "INSERT INTO template_categories (id, label, emoji, sort_order) VALUES (?, ?, ?, ?)",
                (cat_id, label.upper(), emoji, max_order + 1)
            )
            conn.commit()
            conn.close()
            return True
        except Exception:
            conn.close()
            return False
    
    def delete_category(self, cat_id: str) -> bool:
        if cat_id == 'all':
            return False
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM template_categories WHERE id = ?", (cat_id,))
        conn.commit()
        affected = cursor.rowcount
        conn.close()
        return affected > 0
    
    def get_templates(self, active_only: bool = True) -> list:
        conn = self.get_connection()
        cursor = conn.cursor()
        if active_only:
            cursor.execute("SELECT * FROM templates WHERE active = 1 ORDER BY uses DESC")
        else:
            cursor.execute("SELECT * FROM templates ORDER BY created_at DESC")
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        conn.close()
        return [dict(zip(columns, row)) for row in rows]
    
    def get_template(self, template_id: int) -> Optional[Dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM templates WHERE id = ?", (template_id,))
        columns = [desc[0] for desc in cursor.description]
        row = cursor.fetchone()
        conn.close()
        return dict(zip(columns, row)) if row else None
    
    def add_template(self, data: Dict) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO templates (name, description, type, lbl1, lbl2, lbl3, lbl4,
                                   prompt, category, cost, default_ratio, active,
                                   preview, preview_type, uses)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data.get('name', ''), data.get('description', ''),
            data.get('type', 'single'),
            data.get('lbl1', 'Загрузите ваше фото'), data.get('lbl2', ''),
            data.get('lbl3', ''), data.get('lbl4', ''),
            data.get('prompt', ''), data.get('category', 'all'),
            data.get('cost', 10), data.get('default_ratio', '9:16'),
            1 if data.get('active', True) else 0,
            data.get('preview', ''), data.get('preview_type', 'image'),
            data.get('uses', 0)
        ))
        conn.commit()
        new_id = cursor.lastrowid
        conn.close()
        return new_id
    
    def update_template(self, template_id: int, data: Dict) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()
        fields = []
        values = []
        for key in ['name', 'description', 'type', 'lbl1', 'lbl2', 'lbl3', 'lbl4',
                     'prompt', 'category', 'cost', 'default_ratio', 'active',
                     'preview', 'preview_type']:
            if key in data:
                fields.append(f"{key} = ?")
                values.append(data[key])
        if not fields:
            conn.close()
            return False
        values.append(template_id)
        cursor.execute(f"UPDATE templates SET {', '.join(fields)} WHERE id = ?", values)
        conn.commit()
        affected = cursor.rowcount
        conn.close()
        return affected > 0
    
    def delete_template(self, template_id: int) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM templates WHERE id = ?", (template_id,))
        conn.commit()
        affected = cursor.rowcount
        conn.close()
        return affected > 0
    
    def increment_template_uses(self, template_id: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE templates SET uses = uses + 1 WHERE id = ?", (template_id,))
        conn.commit()
        conn.close()

    def add_to_history(self, user_id: int, name: str, prompt: str, ratio: str, image_data: str) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO generation_history (user_id, name, prompt, ratio, image_data) VALUES (?, ?, ?, ?, ?)",
            (user_id, name, prompt, ratio, image_data)
        )
        conn.commit()
        new_id = cursor.lastrowid
        cursor.execute(
            "DELETE FROM generation_history WHERE id NOT IN (SELECT id FROM generation_history WHERE user_id = ? ORDER BY created_at DESC LIMIT 30) AND user_id = ?",
            (user_id, user_id)
        )
        conn.commit()
        conn.close()
        return new_id

    def get_history(self, user_id: int, limit: int = 20) -> list:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, name, prompt, ratio, image_data, created_at FROM generation_history WHERE user_id = ? AND created_at > datetime('now', '-7 days') ORDER BY created_at DESC LIMIT ?",
            (user_id, limit)
        )
        columns = [d[0] for d in cursor.description]
        rows = cursor.fetchall()
        conn.close()
        return [dict(zip(columns, row)) for row in rows]

    def delete_history_item(self, user_id: int, item_id: int) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM generation_history WHERE id = ? AND user_id = ?", (item_id, user_id))
        conn.commit()
        affected = cursor.rowcount
        conn.close()
        return affected > 0


# Global database instance
db = Database()
