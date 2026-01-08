import os
import sqlite3
import random
import asyncio
from datetime import datetime, timezone, timedelta
import re
import discord
from discord import ui
from discord.ext import commands, tasks
import json
import time

from dotenv import load_dotenv
load_dotenv()
# =========================
# CONFIG
# =========================
TOKEN = os.getenv("DISCORD_TOKEN")

SPAWN_EVERY_MINUTES = 180
PITY_LIMIT = 5000
HIGHEST_RARITY = "SS"
COMMAND_PREFIX = "!"
LEGENDARY_TOKEN = "Legendary"
MYTHICAL_TOKEN = "Mythical"

RARITIES = {
    "F":    {"weight": 45.331, "color": 0x7f8c8d},  # grey
    "C":  {"weight": 35, "color": 0x2ecc71},  # light green
    "B":      {"weight": 15, "color": 0x3498db},  # blue
    "A":      {"weight": 6.67,  "color": 0x9b59b6},  # purple
    "S": {"weight": 2,  "color": 0xf22613},  # orange/red
    "SS":  {"weight": 0.001,  "color": 0xFCF97E},  # white
}

RARITY_EMOJI = {
    "F": "🇫",
    "C": "🇨 ",
    "B": "🇧",
    "A": "🇦",
    "S": "🇸",
    "SS": "🌟"
}

RARITY_ORDER = {
    "F": 1,
    "C": 2,
    "B": 3,
    "A": 4,
    "S": 5,
    "SS": 6
}

RARITY_IMAGES = {
    "F": "https://raw.githubusercontent.com/Sxntez/Sxntez-s-Bot/refs/heads/main/F.png",
    "C": "https://raw.githubusercontent.com/Sxntez/Sxntez-s-Bot/refs/heads/main/CC.png",
    "B": "https://raw.githubusercontent.com/Sxntez/Sxntez-s-Bot/refs/heads/main/BB.png",
    "A": "https://raw.githubusercontent.com/Sxntez/Sxntez-s-Bot/refs/heads/main/AA.png",
    "S": "https://raw.githubusercontent.com/Sxntez/Sxntez-s-Bot/refs/heads/main/SS.png",
    "SS": "https://raw.githubusercontent.com/Sxntez/Sxntez-s-Bot/refs/heads/main/S%2B.png",
}

VALID_RARITIES = set(RARITIES.keys())

# Quest/Chest rarities (separate from card rarities)
QUEST_CHEST_RARITIES = {"Common", "Uncommon", "Rare", "Epic", "Legendary", "Mythical"}

def normalize_rarity(r: str) -> str:
    """
    Normalizes card rarity codes only (F, C, B, A, S, SS).
    For quest/chest rarities (Common, Uncommon, etc.), use add_shards/get_shards directly.
    """
    if not r:
        return ""
    r = r.strip().upper()
    if r in ("S+", "SS"):
        # if you still type S+ anywhere, map it to SS or reject it.
        return "SS"
    return r

PACK_IMAGES = {
    "C": "https://raw.githubusercontent.com/Sxntez/Sxntez-s-Bot/refs/heads/main/C2%20pack.PNG",
    "B": "https://raw.githubusercontent.com/Sxntez/Sxntez-s-Bot/refs/heads/main/B%20pack.PNG",
    "A": "https://raw.githubusercontent.com/Sxntez/Sxntez-s-Bot/refs/heads/main/A%20pack.PNG",
    "S": "https://raw.githubusercontent.com/Sxntez/Sxntez-s-Bot/refs/heads/main/S%20Pack.PNG",
    "S+": "https://raw.githubusercontent.com/Sxntez/Sxntez-s-Bot/refs/heads/main/S2%20pack.PNG",
    }

CHESTS = {
    "Common": {
        "key": "Common",
        "spawn_chance": 0.40,  # 25% chance per card spawn
        "gold": (500, 1500),
        "shards": {"Common": (10, 30)},
        "tokens": {}
    },
    "Uncommon": {
        "key": "Uncommon",
        "spawn_chance": 0.30,  # 25% chance per card spawn
        "gold": (500, 1500),
        "shards": {"Common": (10, 30)},
        "tokens": {}
    },
    "Rare": {
        "key": "Rare",
        "spawn_chance": 0.175,
        "gold": (2000, 5000),
        "shards": {
            "Rare": (10, 30),
            "Epic": (5, 15)
        },
        "tokens": {}
    },
    "Epic": {
    "key": "Epic",
    "spawn_chance": 0.075,   # tweak as you like
    "gold": (5000, 10000),
    "shards": {
        "Rare": (15, 40),
        "Epic": (10, 30)
    },
        "tokens": {}
    },
    "Legendary": {
        "key": "Legendary",
        "spawn_chance": 0.05,
        "gold": (8000, 15000),
        "shards": {
            "Epic": (15, 40),
            "Legendary": (5, 15)
        },
        "tokens": {
            LEGENDARY_TOKEN: (0, 1)
        }
    },
}

CHEST_IMAGES = {
    "Common": "https://raw.githubusercontent.com/Sxntez/Sxntez-s-Bot/main/Common%20chest.PNG",
    "Uncommon": "https://raw.githubusercontent.com/Sxntez/Sxntez-s-Bot/main/Uncommon%20Chest.PNG",
    "Rare": "https://raw.githubusercontent.com/Sxntez/Sxntez-s-Bot/main/Rare%20chest.PNG",
    "Epic": "https://raw.githubusercontent.com/Sxntez/Sxntez-s-Bot/main/Epic%20Chest.PNG",
    "Legendary": "https://raw.githubusercontent.com/Sxntez/Sxntez-s-Bot/main/Legendary%20chest.PNG",
}

SELL_BASE_REWARDS = {
    "F":    {"gold": 100,   "shards": 5},
    "C":  {"gold": 250,   "shards": 10},
    "B":      {"gold": 500,   "shards": 25},
    "A":      {"gold": 1750,  "shards": 50},
    "S": {"gold": 5000,  "shards": 100},
    "SS":  {"gold": 20000, "shards": 250},
}

CRAFT_RECIPES = {
    "SS": {
        "gold": 50_000,
        "shards": 500
    },
    "S": {
        "gold": 10_000,
        "shards": 200
    },
    "A": {
        "gold": 2_000,
        "shards": 50
    }
}

DAILY_RARITY_TIERS = {
    1:  ["Common"],
    2:  ["Common", "Uncommon"],
    3:  ["Uncommon"],
    4:  ["Uncommon", "Rare"],
    5:  ["Rare"],
    6:  ["Rare", "Epic"],
    7:  ["Epic"],
    8:  ["Epic", "Legendary"],
    9:  ["Legendary"],
    10: ["Legendary"],
    11: ["Epic", "Legendary"],
    12: ["Legendary"],
    13: ["Legendary"],
    14: ["Mythical"],  # shards only
}

DAILY_GOLD_RANGE = {
    1:  (300, 700),
    2:  (600, 1200),
    3:  (900, 1800),
    4:  (1500, 3000),
    5:  (2500, 5000),
    6:  (4000, 8000),
    7:  (7000, 12000),
    8:  (10000, 15000),
    9:  (13000, 20000),
    10: (17500, 28000),
    11: (25000, 35000),
    12: (32500, 42500),
    13: (38000, 50000),
    14: (42000, 60000),
}

PACK_ODDS = {
    "C": {
        "filler": [
            ("F", 75),
            ("C", 15),
            ("B", 10),
        ],
        "pog": [
            ("C", 75),
            ("B", 25),
        ]
    },
    "B": {
        "filler": [
            ("F", 35),
            ("C", 45),
            ("B", 20),
        ],
        "pog": [
            ("B", 60),
            ("A", 37),
            ("S", 3),
        ]
    },
    "A": {
        "filler": [
            ("F", 10),
            ("C", 35),
            ("B", 40),
            ("A", 15),
        ],
        "pog": [
            ("A", 55),
            ("S", 44),
            ("SS", 1),
        ]
    },
    "S": {
        "filler": [
            ("C", 15),
            ("B", 45),
            ("A", 40),
        ],
        "pog": [
            ("A", 30),
            ("S",60),
            ("SS", 10),
        ]
    },
    "S+": {
        "filler": [
            ("B", 20),
            ("A", 65),
            ("S", 15),
        ],
        "pog": [
            ("S", 65),
            ("SS", 35),
        ]
    },
}

PACK_COSTS = {
    "C": {
        "gold": 500,
        "shards": {
            "Common": 20,
            "Uncommon": 5,
        }
    },
    "B": {
        "gold": 2500,
        "shards": {
            "Uncommon": 25,
            "Rare": 10,
        }
    },
    "A": {
        "gold": 10000,
        "shards": {
            "Rare": 30,
            "Epic": 10,
        }
    },
    "S": {
        "legendary_token": 5
    },
    "S+": {
        "mythical_token": 1
    }
}

# =========================
# PATHS (DB files created automatically)
# =========================
BASE_DIR = os.path.dirname(__file__)
ROSTER_DB_PATH = os.path.join(BASE_DIR, "anime_characters.db")
CLAIMS_DB_PATH = os.path.join(BASE_DIR, "claims.db")


# =========================
# DATABASE CONNECTIONS
# =========================
# NOTE: discord.py runs in one event loop thread, so simple sqlite usage is fine for MVP.
roster_conn = sqlite3.connect(ROSTER_DB_PATH)
roster_cur = roster_conn.cursor()

claims_conn = sqlite3.connect(CLAIMS_DB_PATH)
claims_cur = claims_conn.cursor()
        
async def try_spawn_from_activity(guild_id: int):
    data = server_state.get(guild_id)
    if not data:
        return

    # Don't spawn if one is already active
    if data.get("active_spawn"):
        return

    now = time.time()
    last_spawn = data.get("last_spawn_time", 0)

    # ⏱️ Not enough time passed
    if now - last_spawn < SPAWN_EVERY_MINUTES:
        return

    # 💬 No activity → no spawn
    if not data.get("activity_since_last_spawn"):
        return

    # ✅ Spawn allowed
    await spawn_in_guild(guild_id)

    # Reset gates
    data["last_spawn_time"] = now
    data["activity_since_last_spawn"] = False

def get_user_active_trade(user_id: int):
    """Get the active trade ID for a user"""
    claims_cur.execute("""
        SELECT trade_id FROM trades 
        WHERE (initiator_id = ? OR receiver_id = ?)
        AND status = 'pending'
        ORDER BY created_at DESC
        LIMIT 1
    """, (user_id, user_id))
    
    row = claims_cur.fetchone()
    return row[0] if row else None

def get_opened_chest_image(rarity: str) -> str:
    OPENED_CHEST_IMAGES = {
        "Common": "https://raw.githubusercontent.com/Sxntez/Sxntez-s-Bot/main/Opened%20common.PNG",
        "Uncommon": "https://raw.githubusercontent.com/Sxntez/Sxntez-s-Bot/main/Opened%20uncommon.PNG",
        "Rare": "https://raw.githubusercontent.com/Sxntez/Sxntez-s-Bot/main/Opened%20rare.PNG",
        "Epic": "https://raw.githubusercontent.com/Sxntez/Sxntez-s-Bot/main/Opened%20epic.PNG",
        "Legendary": "https://raw.githubusercontent.com/Sxntez/Sxntez-s-Bot/main/Opened%20legendary.PNG",
    }
    return OPENED_CHEST_IMAGES.get(rarity)

def migrate_quests():
    claims_cur.execute("""
        CREATE TABLE IF NOT EXISTS user_quests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            guild_id INTEGER NOT NULL,
            tier TEXT NOT NULL,
            team_power REAL NOT NULL,
            success_rate REAL NOT NULL,
            rewards TEXT,  -- JSON encoded rewards
            start_time TEXT NOT NULL,
            end_time TEXT NOT NULL,
            completed BOOLEAN DEFAULT FALSE,
            collected BOOLEAN DEFAULT FALSE,
            card_ids TEXT  -- JSON array of card IDs used
        )
    """)
    claims_conn.commit()

def migrate_teams():
    # Check if old table structure exists (with guild_id)
    claims_cur.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='teams'
    """)
    old_table_exists = claims_cur.fetchone() is not None
    
    if old_table_exists:
        # Check if old structure has guild_id column
        claims_cur.execute("PRAGMA table_info(teams)")
        columns = [row[1] for row in claims_cur.fetchall()]
        
        if "guild_id" in columns:
            # Migrate from old structure (guild_id + user_id) to new (user_id only)
            print("Migrating teams from per-server to global...")
            
            # Create new table structure
            claims_cur.execute("""
                CREATE TABLE IF NOT EXISTS teams_new (
                    user_id INTEGER NOT NULL PRIMARY KEY,
                    tank_card_id INTEGER,
                    dps1_card_id INTEGER,
                    dps2_card_id INTEGER,
                    support_card_id INTEGER,
                    FOREIGN KEY (tank_card_id) REFERENCES claimed_cards(id),
                    FOREIGN KEY (dps1_card_id) REFERENCES claimed_cards(id),
                    FOREIGN KEY (dps2_card_id) REFERENCES claimed_cards(id),
                    FOREIGN KEY (support_card_id) REFERENCES claimed_cards(id)
                )
            """)
            
            # Migrate data: for each user, keep the first team found (merge if needed)
            claims_cur.execute("""
                SELECT DISTINCT user_id FROM teams
            """)
            user_ids = [row[0] for row in claims_cur.fetchall()]
            
            for user_id in user_ids:
                # Get all teams for this user across all servers
                claims_cur.execute("""
                    SELECT tank_card_id, dps1_card_id, dps2_card_id, support_card_id
                    FROM teams
                    WHERE user_id = ?
                """, (user_id,))
                
                all_teams = claims_cur.fetchall()
                
                # Merge teams: use first non-null value for each slot
                merged_team = [None, None, None, None]
                for team in all_teams:
                    for i, slot in enumerate(team):
                        if slot is not None and merged_team[i] is None:
                            merged_team[i] = slot
                
                # Insert merged team
                claims_cur.execute("""
                    INSERT OR REPLACE INTO teams_new 
                    (user_id, tank_card_id, dps1_card_id, dps2_card_id, support_card_id)
                    VALUES (?, ?, ?, ?, ?)
                """, (user_id, merged_team[0], merged_team[1], merged_team[2], merged_team[3]))
            
            # Drop old table and rename new one
            claims_cur.execute("DROP TABLE teams")
            claims_cur.execute("ALTER TABLE teams_new RENAME TO teams")
            claims_conn.commit()
            print("Teams migration completed!")
            return
    
    # Create new table structure (if it doesn't exist)
    claims_cur.execute("""
        CREATE TABLE IF NOT EXISTS teams (
            user_id INTEGER NOT NULL PRIMARY KEY,
            tank_card_id INTEGER,
            dps1_card_id INTEGER,
            dps2_card_id INTEGER,
            support_card_id INTEGER,
            FOREIGN KEY (tank_card_id) REFERENCES claimed_cards(id),
            FOREIGN KEY (dps1_card_id) REFERENCES claimed_cards(id),
            FOREIGN KEY (dps2_card_id) REFERENCES claimed_cards(id),
            FOREIGN KEY (support_card_id) REFERENCES claimed_cards(id)
        )
    """)
    claims_conn.commit()
    
def migrate_claimed_cards_softwipe():
    # SQLite doesn't support ADD COLUMN IF NOT EXISTS, so we try and ignore failures.
    for stmt in [
        "ALTER TABLE claimed_cards ADD COLUMN active INTEGER NOT NULL DEFAULT 1",
        "ALTER TABLE claimed_cards ADD COLUMN wiped_at TEXT",
        "ALTER TABLE claimed_cards ADD COLUMN wiped_by INTEGER",
        "ALTER TABLE claimed_cards ADD COLUMN wipe_reason TEXT",
    ]:
        try:
            claims_cur.execute(stmt)
        except sqlite3.OperationalError:
            pass  # column already exists
    claims_conn.commit()
    
def migrate_claimed_cards_stats():
    for stmt in [
        "ALTER TABLE claimed_cards ADD COLUMN role TEXT",
        "ALTER TABLE claimed_cards ADD COLUMN hp INTEGER",
        "ALTER TABLE claimed_cards ADD COLUMN atk INTEGER",
        "ALTER TABLE claimed_cards ADD COLUMN def INTEGER",
        "ALTER TABLE claimed_cards ADD COLUMN luck REAL",
        "ALTER TABLE claimed_cards ADD COLUMN power REAL",
    ]:
        try:
            claims_cur.execute(stmt)
        except sqlite3.OperationalError:
            pass
    claims_conn.commit()
    
def migrate_user_shards():
    claims_cur.execute("""
    CREATE TABLE IF NOT EXISTS user_shards (
        user_id INTEGER NOT NULL,
        rarity TEXT NOT NULL,
        amount INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (user_id, rarity)
    )
    """)
    claims_conn.commit()

def migrate_card_tags():
    claims_cur.execute("""
    CREATE TABLE IF NOT EXISTS card_tags (
        card_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        tag TEXT NOT NULL,
        created_at TEXT NOT NULL,
        PRIMARY KEY (card_id, user_id, tag),
        FOREIGN KEY (card_id) REFERENCES claimed_cards(id)
    )
    """)
    claims_conn.commit()

def migrate_card_tags_single():
    # Remove extra tags, keep the oldest per (card, user)
    claims_cur.execute("""
        DELETE FROM card_tags
        WHERE rowid NOT IN (
            SELECT MIN(rowid)
            FROM card_tags
            GROUP BY card_id, user_id
        )
    """)
    claims_conn.commit()
    
def migrate_user_gold():
    claims_cur.execute("""
        CREATE TABLE IF NOT EXISTS user_gold (
            user_id INTEGER PRIMARY KEY,
            gold INTEGER NOT NULL DEFAULT 0
        )
    """)
    claims_conn.commit()
    
def migrate_user_tokens():
    claims_cur.execute("""
        CREATE TABLE IF NOT EXISTS user_tokens (
            user_id INTEGER NOT NULL,
            token_type TEXT NOT NULL,
            amount INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (user_id, token_type)
        )
    """)
    claims_conn.commit()
    
def init_databases():
    # ---- roster.db: characters available to spawn naturally
    roster_cur.execute("""
        CREATE TABLE IF NOT EXISTS Characters (
        Id INTEGER PRIMARY KEY AUTOINCREMENT,
        Name TEXT NOT NULL,
        Source TEXT NOT NULL
        )
        """)

    roster_cur.execute("""
        CREATE TABLE IF NOT EXISTS character_images (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        character_id INTEGER NOT NULL,
        image_url TEXT NOT NULL,
        UNIQUE(character_id, image_url),
        FOREIGN KEY(character_id) REFERENCES Characters(Id)
        )
        """)

    roster_conn.commit()

    # ---- claims.db: per-server config + claimed cards with global IDs
    claims_cur.execute("""
    CREATE TABLE IF NOT EXISTS guild_config (
        guild_id INTEGER PRIMARY KEY,
        spawn_channel_id INTEGER,
        spawns_since_mythical INTEGER NOT NULL DEFAULT 0
    )
    """)

    # Global ID exists ONLY when claimed (id is autoincrement)
    # Store full snapshot (name/anime/image/rarity) so forced spawns work too.
    claims_cur.execute("""
    CREATE TABLE IF NOT EXISTS claimed_cards (
        id INTEGER PRIMARY KEY AUTOINCREMENT,   -- GLOBAL ID
        guild_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        roster_character_id INTEGER,            -- nullable (forced spawns may not exist in roster)
        name TEXT NOT NULL,
        anime TEXT NOT NULL,
        rarity TEXT NOT NULL,
        image_url TEXT NOT NULL,
        claimed_at TEXT NOT NULL
    )
    """)
    claims_conn.commit()


# =========================
# BOT SETUP
# =========================
intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix=COMMAND_PREFIX, intents=intents, help_command=None)

# List of user IDs allowed to use admin/bot owner commands
# Add your Discord user ID and any bot developer IDs here
BOT_OWNER_IDS = [515880067268739072, 432008257175945237, 717613530219806720, 266334464978845696, 558068224575537152, 894576628255055892
    # Add your Discord user ID here (right-click your profile in Discord -> Copy ID)
    # Example: 123456789012345678,
    # Add other bot developer IDs here
]

def is_bot_owner():
    """Check if the user is a bot owner (by user ID)"""
    def predicate(ctx):
        if not BOT_OWNER_IDS:
            # If list is empty, fall back to checking if user is bot owner
            return ctx.author.id == bot.owner_id if bot.owner_id else False
        return ctx.author.id in BOT_OWNER_IDS
    return commands.check(predicate)

# In-memory active spawns only (unclaimed spawns are ephemeral by your design)
# server_state[guild_id]["active_chest"] = {
#   "rarity": str,
#   "message_id": int,
#   "opened": bool
# }
#   "channel_id": int | None,
#   "spawns_since_mythical": int,
#   "active_spawn": dict | None,  # {name, anime, rarity, image_url, roster_character_id, claimed}
#   "claimed": bool
# }
server_state = {}
active_trades = {}


# =========================
# DB HELPERS (claims.db)
# =========================

# Base-36 character set for alphanumeric codes (0-9, a-z only, lowercase)
BASE36_CHARS = "0123456789abcdefghijklmnopqrstuvwxyz"
BASE36 = len(BASE36_CHARS)

def int_to_code(n: int) -> str:
    """
    Convert an integer ID to a base-36 alphanumeric code (lowercase + numbers only).
    Codes start at 3 characters and increase length when needed.
    Examples: 1 -> "001", 36 -> "010", 1296 -> "100", 46656 -> "1000", 46657 -> "1001"
    """
    if n <= 0:
        return "000"
    
    # Determine minimum code length based on ID value
    # IDs 1-46656 use 3 chars, IDs 46657-1679616 use 4 chars, etc.
    if n <= 36**3:  # IDs 1-46,656 -> 3 chars
        code_length = 3
    elif n <= 36**4:  # IDs 46,657-1,679,616 -> 4 chars
        code_length = 4
    elif n <= 36**5:  # IDs 1,679,617-60,466,176 -> 5 chars
        code_length = 5
    elif n <= 36**6:  # IDs 60,466,177-2,176,782,336 -> 6 chars
        code_length = 6
    else:
        # For very large IDs, calculate minimum length needed
        code_length = 7
        threshold = 36**6
        while n > threshold:
            code_length += 1
            threshold *= BASE36
    
    # Convert to base-36
    result = []
    temp = n
    
    while temp > 0:
        result.append(BASE36_CHARS[temp % BASE36])
        temp //= BASE36
    
    # Reverse to get correct order
    code = ''.join(reversed(result)) if result else '0'
    
    # Pad to minimum length (always pad to code_length)
    code = code.zfill(code_length)
    
    return code

def code_to_int(code: str) -> int:
    """
    Convert a base-36 alphanumeric code (lowercase + numbers only) back to an integer ID.
    Returns None if invalid code.
    Codes must be at least 3 characters long.
    """
    if not code:
        return None
    
    # Normalize to lowercase
    code = code.lower()
    
    # Reject codes shorter than 3 characters (codes start at 3 chars)
    if len(code) < 3:
        return None
    
    # Validate characters (only lowercase letters and numbers)
    for char in code:
        if char not in BASE36_CHARS:
            return None
    
    # Convert from base-36 (direct conversion, no offset)
    result = 0
    for char in code:
        result = result * BASE36 + BASE36_CHARS.index(char)
    
    return result

def parse_card_id(input_str: str):
    """
    Parse a card ID input (alphanumeric code only, no numeric IDs).
    Returns the integer ID, or None if invalid.
    Codes must be at least 3 characters long.
    """
    if not input_str:
        return None
    
    # Remove any whitespace and normalize to lowercase
    input_str = input_str.strip().lower()
    
    # Only accept alphanumeric codes (no numeric-only IDs)
    return code_to_int(input_str)
def finalize_quest(user_id: int, guild_id: int, forced: bool = False):
    """
    Finalize a quest:
    - Rolls success
    - Grants rewards
    - Marks completed + collected
    """
    now = datetime.now(timezone.utc)
    
    # Get active quest that's not collected yet
    claims_cur.execute("""
        SELECT id, tier, success_rate, end_time, completed, collected
        FROM user_quests
        WHERE user_id = ? AND guild_id = ?
        AND collected = FALSE
    """, (user_id, guild_id))
    
    quest = claims_cur.fetchone()
    if not quest:
        return None
    
    quest_id, tier, success_rate, end_str, completed, collected = quest
    end_time = datetime.fromisoformat(end_str)
    
    # If quest is already completed but not collected, just collect it
    if completed:
        # Get quest config for rewards
        if tier not in QUEST_CONFIG:
            return None
        
        config = QUEST_CONFIG[tier]
        rewards = {
            "success": True,
            "gold": 0,
            "shards": {},
            "tokens": {}
        }
        
        # Get stored rewards if any, or calculate them
        # (You might want to store rewards when quest completes, but for now we'll calculate)
        
        # For now, just mark as collected
        claims_cur.execute("""
            UPDATE user_quests
            SET collected = TRUE
            WHERE id = ?
        """, (quest_id,))
        claims_conn.commit()
        
        return rewards
    
    # Not finished yet and not forced
    if not forced and now < end_time:
        return None
    
    # Roll success
    success = random.random() <= success_rate
    config = QUEST_CONFIG[tier]
    
    rewards = {
        "success": success,
        "gold": 0,
        "shards": {},
        "tokens": {}
    }
    
    if success:
        # GOLD
        if "gold_rewards" in config:
            rewards["gold"] = random.randint(*config["gold_rewards"])
            add_gold(user_id, rewards["gold"])
        
        # SHARDS
        if "shard_rewards" in config:
            for rarity, (lo, hi) in config["shard_rewards"].items():
                amt = random.randint(lo, hi)
                add_shards(user_id, rarity, amt)
                rewards["shards"][rarity] = amt
        
        # TOKENS
        if "token_rewards" in config:
            for token, (lo, hi) in config["token_rewards"].items():
                amt = random.randint(lo, hi)
                if amt > 0:
                    add_tokens(user_id, token, amt)
                    rewards["tokens"][token] = amt
    
    # Mark quest finished and collected
    claims_cur.execute("""
        UPDATE user_quests
        SET completed = TRUE,
            collected = TRUE
        WHERE id = ?
    """, (quest_id,))
    claims_conn.commit()
    
    return rewards

def add_legendary_tokens(user_id: int, amount: int):
    claims_cur.execute("""
        INSERT INTO user_tokens (user_id, token_type, amount)
        VALUES (?, 'Legendary', ?)
        ON CONFLICT(user_id, token_type)
        DO UPDATE SET amount = amount + excluded.amount
    """, (user_id, amount))
    claims_conn.commit()
    
def add_mythical_tokens(user_id: int, amount: int):
    claims_cur.execute("""
        INSERT INTO user_tokens (user_id, token_type, amount)
        VALUES (?, 'Mythical', ?)
        ON CONFLICT(user_id, token_type)
        DO UPDATE SET amount = amount + excluded.amount
    """, (user_id, amount))
    claims_conn.commit()
    
def normalize_name(s: str) -> str:
    s = s.lower().strip()
    s = re.sub(r"[^\w\s]", "", s)   # remove punctuation
    s = re.sub(r"\s+", " ", s)      # collapse spaces
    return s

def get_or_create_team(user_id: int):
    """Get or create a team for a user (global, not per-server)"""
    claims_cur.execute("""
        INSERT OR IGNORE INTO teams (user_id)
        VALUES (?)
    """, (user_id,))
    claims_conn.commit()

    claims_cur.execute("""
        SELECT tank_card_id, dps1_card_id, dps2_card_id, support_card_id
        FROM teams
        WHERE user_id = ?
    """, (user_id,))
    return claims_cur.fetchone()

def remove_from_team(user_id, column):
    """Remove a card from a team slot (global, not per-server)"""
    claims_cur.execute(f"""
        UPDATE teams SET {column} = NULL
        WHERE user_id = ?
    """, (user_id,))
    claims_conn.commit()

def roll_from_odds(odds):
    """
    odds = [("Common", 75), ("Rare", 25)]
    """
    rarities = [r for r, _ in odds]
    weights = [w for _, w in odds]
    return random.choices(rarities, weights=weights, k=1)[0]

def find_character_by_name_or_id(input_str: str):
    """
    Find a character by name or ID.
    If input is numeric, searches by ID. Otherwise searches by name.
    Returns dict with 'id', 'name', 'anime' keys, or None if not found.
    """
    input_str = input_str.strip()
    
    # Check if input is a numeric ID
    if input_str.isdigit():
        try:
            char_id = int(input_str)
            roster_cur.execute("SELECT Id, Name, Source FROM Characters WHERE Id = ?", (char_id,))
            row = roster_cur.fetchone()
            if row:
                cid, cname, source = row
                return {
                    "id": cid,
                    "name": cname,
                    "anime": source
                }
        except (ValueError, TypeError):
            pass
    
    # Search by name (original logic)
    target = normalize_name_words(input_str)
    roster_cur.execute("SELECT Id, Name, Source FROM Characters")
    for cid, cname, source in roster_cur.fetchall():
        if normalize_name_words(cname) == target:
            return {
                "id": cid,
                "name": cname,
                "anime": source
            }
    return None

def get_wishlist_info(character_id: int, user_id: int):
    claims_cur.execute("""
        SELECT COUNT(*) FROM wishlists WHERE roster_character_id = ?
    """, (character_id,))
    total = claims_cur.fetchone()[0]

    claims_cur.execute("""
        SELECT 1 FROM wishlists
        WHERE roster_character_id = ? AND user_id = ?
    """, (character_id, user_id))
    user_wants = claims_cur.fetchone() is not None

    return total, user_wants

def get_image_for_character_by_rarity(character_id: int, rarity: str):
    """
    Image rules:
    - SS rarity -> GIF ONLY
    - All other rarities -> NON-GIF ONLY
    """
    rarity = (rarity or "").strip().upper()

    if rarity == "SS":
        roster_cur.execute("""
            SELECT image_url
            FROM character_images
            WHERE character_id = ?
              AND LOWER(image_url) LIKE '%.gif'
            ORDER BY RANDOM()
            LIMIT 1
        """, (character_id,))
    else:
        roster_cur.execute("""
            SELECT image_url
            FROM character_images
            WHERE character_id = ?
              AND LOWER(image_url) NOT LIKE '%.gif'
            ORDER BY RANDOM()
            LIMIT 1
        """, (character_id,))

    row = roster_cur.fetchone()
    return row[0] if row else None

def get_random_image_for_character_by_rarity(character_id: int, rarity: str):
    roster_cur.execute("""
        SELECT image_url
        FROM character_images
        WHERE character_id = ?
    """, (character_id,))

    rows = roster_cur.fetchall()
    if not rows:
        return None

    # Split images
    gifs = [url for (url,) in rows if is_gif(url)]
    statics = [url for (url,) in rows if not is_gif(url)]

    if rarity == HIGHEST_RARITY:
        # Mythical → GIF ONLY
        return random.choice(gifs) if gifs else None
    else:
        # Non-mythical → STATIC ONLY
        return random.choice(statics) if statics else None

def get_random_image_for_character(character_id: int) -> str | None:
    roster_cur.execute("""
        SELECT image_url
        FROM character_images
        WHERE character_id = ?
        ORDER BY RANDOM()
        LIMIT 1
    """, (character_id,))
    row = roster_cur.fetchone()
    return row[0] if row else None

def get_character_display_row(character_id: int) -> tuple[str, str, str] | None:
    """
    Returns (name, anime/source, image_url) for a character_id,
    where image_url is randomly chosen from character_images.
    """
    roster_cur.execute("""
        SELECT Name, Source
        FROM Characters
        WHERE Id = ?
        LIMIT 1
    """, (character_id,))
    row = roster_cur.fetchone()
    if not row:
        return None

    name, source = row
    image_url = get_random_image_for_character(character_id)
    return (name, source, image_url)

def add_tokens(user_id: int, token_type: str, amount: int):
    claims_cur.execute("""
        INSERT INTO user_tokens (user_id, token_type, amount)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id, token_type)
        DO UPDATE SET amount = amount + excluded.amount
    """, (user_id, token_type, amount))
    claims_conn.commit()

def get_tokens(user_id: int):
    claims_cur.execute("""
        SELECT token_type, amount
        FROM user_tokens
        WHERE user_id = ?
    """, (user_id,))
    
    return {token_type.capitalize(): amount for token_type, amount in claims_cur.fetchall()}

def set_favorite_card(user_id: int, card_id: int | None):
    claims_cur.execute("""
        INSERT INTO user_favorite (user_id, card_id)
        VALUES (?, ?)
        ON CONFLICT(user_id)
        DO UPDATE SET card_id = excluded.card_id
    """, (user_id, card_id))
    claims_conn.commit()

def get_favorite_card_image(user_id: int):
    """
    Returns (image_url, rarity) or None
    Only returns if the favorite card is still active.
    """
    claims_cur.execute("""
        SELECT c.image_url, c.rarity
        FROM user_favorite f
        JOIN claimed_cards c ON c.id = f.card_id
        WHERE f.user_id = ? AND c.active = 1
    """, (user_id,))
    return claims_cur.fetchone()

def utc_now():
    return datetime.now(timezone.utc)

def get_keys(user_id: int) -> dict:
    claims_cur.execute("""
        SELECT rarity, amount
        FROM user_keys
        WHERE user_id = ?
    """, (user_id,))
    return {rarity: amount for rarity, amount in claims_cur.fetchall()}

def add_keys(user_id: int, rarity: str, amount: int):
    rarity = rarity.capitalize()
    claims_cur.execute("""
        INSERT INTO user_keys (user_id, rarity, amount)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id, rarity)
        DO UPDATE SET amount = amount + excluded.amount
    """, (user_id, rarity, amount))
    claims_conn.commit()
    
def migrate_user_keys():
    claims_cur.execute("""
        CREATE TABLE IF NOT EXISTS user_keys (
            user_id INTEGER NOT NULL,
            rarity TEXT NOT NULL,
            amount INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (user_id, rarity)
        )
    """)
    claims_conn.commit()
    
def migrate_user_daily():
    claims_cur.execute("""
        CREATE TABLE IF NOT EXISTS user_daily (
            user_id INTEGER PRIMARY KEY,
            streak INTEGER NOT NULL,
            last_claim TEXT NOT NULL
        )
    """)
    claims_conn.commit()

def migrate_user_favorite():
    claims_cur.execute("""
        CREATE TABLE IF NOT EXISTS user_favorite (
            user_id INTEGER PRIMARY KEY,
            card_id INTEGER
        )
    """)
    claims_conn.commit()
    
def migrate_trading():
    claims_cur.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
            initiator_id INTEGER NOT NULL,
            receiver_id INTEGER NOT NULL,
            initiator_cards TEXT NOT NULL DEFAULT '[]',  -- JSON array of card IDs
            receiver_cards TEXT NOT NULL DEFAULT '[]',  -- JSON array of card IDs
            initiator_gold INTEGER DEFAULT 0,
            receiver_gold INTEGER DEFAULT 0,
            initiator_shards TEXT DEFAULT '{}',  -- JSON dict {rarity: amount}
            receiver_shards TEXT DEFAULT '{}',  -- JSON dict {rarity: amount}
            initiator_confirmed BOOLEAN DEFAULT FALSE,
            receiver_confirmed BOOLEAN DEFAULT FALSE,
            status TEXT DEFAULT 'pending',  -- pending/accepted/declined/cancelled/completed
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            completed_at TEXT,
            channel_id INTEGER NOT NULL,
            message_id INTEGER
        )
    """)
    claims_conn.commit()
    
def roll_daily_gold(day: int) -> int:
    lo, hi = DAILY_GOLD_RANGE[day]
    return random.randint(lo, hi)


def roll_daily_shards(day: int) -> dict:
    rarities = DAILY_RARITY_TIERS[day]
    count = random.choice([1, 2])  # 1–2 shard types

    chosen = random.sample(rarities, min(count, len(rarities)))
    result = {}

    for rarity in chosen:
        if rarity == "Mythical":
            amt = random.randint(10, 40)
        elif rarity == "Legendary":
            amt = random.randint(10, 30)
        elif rarity == "Epic":
            amt = random.randint(15, 40)
        elif rarity == "Rare":
            amt = random.randint(20, 50)
        else:
            amt = random.randint(25, 70)

        result[rarity] = amt

    return result


def roll_daily_keys(day: int) -> dict:
    if day < 3:
        return {}  # no keys early

    rarities = [r for r in DAILY_RARITY_TIERS[day] if r != "Mythical"]
    if not rarities:
        return {}

    count = random.choice([0, 1, 2])
    chosen = random.sample(rarities, min(count, len(rarities)))

    result = {}
    for rarity in chosen:
        result[rarity] = random.randint(1, 2)

    return result

def chunk_list(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]
    
def reaction_check(message, user):
    def check(reaction, reactor):
        return (
            reactor == user
            and reaction.message.id == message.id
            and str(reaction.emoji) in ("✅", "❌")
        )
    return check

def upsert_guild_row(guild_id: int):
    claims_cur.execute("INSERT OR IGNORE INTO guild_config (guild_id) VALUES (?)", (guild_id,))
    claims_conn.commit()

def set_guild_spawn_channel(guild_id: int, channel_id: int):
    upsert_guild_row(guild_id)
    claims_cur.execute(
        "UPDATE guild_config SET spawn_channel_id=? WHERE guild_id=?",
        (channel_id, guild_id)
    )
    claims_conn.commit()

def set_guild_pity_counter(guild_id: int, spawns_since_mythical: int):
    upsert_guild_row(guild_id)
    claims_cur.execute(
        "UPDATE guild_config SET spawns_since_mythical=? WHERE guild_id=?",
        (spawns_since_mythical, guild_id)
    )
    claims_conn.commit()

def load_guild_configs_into_memory():
    server_state.clear()
    claims_cur.execute("""
        SELECT guild_id, spawn_channel_id, spawns_since_mythical
        FROM guild_config
    """)

    for guild_id, channel_id, pity in claims_cur.fetchall():
        server_state[guild_id] = {
            "channel_id": channel_id,
            "spawns_since_mythical": pity,

            # spawn state
            "active_spawn": None,
            "claimed": False,

            # NEW: activity-based spawning
            "last_spawn_time": 0,                 # or time.time() if you want a delay after restart
            "activity_since_last_spawn": False,
            }

def get_card_tag(card_id: int, user_id: int):
    claims_cur.execute("""
        SELECT tag
        FROM card_tags
        WHERE card_id = ? AND user_id = ?
        LIMIT 1
    """, (card_id, user_id))
    row = claims_cur.fetchone()
    return row[0] if row else None

ROLES = ["Tank", "DPS", "Support"]

HP_MIN, HP_MAX = 5000, 10000
ATK_MIN, ATK_MAX = 500, 2500
DEF_MIN, DEF_MAX = 500, 2500
LUCK_MIN, LUCK_MAX = 1.0, 10.0

def pct(value: float, vmin: float, vmax: float) -> float:
    # 0..100 percentage position within [vmin, vmax]
    return ((value - vmin) / (vmax - vmin)) * 100.0

def roll_stats(rarity: str | None = None):
    rarity = normalize_rarity(rarity) if rarity else None

    # Minimum power rules by rarity
    MIN_POWER_BY_RARITY = {
        "S": 62.50,
        "SS": 75.0,
    }

    min_power = MIN_POWER_BY_RARITY.get(rarity)

    for _ in range(50):  # safety cap
        role = random.choice(ROLES)

        # Roll raw stats
        hp = random.randint(HP_MIN, HP_MAX)
        atk = random.randint(ATK_MIN, ATK_MAX)
        deff = random.randint(DEF_MIN, DEF_MAX)
        luck = round(random.uniform(LUCK_MIN, LUCK_MAX), 2)

        # Convert to percentages
        hp_p = pct(hp, HP_MIN, HP_MAX)
        atk_p = pct(atk, ATK_MIN, ATK_MAX)
        def_p = pct(deff, DEF_MIN, DEF_MAX)
        luck_p = pct(luck, LUCK_MIN, LUCK_MAX)

        # Role-based weighting
        if role == "Tank":
            weights = {"hp": 0.40, "def": 0.35, "atk": 0.15, "luck": 0.10}
        elif role == "DPS":
            weights = {"atk": 0.40, "hp": 0.20, "def": 0.25, "luck": 0.15}
        else:  # Support
            weights = {"luck": 0.35, "hp": 0.25, "def": 0.20, "atk": 0.20}

        power = (
            hp_p * weights["hp"] +
            atk_p * weights["atk"] +
            def_p * weights["def"] +
            luck_p * weights["luck"]
        )

        power = round(min(100.0, max(1.0, power)), 2)

        # Enforce rarity minimums
        if min_power is None or power >= min_power:
            return {
                "role": role,
                "hp": hp,
                "atk": atk,
                "def": deff,
                "luck": luck,
                "power": power
            }

    # Fallback (extremely unlikely)
    power = max(power, min_power or power)
    return {
        "role": role,
        "hp": hp,
        "atk": atk,
        "def": deff,
        "luck": luck,
        "power": round(power, 2)
    }


def add_shards(user_id: int, rarity: str, amount: int):
    """
    Add shards to a user's inventory.
    Accepts both card rarities (F, C, B, A, S, SS) and quest/chest rarities (Common, Uncommon, Rare, Epic, Legendary, Mythical).
    """
    # Debug: print what we received
    print(f"DEBUG add_shards: user_id={user_id}, rarity={repr(rarity)} (type: {type(rarity)}), amount={amount}")
    
    if amount == 0:
        return
    
    # Validate rarity input
    if not isinstance(rarity, str):
        raise ValueError(f"Rarity must be a string. Received type: {type(rarity)}, value: {repr(rarity)}")
    
    if not rarity:
        raise ValueError(f"Rarity cannot be empty or None. Received: {repr(rarity)}")
    
    # Check if it's a quest/chest rarity first (case-insensitive)
    rarity_original = rarity.strip()
    if not rarity_original:
        raise ValueError(f"Rarity cannot be empty after stripping whitespace. Received: {repr(rarity)}")
    
    rarity_lower = rarity_original.lower()
    
    # Map to proper case for quest/chest rarities
    quest_chest_map = {
        "common": "Common",
        "uncommon": "Uncommon", 
        "rare": "Rare",
        "epic": "Epic",
        "legendary": "Legendary",
        "mythical": "Mythical"
    }
    
    if rarity_lower in quest_chest_map:
        # It's a quest/chest rarity - use as-is
        final_rarity = quest_chest_map[rarity_lower]
        # Double-check that QUEST_CHEST_RARITIES is accessible
        try:
            if final_rarity not in QUEST_CHEST_RARITIES:
                raise ValueError(f"Invalid quest/chest rarity: '{rarity}' (normalized to '{final_rarity}'). Valid quest/chest rarities: {', '.join(sorted(QUEST_CHEST_RARITIES))}")
        except NameError:
            # QUEST_CHEST_RARITIES not found - this shouldn't happen but handle it
            raise ValueError(f"QUEST_CHEST_RARITIES not found! Rarity received: '{rarity}' (normalized to '{final_rarity}')")
    else:
        # It's a card rarity - normalize it
        final_rarity = normalize_rarity(rarity)
        try:
            if final_rarity not in RARITIES:
                raise ValueError(f"Invalid rarity: '{rarity}' (normalized to '{final_rarity}'). Valid card rarities: {', '.join(sorted(RARITIES.keys()))}. Valid quest/chest rarities: {', '.join(sorted(QUEST_CHEST_RARITIES))}")
        except NameError as e:
            # One of the constants not found
            raise ValueError(f"Constants not accessible! Rarity received: '{rarity}' (normalized to '{final_rarity}'). Error: {e}")
    
    claims_cur.execute("""
        INSERT INTO user_shards (user_id, rarity, amount)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id, rarity) DO UPDATE SET amount = amount + excluded.amount
    """, (user_id, final_rarity, amount))
    claims_conn.commit()

def get_shards(user_id: int, rarity: str | None = None):
    """
    Get shards from a user's inventory.
    Accepts both card rarities (F, C, B, A, S, SS) and quest/chest rarities (Common, Uncommon, Rare, Epic, Legendary, Mythical).
    """
    if rarity is None:
        claims_cur.execute("""
            SELECT rarity, amount
            FROM user_shards
            WHERE user_id = ?
        """, (user_id,))
        return {r: a for r, a in claims_cur.fetchall()}

    # Check if it's a quest/chest rarity first (case-insensitive)
    rarity_original = rarity.strip()
    rarity_lower = rarity_original.lower()
    
    # Map to proper case for quest/chest rarities
    quest_chest_map = {
        "common": "Common",
        "uncommon": "Uncommon", 
        "rare": "Rare",
        "epic": "Epic",
        "legendary": "Legendary",
        "mythical": "Mythical"
    }
    
    if rarity_lower in quest_chest_map:
        # It's a quest/chest rarity - use as-is
        final_rarity = quest_chest_map[rarity_lower]
    else:
        # It's a card rarity - normalize it
        final_rarity = normalize_rarity(rarity)
    
    claims_cur.execute("""
        SELECT amount
        FROM user_shards
        WHERE user_id = ? AND rarity = ?
    """, (user_id, final_rarity))
    row = claims_cur.fetchone()
    return row[0] if row else 0

def get_legendary_tokens(user_id: int) -> int:
    claims_cur.execute("""
        SELECT amount
        FROM user_tokens
        WHERE user_id = ? AND token_type = 'Legendary'
    """, (user_id,))
    row = claims_cur.fetchone()
    return row[0] if row else 0

def add_legendary_tokens(user_id: int, amount: int):
    claims_cur.execute("""
        INSERT INTO user_tokens (user_id, token_type, amount)
        VALUES (?, 'Legendary', ?)
        ON CONFLICT(user_id, token_type)
        DO UPDATE SET amount = amount + ?
    """, (user_id, amount, amount))
    claims_conn.commit()
    
def get_mythical_tokens(user_id: int) -> int:
    claims_cur.execute("""
        SELECT amount
        FROM user_tokens
        WHERE user_id = ? AND token_type = 'Mythical'
    """, (user_id,))
    row = claims_cur.fetchone()
    return row[0] if row else 0  

def add_mythical_tokens(user_id: int, amount: int):
    claims_cur.execute("""
        INSERT INTO user_tokens (user_id, token_type, amount)
        VALUES (?, 'Mythical', ?)
        ON CONFLICT(user_id, token_type)
        DO UPDATE SET amount = amount + ?
    """, (user_id, amount, amount))
    claims_conn.commit()
    
def can_afford_pack(user_id: int, tier: str):
    cost = PACK_COSTS[tier]

    # Token-only packs
    if "legendary_token" in cost:
        tokens = get_tokens(user_id)
        return tokens.get("Legendary", 0) >= cost["legendary_token"]

    if "mythical_token" in cost:
        tokens = get_tokens(user_id)
        return tokens.get("Mythical", 0) >= cost["mythical_token"]

    # Gold + shards packs
    if get_gold(user_id) < cost.get("gold", 0):
        return False

    for rarity, amt in cost.get("shards", {}).items():
        if get_shards(user_id, rarity) < amt:
            return False

    return True

def deduct_pack_cost(user_id: int, tier: str):
    cost = PACK_COSTS[tier]

    if "legendary_token" in cost:
        add_tokens(user_id, "Legendary", -cost["legendary_token"])
        return

    if "mythical_token" in cost:
        add_tokens(user_id, "Mythical", -cost["mythical_token"])
        return

    add_gold(user_id, -cost["gold"])
    for rarity, amt in cost["shards"].items():
        add_shards(user_id, rarity, -amt)

def roll_rarity_random() -> str:
    rarities = list(RARITIES.keys())
    weights = [RARITIES[r]["weight"] for r in rarities]
    return random.choices(rarities, weights=weights, k=1)[0]

def add_gold(user_id: int, amount: int):
    claims_cur.execute("""
        INSERT INTO user_gold (user_id, gold)
        VALUES (?, ?)
        ON CONFLICT(user_id)
        DO UPDATE SET gold = gold + excluded.gold
    """, (user_id, amount))
    claims_conn.commit()
    
def get_gold(user_id: int) -> int:
    claims_cur.execute(
        "SELECT gold FROM user_gold WHERE user_id = ?",
        (user_id,)
    )
    row = claims_cur.fetchone()
    return row[0] if row else 0

class SellConfirmView(discord.ui.View):
    def __init__(self, ctx):
        super().__init__(timeout=30)
        self.ctx = ctx
        self.confirmed = False

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.ctx.author.id:
            await interaction.response.send_message(
                "❌ This confirmation isn’t for you.",
                ephemeral=True
            )
            return False
        return True

    @discord.ui.button(label="Confirm Sale", style=discord.ButtonStyle.green)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.confirmed = True

        # ✅ ACKNOWLEDGE + SEND NEW MESSAGE
        await interaction.response.send_message(
            "✅ Sale confirmed.",
            ephemeral=True
        )

        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.red)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            "❌ Sale cancelled.",
            ephemeral=True
        )

        self.stop()
        
def build_card_view_embed(row, owner_name, tag=None):
    (
        card_id, name, anime, rarity, image_url,
        role, hp, atk, defense, luck, power,
        owner_id
    ) = row
    
    # Convert numeric ID to alphanumeric code for display
    card_code = int_to_code(card_id)
    
    embed = discord.Embed(
        title=f"{RARITY_EMOJI.get(rarity,'✨')} | {name}",
        description=(
            f"🆔 **ID:** `{card_code}`\n"
            f"🎬 **Series:** {anime}\n\n"
            f"{ROLE_EMOJI[role]} **{role}**\n"
            f"💪 **Power:** {power:.2f}\n\n"
            f"⚔️ ATK: {atk}\n"
            f"💗 HP: {hp}\n"
            f"🛡️ DEF: {defense}\n"
            f"🍀 Luck: {luck}"
        ),
        color=RARITIES[rarity]["color"]
    )
    
    if tag:
        embed.add_field(
        name="🏷️ Tag",
        value=f"{tag}",
        inline=True
    )
        
    embed.set_image(url=image_url)
    embed.set_thumbnail(url=RARITY_IMAGES[rarity])

    if owner_name:
        embed.set_footer(text=f"Owned by {owner_name}")

    return embed

def create_trade(initiator_id, receiver_id, channel_id):
    created_at = datetime.now(timezone.utc).isoformat()
    expires_at = (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat()
    
    claims_cur.execute("""
        INSERT INTO trades (
            initiator_id, receiver_id, 
            initiator_cards, receiver_cards,
            initiator_shards, receiver_shards,
            status, created_at, expires_at, channel_id,
            initiator_confirmed, receiver_confirmed
        ) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?, FALSE, FALSE)
    """, (
        initiator_id, receiver_id,
        json.dumps([]), json.dumps([]),
        json.dumps({}), json.dumps({}),
        created_at, expires_at, channel_id
    ))
    claims_conn.commit()
    return claims_cur.lastrowid

def get_trade(trade_id):
    claims_cur.execute("SELECT * FROM trades WHERE trade_id = ?", (trade_id,))
    row = claims_cur.fetchone()
    if not row:
        return None
    
    # Convert columns to proper format
    columns = [desc[0] for desc in claims_cur.description]
    trade = dict(zip(columns, row))
    
    # Parse JSON fields
    for field in ['initiator_cards', 'receiver_cards', 'initiator_shards', 'receiver_shards']:
        trade[field] = json.loads(trade[field])
    
    return trade

def update_trade_message(trade_id, message_id):
    claims_cur.execute("""
        UPDATE trades 
        SET message_id = ?
        WHERE trade_id = ?
    """, (message_id, trade_id))
    claims_conn.commit()

def delete_expired_trades():
    """Clean up expired trades"""
    now = datetime.now(timezone.utc).isoformat()
    claims_cur.execute("""
        DELETE FROM trades 
        WHERE status = 'pending' AND expires_at < ?
    """, (now,))
    claims_conn.commit()
    return claims_cur.rowcount

def confirm_trade(trade_id, user_id):
    """Mark a user as having confirmed the trade"""
    trade = get_trade(trade_id)
    if not trade:
        return False
    
    if user_id == trade['initiator_id']:
        claims_cur.execute("""
            UPDATE trades 
            SET initiator_confirmed = TRUE
            WHERE trade_id = ?
        """, (trade_id,))
    elif user_id == trade['receiver_id']:
        claims_cur.execute("""
            UPDATE trades 
            SET receiver_confirmed = TRUE
            WHERE trade_id = ?
        """, (trade_id,))
    else:
        return False
    
    claims_conn.commit()
    return True

def unconfirm_trade(trade_id, user_id):
    """Unconfirm a trade (for editing)"""
    trade = get_trade(trade_id)
    if not trade:
        return False
    
    if user_id == trade['initiator_id']:
        claims_cur.execute("""
            UPDATE trades 
            SET initiator_confirmed = FALSE
            WHERE trade_id = ?
        """, (trade_id,))
    elif user_id == trade['receiver_id']:
        claims_cur.execute("""
            UPDATE trades 
            SET receiver_confirmed = FALSE
            WHERE trade_id = ?
        """, (trade_id,))
    else:
        return False
    
    claims_conn.commit()
    return True

# =========================
# ROSTER HELPERS (roster.db)
# =========================
def spawn_card_for_user(user_id: int, rarity: str):
    for _ in range(15):
        chosen = get_random_roster_entry()
        if not chosen:
            continue

        image_url = get_random_image_for_character_by_rarity(
            chosen["roster_character_id"],
            rarity
        )
        if not image_url:
            continue

        stats = roll_stats(rarity)
        claimed_at = datetime.now(timezone.utc).isoformat()

        claims_cur.execute("""
            INSERT INTO claimed_cards (
                guild_id, user_id, roster_character_id,
                name, anime, rarity, image_url, claimed_at,
                role, hp, atk, def, luck, power,
                active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
        """, (
            0,
            user_id,
            chosen["roster_character_id"],
            chosen["name"],
            chosen["anime"],
            rarity,
            image_url,
            claimed_at,
            stats["role"],
            stats["hp"],
            stats["atk"],
            stats["def"],
            stats["luck"],
            stats["power"],
        ))
        claims_conn.commit()

        card_id = claims_cur.lastrowid

        return {
            "id": card_id,
            "name": chosen["name"],
            "anime": chosen["anime"],
            "rarity": rarity,
            "image_url": image_url,
            "stats": stats
        }

    return None

def make_pack_card_embed(card, index, total):
    embed = discord.Embed(
        title=f"{RARITY_EMOJI.get(card['rarity'],'✨')} {card['name']}",
        description=(
            f"📺 **Anime:** {card['anime']}\n"
            f"⭐ **Rarity:** {card['rarity']}\n"
            f"🆔 **Card ID:** `{int_to_code(card['id'])}`\n"
            f"📦 **Pack Card:** {index + 1}/{total}"
        ),
        color=RARITIES[card["rarity"]]["color"]
    )

    stats = card["stats"]
    embed.add_field(
        name="Stats",
        value=(
            f"{ROLE_EMOJI.get(stats['role'],'🎭')} **Role:** {stats['role']}\n"
            f"{STAT_EMOJI['power']} **Power:** {stats['power']:.2f}\n"
            f"{STAT_EMOJI['hp']} **HP:** {stats['hp']}\n"
            f"{STAT_EMOJI['atk']} **ATK:** {stats['atk']}\n"
            f"{STAT_EMOJI['def']} **DEF:** {stats['def']}\n"
            f"{STAT_EMOJI['luck']} **Luck:** {stats['luck']:.2f}"
        ),
        inline=False
    )

    embed.set_image(url=card["image_url"])
    embed.set_thumbnail(url=RARITY_IMAGES[card["rarity"]])

    return embed

def is_gif(url: str) -> bool:
    return url.lower().endswith(".gif")

def get_random_roster_entry():
    roster_cur.execute("""
        SELECT Id, Name, Source
        FROM Characters
        ORDER BY RANDOM()
        LIMIT 1
    """)
    row = roster_cur.fetchone()
    if not row:
        return None

    char_id, name, source = row
    image_url = get_random_image_for_character(char_id)

    # If a character has no images, you can either return None (skip them)
    # or allow spawning with a placeholder.
    if not image_url:
        return None

    return {
        "roster_character_id": char_id,
        "name": name,
        "anime": source,        # keep key name 'anime' so rest of bot doesn't change
        "image_url": image_url
    }

def get_initials(name: str) -> str:
    return " ".join(word[0].upper() for word in name.split() if word)

def normalize_name_words(name: str) -> set[str]:
    """
    Lowercase, remove punctuation, split into words,
    and return a set so order does not matter.
    """
    cleaned = re.sub(r"[^a-zA-Z0-9\s]", "", name.lower())
    return set(cleaned.split())

def is_gif(url: str) -> bool:
    return url.lower().endswith(".gif")

def get_random_image_for_character_by_rarity(character_id: int, rarity: str):
    rarity = normalize_rarity(rarity)

    roster_cur.execute("""
        SELECT image_url
        FROM character_images
        WHERE character_id = ?
    """, (character_id,))
    rows = [url for (url,) in roster_cur.fetchall()]
    if not rows:
        return None

    gifs = [u for u in rows if is_gif(u)]
    statics = [u for u in rows if not is_gif(u)]

    # SS must be GIF, everything else must be static
    if rarity == HIGHEST_RARITY:
        return random.choice(gifs) if gifs else None
    else:
        return random.choice(statics) if statics else None

# =========================
# GAME LOGIC
# =========================
def roll_rarity(spawns_since_mythical: int) -> str:
    # pity for highest rarity only
    if spawns_since_mythical >= PITY_LIMIT:
        return HIGHEST_RARITY

    rarities = list(RARITIES.keys())
    weights = [RARITIES[r]["weight"] for r in rarities]
    return random.choices(rarities, weights=weights, k=1)[0]


async def spawn_in_guild(guild_id: int):
    data = server_state.get(guild_id)
    if not data or not data.get("channel_id"):
        return

    channel = bot.get_channel(int(data["channel_id"]))
    if not channel:
        return

    chosen = None
    image_url = None

    # --------------------------------
    # TRY UP TO 10 TIMES TO FIND
    # A CHARACTER THAT MATCHES
    # RARITY → IMAGE RULES
    # --------------------------------
    for _ in range(10):
        # 1) Pick random character
        chosen = get_random_roster_entry()
        if not chosen:
            continue

        # 2) Roll rarity
        rarity = roll_rarity(int(data.get("spawns_since_mythical", 0)))

        # 3) Pick image BASED ON RARITY
        image_url = get_random_image_for_character_by_rarity(
            chosen["roster_character_id"],
            rarity
        )

        if image_url:
            break  # ✅ valid spawn found

    # ❌ Failed after retries
    if not chosen or not image_url:
        return

    # --------------------------------
    # UPDATE PITY COUNTER
    # --------------------------------
    if rarity == HIGHEST_RARITY:
        data["spawns_since_mythical"] = 0
    else:
        data["spawns_since_mythical"] = int(data.get("spawns_since_mythical", 0)) + 1

    set_guild_pity_counter(guild_id, int(data["spawns_since_mythical"]))

    # --------------------------------
    # BUILD SPAWN DATA
    # --------------------------------
    spawn_data = {
        "name": chosen["name"],
        "anime": chosen["anime"],
        "rarity": rarity,
        "image_url": image_url,
        "roster_character_id": chosen["roster_character_id"],
    }

    # Store active spawn
    data["active_spawn"] = spawn_data
    data["claimed"] = False

    # --------------------------------
    # BUILD EMBED
    # --------------------------------
    initials = get_initials(spawn_data["name"])
    embed = discord.Embed(
        title=f"**{initials}**",
        description=(
            f"A new character has spawned!\n"
            f"📺 **Anime:** {spawn_data['anime']}\n"
            f"✏️ **Claim this character by typing:**\n"
            f"`!claim <character name>`"
        ),
        color=RARITIES[rarity]["color"]
    )
    embed.set_thumbnail(url=RARITY_IMAGES[rarity])
    embed.set_image(url=image_url)

    await channel.send(embed=embed)

    # ---------- CHEST SPAWN ----------
    # Allow new chests to spawn even if there's an existing one (replace old chest)
    # Clear any existing chest first
    if "active_chest" in data:
        old_chest = data.get("active_chest")
        # Try to edit the old chest message to indicate it was replaced (optional)
        if old_chest and old_chest.get("message_id"):
            try:
                old_msg = await channel.fetch_message(old_chest["message_id"])
                if old_msg and not old_chest.get("opened", False):
                    # Optionally edit the old message to show it expired
                    expired_embed = discord.Embed(
                        title=f"🧰 {old_chest.get('rarity', 'Chest')} Chest Expired",
                        description="This chest has been replaced by a new spawn.",
                        color=discord.Color.dark_grey()
                    )
                    await old_msg.edit(embed=expired_embed)
            except (discord.NotFound, discord.HTTPException):
                pass  # Old message doesn't exist or can't be edited, that's fine
        
        # Clear the old chest
        del data["active_chest"]

    available = []
    weights = []

    for chest_rarity, chest in CHESTS.items():
        available.append(chest_rarity)
        weights.append(chest["spawn_chance"])

    # Calculate total weight for probability check
    total_weight = sum(weights)
    
    # Roll to see if a chest should spawn (using total weight as probability)
    # This allows for chest spawn chance to be configurable
    if total_weight > 0 and random.random() <= min(total_weight, 1.0):
        picked = random.choices(available, weights=weights, k=1)[0]
        chest = CHESTS[picked]

        chest_embed = discord.Embed(
            title=f"🧰 {picked} Chest Appeared!",
            description=(
                f"🔑 Requires **{chest['key']} Key** to open\n"
                f"Use `!open {picked.lower()}`"
            ),
            color=RARITIES[picked]["color"]
        )
        chest_embed.set_image(url=CHEST_IMAGES[picked])

        try:
            msg = await channel.send(embed=chest_embed)
            
            data["active_chest"] = {
                "rarity": picked,
                "message_id": msg.id,
                "opened": False
            }
        except Exception as e:
            # If message send fails, don't set active_chest so it can retry next time
            print(f"Error sending chest embed: {e}")

# =========================
# EVENTS
# =========================    
@bot.event
async def on_message(message):
    if message.author.bot:
        return

    if message.guild:
        guild_id = message.guild.id
        data = server_state.get(guild_id)

        # Only activity/spawn logic depends on server_state
        if data and not message.content.startswith("!"):
            data["activity_since_last_spawn"] = True
            await try_spawn_from_activity(guild_id)

    # 🚨 ALWAYS process commands
    await bot.process_commands(message)

@bot.event
async def on_raw_reaction_add(payload):
    # Ignore bot's own reactions
    if payload.user_id == bot.user.id:
        return
    
    # Check if this is a trade message
    trade_id = None
    for tid, data in active_trades.items():
        if data.get("message_id") == payload.message_id:
            trade_id = tid
            break
    
    if not trade_id:
        return
    
    trade = get_trade(trade_id)
    if not trade or trade['status'] != 'pending':
        return
    
    channel = bot.get_channel(payload.channel_id)
    if not channel:
        return
    
    message = await channel.fetch_message(payload.message_id)
    user = await bot.fetch_user(payload.user_id)
    
    # Check if user is part of the trade
    if user.id not in [trade['initiator_id'], trade['receiver_id']]:
        try:
            await message.remove_reaction(payload.emoji, user)
        except:
            pass
        return
    
    # Handle different reactions
    emoji = str(payload.emoji)
    
    if emoji == "✅":
        await handle_trade_confirm(trade_id, user, message)
    elif emoji == "❌":
        await handle_trade_cancel(trade_id, user, message)
    elif emoji == "➕":
        await handle_trade_add_prompt(trade_id, user, channel)
    elif emoji == "➖":
        await handle_trade_remove_prompt(trade_id, user, channel)
    elif emoji == "💰":
        await handle_trade_gold_prompt(trade_id, user, channel)
    elif emoji == "🧩":
        await handle_trade_shards_prompt(trade_id, user, channel)
    elif emoji == "📋":
        await handle_trade_details(trade_id, user, channel)
    
    # Remove the user's reaction after handling
    try:
        await message.remove_reaction(payload.emoji, user)
    except:
        pass

async def handle_trade_confirm(trade_id, user, message):
    """Handle confirm reaction"""
    trade = get_trade(trade_id)
    
    # Check if both parties have already confirmed once
    if trade['initiator_confirmed'] and trade['receiver_confirmed']:
        # This is the final confirmation - execute trade
        await execute_trade(trade_id, message.channel)
    else:
        # First confirmation - mark user as confirmed
        confirm_trade(trade_id, user.id)
        
        # Update the trade message
        embed = await create_trade_embed(trade_id, message)
        await message.edit(embed=embed)
        
        # Send confirmation message
        await message.channel.send(
            f"{user.mention} has confirmed the trade #{trade_id}. "
            f"Waiting for the other party...",
            delete_after=10
        )

async def execute_trade(trade_id, channel):
    """Execute a confirmed trade"""
    trade = get_trade(trade_id)
    
    if not trade or trade['status'] != 'pending':
        return
    
    # Validate resources still exist
    errors = []
    
    # Check initiator resources
    initiator_gold = get_gold(trade['initiator_id'])
    if initiator_gold < trade['initiator_gold']:
        errors.append(f"Initiator doesn't have enough gold ({initiator_gold}/{trade['initiator_gold']})")
    
    for rarity, amount in trade['initiator_shards'].items():
        shards = get_shards(trade['initiator_id'], rarity)
        if shards < amount:
            errors.append(f"Initiator doesn't have enough {rarity} shards ({shards}/{amount})")
    
    # Check receiver resources
    receiver_gold = get_gold(trade['receiver_id'])
    if receiver_gold < trade['receiver_gold']:
        errors.append(f"Receiver doesn't have enough gold ({receiver_gold}/{trade['receiver_gold']})")
    
    for rarity, amount in trade['receiver_shards'].items():
        shards = get_shards(trade['receiver_id'], rarity)
        if shards < amount:
            errors.append(f"Receiver doesn't have enough {rarity} shards ({shards}/{amount})")
    
    # Check card ownership
    all_cards = trade['initiator_cards'] + trade['receiver_cards']
    card_errors = []
    for card_id in all_cards:
        claims_cur.execute("""
            SELECT user_id, active FROM claimed_cards 
            WHERE id = ?
        """, (card_id,))
        row = claims_cur.fetchone()
        if not row or not row[1]:
            card_errors.append(f"Card {card_id} no longer exists!")
    
    if errors or card_errors:
        # Trade failed - reset confirmations
        unconfirm_trade(trade_id, trade['initiator_id'])
        unconfirm_trade(trade_id, trade['receiver_id'])
        
        error_msg = "\n".join(errors + card_errors[:3])
        if len(errors + card_errors) > 3:
            error_msg += f"\n...and {len(errors + card_errors) - 3} more issues"
        
        await channel.send(
            f"❌ Trade #{trade_id} failed:\n{error_msg}\n"
            f"Please adjust your trade and confirm again.",
            delete_after=15
        )
        
        # Update trade message
        try:
            message = await channel.fetch_message(trade['message_id'])
            embed = await create_trade_embed(trade_id, message)
            await message.edit(embed=embed)
        except:
            pass
        
        return
    
    # Execute the trade
    try:
        # Transfer gold
        add_gold(trade['initiator_id'], -trade['initiator_gold'])
        add_gold(trade['receiver_id'], trade['initiator_gold'])
        
        add_gold(trade['receiver_id'], -trade['receiver_gold'])
        add_gold(trade['initiator_id'], trade['receiver_gold'])
        
        # Transfer shards
        for rarity, amount in trade['initiator_shards'].items():
            add_shards(trade['initiator_id'], rarity, -amount)
            add_shards(trade['receiver_id'], rarity, amount)
        
        for rarity, amount in trade['receiver_shards'].items():
            add_shards(trade['receiver_id'], rarity, -amount)
            add_shards(trade['initiator_id'], rarity, amount)
        
        # Transfer cards
        for card_id in trade['initiator_cards']:
            claims_cur.execute("""
                UPDATE claimed_cards 
                SET user_id = ?
                WHERE id = ?
            """, (trade['receiver_id'], card_id))
        
        for card_id in trade['receiver_cards']:
            claims_cur.execute("""
                UPDATE claimed_cards 
                SET user_id = ?
                WHERE id = ?
            """, (trade['initiator_id'], card_id))
        
        # Update trade status
        claims_cur.execute("""
            UPDATE trades 
            SET status = 'completed', completed_at = ?
            WHERE trade_id = ?
        """, (datetime.now(timezone.utc).isoformat(), trade_id))
        
        claims_conn.commit()
        
        # Remove from active trades
        if trade_id in active_trades:
            del active_trades[trade_id]
        
        # Update trade message
        try:
            message = await channel.fetch_message(trade['message_id'])
            
            # Get user mentions
            initiator = channel.guild.get_member(trade['initiator_id'])
            receiver = channel.guild.get_member(trade['receiver_id'])
            
            embed = discord.Embed(
                title=f"✅ Trade #{trade_id} Completed!",
                description=(
                    f"**Trade successfully executed!**\n\n"
                    f"**Transferred from {initiator.display_name if initiator else 'Initiator'}:**\n"
                    f"• {len(trade['initiator_cards'])} cards\n"
                    f"• {trade['initiator_gold']:,} gold\n"
                    f"• {sum(trade['initiator_shards'].values())} shards\n\n"
                    f"**Transferred from {receiver.display_name if receiver else 'Receiver'}:**\n"
                    f"• {len(trade['receiver_cards'])} cards\n"
                    f"• {trade['receiver_gold']:,} gold\n"
                    f"• {sum(trade['receiver_shards'].values())} shards"
                ),
                color=discord.Color.green()
            )
            
            await message.edit(embed=embed)
            await message.clear_reactions()
            
            # Send success notification
            await channel.send(
                f"{initiator.mention if initiator else ''} "
                f"{receiver.mention if receiver else ''} "
                f"✅ Trade #{trade_id} has been successfully completed!",
                delete_after=15
            )
            
        except:
            pass
        
    except Exception as e:
        # Rollback on error
        claims_conn.rollback()
        
        # Reset confirmations
        unconfirm_trade(trade_id, trade['initiator_id'])
        unconfirm_trade(trade_id, trade['receiver_id'])
        
        await channel.send(
            f"❌ An error occurred during trade #{trade_id}: {str(e)}\n"
            f"The trade has been reset. Please try again.",
            delete_after=15
        )
        
        # Update trade message
        try:
            message = await channel.fetch_message(trade['message_id'])
            embed = await create_trade_embed(trade_id, message)
            await message.edit(embed=embed)
        except:
            pass

async def handle_trade_cancel(trade_id, user, message):
    """Handle cancel reaction"""
    trade = get_trade(trade_id)
    
    # Update trade status
    claims_cur.execute("""
        UPDATE trades 
        SET status = 'cancelled', completed_at = ?
        WHERE trade_id = ?
    """, (datetime.now(timezone.utc).isoformat(), trade_id))
    claims_conn.commit()
    
    # Remove from active trades
    if trade_id in active_trades:
        del active_trades[trade_id]
    
    # Update message
    embed = discord.Embed(
        title=f"❌ Trade #{trade_id} Cancelled",
        description=f"{user.mention} cancelled the trade.",
        color=discord.Color.red()
    )
    await message.edit(embed=embed)
    await message.clear_reactions()
    
    await message.channel.send(f"Trade #{trade_id} has been cancelled.", delete_after=10)

async def handle_trade_add_prompt(trade_id, user, channel):
    """Prompt user to add a card"""
    await channel.send(
        f"{user.mention} To add a card, use: `!tradeadd {trade_id} <card_id>`\n"
        f"Example: `!tradeadd {trade_id} 123`",
        delete_after=15
    )

async def handle_trade_remove_prompt(trade_id, user, channel):
    """Prompt user to remove a card"""
    await channel.send(
        f"{user.mention} To remove a card, use: `!traderemove {trade_id} <card_id>`\n"
        f"Example: `!traderemove {trade_id} 123`",
        delete_after=15
    )

async def handle_trade_gold_prompt(trade_id, user, channel):
    """Prompt user to add gold"""
    await channel.send(
        f"{user.mention} To add gold, use: `!tradegold {trade_id} <amount>`\n"
        f"Example: `!tradegold {trade_id} 1000`",
        delete_after=15
    )

async def handle_trade_shards_prompt(trade_id, user, channel):
    """Prompt user to add shards"""
    await channel.send(
        f"{user.mention} To add shards, use: `!tradeshards {trade_id} <rarity> <amount>`\n"
        f"Example: `!tradeshards {trade_id} Epic 50`",
        delete_after=15
    )

async def handle_trade_details(trade_id, user, channel):
    """Show detailed view of cards in trade"""
    trade = get_trade(trade_id)
    
    # Get detailed card info
    def get_detailed_cards(card_ids, user_name):
        if not card_ids:
            return f"**{user_name}:** No cards"
        
        details = []
        for cid in card_ids:
            claims_cur.execute("""
                SELECT name, anime, rarity, power, role 
                FROM claimed_cards 
                WHERE id = ?
            """, (cid,))
            row = claims_cur.fetchone()
            if row:
                name, anime, rarity, power, role = row
                details.append(
                    f"`{cid}` - {RARITY_EMOJI.get(rarity,'✨')} **{name}** "
                    f"({anime})\n   ⚔️ Power: {power:.1f} | 🎭 {role}"
                )
        return f"**{user_name} ({len(card_ids)} cards):**\n" + "\n".join(details)
    
    initiator_name = f"User {trade['initiator_id']}"
    receiver_name = f"User {trade['receiver_id']}"
    
    embed = discord.Embed(
        title=f"📋 Trade #{trade_id} - Detailed View",
        description=(
            f"{get_detailed_cards(trade['initiator_cards'], 'Initiator')}\n\n"
            f"{get_detailed_cards(trade['receiver_cards'], 'Receiver')}\n\n"
            f"**Gold:** {trade['initiator_gold']:,} ↔️ {trade['receiver_gold']:,}\n"
            f"**Shards:** {', '.join([f'{k}: {v}' for k, v in trade['initiator_shards'].items()]) or 'None'} "
            f"↔️ {', '.join([f'{k}: {v}' for k, v in trade['receiver_shards'].items()]) or 'None'}"
        ),
        color=discord.Color.blue()
    )
    
    await channel.send(embed=embed, delete_after=30)


@bot.event
async def on_guild_available(guild):
    # Only set default if not already in memory (load_guild_configs_into_memory handles loading)
    if guild.id not in server_state:
        upsert_guild_row(guild.id)
        claims_cur.execute("""
            SELECT spawn_channel_id, spawns_since_mythical
            FROM guild_config
            WHERE guild_id = ?
        """, (guild.id,))
        row = claims_cur.fetchone()
        channel_id = row[0] if row and row[0] else None
        pity = row[1] if row and row[1] else 0
        
        server_state[guild.id] = {
            "channel_id": channel_id,
            "spawns_since_mythical": pity,
            "active_spawn": None,
            "claimed": False,
            "last_spawn_time": 0,
            "activity_since_last_spawn": False,
        }

@bot.event
async def on_guild_join(guild: discord.Guild):
    # Ensure config row exists
    upsert_guild_row(guild.id)
    # Load config from database
    claims_cur.execute("""
        SELECT spawn_channel_id, spawns_since_mythical
        FROM guild_config
        WHERE guild_id = ?
    """, (guild.id,))
    row = claims_cur.fetchone()
    channel_id = row[0] if row and row[0] else None
    pity = row[1] if row and row[1] else 0
    
    # Add to memory with loaded values
    server_state[guild.id] = {
        "channel_id": channel_id,
        "spawns_since_mythical": pity,
        "active_spawn": None,
        "claimed": False,
        "last_spawn_time": 0,
        "activity_since_last_spawn": False,
    }

@bot.event
async def on_guild_remove(guild: discord.Guild):
    # Keep DB record (optional). Remove memory entry:
    server_state.pop(guild.id, None)


# =========================
# COMMANDS
# =========================
@bot.command()
async def help(ctx):
    """Get a list of all available commands via DM"""
    try:
        embed = discord.Embed(
            title="📚 Command Help",
            description="Here are all available commands organized by category:",
            color=discord.Color.blue()
        )
        
        # Cards Category
        embed.add_field(
            name="🎴 **CARDS**",
            value=(
                "`!claim <name>` - Claim a spawned card by guessing its name\n"
                "`!view <card_id> [card_id...]` - View card details (aliases: `!v`)\n"
                "`!latest` - View your most recently claimed card\n"
                "`!collection [user] [filters]` - View your card collection (aliases: `!c`, `!coll`)\n"
                "  Filters: `r:rarity`, `t:tag`, `n:name`, `s:series`, `role:tank/dps/support`, `o:time/rarity/power/wishlist`\n"
                "`!destroy <card_id> [card_id...]` - Destroy cards for gold and shards (aliases: `!d`)\n"
                "`!destroylatest` - Destroy your most recent card (aliases: `!dl`)\n"
                "`!tag <card_id> <emoji>` - Tag a card with an emoji\n"
                "`!taglatest <emoji>` - Tag your latest card (aliases: `!tl`)\n"
                "`!tags [user]` - View all your tags and how many cards use each\n"
                "`!favorite <card_id>` - Set a card as your favorite (aliases: `!fav`)\n"
                "`!favorite clear` - Clear your favorite card"
            ),
            inline=False
        )
        
        # Trading Category
        embed.add_field(
            name="🔄 **TRADING**",
            value=(
                "`!trade <@user>` - Start a trade with another user (aliases: `!traderequest`, `!t`)\n"
                "`!tradeadd <card_id>` - Add a card to your trade (aliases: `!tadd`)\n"
                "`!traderemove <card_id>` - Remove a card from your trade (aliases: `!tremove`)\n"
                "`!tradegold <amount>` - Add gold to your trade (aliases: `!tgold`)\n"
                "`!tradeshards <rarity> <amount>` - Add shards to your trade (aliases: `!tshards`)\n"
                "`!tradeclear` - Clear your side of the trade (aliases: `!tclear`)\n"
                "`!tradeconfirm` - Confirm your side of the trade\n"
                "React with ✅ on the trade message to confirm and complete the trade"
            ),
            inline=False
        )
        
        # Teams Category
        embed.add_field(
            name="⚔️ **TEAMS**",
            value=(
                "`!tankadd <card_id>` - Add a tank card to your team (aliases: `!tank`)\n"
                "`!dps1add <card_id>` - Add a DPS card to slot 1 (aliases: `!dps1`)\n"
                "`!dps2add <card_id>` - Add a DPS card to slot 2 (aliases: `!dps2`)\n"
                "`!supportadd <card_id>` - Add a support card to your team (aliases: `!support`)\n"
                "`!tankremove` - Remove tank from your team\n"
                "`!dps1remove` - Remove DPS1 from your team\n"
                "`!dps2remove` - Remove DPS2 from your team\n"
                "`!supportremove` - Remove support from your team\n"
                "`!teamview [user]` - View your team (aliases: `!team`)\n"
                "`!teamclear` - Clear your entire team"
            ),
            inline=False
        )
        
        # Quests Category
        embed.add_field(
            name="📜 **QUESTS**",
            value=(
                "`!questboard` - View available quests and requirements (aliases: `!quests`)\n"
                "`!quest <tier>` - Start a quest (tiers: Common, Uncommon, Rare, Epic, Legendary, Mythical)\n"
                "`!queststatus [user]` - View your active quest status\n"
                "`!collectquest` - Collect rewards from a completed quest\n"
                "`!cancelquest` - Cancel your current active quest"
            ),
            inline=False
        )
        
        # Shop & Packs Category
        embed.add_field(
            name="🛒 **SHOP & PACKS**",
            value=(
                "`!shop` - View available packs and their prices\n"
                "`!buy <tier>` - Buy a pack (tiers: C, B, A, S, S+)\n"
                "React with ✅ on the pack message to open it"
            ),
            inline=False
        )
        
        # Crafting Category
        embed.add_field(
            name="🔨 **CRAFTING**",
            value=(
                "`!craft <rarity>` - Craft a card of the specified rarity\n"
                "`!recipes` - View crafting recipes and requirements"
            ),
            inline=False
        )
        
        # Chests Category
        embed.add_field(
            name="📦 **CHESTS**",
            value=(
                "`!open <chest_name>` - Open a chest you've obtained\n"
                "Chests spawn randomly in the server and can be opened with keys"
            ),
            inline=False
        )
        
        # Inventory Category
        embed.add_field(
            name="💰 **INVENTORY**",
            value=(
                "`!inventory [user]` - View your inventory (gold, shards, keys, tokens) (aliases: `!inv`)\n"
                "`!daily` - Claim your daily reward"
            ),
            inline=False
        )
        
        # Wishlist Category
        embed.add_field(
            name="⭐ **WISHLIST**",
            value=(
                "`!wishadd <name_or_id>` - Add a character to your wishlist (aliases: `!wa`)\n"
                "`!wishremove <name_or_id>` - Remove a character from your wishlist (aliases: `!wr`)\n"
                "`!wishlist [user]` - View your wishlist (aliases: `!wl`)\n"
                "*You can use character name or ID (e.g., `!wa 123` or `!wa Character Name`)*"
            ),
            inline=False
        )
        
        # Lookup/Search Category
        embed.add_field(
            name="🔍 **LOOKUP & SEARCH**",
            value=(
                "`!lookup <query>` - Search for characters by name or ID (aliases: `!lu`)\n"
                "`!series <query>` - Search for anime/series (aliases: `!sc`, `!game`, `!source`)\n"
                "`!info <name_or_id>` - Get information about a character (aliases: `!i`)\n"
                "*Lookup and info commands accept character names or IDs*"
            ),
            inline=False
        )
        
        # Utility Category
        embed.add_field(
            name="🛠️ **UTILITY**",
            value=(
                "`!ping` - Check if the bot is online\n"
                "`!help` - Show this help message"
            ),
            inline=False
        )
        
        embed.set_footer(text="Note: Card IDs are alphanumeric codes (e.g., 001, 00a, 100). Minimum 3 characters.")
        
        # Send DM (no message in channel)
        try:
            await ctx.author.send(embed=embed)
        except discord.Forbidden:
            # If DMs are disabled, silently fail (user requested no channel message)
            pass
    
    except Exception as e:
        await ctx.send(f"❌ An error occurred: {str(e)}")

@bot.command()
async def ping(ctx):
    await ctx.send("pong")


@bot.command()
@commands.has_permissions(administrator=True)
async def setspawnchannel(ctx, channel: discord.TextChannel):
    guild_id = ctx.guild.id

    # Save to DB permanently
    set_guild_spawn_channel(guild_id, channel.id)

    # Update memory
    server_state.setdefault(guild_id, {
        "channel_id": None,
        "spawns_since_mythical": 0,
        "active_spawn": None,
        "claimed": False,
    })
    server_state[guild_id]["channel_id"] = channel.id

    await ctx.send(f"✅ Spawn channel set to {channel.mention} (saved)")


@setspawnchannel.error
async def setspawnchannel_error(ctx, error):
    if isinstance(error, commands.MissingPermissions):
        await ctx.send("❌ You must be an administrator to use this command.")
    elif isinstance(error, commands.BadArgument):
        await ctx.send("❌ Please mention a text channel like `#waifus`.")
    else:
        await ctx.send("❌ An error occurred.")
        raise error


@bot.command()
async def claim(ctx, *, guessed_name: str):
    guild_id = ctx.guild.id
    data = server_state.get(guild_id)

    if not data or not data.get("active_spawn"):
        await ctx.send("❌ There is no active character to claim.")
        return

    if data.get("claimed"):
        await ctx.send("❌ This character has already been claimed.")
        return

    spawn = data["active_spawn"]

    guess_words = normalize_name_words(guessed_name)
    actual_words = normalize_name_words(spawn["name"])

    if not guess_words.issuperset(actual_words):
        await ctx.send("❌ Incorrect name. Try again!")
        return

    # Correct guess → claim
    claimed_at = datetime.now(timezone.utc).isoformat()
    stats = roll_stats(spawn["rarity"])

    claims_cur.execute("""
    INSERT INTO claimed_cards (
        guild_id, user_id, roster_character_id,
        name, anime, rarity, image_url, claimed_at,
        role, hp, atk, def, luck, power,
        active
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
    """, (
        guild_id,
        ctx.author.id,
        spawn.get("roster_character_id"),
        spawn["name"],
        spawn["anime"],
        spawn["rarity"],
        spawn["image_url"],
        claimed_at,
        stats["role"],
        stats["hp"],
        stats["atk"],
        stats["def"],
        stats["luck"],
        stats["power"],
        ))
    claims_conn.commit()

    card_id = claims_cur.lastrowid

    data["claimed"] = True
    data["active_spawn"] = None

    await ctx.send(
        f"🎉 Correct! {ctx.author.mention} claimed **{spawn['name']}** "
        f"({spawn['rarity']})"
    )

def get_random_character_by_name(name: str):
    roster_cur.execute("""
        SELECT Id, Name, Source
        FROM Characters
        WHERE LOWER(Name) = LOWER(?)
        ORDER BY RANDOM()
        LIMIT 1
    """, (name,))
    row = roster_cur.fetchone()
    if not row:
        return None

    char_id, real_name, source = row
    image_url = get_random_image_for_character(char_id)
    if not image_url:
        return None

    return {
        "roster_character_id": char_id,
        "name": real_name,
        "anime": source,
        "image_url": image_url
    }

# =========================
# FORCE SPAWN (NO DB CHECKS)
# =========================
# Format uses | separators so multi-word names are easy:
# !forcespawn Legendary | Zero Two | Darling in the FranXX | https://i.imgur.com/xxx.png
@bot.command(aliases=["fs"])
@is_bot_owner()
async def forcespawn(ctx, *, arg: str = None):
    guild_id = ctx.guild.id
    data = server_state.get(guild_id)

    if not data or not data.get("channel_id"):
        await ctx.send("❌ Spawn channel not set.")
        return

    channel = bot.get_channel(int(data["channel_id"]))
    if not channel:
        await ctx.send("❌ Invalid spawn channel.")
        return

    forced_rarity = None
    forced_name = None

    # --------------------------------
    # PARSE ARGUMENTS (if any)
    # --------------------------------
    if arg:
        parts = arg.split()
        maybe_rarity = normalize_rarity(parts[0])

        if maybe_rarity in RARITIES:
            forced_rarity = maybe_rarity
            forced_name = " ".join(parts[1:]).strip() if len(parts) > 1 else None
        else:
            forced_name = arg.strip()

    # --------------------------------
    # PICK CHARACTER!
    # --------------------------------
    if forced_name:
        forced_words = normalize_name_words(forced_name)

        roster_cur.execute("""
            SELECT Id, Name
            FROM Characters
        """)
        rows = roster_cur.fetchall()

        matched = []
        for cid, name in rows:
            if forced_words == normalize_name_words(name):
                matched.append((cid, name))

        if not matched:
            await ctx.send("❌ Character not found in roster.")
            return

        cid, name = random.choice(matched)

    else:
        # 🔥 NEW FEATURE: !fs with NO args → fully random character
        roster_cur.execute("""
            SELECT Id, Name
            FROM Characters
            ORDER BY RANDOM()
            LIMIT 1
        """)
        row = roster_cur.fetchone()

        if not row:
            await ctx.send("❌ Failed to pick a random character.")
            return

        cid, name = row
        
    # --------------------------------
    # PICK RARITY
    # --------------------------------
    if forced_rarity:
        rarity = forced_rarity
    else:
        # 🔥 Random rarity if not forced
        rarity = roll_rarity(int(data.get("spawns_since_mythical", 0)))
    
    # --------------------------------
    # PICK IMAGE
    # --------------------------------
    image_url = get_random_image_for_character_by_rarity(cid, rarity)

    if not image_url:
        if rarity == HIGHEST_RARITY:
            await ctx.send("❌ This character has no GIF image for a mythical spawn.")
        else:
            await ctx.send("❌ This character has no static image for a non-mythical spawn.")
        return
    
    # --------------------------------
    # GET ANIME / SOURCE
    # --------------------------------
    roster_cur.execute(
        "SELECT Source FROM Characters WHERE Id = ? LIMIT 1",
        (cid,)
    )
    r = roster_cur.fetchone()
    anime = r[0] if r else "Unknown"

    chosen = {
        "roster_character_id": cid,
        "name": name,
        "anime": anime,
        "image_url": image_url
    }
    
    # --------------------------------
    # UPDATE PITY (same as natural spawn)
    # --------------------------------
    if rarity == HIGHEST_RARITY:
        data["spawns_since_mythical"] = 0
    else:
        data["spawns_since_mythical"] = int(data.get("spawns_since_mythical", 0)) + 1

    set_guild_pity_counter(guild_id, int(data["spawns_since_mythical"]))

    # --------------------------------
    # BUILD EMBED
    # --------------------------------
    initials = get_initials(chosen["name"])

    embed = discord.Embed(
        title=f"**{initials}**",
        description=(
            f"A character has spawned!\n"
            f"📺 **Anime:** {chosen['anime']}\n"
            f"✏️ Claim with:\n"
            f"`!claim <character name>`"
        ),
        color=RARITIES[rarity]["color"]
    )

    embed.set_image(url=chosen["image_url"])
    embed.set_thumbnail(url=RARITY_IMAGES[rarity])

    await channel.send(embed=embed)

    # --------------------------------
    # STORE ACTIVE SPAWN
    # --------------------------------
    data["active_spawn"] = {
        "name": chosen["name"],
        "anime": chosen["anime"],
        "rarity": rarity,
        "image_url": chosen["image_url"],
        "roster_character_id": chosen["roster_character_id"],
    }
    data["claimed"] = False



# =========================
# OPTIONAL: ADD CHARACTER TO ROSTER DB
# =========================
@bot.command()
@is_bot_owner()
async def addchar(ctx, *, payload: str):
    # !addchar Name | Source | image_url
    parts = [p.strip() for p in payload.split("|")]
    if len(parts) != 3:
        await ctx.send("❌ Use: `!addchar Name | Source | image_url`")
        return

    name, source, image_url = parts

    # 1) Insert character
    roster_cur.execute(
        "INSERT INTO Characters (Name, Source) VALUES (?, ?)",
        (name, source)
    )
    roster_character_id = roster_cur.lastrowid

    # 2) Insert image
    roster_cur.execute(
        "INSERT INTO character_images (character_id, image_url) VALUES (?, ?)",
        (roster_character_id, image_url)
    )

    roster_conn.commit()
    await ctx.send(f"✅ Added **{name}** ({source})")

# =========================
# OPTIONAL: VIEW PITY (admin debug)
# =========================
@bot.command()
@is_bot_owner()
async def pity(ctx):
    data = server_state.get(ctx.guild.id)
    if not data:
        await ctx.send("No server data.")
        return
    await ctx.send(f"🔮 Spawns since Mythical: **{data['spawns_since_mythical']} / {PITY_LIMIT}**")


# =========================
# RUN (works in Spyder/Jupyter AND normal python)
# =========================
async def _runner():
    async with bot:
        await bot.start(TOKEN)

def run_bot():
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # Spyder/Jupyter: schedule it
            asyncio.create_task(_runner())
        else:
            # Normal python
            loop.run_until_complete(_runner())
    except RuntimeError:
        # In some environments get_event_loop behaves oddly
        asyncio.run(_runner())


@bot.command(aliases=["c", "coll"])
async def collection(ctx, *args):
    import re
    import asyncio

    # ---------- helpers ----------
    def parse_mention(token: str):
        m = re.fullmatch(r"<@!?(\d+)>", token)
        return int(m.group(1)) if m else None

    # ---------- defaults ----------
    target_user_id = ctx.author.id
    target_display = ctx.author.display_name
    
    name_filter = None
    series_filter = None
    rarity_filter = None
    tag_filter = None
    role_filter = None
    show_role = False
    order_by = "time"  # time | rarity | power | wishlist

    # ---------- parse args ----------
    tokens = list(args)
    remaining = []

    for t in tokens:
        uid = parse_mention(t)
        if uid and target_user_id == ctx.author.id:
            member = ctx.guild.get_member(uid) if ctx.guild else None
            if member:
                target_user_id = member.id
                target_display = member.display_name
            else:
                try:
                    user = await bot.fetch_user(uid)
                    target_user_id = user.id
                    target_display = user.name
                except discord.NotFound:
                    await ctx.send("❌ I couldn’t find that user.")
                    return
        else:
            remaining.append(t)

    i = 0
    while i < len(remaining):
        arg = remaining[i]
        arg_low = arg.lower()

        if arg_low.startswith("r:"):
            rarity_filter = arg_low[2:].capitalize()
            if rarity_filter not in RARITIES:
                await ctx.send(f"❌ Invalid rarity `{rarity_filter}`.")
                return
            i += 1

        elif arg_low.startswith("t:"):
            tag_filter = arg[2:].strip()
            i += 1

        elif arg_low.startswith("o:"):
            order_by = arg_low[2:]
            i += 1

        elif arg_low.startswith("n:"):
            # consume rest of args as name
            name_filter = " ".join([arg[2:]] + remaining[i+1:])
            break

        elif arg_low.startswith("s:"):
            # consume rest of args as series
            series_filter = " ".join([arg[2:]] + remaining[i+1:])
            break
        
        elif arg_low.startswith("role:"):
            role_value = arg_low[5:].strip()
            role_map = {
                "tank": "Tank",
                "dps": "DPS",
                "support": "Support"
                }

            if role_value not in role_map:
                await ctx.send("❌ Role must be `tank`, `dps`, or `support`.")
                return

            role_filter = role_map[role_value]  # ✅ exact DB value
            show_role = True
            i += 1
            
        else:
            await ctx.send(f"❌ Unknown filter `{arg}`.")
            return

    # ---------- SQL ----------
    query = """
        SELECT
            c.id,
            c.name,
            c.anime,
            c.rarity,
            c.power,
            c.role,
            IFNULL(wc.wish_count, 0) AS wish_count,
            t.tag
        FROM claimed_cards c
        LEFT JOIN (
            SELECT roster_character_id, COUNT(*) AS wish_count
            FROM wishlists
            GROUP BY roster_character_id
        ) wc ON wc.roster_character_id = c.roster_character_id
        LEFT JOIN card_tags t
            ON t.card_id = c.id
           AND t.user_id = c.user_id
        WHERE c.user_id = ?
          AND c.active = 1
    """
    params = [target_user_id]

    if rarity_filter:
        query += " AND c.rarity = ?"
        params.append(rarity_filter)

    if tag_filter:
        query += " AND t.tag = ?"
        params.append(tag_filter)
        
    if name_filter:
        query += " AND LOWER(c.name) LIKE ?"
        params.append(f"%{name_filter.lower()}%")

    if series_filter:
        query += " AND LOWER(c.anime) = ?"
        params.append(series_filter.lower())
        
    if role_filter:
        query += " AND c.role = ?"
        params.append(role_filter)
        
    # ---------- ordering ----------
    if order_by == "wishlist":
        query += " ORDER BY wish_count DESC, c.claimed_at ASC"

    elif order_by == "rarity":
        query += """
            ORDER BY
                CASE c.rarity
                    WHEN 'SS' THEN 6
                    WHEN 'S' THEN 5
                    WHEN 'A' THEN 4
                    WHEN 'B' THEN 3
                    WHEN 'C' THEN 2
                    WHEN 'F' THEN 1
                END DESC,
                c.claimed_at ASC
        """

    elif order_by == "power":
        query += " ORDER BY c.power DESC, c.claimed_at ASC"

    else:
        query += " ORDER BY c.claimed_at ASC"

    # ---------- execute ----------
    claims_cur.execute(query, params)
    rows = claims_cur.fetchall()

    if not rows:
        await ctx.send("📭 No cards found with those filters.")
        return

    # ---------- pagination ----------
    PAGE_SIZE = 20
    page = 0
    total_pages = (len(rows) - 1) // PAGE_SIZE + 1

    def build_page_embed(page: int):
        start = page * PAGE_SIZE
        end = start + PAGE_SIZE
        page_rows = rows[start:end]

        embed = discord.Embed(
            title=f"📚 {target_display}'s Collection",
            color=discord.Color.blurple()
        )
        embed.set_footer(text=f"Page {page + 1}/{total_pages} • {len(rows)} cards")

        lines = []
        for (gid, name, anime, rarity, power, role, wish_count, tag) in page_rows:
            emoji = RARITY_EMOJI.get(rarity, "❔")
            tag_part = f"{tag} " if tag else ""

            power_part = (
                f" • {STAT_EMOJI['power']} {float(power):.1f}"
                if order_by == "power"
                else ""
                )
            wish_part = (
                f" • ♡ {wish_count}"
                if order_by == "wishlist" and wish_count > 0
                else ""
                )
            role_line = (
                f"\n{ROLE_EMOJI.get(role,'🎭')}"
                if show_role
                else ""
                )
            
            card_code = int_to_code(gid)
            lines.append(
                f"{tag_part}`{card_code}` • {emoji} • **{name}** — {anime}{power_part}{wish_part}{role_line}"
            )

        embed.description = "\n".join(lines)
        return embed
    # ---------- send ----------
    msg = await ctx.send(embed=build_page_embed(page))

    if total_pages == 1:
        return

    await msg.add_reaction("⬅️")
    await msg.add_reaction("➡️")
    await msg.add_reaction("⏹️")

    def check(reaction, user):
        return (
            user == ctx.author
            and reaction.message.id == msg.id
            and str(reaction.emoji) in ("⬅️", "➡️", "⏹️")
        )

    while True:
        try:
            reaction, user = await bot.wait_for("reaction_add", timeout=60, check=check)
        except asyncio.TimeoutError:
            break

        if reaction.emoji == "⏹️":
            break
        elif reaction.emoji == "➡️":
            page = (page + 1) % total_pages
        elif reaction.emoji == "⬅️":
            page = (page - 1) % total_pages

        await msg.edit(embed=build_page_embed(page))

        try:
            await msg.remove_reaction(reaction.emoji, user)
        except discord.Forbidden:
            pass

    try:
        await msg.clear_reactions()
    except discord.Forbidden:
        pass

STAT_EMOJI = {
    "power": "💪🏻",
    "hp": "💗",
    "atk": "⚔️",
    "def": "🛡️",
    "luck": "🍀"
}

ROLE_EMOJI = {
    "Tank": "🧱",
    "DPS": "🪄",
    "Support": "🩹"
}

CUSTOM_EMOJI_RE = re.compile(r"^<a?:\w{2,}:\d{5,}>$")

def is_valid_tag_emoji(s: str) -> bool:
    s = s.strip()
    # Custom emoji like <:name:id> or <a:name:id>
    if CUSTOM_EMOJI_RE.match(s):
        return True
    # Basic unicode emoji (simple check: non-ascii)
    # This is permissive (good for emojis & symbols).
    return any(ord(ch) > 127 for ch in s)

@bot.command(aliases=["v"])
async def view(ctx, *ids):
    if not ids:
        await ctx.send("❌ Usage: `!view <card_id> [card_id ...]`")
        return

    if len(ids) > 10:
        await ctx.send("❌ You can view a maximum of 10 cards at once.")
        return

    # Parse IDs (supports both numeric and alphanumeric codes)
    parsed_ids = []
    invalid_inputs = []
    
    for input_id in ids:
        parsed = parse_card_id(input_id)
        if parsed is None:
            invalid_inputs.append(input_id)
        else:
            parsed_ids.append(parsed)
    
    if invalid_inputs:
        await ctx.send(f"❌ Invalid ID format: {', '.join(invalid_inputs)}. Use alphanumeric codes (minimum 3 characters).")
        return
    
    if not parsed_ids:
        await ctx.send("❌ No valid IDs provided.")
        return

    placeholders = ",".join("?" for _ in parsed_ids)

    claims_cur.execute(f"""
        SELECT
            id, name, anime, rarity, image_url,
            role, hp, atk, def, luck, power,
            user_id
        FROM claimed_cards
        WHERE id IN ({placeholders})
    """, parsed_ids)

    rows = claims_cur.fetchall()

    if not rows:
        await ctx.send("❌ No cards found for those IDs.")
        return

    # keep order user typed
    row_map = {row[0]: row for row in rows}

    for card_id in parsed_ids:
        row = row_map.get(card_id)
        if not row:
            await ctx.send(f"⚠️ Card `{card_id}` not found.")
            continue

        owner_id = row[-1]

        # resolve owner name properly
        owner_name = None
        if ctx.guild:
            member = ctx.guild.get_member(owner_id)
            if member:
                owner_name = member.display_name

        if owner_name is None:
            try:
                user = await bot.fetch_user(owner_id)
                owner_name = user.name
            except discord.NotFound:
                owner_name = f"User ID {owner_id}"
                
        # ✅ FETCH TAG HERE
        tag = get_card_tag(card_id, owner_id)
    
        embed = build_card_view_embed(row, owner_name, tag)
        await ctx.send(embed=embed)

@bot.command()
async def latest(ctx):
    claims_cur.execute("""
        SELECT id, name, anime, rarity, image_url, role, hp, atk, def, luck, power
        FROM claimed_cards
        WHERE user_id = ? AND active = 1
        ORDER BY claimed_at DESC
        LIMIT 1
    """, (ctx.author.id,))

    row = claims_cur.fetchone()
    if not row:
        await ctx.send("📭 You haven’t claimed any cards yet.")
        return

    (global_id, name, anime, rarity, image_url,
     role, hp, atk, deff, luck, power) = row

    embed = discord.Embed(
        title=f"{RARITY_EMOJI.get(rarity,'✨')} | {name}",
        description=(
            f"🎬 **Anime:** {anime}\n"
            f"🆔 **ID:** `{int_to_code(global_id)}`"
        ),
        color=RARITIES[rarity]["color"]
    )

    tag = get_card_tag(global_id, ctx.author.id)

    stats_text = (
        f"{ROLE_EMOJI.get(role,'🎭')} **Role:** {role}\n"
        f"{STAT_EMOJI['power']} **Power:** {float(power):.2f}\n"
        f"{STAT_EMOJI['hp']} **HP:** {hp}\n"
        f"{STAT_EMOJI['atk']} **ATK:** {atk}\n"
        f"{STAT_EMOJI['def']} **DEF:** {deff}\n"
        f"{STAT_EMOJI['luck']} **Luck:** {float(luck):.2f}"
        f""
        )

    if tag:
        stats_text += f"\n🏷️ **Tag:** {tag}"

    embed.add_field(
        name="Stats",
        value=stats_text,
        inline=False
        )
    
    embed.set_thumbnail(url=RARITY_IMAGES[rarity])
    embed.set_image(url=image_url)
    await ctx.send(embed=embed)

@bot.command()
@is_bot_owner()
async def wipe(ctx, member: discord.Member, *, reason: str = "No reason provided"):
    user_id = member.id
    wiped_at = datetime.now(timezone.utc).isoformat()

    # Archive all currently active cards for that user
    claims_cur.execute("""
        UPDATE claimed_cards
        SET active = 0,
            wiped_at = ?,
            wiped_by = ?,
            wipe_reason = ?
        WHERE user_id = ? AND active = 1
    """, (wiped_at, ctx.author.id, reason, user_id))
    claims_conn.commit()

    count = claims_cur.rowcount
    await ctx.send(f"🧹 Archived **{count}** cards for {member.mention}.")

@bot.command()
@is_bot_owner()
async def restore(ctx, member: discord.Member):
    user_id = member.id

    claims_cur.execute("""
        UPDATE claimed_cards
        SET active = 1,
            wiped_at = NULL,
            wiped_by = NULL,
            wipe_reason = NULL
        WHERE user_id = ? AND active = 0
    """, (user_id,))
    claims_conn.commit()

    count = claims_cur.rowcount
    await ctx.send(f"♻️ Restored **{count}** cards for {member.mention}.")

@bot.command(name="resetdata")
@is_bot_owner()
async def resetdata(ctx, confirm: str = None):
    if confirm != "CONFIRM":
        await ctx.send(
            "⚠️ **FULL DATA RESET** ⚠️\n\n"
            "This will DELETE:\n"
            "• All claimed cards\n"
            "• All card tags\n"
            "• All inventories (gold, shards, keys, tokens)\n"
            "• All server configuration\n\n"
            "✅ Roster & images are NOT deleted\n"
            "✅ Global IDs will restart at **0**\n\n"
            "Run: `!resetdata CONFIRM`"
        )
        return

    # ---------- WIPE CLAIMED CARDS ----------
    claims_cur.execute("DELETE FROM claimed_cards")
    
    # ---------- WIPE CARD TAGS ----------
    claims_cur.execute("DELETE FROM card_tags")
    
    # Also clean up any orphaned tags (defensive - in case tags weren't properly cleaned)
    claims_cur.execute("""
        DELETE FROM card_tags
        WHERE card_id NOT IN (SELECT id FROM claimed_cards)
    """)

    # ---------- WIPE SERVER CONFIG ----------
    claims_cur.execute("DELETE FROM guild_config")

    # ---------- WIPE PLAYER ECONOMY ----------
    # ⚠️ CHANGE THESE TABLE NAMES IF YOURS DIFFER
    claims_cur.execute("DELETE FROM user_gold")
    claims_cur.execute("DELETE FROM user_shards")
    claims_cur.execute("DELETE FROM user_keys")
    claims_cur.execute("DELETE FROM user_tokens")

    # If you have a unified inventory table, include it too:
    # claims_cur.execute("DELETE FROM inventories")

    # ---------- RESET AUTOINCREMENT ----------
    tables_to_reset = [
        "claimed_cards",
        "user_gold",
        "user_shards",
        "user_keys",
        "user_tokens",
    ]

    for table in tables_to_reset:
        claims_cur.execute(
            "DELETE FROM sqlite_sequence WHERE name = ?",
            (table,)
        )
        claims_cur.execute(
            "INSERT INTO sqlite_sequence (name, seq) VALUES (?, -1)",
            (table,)
        )

    claims_conn.commit()

    # ---------- CLEAR RUNTIME STATE ----------
    server_state.clear()

    await ctx.send(
        "✅ **Full reset complete.**\n"
        "• All player inventories wiped\n"
        "• All claimed cards deleted\n"
        "• Server config cleared\n"
        "• Roster & images preserved"
    )
    
@bot.command(aliases=["inv"])
async def inventory(ctx, *args):
    import re
    import asyncio

    def parse_mention(token: str):
        m = re.fullmatch(r"<@!?(\d+)>", token)
        return int(m.group(1)) if m else None

    # ---------- resolve target user ----------
    target_user_id = ctx.author.id
    target_display = ctx.author.display_name

    for t in args:
        uid = parse_mention(t)
        if uid:
            member = ctx.guild.get_member(uid) if ctx.guild else None
            if member:
                target_user_id = member.id
                target_display = member.display_name
            else:
                try:
                    user = await bot.fetch_user(uid)
                    target_user_id = user.id
                    target_display = user.name
                except discord.NotFound:
                    await ctx.send("❌ I couldn't find that user.")
                    return
            break

    # ---------- compute stats ----------
    claims_cur.execute("""
        SELECT COUNT(*)
        FROM claimed_cards
        WHERE user_id = ? AND active = 1
    """, (target_user_id,))
    total_cards = claims_cur.fetchone()[0]

    claims_cur.execute("""
        SELECT COUNT(DISTINCT LOWER(name))
        FROM claimed_cards
        WHERE user_id = ? AND active = 1
    """, (target_user_id,))
    unique_chars = claims_cur.fetchone()[0]

    gold = get_gold(target_user_id)
    shards = get_shards(target_user_id)          # dict
    keys = get_keys(target_user_id)              # dict
    tokens = get_tokens(target_user_id) or {}    # dict (safe even if None)

    # Card rarities for shards (F, C, B, A, S, SS)
    card_rarity_order = ["F", "C", "B", "A", "S", "SS"]
    
    # Chest rarities for shards (Common, Uncommon, Rare, Epic ONLY - no Legendary/Mythical)
    chest_rarity_order = ["Common", "Uncommon", "Rare", "Epic"]
    
    # Chest rarities for keys (all including Legendary)
    key_rarity_order = ["Common", "Uncommon", "Rare", "Epic", "Legendary"]

    # ---------- PAGE 1 (Overview) ----------
    page1 = discord.Embed(
        title=f"🎒 {target_display}'s Inventory",
        description=(
            f"📦 **Total Cards Owned**: **{total_cards}**\n"
            f"🎭 **Unique Characters**: **{unique_chars}**"
        ),
        color=discord.Color.dark_grey()
    )
    page1.set_footer(text="Page 1/4 • Overview")

    fav = get_favorite_card_image(target_user_id)
    if fav:
        fav_image_url, fav_rarity = fav
        page1.set_image(url=fav_image_url)

    # ---------- PAGE 2 (Card Rarity Shards - F, C, B, A, S, SS) ----------
    shard_lines_card = []
    for r in card_rarity_order:
        symbol = "⚒️"
        # Get shards for this card rarity
        shard_count = get_shards(target_user_id, r)
        shard_lines_card.append(f"{symbol} **{r} Shards**: {shard_count}")

    desc_card = "**Card Shards**\n" + "\n".join(shard_lines_card)

    page2 = discord.Embed(
        title="💰 Currency - Card Shards",
        description=desc_card,
        color=discord.Color.gold()
    )
    if fav:
        fav_image_url, fav_rarity = fav
        page2.set_thumbnail(url=fav_image_url)
    page2.set_footer(text="Page 2/4 • Card Shards ")

    # ---------- PAGE 3 (Chest Rarity Shards + Gold + Tokens) ----------
    shard_lines_chest = []
    for r in chest_rarity_order:
        symbol = "🧩"  # Generic shard emoji for chest rarities
        # Get shards for this chest rarity
        shard_count = get_shards(target_user_id, r)
        shard_lines_chest.append(f"{symbol} **{r} Shards**: {shard_count}")

    desc_chest = f"💰 **Gold**: **{gold:,}**\n\n**Pack Shards**\n" + "\n".join(shard_lines_chest)

     # ALWAYS show Legendary and Mythical tokens, even if 0
    token_types = ["Legendary", "Mythical"]
    token_lines = []
    for token_type in token_types:
        token_amount = tokens.get(token_type, 0)  # Get amount or default to 0
        token_lines.append(f"🔮 **{token_type} Tokens**: {token_amount}")
    
    desc_chest += "\n\n**Tokens**\n" + "\n".join(token_lines)

    page3 = discord.Embed(
        title="💰 Currency - Chest Shards & Tokens",
        description=desc_chest,
        color=discord.Color.green()
    )
    if fav:
        fav_image_url, fav_rarity = fav
        page3.set_thumbnail(url=fav_image_url)
    page3.set_footer(text="Page 3/4 • Pack Shards & Tokens")

    # ---------- PAGE 4 (Keys) ----------
    key_lines = []
    for r in key_rarity_order:
        key_lines.append(f"🗝️ **{r} Keys**: {keys.get(r, 0)}")

    page4 = discord.Embed(
        title="🗝️ Keys",
        description="\n".join(key_lines),
        color=discord.Color.dark_gold()
    )
    if fav:
        fav_image_url, fav_rarity = fav
        page4.set_thumbnail(url=fav_image_url)
    page4.set_footer(text="Page 4/4 • Keys")

    pages = [page1, page2, page3, page4]  # Changed from 3 to 4 pages
    page_idx = 0

    msg = await ctx.send(embed=pages[page_idx])
    await msg.add_reaction("⬅️")
    await msg.add_reaction("➡️")
    await msg.add_reaction("⏹️")

    def check(reaction, user):
        return (
            user == ctx.author
            and reaction.message.id == msg.id
            and str(reaction.emoji) in ("⬅️", "➡️", "⏹️")
        )

    while True:
        try:
            reaction, user = await bot.wait_for("reaction_add", timeout=60, check=check)
        except asyncio.TimeoutError:
            break

        emoji = str(reaction.emoji)

        if emoji == "⏹️":
            break
        elif emoji == "➡️":
            page_idx = (page_idx + 1) % len(pages)
            await msg.edit(embed=pages[page_idx])
        elif emoji == "⬅️":
            page_idx = (page_idx - 1) % len(pages)
            await msg.edit(embed=pages[page_idx])

        try:
            await msg.remove_reaction(reaction.emoji, user)
        except discord.Forbidden:
            pass

    try:
        await msg.clear_reactions()
    except discord.Forbidden:
        pass

@bot.command(name="addshards", aliases=["shards"])
@is_bot_owner()
async def addshards_cmd(ctx, member: discord.Member, rarity: str, amount: int):
    rarity = rarity.capitalize()

    if rarity not in RARITIES:
        await ctx.send(f"❌ Invalid rarity. Valid: {', '.join(RARITIES.keys())}")
        return

    if amount == 0:
        await ctx.send("❌ Amount must not be 0.")
        return

    # Fetch current amount
    claims_cur.execute("""
        SELECT amount
        FROM user_shards
        WHERE user_id = ? AND rarity = ?
    """, (member.id, rarity))
    row = claims_cur.fetchone()
    current = row[0] if row else 0

    new_amount = current + amount
    if new_amount < 0:
        new_amount = 0

    # Upsert with final clamped amount
    claims_cur.execute("""
        INSERT INTO user_shards (user_id, rarity, amount)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id, rarity)
        DO UPDATE SET amount = excluded.amount
    """, (member.id, rarity, new_amount))
    claims_conn.commit()

    change = new_amount - current
    sign = "+" if change > 0 else ""

    await ctx.send(
        f"✅ {member.mention} now has **{new_amount}** "
        f"{RARITY_EMOJI.get(rarity,'')} **{rarity}** shards "
        f"({sign}{change})."
    )

@bot.command()
async def tag(ctx, card_input: str, emoji: str):
    # Validate emoji
    if not is_valid_tag_emoji(emoji):
        await ctx.send("❌ That doesn't look like a valid emoji.")
        return

    # Parse card ID (supports both numeric and alphanumeric codes)
    global_id = parse_card_id(card_input)
    if global_id is None:
        await ctx.send(f"❌ Invalid card ID format: `{card_input}`. Use alphanumeric codes (minimum 3 characters).")
        return

    # Ensure card exists and user owns it
    claims_cur.execute(
        "SELECT user_id FROM claimed_cards WHERE id = ? AND active = 1",
        (global_id,)
    )
    row = claims_cur.fetchone()
    if not row:
        card_code = int_to_code(global_id) if global_id else card_input
        await ctx.send(f"❌ No active card found with ID `{card_code}`.")
        return
    if row[0] != ctx.author.id:
        await ctx.send("❌ You can only tag cards you own.")
        return

    now = datetime.now(timezone.utc).isoformat()

    # Delete existing tag (if any)
    claims_cur.execute("""
        DELETE FROM card_tags
        WHERE card_id = ? AND user_id = ?
    """, (global_id, ctx.author.id))

    # Insert new tag
    claims_cur.execute("""
        INSERT INTO card_tags (card_id, user_id, tag, created_at)
        VALUES (?, ?, ?, ?)
    """, (global_id, ctx.author.id, emoji, now))

    claims_conn.commit()
    card_code = int_to_code(global_id)
    await ctx.send(f"🏷️ Successfully tagged card `{card_code}` with {emoji}")

@bot.command()
async def tags(ctx, *args):
    # --- helper to parse <@123> or <@!123> ---
    def parse_mention(token: str):
        m = re.fullmatch(r"<@!?(\d+)>", token)
        return int(m.group(1)) if m else None

    # Defaults: self
    target_user_id = ctx.author.id
    target_display = ctx.author.display_name
    
    # Check for mention
    for t in args:
        uid = parse_mention(t)
        if uid:
            member = ctx.guild.get_member(uid) if ctx.guild else None
            if member:
                target_user_id = member.id
                target_display = member.display_name
            else:
                try:
                    user = await bot.fetch_user(uid)
                    target_user_id = user.id
                    target_display = user.name
                except discord.NotFound:
                    await ctx.send("❌ I couldn’t find that user.")
                    return
            break

    # Fetch tag counts (only for cards that exist and are active)
    claims_cur.execute("""
        SELECT t.tag, COUNT(*) as count
        FROM card_tags t
        INNER JOIN claimed_cards c ON t.card_id = c.id
        WHERE t.user_id = ? AND c.active = 1
        GROUP BY t.tag
        ORDER BY count DESC
    """, (target_user_id,))
    rows = claims_cur.fetchall()

    if not rows:
        if target_user_id == ctx.author.id:
            await ctx.send("🏷️ You haven't tagged any cards yet.")
        else:
            await ctx.send(f"🏷️ **{target_display}** hasn't tagged any cards yet.")
        return

    embed = discord.Embed(
        title=f"🏷️ {target_display}'s Tags",
        description="\n".join(f"{tag} → **{count}** card(s)" for tag, count in rows),
        color=discord.Color.blurple()
    )

    await ctx.send(embed=embed)    
    
@bot.command(aliases=["d"])
async def destroy(ctx, *card_inputs: str):
    if not card_inputs:
        await ctx.send("❌ Usage: `!destroy <card_id> [card_id ...]`")
        return

    # Parse card IDs (supports both numeric and alphanumeric codes)
    card_ids = []
    invalid_inputs = []
    
    for card_input in card_inputs:
        parsed = parse_card_id(card_input)
        if parsed is None:
            invalid_inputs.append(card_input)
        else:
            card_ids.append(parsed)
    
    if invalid_inputs:
        await ctx.send(f"❌ Invalid ID format: {', '.join(invalid_inputs)}. Use alphanumeric codes (minimum 3 characters).")
        return
    
    if not card_ids:
        await ctx.send("❌ No valid card IDs provided.")
        return

    cards = []
    total_gold = 0
    shard_totals = {}

    # ---------- FETCH & VALIDATE ----------
    for cid in card_ids:
        claims_cur.execute("""
            SELECT id, user_id, rarity, name, power
            FROM claimed_cards
            WHERE id = ? AND active = 1
        """, (cid,))
        row = claims_cur.fetchone()

        if not row:
            card_code = int_to_code(cid)
            await ctx.send(f"❌ Card `{card_code}` not found or already sold.")
            return

        card_id, owner_id, rarity, name, power = row

        # Ensure consistent casing
        if isinstance(rarity, str):
            rarity = rarity.capitalize()

        if owner_id != ctx.author.id:
            await ctx.send("❌ You can only destroyl cards you own.")
            return

        if rarity not in SELL_BASE_REWARDS:
            card_code = int_to_code(cid)
            await ctx.send(f"❌ Card `{card_code}` has invalid rarity `{rarity}`.")
            return

        base = SELL_BASE_REWARDS[rarity]
        multiplier = max(0.1, float(power) / 100.0)

        gold = int(base["gold"] * multiplier)
        shards = max(1, round(base["shards"] * multiplier))

        total_gold += gold
        shard_totals[rarity] = shard_totals.get(rarity, 0) + shards
        cards.append((card_id, name, rarity))

    # ---------- HIGHEST RARITY FOR UI ----------
    highest_rarity = max(
        (r for _, _, r in cards),
        key=lambda r: RARITY_ORDER.get(r, 0)
    )

    # ---------- BUILD SHARD LINES ----------
    # Sort shard lines by rarity from low->high or high->low (choose)
    ordered = sorted(shard_totals.items(), key=lambda kv: RARITY_ORDER.get(kv[0], 0), reverse=True)

    shard_lines = [
        f"🧩 **{rarity} Shards**: +{amt}"
        for rarity, amt in ordered
    ]

    # ---------- CONFIRM EMBED ----------
    embed = discord.Embed(
        title="⚠️ Confirm Destruction",
        description=(
            f"Cards to destroy: **{len(cards)}**\n\n"
            f"💰 **Total Gold**: **+{total_gold:,}**\n"
            + "\n".join(shard_lines)
            + "\n\nReact with ✅ to confirm or ❌ to cancel."
        ),
        color=RARITIES[highest_rarity]["color"]
    )

    # Thumbnail = highest rarity badge
    if highest_rarity in RARITY_IMAGES:
        embed.set_thumbnail(url=RARITY_IMAGES[highest_rarity])

    msg = await ctx.send(embed=embed)
    await msg.add_reaction("✅")
    await msg.add_reaction("❌")

    def check(reaction, user):
        return (
            user == ctx.author
            and reaction.message.id == msg.id
            and str(reaction.emoji) in ("✅", "❌")
        )

    # ---------- WAIT FOR REACTION ----------
    try:
        reaction, _ = await bot.wait_for("reaction_add", timeout=30, check=check)
    except asyncio.TimeoutError:
        await ctx.send("⏱️ Destruction timed out.")
        return

    if str(reaction.emoji) == "❌":
        await ctx.send("❌ Destruction cancelled.")
        return

    # ---------- EXECUTE SALE ----------
    for card_id, _, _ in cards:
        claims_cur.execute("""
            UPDATE claimed_cards
            SET active = 0
            WHERE id = ?
        """, (card_id,))

    claims_conn.commit()

    add_gold(ctx.author.id, total_gold)
    for rarity, amt in shard_totals.items():
        add_shards(ctx.author.id, rarity, amt)

    # ---------- RESULT EMBED (same styling) ----------
    result = discord.Embed(
        title="💰 Cards Destroyed",
        description=(
            f"Cards sold: **{len(cards)}**\n\n"
            f"💰 **+{total_gold:,} Gold**\n"
            + "\n".join(shard_lines)
        ),
        color=RARITIES[highest_rarity]["color"]
    )
    if highest_rarity in RARITY_IMAGES:
        result.set_thumbnail(url=RARITY_IMAGES[highest_rarity])

    await ctx.send(embed=result)

@bot.command(aliases=["dl"])
async def destroylatest(ctx):
    # ---------- FETCH LATEST CARD ----------
    claims_cur.execute("""
        SELECT id, user_id, rarity, name, power
        FROM claimed_cards
        WHERE user_id = ? AND active = 1
        ORDER BY claimed_at DESC
        LIMIT 1
    """, (ctx.author.id,))

    row = claims_cur.fetchone()
    if not row:
        await ctx.send("❌ You have no active cards to destroy.")
        return

    card_id, owner_id, rarity, name, power = row

    # Normalize rarity casing
    if isinstance(rarity, str):
        rarity = rarity.capitalize()

    if rarity not in SELL_BASE_REWARDS:
        await ctx.send(f"❌ Card has invalid rarity `{rarity}`.")
        return

    # ---------- CALCULATE REWARDS ----------
    base = SELL_BASE_REWARDS[rarity]
    multiplier = max(0.1, float(power) / 100.0)

    gold = int(base["gold"] * multiplier)
    shards = max(1, round(base["shards"] * multiplier))

    shard_lines = [f"🧩 **{rarity} Shards**: +{shards}"]

    # ---------- CONFIRM EMBED ----------
    embed = discord.Embed(
        title="⚠️ Confirm Destruction",
        description=(
            f"Card to destroy:\n"
            f"• **{name}** (🆔 `{card_id}`)\n\n"
            f"💰 **Gold**: **+{gold:,}**\n"
            + "\n".join(shard_lines)
            + "\n\nReact with ✅ to confirm or ❌ to cancel."
        ),
        color=RARITIES[rarity]["color"]
    )

    if rarity in RARITY_IMAGES:
        embed.set_thumbnail(url=RARITY_IMAGES[rarity])

    msg = await ctx.send(embed=embed)
    await msg.add_reaction("✅")
    await msg.add_reaction("❌")

    def check(reaction, user):
        return (
            user == ctx.author
            and reaction.message.id == msg.id
            and str(reaction.emoji) in ("✅", "❌")
        )

    # ---------- WAIT FOR CONFIRM ----------
    try:
        reaction, _ = await ctx.bot.wait_for(
            "reaction_add",
            timeout=30,
            check=check
        )
    except asyncio.TimeoutError:
        await ctx.send("⏱️ Destruction timed out.")
        return

    if str(reaction.emoji) == "❌":
        await ctx.send("❌ Destruction cancelled.")
        return

    # ---------- EXECUTE DESTRUCTION ----------
    claims_cur.execute("""
        UPDATE claimed_cards
        SET active = 0
        WHERE id = ?
    """, (card_id,))
    claims_conn.commit()

    add_gold(ctx.author.id, gold)
    add_shards(ctx.author.id, rarity, shards)

    # ---------- RESULT EMBED ----------
    result = discord.Embed(
        title="💰 Card Destroyed",
        description=(
            f"• **{name}** destroyed\n\n"
            f"💰 **+{gold:,} Gold**\n"
            + "\n".join(shard_lines)
        ),
        color=RARITIES[rarity]["color"]
    )

    if rarity in RARITY_IMAGES:
        result.set_thumbnail(url=RARITY_IMAGES[rarity])

    await ctx.send(embed=result)

@bot.command(aliases=["lu"])
async def lookup(ctx, *, query: str):
    import math
    import asyncio

    query = query.strip()
    if len(query) < 1:
        await ctx.send("❌ Please provide at least **1 character** to search.")
        return

    # Check if query is numeric (ID search)
    exact_id_match = None
    if query.isdigit():
        try:
            char_id = int(query)
            roster_cur.execute("SELECT Id, Name, Source FROM Characters WHERE Id = ?", (char_id,))
            row = roster_cur.fetchone()
            if row:
                exact_id_match = row
        except (ValueError, TypeError):
            pass

    query_words = normalize_name_words(query)

    def matches_name(name: str) -> bool:
        name_words = normalize_name_words(name)
        return any(q in w for q in query_words for w in name_words)

    # Fetch all characters with IDs
    roster_cur.execute("SELECT Id, Name, Source FROM Characters")
    all_rows = roster_cur.fetchall()

    # Filter by name match (exclude exact ID match if found, we'll add it separately)
    filtered = []
    for cid, name, source in all_rows:
        if matches_name(name):
            # Skip if this is the exact ID match (we'll add it at the beginning)
            if exact_id_match and cid == exact_id_match[0]:
                continue
            filtered.append((cid, name, source))
    
    # Sort by name
    filtered.sort(key=lambda x: x[1].lower())
    
    # If we have an exact ID match, prepend it
    if exact_id_match:
        filtered.insert(0, exact_id_match)
    
    total = len(filtered)

    if total == 0:
        await ctx.send("❌ No characters found.")
        return

    per_page = 15
    max_pages = max(1, math.ceil(total / per_page))
    page = 1

    def fetch_page(p: int):
        start = (p - 1) * per_page
        end = start + per_page
        return filtered[start:end]

    def make_embed(p: int):
        rows = fetch_page(p)
        lines = [f"• **{name}** — *{source}* `[ID: {cid}]`" for (cid, name, source) in rows]
        embed = discord.Embed(
            title=f"🔎 Character Lookup: {query}",
            description="\n".join(lines),
            color=discord.Color.blurple()
        )
        embed.set_footer(text=f"Page {p}/{max_pages} • {total} characters")
        return embed

    msg = await ctx.send(embed=make_embed(page))

    if max_pages == 1:
        return

    await msg.add_reaction("⬅️")
    await msg.add_reaction("➡️")
    await msg.add_reaction("⏹️")

    def check(reaction, user):
        return (
            user == ctx.author
            and reaction.message.id == msg.id
            and str(reaction.emoji) in ("⬅️", "➡️", "⏹️")
        )

    while True:
        try:
            reaction, user = await bot.wait_for("reaction_add", timeout=60, check=check)
        except asyncio.TimeoutError:
            break

        emoji = str(reaction.emoji)

        if emoji == "⏹️":
            break
        elif emoji == "➡️" and page < max_pages:
            page += 1
            await msg.edit(embed=make_embed(page))
        elif emoji == "⬅️" and page > 1:
            page -= 1
            await msg.edit(embed=make_embed(page))

        try:
            await msg.remove_reaction(reaction.emoji, user)
        except discord.Forbidden:
            pass

    try:
        await msg.clear_reactions()
    except discord.Forbidden:
        pass

@bot.command(name="sc", aliases=["series", "game", "source"])
async def series_lookup(ctx, *, query: str):
    import math
    import asyncio

    query = query.strip()
    if len(query) < 2:
        await ctx.send("❌ Please provide at least **2 characters** to search.")
        return

    query_words = normalize_name_words(query)

    def matches_source(source: str) -> bool:
        return normalize_name(source) == normalize_name(query)

    # Fetch characters whose Source matches
    roster_cur.execute("SELECT Name, Source FROM Characters")
    all_rows = roster_cur.fetchall()

    filtered = [(n, s) for (n, s) in all_rows if matches_source(s)]
    filtered.sort(key=lambda x: x[0].lower())

    total = len(filtered)

    if total == 0:
        await ctx.send("❌ No characters found for that series.")
        return

    per_page = 15
    max_pages = max(1, math.ceil(total / per_page))
    page = 1

    def fetch_page(p: int):
        start = (p - 1) * per_page
        end = start + per_page
        return filtered[start:end]

    def make_embed(p: int):
        rows = fetch_page(p)
        lines = [f"• **{name}**" for (name, _) in rows]

        # get unique series names for header
        series_names = sorted({s for (_, s) in filtered})

        embed = discord.Embed(
            title=f"📺 Series Lookup: {query}",
            description="\n".join(lines),
            color=discord.Color.dark_teal()
        )

        if len(series_names) == 1:
            embed.add_field(
                name="Series",
                value=series_names[0],
                inline=False
            )

        embed.set_footer(text=f"Page {p}/{max_pages} • {total} characters")
        return embed

    # send first page
    msg = await ctx.send(embed=make_embed(page))

    if max_pages == 1:
        return

    await msg.add_reaction("⬅️")
    await msg.add_reaction("➡️")
    await msg.add_reaction("⏹️")

    def check(reaction, user):
        return (
            user == ctx.author
            and reaction.message.id == msg.id
            and str(reaction.emoji) in ("⬅️", "➡️", "⏹️")
        )

    while True:
        try:
            reaction, user = await bot.wait_for(
                "reaction_add",
                timeout=60,
                check=check
            )
        except asyncio.TimeoutError:
            break

        emoji = str(reaction.emoji)

        if emoji == "⏹️":
            break
        elif emoji == "➡️" and page < max_pages:
            page += 1
            await msg.edit(embed=make_embed(page))
        elif emoji == "⬅️" and page > 1:
            page -= 1
            await msg.edit(embed=make_embed(page))

        try:
            await msg.remove_reaction(reaction.emoji, user)
        except discord.Forbidden:
            pass

    try:
        await msg.clear_reactions()
    except discord.Forbidden:
        pass

@bot.command()
async def craft(ctx, rarity: str):
    rarity = rarity.strip().upper()
    if rarity not in CRAFT_RECIPES:
        await ctx.send(f"❌ Invalid rarity. Available: {', '.join(CRAFT_RECIPES.keys())}")
        return

    recipe = CRAFT_RECIPES[rarity]
    user_id = ctx.author.id
    guild_id = ctx.guild.id if ctx.guild else 0

    # ---------- CHECK RESOURCES ----------
    gold = get_gold(user_id)
    shards = get_shards(user_id, rarity)  # returns int

    if gold < recipe["gold"]:
        await ctx.send(f"❌ Not enough gold. Required: {recipe['gold']:,} | You have: {gold:,}")
        return
    if shards < recipe["shards"]:
        await ctx.send(f"❌ Not enough {rarity} shards. Required: {recipe['shards']} | You have: {shards}")
        return

    # ---------- GET RANDOM VALID CHARACTER + IMAGE ----------
    MAX_TRIES = 50
    roster_id = None
    name = None
    anime = None
    image_url = None

    for _ in range(MAX_TRIES):
        roster_cur.execute("""
        SELECT Id, Name, Source
        FROM Characters
        ORDER BY RANDOM()
        LIMIT 1
    """)
        row = roster_cur.fetchone()
        if not row:
            continue

        cid, cname, canime = row

        img = get_image_for_character_by_rarity(cid, rarity)
        if img:
            roster_id = cid
            name = cname
            anime = canime
            image_url = img
            break

    if not image_url:
        await ctx.send(
        "❌ Could not find a valid character for this craft.\n"
        f"(No {'GIF' if rarity == 'Mythical' else 'static'} images available.)"
        )
        return

    # ---------- DEDUCT COST ----------
    add_gold(user_id, -recipe["gold"])
    add_shards(user_id, rarity, -recipe["shards"])

    # ---------- STATS ----------
    stats = roll_stats(rarity)
    claimed_at = datetime.now(timezone.utc).isoformat()

    # ---------- INSERT CLAIMED CARD (matches your table: uses 'def') ----------
    claims_cur.execute("""
        INSERT INTO claimed_cards (
            guild_id, user_id, roster_character_id,
            name, anime, rarity, image_url, claimed_at,
            role, hp, atk, def, luck, power,
            active
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
    """, (
        guild_id, user_id, roster_id,
        name, anime, rarity, image_url, claimed_at,
        stats["role"], stats["hp"], stats["atk"], stats["def"], stats["luck"], stats["power"]
    ))
    claims_conn.commit()
    global_id = claims_cur.lastrowid

    # ---------- RESULT EMBED ----------
    embed = discord.Embed(
        title=f"{RARITY_EMOJI.get(rarity,'✨')} Congrats! You just crafted: {name}!",
        description=(
            f"🆔 **ID:** `{int_to_code(global_id)}`\n"
            f"💰 **Spent:** {recipe['gold']:,} Gold\n"
            f"🧩 **Spent:** {recipe['shards']} {rarity} Shards"
        ),
        color=RARITIES[rarity]["color"]
    )
    embed.set_thumbnail(url=RARITY_IMAGES[rarity])
    embed.set_image(url=image_url)
    await ctx.send(embed=embed)
    
@bot.command()
async def recipes(ctx):
    for rarity, recipe in CRAFT_RECIPES.items():
        embed = discord.Embed(
            title=f"{RARITY_EMOJI[rarity]} {rarity} Crafting",
            description=(
                f"💰 **Gold Cost**: {recipe['gold']:,}\n"
                f"🧩 **{rarity} Shards Required**: {recipe['shards']}\n\n"
                f"Use `!craft {rarity.lower()}` to craft."
            ),
            color=RARITIES[rarity]["color"]
        )
        embed.set_thumbnail(url=RARITY_IMAGES[rarity])
        await ctx.send(embed=embed)
        
@bot.command()
async def daily(ctx):
    user_id = ctx.author.id
    now = utc_now()

    claims_cur.execute("""
        SELECT streak, last_claim
        FROM user_daily
        WHERE user_id = ?
    """, (user_id,))
    row = claims_cur.fetchone()

    if row:
        streak, last_claim = row
        last_claim = datetime.fromisoformat(last_claim)

        if now - last_claim < timedelta(hours=24):
            remaining = timedelta(hours=24) - (now - last_claim)
            h, rem = divmod(int(remaining.total_seconds()), 3600)
            m = rem // 60
            await ctx.send(f"⏳ You can claim again in **{h}h {m}m**.")
            return

        if now - last_claim > timedelta(hours=48):
            streak = 1
        else:
            streak += 1
            if streak > 14:
                streak = 1
    else:
        streak = 1

    # ---------- ROLL REWARDS ----------
    gold = roll_daily_gold(streak)
    shards = roll_daily_shards(streak)
    keys = roll_daily_keys(streak)

    # ---------- APPLY ----------
    add_gold(user_id, gold)

    for rarity, amt in shards.items():
        add_shards(user_id, rarity, amt)

    for rarity, amt in keys.items():
        add_keys(user_id, rarity, amt)

    # ---------- SAVE ----------
    claims_cur.execute("""
        INSERT INTO user_daily (user_id, streak, last_claim)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id)
        DO UPDATE SET streak = ?, last_claim = ?
    """, (user_id, streak, now.isoformat(), streak, now.isoformat()))
    claims_conn.commit()

    # ---------- EMBED ----------
    lines = [f"💰 **Gold**: +{gold:,}"]

    for rarity, amt in shards.items():
        lines.append(f"🧩 **{rarity} Shards**: +{amt}")

    for rarity, amt in keys.items():
        lines.append(f"🗝️ **{rarity} Keys**: +{amt}")

    embed = discord.Embed(
        title="📅 Daily Reward Claimed!",
        description="\n".join(lines),
        color=discord.Color.gold()
    )
    embed.set_footer(text=f"🔥 Streak Day {streak}/14")

    await ctx.send(embed=embed)

@bot.command(aliases=["fav"])
async def favorite(ctx, arg: str):
    if arg.lower() == "clear":
        set_favorite_card(ctx.author.id, None)
        await ctx.send("⭐ Favorite cleared.")
        return

    if not arg.isdigit():
        await ctx.send("❌ Usage: `!fav <card_id>` or `!fav clear`")
        return

    global_id = int(arg)

    # must own the card (active)
    claims_cur.execute("""
        SELECT id
        FROM claimed_cards
        WHERE id = ? AND user_id = ? AND active = 1
    """, (global_id, ctx.author.id))
    row = claims_cur.fetchone()

    if not row:
        await ctx.send("❌ You don’t own an active card with that ID.")
        return

    set_favorite_card(ctx.author.id, global_id)
    await ctx.send("⭐ Favorite set! (It will show in your inventory overview.)")

@bot.command()
async def open(ctx, chest_name: str):
    data = server_state.get(ctx.guild.id)
    if not data or not data.get("active_chest"):
        await ctx.send("❌ There is no chest to open.")
        return

    chest_state = data["active_chest"]

    if chest_state.get("opened"):
        await ctx.send("❌ This chest has already been opened.")
        return

    chest_name = chest_name.capitalize()

    # 🔒 ENSURE PLAYER IS OPENING THE ACTIVE CHEST
    if chest_state["rarity"] != chest_name:
        await ctx.send(
            f"❌ The active chest is **{chest_state['rarity']}**, not **{chest_name}**."
        )
        return

    if chest_name not in CHESTS:
        await ctx.send("❌ Invalid chest type.")
        return

    chest = CHESTS[chest_name]
    user_id = ctx.author.id

    keys = get_keys(user_id)
    if keys.get(chest["key"], 0) <= 0:
        await ctx.send(f"❌ You need a **{chest['key']} Key** to open this chest.")
        return

    # 🔒 LOCK CHEST IMMEDIATELY (prevents double-open)
    chest_state["opened"] = True

    # ---------- CONSUME KEY ----------
    add_keys(user_id, chest["key"], -1)

    rewards = []

    # ---------- GOLD ----------
    gold = random.randint(*chest["gold"])
    add_gold(user_id, gold)
    rewards.append(f"💰 **Gold**: +{gold:,}")

    # ---------- SHARDS ----------
    for rarity, (lo, hi) in chest["shards"].items():
        amt = random.randint(lo, hi)
        add_shards(user_id, rarity, amt)
        rewards.append(f"🧩 **{rarity} Shards**: +{amt}")

    # ---------- TOKENS ----------
    for token, (lo, hi) in chest["tokens"].items():
        amt = random.randint(lo, hi)
        add_tokens(user_id, token, amt)
        rewards.append(f"🔮 **{token} Tokens**: +{amt}")

    # ---------- REWARD EMBED ----------
    reward_embed = discord.Embed(
        title=f"🧰 {chest_name} Chest Rewards",
        description="\n".join(rewards),
        color=RARITIES[chest_name]["color"]
    )
    await ctx.send(embed=reward_embed)

    # ---------- EDIT SPAWNED CHEST EMBED ----------
    try:
        chest_msg = await ctx.channel.fetch_message(chest_state["message_id"])
    except discord.NotFound:
        chest_msg = None

    opened_embed = discord.Embed(
        title=f"🧰 {chest_name} Chest Opened!",
        description=(
            f"👤 Opened by **{ctx.author.mention}**\n"
        ),
        color=RARITIES[chest_name]["color"]
    )
    opened_embed.set_image(url=get_opened_chest_image(chest_name))

    if chest_msg:
        await chest_msg.edit(embed=opened_embed)

    # ---------- CLEAR ACTIVE CHEST ----------
    # Explicitly clear active_chest to allow new chests to spawn
    if "active_chest" in data:
        del data["active_chest"]
    # Also set to None as backup
    data["active_chest"] = None


@bot.command(name="givekey", aliases=["addkey", "gkey", "gk"])
@is_bot_owner()
async def givekeys(ctx, member: discord.Member, rarity: str, amount: int):
    rarity = rarity.capitalize()

    # Validate rarity
    if rarity not in RARITIES or rarity == "Mythical":
        await ctx.send(
            f"❌ Invalid key rarity. Valid: Common, Uncommon, Rare, Epic, Legendary"
        )
        return

    if amount == 0:
        await ctx.send("❌ Amount must not be 0.")
        return

    # Get current keys
    current_keys = get_keys(member.id).get(rarity, 0)

    # Clamp result to >= 0
    new_amount = max(0, current_keys + amount)
    delta = new_amount - current_keys

    # Apply update
    claims_cur.execute("""
        INSERT INTO user_keys (user_id, rarity, amount)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id, rarity)
        DO UPDATE SET amount = excluded.amount
    """, (member.id, rarity, new_amount))
    claims_conn.commit()

    sign = "+" if delta > 0 else ""
    await ctx.send(
        f"🗝️ {member.mention} now has **{new_amount}** **{rarity} Keys** "
        f"({sign}{delta})."
    )

@bot.command(name="givegold", aliases=["addgold", "ggold", "gg"])
@is_bot_owner()
async def givegold(ctx, member: discord.Member, amount: int):
    if amount == 0:
        await ctx.send("❌ Amount must not be 0.")
        return

    # Get current gold
    claims_cur.execute(
        "SELECT gold FROM user_gold WHERE user_id = ? LIMIT 1",
        (member.id,)
    )
    row = claims_cur.fetchone()
    current_gold = row[0] if row else 0

    new_amount = max(0, current_gold + amount)
    delta = new_amount - current_gold

    # Upsert (user_id is PRIMARY KEY)
    claims_cur.execute("""
        INSERT INTO user_gold (user_id, gold)
        VALUES (?, ?)
        ON CONFLICT(user_id)
        DO UPDATE SET gold = excluded.gold
    """, (member.id, new_amount))

    claims_conn.commit()

    sign = "+" if delta > 0 else ""
    await ctx.send(
        f"💰 {member.mention} now has **{new_amount:,} Gold** ({sign}{delta:,})."
    )

@bot.command(name="giveshard", aliases=["addshard", "gshard", "gs"])
@is_bot_owner()
async def giveshard(ctx, member: discord.Member, rarity: str, amount: int):
    rarity = rarity.capitalize()

    if amount == 0:
        await ctx.send("❌ Amount must not be 0.")
        return

    # Get current shards
    claims_cur.execute("""
        SELECT amount
        FROM user_shards
        WHERE user_id = ? AND rarity = ?
        LIMIT 1
    """, (member.id, rarity))
    row = claims_cur.fetchone()
    current_shards = row[0] if row else 0

    new_amount = max(0, current_shards + amount)
    delta = new_amount - current_shards

    # Upsert (composite PK)
    claims_cur.execute("""
        INSERT INTO user_shards (user_id, rarity, amount)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id, rarity)
        DO UPDATE SET amount = excluded.amount
    """, (member.id, rarity, new_amount))

    claims_conn.commit()

    sign = "+" if delta > 0 else ""
    await ctx.send(
        f"🧩 {member.mention} now has **{new_amount} {rarity} Shards** ({sign}{delta})."
    )

@bot.command(name="givetoken", aliases=["addtoken", "gtoken", "gt"])
@is_bot_owner()
async def givetoken(ctx, member: discord.Member, token_type: str, amount: int):
    token_type = token_type.strip().lower()

    if token_type not in ("legendary", "mythical"):
        await ctx.send("❌ Invalid token type. Use `legendary` or `mythical`.")
        return

    token_type = token_type.capitalize()

    if amount == 0:
        await ctx.send("❌ Amount must not be 0.")
        return

    # Get current amount (if exists)
    claims_cur.execute("""
        SELECT amount
        FROM user_tokens
        WHERE user_id = ? AND token_type = ?
    """, (member.id, token_type))
    row = claims_cur.fetchone()

    current = row[0] if row else 0
    new_amount = max(0, current + amount)
    delta = new_amount - current

    # Upsert
    claims_cur.execute("""
        INSERT INTO user_tokens (user_id, token_type, amount)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id, token_type)
        DO UPDATE SET amount = excluded.amount
    """, (member.id, token_type, new_amount))

    claims_conn.commit()

    emoji = "🪙" if token_type == "legendary" else "🌠"
    sign = "+" if delta > 0 else ""

    await ctx.send(
        f"{emoji} {member.mention} now has **{new_amount}** "
        f"**{token_type.capitalize()} Tokens** ({sign}{delta})."
    )

@bot.command(aliases=["trade","t"])
async def traderequest(ctx, member: discord.Member):
    """Start a trade with another player using reaction-based interface"""
    if member.id == ctx.author.id:
        await ctx.send("❌ You can't trade with yourself!")
        return
    
    if member.bot:
        await ctx.send("❌ You can't trade with bots!")
        return
    
    # Check if there's already a pending trade
    claims_cur.execute("""
        SELECT trade_id FROM trades 
        WHERE ((initiator_id = ? AND receiver_id = ?) OR 
               (initiator_id = ? AND receiver_id = ?))
        AND status = 'pending'
    """, (ctx.author.id, member.id, member.id, ctx.author.id))
    
    if claims_cur.fetchone():
        await ctx.send("❌ There's already a pending trade with this user!")
        return
    
    # Create a new trade
    trade_id = create_trade(
        initiator_id=ctx.author.id,
        receiver_id=member.id,
        channel_id=ctx.channel.id
    )
    
    # Create the trade embed
    embed = await create_trade_embed(trade_id, ctx)
    
    # Send the trade message
    trade_msg = await ctx.send(embed=embed)
    
    # Store message ID
    update_trade_message(trade_id, trade_msg.id)
    
    # Add reactions
    await trade_msg.add_reaction("✅")  # Confirm
    await trade_msg.add_reaction("❌")  # Cancel/Decline
    await trade_msg.add_reaction("➕")  # Add card
    await trade_msg.add_reaction("➖")  # Remove card
    await trade_msg.add_reaction("💰")  # Add gold
    await trade_msg.add_reaction("🧩")  # Add shards
    await trade_msg.add_reaction("📋")  # View details
    
    # Store in memory for quick access
    if trade_id not in active_trades:
        active_trades[trade_id] = {
            "initiator_id": ctx.author.id,
            "receiver_id": member.id,
            "message_id": trade_msg.id,
            "channel_id": ctx.channel.id
        }
    
    # Send instructions
    instructions = discord.Embed(
        title="🎮 Trade Interface Guide",
        description=(
            "**Reactions Guide:**\n"
            "✅ - Confirm/Accept trade\n"
            "❌ - Cancel/Decline trade\n"
            "➕ - Add a card (use `!tradeadd <card_id>` after reacting)\n"
            "➖ - Remove a card (use `!traderemove <card_id>` after reacting)\n"
            "💰 - Add gold (use `!tradegold <amount>` after reacting)\n"
            "🧩 - Add shards (use `!tradeshards <rarity> <amount>` after reacting)\n"
            "📋 - Show detailed view\n\n"
            "**How to trade:**\n"
            "1. Both players click ✅ to confirm they want to trade\n"
            "2. Use the reactions to add/remove items\n"
            "3. Both players click ✅ again to finalize the trade\n"
            "4. Either player can click ❌ at any time to cancel"
        ),
        color=discord.Color.blue()
    )
    await ctx.send(embed=instructions, delete_after=30)

async def create_trade_embed(trade_id, ctx):
    """Create an embed showing the current trade state"""
    trade = get_trade(trade_id)
    if not trade:
        return discord.Embed(title="Trade Not Found", color=discord.Color.red())
    
    initiator = ctx.guild.get_member(trade['initiator_id'])
    receiver = ctx.guild.get_member(trade['receiver_id'])
    
    initiator_name = initiator.display_name if initiator else f"User {trade['initiator_id']}"
    receiver_name = receiver.display_name if receiver else f"User {trade['receiver_id']}"
    
    # Get card details
    def get_card_details(card_ids):
        if not card_ids:
            return "None"
        
        details = []
        for cid in card_ids:
            claims_cur.execute("""
                SELECT name, rarity FROM claimed_cards 
                WHERE id = ?
            """, (cid,))
            row = claims_cur.fetchone()
            if row:
                name, rarity = row
                details.append(f"{RARITY_EMOJI.get(rarity,'✨')} **{name}** (ID: {int_to_code(cid)})")
        return "\n".join(details) if details else "None"
    
    # Calculate totals
    initiator_total = trade['initiator_gold']
    receiver_total = trade['receiver_gold']
    
    # Add card values (simplified - could use sell value or power)
    for card_id in trade['initiator_cards']:
        claims_cur.execute("SELECT rarity FROM claimed_cards WHERE id = ?", (card_id,))
        row = claims_cur.fetchone()
        if row:
            rarity = row[0]
            base_value = SELL_BASE_REWARDS.get(rarity, {}).get('gold', 0)
            initiator_total += base_value
    
    for card_id in trade['receiver_cards']:
        claims_cur.execute("SELECT rarity FROM claimed_cards WHERE id = ?", (card_id,))
        row = claims_cur.fetchone()
        if row:
            rarity = row[0]
            base_value = SELL_BASE_REWARDS.get(rarity, {}).get('gold', 0)
            receiver_total += base_value
    
    # Determine embed color based on status
    if trade['status'] == 'pending':
        if trade['initiator_confirmed'] and trade['receiver_confirmed']:
            color = discord.Color.green()  # Both confirmed
        elif trade['initiator_confirmed'] or trade['receiver_confirmed']:
            color = discord.Color.orange()  # One confirmed
        else:
            color = discord.Color.blue()  # Neither confirmed
    elif trade['status'] == 'completed':
        color = discord.Color.green()
    else:
        color = discord.Color.red()
    
    embed = discord.Embed(
        title=f"🔀 Trade #{trade_id}",
        color=color
    )
    
    # Trade status
    status_text = f"**Status:** {trade['status'].upper()}\n"
    if trade['status'] == 'pending':
        status_text += f"**⌛ Expires:** <t:{int(datetime.fromisoformat(trade['expires_at']).timestamp())}:R>\n"
        status_text += f"**✅ Confirmed:** "
        if trade['initiator_confirmed'] and trade['receiver_confirmed']:
            status_text += "**BOTH** - Ready to finalize!"
        elif trade['initiator_confirmed']:
            status_text += f"{initiator_name}"
        elif trade['receiver_confirmed']:
            status_text += f"{receiver_name}"
        else:
            status_text += "Neither"
    
    embed.description = status_text
    
    # Initiator side
    initiator_field = (
        f"**Cards ({len(trade['initiator_cards'])}):**\n{get_card_details(trade['initiator_cards'])}\n"
        f"**Gold:** {trade['initiator_gold']:,}\n"
        f"**Shards:** {', '.join([f'{k}: {v}' for k, v in trade['initiator_shards'].items()]) if trade['initiator_shards'] else 'None'}\n"
        f"**Total Value:** ≈{initiator_total:,}"
    )
    
    # Receiver side
    receiver_field = (
        f"**Cards ({len(trade['receiver_cards'])}):**\n{get_card_details(trade['receiver_cards'])}\n"
        f"**Gold:** {trade['receiver_gold']:,}\n"
        f"**Shards:** {', '.join([f'{k}: {v}' for k, v in trade['receiver_shards'].items()]) if trade['receiver_shards'] else 'None'}\n"
        f"**Total Value:** ≈{receiver_total:,}"
    )
    
    embed.add_field(
        name=f"👤 {initiator_name} {'✅' if trade['initiator_confirmed'] else '❌'}",
        value=initiator_field,
        inline=True
    )
    
    embed.add_field(
        name=f"👤 {receiver_name} {'✅' if trade['receiver_confirmed'] else '❌'}",
        value=receiver_field,
        inline=True
    )
    
    # Footer with instructions
    if trade['status'] == 'pending':
        if trade['initiator_confirmed'] and trade['receiver_confirmed']:
            embed.set_footer(text="Both confirmed! Click ✅ again to finalize or ❌ to cancel")
        else:
            embed.set_footer(text="Click ✅ to confirm trade, ❌ to cancel. Use reactions to modify.")
    
    return embed

@bot.command(aliases=["tadd"])
async def tradeadd(ctx, card_input: str):
    """Add a card to your side of the trade"""
    # Parse card ID (supports both numeric and alphanumeric codes)
    card_id = parse_card_id(card_input)
    if card_id is None:
        await ctx.send(f"❌ Invalid card ID format: `{card_input}`. Use alphanumeric codes (minimum 3 characters).")
        return
    
    # Get user's active trade
    trade_id = get_user_active_trade(ctx.author.id)
    
    if not trade_id:
        await ctx.send("❌ You don't have any active trades! Start one with `!trade @user`")
        return
    
    trade = get_trade(trade_id)
    
    if not trade or trade['status'] != 'pending':
        await ctx.send("❌ Invalid or completed trade!")
        return
    
    # Check if both parties have confirmed (can't modify after confirmation)
    if trade['initiator_confirmed'] or trade['receiver_confirmed']:
        # Unconfirm both parties when modifying
        unconfirm_trade(trade_id, trade['initiator_id'])
        unconfirm_trade(trade_id, trade['receiver_id'])
        await ctx.send("⚠️ Trade modifications reset confirmation for both parties.", delete_after=5)
    
    # Check if user owns the card
    claims_cur.execute("""
        SELECT id FROM claimed_cards 
        WHERE id = ? AND user_id = ? AND active = 1
    """, (card_id, ctx.author.id))
    
    if not claims_cur.fetchone():
        await ctx.send("❌ You don't own this card or it's not active!")
        return
    
    # Check if card is already in any active trade
    claims_cur.execute("""
        SELECT t.trade_id FROM trades t
        WHERE t.status = 'pending'
        AND t.trade_id != ?
        AND (
            EXISTS (SELECT 1 FROM json_each(t.initiator_cards) WHERE value = ?) OR
            EXISTS (SELECT 1 FROM json_each(t.receiver_cards) WHERE value = ?)
        )
    """, (trade_id, card_id, card_id))
    
    existing = claims_cur.fetchone()
    if existing:
        await ctx.send("❌ This card is already in another active trade!")
        return
    
    # Update the trade
    is_initiator = ctx.author.id == trade['initiator_id']
    cards_field = 'initiator_cards' if is_initiator else 'receiver_cards'
    
    cards = trade[cards_field]
    if card_id in cards:
        await ctx.send("❌ This card is already in the trade!")
        return
    
    cards.append(card_id)
    
    claims_cur.execute(f"""
        UPDATE trades 
        SET {cards_field} = ?
        WHERE trade_id = ?
    """, (json.dumps(cards), trade_id))
    claims_conn.commit()
    
    # Update the trade message
    try:
        channel = bot.get_channel(trade['channel_id'])
        if channel:
            message = await channel.fetch_message(trade['message_id'])
            embed = await create_trade_embed(trade_id, ctx)
            await message.edit(embed=embed)
    except:
        pass
    
    card_code = int_to_code(card_id)
    await ctx.send(f"✅ Added card `{card_code}` to trade #{trade_id}", delete_after=10)

@bot.command(aliases=["tremove"])
async def traderemove(ctx, card_input: str):
    """Remove a card from your side of the trade"""
    # Parse card ID (supports both numeric and alphanumeric codes)
    card_id = parse_card_id(card_input)
    if card_id is None:
        await ctx.send(f"❌ Invalid card ID format: `{card_input}`. Use alphanumeric codes (minimum 3 characters).")
        return
    
    # Get user's active trade
    trade_id = get_user_active_trade(ctx.author.id)
    
    if not trade_id:
        await ctx.send("❌ You don't have any active trades!")
        return
    
    trade = get_trade(trade_id)
    
    if not trade or trade['status'] != 'pending':
        await ctx.send("❌ Invalid or completed trade!")
        return
    
    # Check if both parties have confirmed (can't modify after confirmation)
    if trade['initiator_confirmed'] or trade['receiver_confirmed']:
        # Unconfirm both parties when modifying
        unconfirm_trade(trade_id, trade['initiator_id'])
        unconfirm_trade(trade_id, trade['receiver_id'])
        await ctx.send("⚠️ Trade modifications reset confirmation for both parties.", delete_after=5)
    
    is_initiator = ctx.author.id == trade['initiator_id']
    cards_field = 'initiator_cards' if is_initiator else 'receiver_cards'
    
    cards = trade[cards_field]
    if card_id not in cards:
        await ctx.send("❌ This card isn't in your side of the trade!")
        return
    
    cards.remove(card_id)
    
    claims_cur.execute(f"""
        UPDATE trades 
        SET {cards_field} = ?
        WHERE trade_id = ?
    """, (json.dumps(cards), trade_id))
    claims_conn.commit()
    
    # Update the trade message
    try:
        channel = bot.get_channel(trade['channel_id'])
        if channel:
            message = await channel.fetch_message(trade['message_id'])
            embed = await create_trade_embed(trade_id, ctx)
            await message.edit(embed=embed)
    except:
        pass
    
    card_code = int_to_code(card_id)
    await ctx.send(f"✅ Removed card `{card_code}` from trade #{trade_id}", delete_after=10)

@bot.command(aliases=["tgold"])
async def tradegold(ctx, amount: int):
    """Add gold to your side of the trade"""
    if amount < 0:
        await ctx.send("❌ Amount must be positive!", delete_after=5)
        return
    
    # Get user's active trade
    trade_id = get_user_active_trade(ctx.author.id)
    
    if not trade_id:
        await ctx.send("❌ You don't have any active trades! Start one with `!trade @user`")
        return
    
    trade = get_trade(trade_id)
    
    if not trade or trade['status'] != 'pending':
        await ctx.send("❌ Invalid or completed trade!")
        return
    
    # Check if both parties have confirmed (can't modify after confirmation)
    if trade['initiator_confirmed'] or trade['receiver_confirmed']:
        # Unconfirm both parties when modifying
        unconfirm_trade(trade_id, trade['initiator_id'])
        unconfirm_trade(trade_id, trade['receiver_id'])
        await ctx.send("⚠️ Trade modifications reset confirmation for both parties.", delete_after=5)
    
    # Check if user has enough gold
    user_gold = get_gold(ctx.author.id)
    if user_gold < amount:
        await ctx.send(f"❌ You only have {user_gold:,} gold!", delete_after=5)
        return
    
    is_initiator = ctx.author.id == trade['initiator_id']
    gold_field = 'initiator_gold' if is_initiator else 'receiver_gold'
    
    claims_cur.execute(f"""
        UPDATE trades 
        SET {gold_field} = ?
        WHERE trade_id = ?
    """, (amount, trade_id))
    claims_conn.commit()
    
    # Update the trade message
    try:
        channel = bot.get_channel(trade['channel_id'])
        if channel:
            message = await channel.fetch_message(trade['message_id'])
            embed = await create_trade_embed(trade_id, ctx)
            await message.edit(embed=embed)
    except:
        pass
    
    await ctx.send(f"💰 Added {amount:,} gold to trade #{trade_id}", delete_after=10)

@bot.command(aliases=["tshards"])
async def tradeshards(ctx, rarity: str, amount: int):
    """Add shards to your side of the trade"""
    if amount < 0:
        await ctx.send("❌ Amount must be positive!", delete_after=5)
        return
    
    rarity = rarity.capitalize()
    if rarity not in RARITIES:
        await ctx.send(f"❌ Invalid rarity. Valid: {', '.join(RARITIES.keys())}", delete_after=5)
        return
    
    # Get user's active trade
    trade_id = get_user_active_trade(ctx.author.id)
    
    if not trade_id:
        await ctx.send("❌ You don't have any active trades! Start one with `!trade @user`")
        return
    
    trade = get_trade(trade_id)
    
    if not trade or trade['status'] != 'pending':
        await ctx.send("❌ Invalid or completed trade!")
        return
    
    # Check if both parties have confirmed (can't modify after confirmation)
    if trade['initiator_confirmed'] or trade['receiver_confirmed']:
        # Unconfirm both parties when modifying
        unconfirm_trade(trade_id, trade['initiator_id'])
        unconfirm_trade(trade_id, trade['receiver_id'])
        await ctx.send("⚠️ Trade modifications reset confirmation for both parties.", delete_after=5)
    
    # Check if user has enough shards
    user_shards = get_shards(ctx.author.id, rarity)
    if user_shards < amount:
        await ctx.send(f"❌ You only have {user_shards} {rarity} shards!", delete_after=5)
        return
    
    is_initiator = ctx.author.id == trade['initiator_id']
    shards_field = 'initiator_shards' if is_initiator else 'receiver_shards'
    
    # Get current shards
    current_shards = trade[shards_field]
    current_shards[rarity] = amount
    
    claims_cur.execute(f"""
        UPDATE trades 
        SET {shards_field} = ?
        WHERE trade_id = ?
    """, (json.dumps(current_shards), trade_id))
    claims_conn.commit()
    
    # Update the trade message
    try:
        channel = bot.get_channel(trade['channel_id'])
        if channel:
            message = await channel.fetch_message(trade['message_id'])
            embed = await create_trade_embed(trade_id, ctx)
            await message.edit(embed=embed)
    except:
        pass
    
    await ctx.send(f"🧩 Added {amount} {rarity} shards to trade #{trade_id}", delete_after=10)

@bot.command(aliases=["tclear"])
async def tradeclear(ctx):
    """Clear all items from your side of the trade"""
    # Get user's active trade
    trade_id = get_user_active_trade(ctx.author.id)
    
    if not trade_id:
        await ctx.send("❌ You don't have any active trades!")
        return
    
    trade = get_trade(trade_id)
    
    if not trade or trade['status'] != 'pending':
        await ctx.send("❌ Invalid or completed trade!")
        return
    
    # Check if both parties have confirmed (can't modify after confirmation)
    if trade['initiator_confirmed'] or trade['receiver_confirmed']:
        # Unconfirm both parties when modifying
        unconfirm_trade(trade_id, trade['initiator_id'])
        unconfirm_trade(trade_id, trade['receiver_id'])
        await ctx.send("⚠️ Trade modifications reset confirmation for both parties.", delete_after=5)
    
    is_initiator = ctx.author.id == trade['initiator_id']
    
    # Clear all items
    if is_initiator:
        claims_cur.execute("""
            UPDATE trades 
            SET initiator_cards = '[]',
                initiator_gold = 0,
                initiator_shards = '{}'
            WHERE trade_id = ?
        """, (trade_id,))
    else:
        claims_cur.execute("""
            UPDATE trades 
            SET receiver_cards = '[]',
                receiver_gold = 0,
                receiver_shards = '{}'
            WHERE trade_id = ?
        """, (trade_id,))
    
    claims_conn.commit()
    
    # Update the trade message
    try:
        channel = bot.get_channel(trade['channel_id'])
        if channel:
            message = await channel.fetch_message(trade['message_id'])
            embed = await create_trade_embed(trade_id, ctx)
            await message.edit(embed=embed)
    except:
        pass
    
    await ctx.send(f"🗑️ Cleared all items from your side of trade #{trade_id}", delete_after=10)
    
async def cleanup_expired_trades():
    """Clean up expired trades every minute"""
    now = datetime.now(timezone.utc).isoformat()
    
    # Get expired trades
    claims_cur.execute("""
        SELECT trade_id, channel_id, message_id 
        FROM trades 
        WHERE status = 'pending' AND expires_at < ?
    """, (now,))
    
    expired = claims_cur.fetchall()
    
    for trade_id, channel_id, message_id in expired:
        # Update status
        claims_cur.execute("""
            UPDATE trades 
            SET status = 'expired', completed_at = ?
            WHERE trade_id = ?
        """, (now, trade_id))
        
        # Remove from active trades
        if trade_id in active_trades:
            del active_trades[trade_id]
        
        # Try to update the message
        try:
            channel = bot.get_channel(channel_id)
            if channel:
                message = await channel.fetch_message(message_id)
                embed = discord.Embed(
                    title=f"⏰ Trade #{trade_id} Expired",
                    description="This trade has expired due to inactivity.",
                    color=discord.Color.red()
                )
                await message.edit(embed=embed)
                await message.clear_reactions()
        except:
            pass
    
    if expired:
        claims_conn.commit()
        
@bot.command()
@is_bot_owner()
async def delimage(ctx, *image_ids):
    """
    Usage:
    !delimage <image_id> [image_id ...]   (max 10)
    """

    if not image_ids:
        await ctx.send("❌ Usage: `!delimage <image_id> [image_id ...]`")
        return

    if len(image_ids) > 10:
        await ctx.send("❌ You can delete a maximum of 10 images at once.")
        return

    try:
        image_ids = [int(i) for i in image_ids]
    except ValueError:
        await ctx.send("❌ All image IDs must be numbers.")
        return

    placeholders = ",".join("?" for _ in image_ids)

    # Fetch images that exist
    roster_cur.execute(f"""
        SELECT ci.image_id, ci.image_url, c.Name
        FROM character_images ci
        JOIN Characters c ON c.Id = ci.character_id
        WHERE ci.image_id IN ({placeholders})
    """, image_ids)

    rows = roster_cur.fetchall()

    if not rows:
        await ctx.send("❌ No matching images found.")
        return

    # Delete found images
    roster_cur.execute(f"""
        DELETE FROM character_images
        WHERE image_id IN ({placeholders})
    """, image_ids)
    roster_conn.commit()

    # Build response
    lines = []
    for image_id, image_url, character_name in rows:
        lines.append(
            f"🗑️ `{image_id}` — **{character_name}**\n🔗 `{image_url}`"
        )

    await ctx.send(
        f"✅ Deleted **{len(rows)}** image(s):\n\n" + "\n\n".join(lines)
    )

@bot.command()
@is_bot_owner()
async def addimage(ctx, *, payload: str):
    """
    Usage:
    !addimage Character Name | image_url
    """
    if "|" not in payload:
        await ctx.send("❌ Use: `!addimage Character Name | image_url`")
        return

    name, image_url = [p.strip() for p in payload.split("|", 1)]

    if not image_url.startswith("http"):
        await ctx.send("❌ Invalid image URL.")
        return

    # Find character (case-insensitive)
    roster_cur.execute("""
        SELECT Id, Name
        FROM Characters
        WHERE LOWER(Name) = LOWER(?)
        LIMIT 1
    """, (name,))
    row = roster_cur.fetchone()

    if not row:
        await ctx.send(f"❌ Character **{name}** not found.")
        return

    character_id, real_name = row

    # Insert image (ignore duplicates safely)
    try:
        roster_cur.execute("""
            INSERT OR IGNORE INTO character_images (character_id, image_url)
            VALUES (?, ?)
        """, (character_id, image_url))
        roster_conn.commit()
    except Exception as e:
        await ctx.send(f"❌ Database error: `{e}`")
        return

    if roster_cur.rowcount == 0:
        await ctx.send("⚠️ That image already exists for this character.")
        return

    await ctx.send(f"✅ Added image to **{real_name}**")

@bot.command(aliases=["i"])
async def info(ctx, *, character_input: str):
    import asyncio

    # ---------- FIND CHARACTER ----------
    char = find_character_by_name_or_id(character_input)
    
    if not char:
        await ctx.send(f"❌ Character not found. Use character name or ID (e.g., `!info 123` or `!info Character Name`).")
        return

    character_id = char["id"]
    real_name = char["name"]
    source = char["anime"]

    # ---------- FETCH IMAGES ----------
    roster_cur.execute("""
        SELECT image_id, image_url
        FROM character_images
        WHERE character_id = ?
        ORDER BY image_id ASC
    """, (character_id,))
    images = roster_cur.fetchall()

    if not images:
        await ctx.send(f"⚠️ **{real_name}** has no images.")
        return

    index = 0
    total = len(images)

    # ---------- WISHLIST INFO (ONCE) ----------
    total_wishes, user_wants = get_wishlist_info(character_id, ctx.author.id)

    wish_text = f"💖 **Wishlist:** {total_wishes}"
    if user_wants:
        wish_text += "\n✨ *This character is on your wishlist*"

    # ---------- EMBED BUILDER ----------
    def make_embed(idx: int):
        image_id, image_url = images[idx]
        embed = discord.Embed(
            title=real_name,
            description=f"*{source}*\n🖼️ Image {idx + 1}/{total}\n**Character ID:** {character_id}",
            color=discord.Color.pink()
        )
        embed.set_image(url=image_url)
        embed.set_footer(text=f"Image ID: {image_id}")

        embed.add_field(
            name="Wishlist",
            value=wish_text,
            inline=False
        )

        return embed

    # ---------- SEND FIRST IMAGE ----------
    msg = await ctx.send(embed=make_embed(index))

    if total == 1:
        return

    # ---------- ADD REACTIONS ----------
    await msg.add_reaction("⬅️")
    await msg.add_reaction("➡️")
    await msg.add_reaction("⏹️")

    def check(reaction, user):
        return (
            user == ctx.author
            and reaction.message.id == msg.id
            and str(reaction.emoji) in ("⬅️", "➡️", "⏹️")
        )

    # ---------- PAGINATION LOOP ----------
    while True:
        try:
            reaction, user = await bot.wait_for(
                "reaction_add",
                timeout=60,
                check=check
            )
        except asyncio.TimeoutError:
            break

        emoji = str(reaction.emoji)

        if emoji == "⏹️":
            break
        elif emoji == "➡️":
            index = (index + 1) % total
            await msg.edit(embed=make_embed(index))
        elif emoji == "⬅️":
            index = (index - 1) % total
            await msg.edit(embed=make_embed(index))

        try:
            await msg.remove_reaction(reaction.emoji, user)
        except discord.Forbidden:
            pass

    try:
        await msg.clear_reactions()
    except discord.Forbidden:
        pass


@bot.command()
async def untag(ctx, global_id: int):
    # Ensure card exists and user owns it
    claims_cur.execute(
        "SELECT user_id FROM claimed_cards WHERE id = ? AND active = 1",
        (global_id,)
    )
    row = claims_cur.fetchone()

    if not row:
        await ctx.send(f"❌ No active card found with ID `{global_id}`.")
        return

    if row[0] != ctx.author.id:
        await ctx.send("❌ You can only untag cards you own.")
        return

    # Remove all tags for this card
    claims_cur.execute("""
        DELETE FROM card_tags
        WHERE card_id = ? AND user_id = ?
    """, (global_id, ctx.author.id))

    claims_conn.commit()

    await ctx.send(f"🗑️ Card `{global_id}` untagged")
    
@bot.command(aliases=["tl"])
async def taglatest(ctx, emoji: str):
    # Validate emoji
    if not is_valid_tag_emoji(emoji):
        await ctx.send("❌ That doesn’t look like a valid emoji.")
        return

    # Find user's latest active card
    claims_cur.execute("""
        SELECT id
        FROM claimed_cards
        WHERE user_id = ? AND active = 1
        ORDER BY claimed_at DESC
        LIMIT 1
    """, (ctx.author.id,))

    row = claims_cur.fetchone()
    if not row:
        await ctx.send("❌ You have no active cards to tag.")
        return

    card_id = row[0]
    now = datetime.now(timezone.utc).isoformat()

    # Remove existing tag(s)
    claims_cur.execute("""
        DELETE FROM card_tags
        WHERE card_id = ? AND user_id = ?
    """, (card_id, ctx.author.id))

    # Insert new tag
    claims_cur.execute("""
        INSERT INTO card_tags (card_id, user_id, tag, created_at)
        VALUES (?, ?, ?, ?)
    """, (card_id, ctx.author.id, emoji, now))

    claims_conn.commit()

    card_code = int_to_code(card_id)
    await ctx.send(
        f"🏷️ Tagged your latest card (🆔 `{card_code}`) with {emoji}"
    )

@bot.command(aliases=["wa"])
async def wishadd(ctx, *, character_input: str):
    char = find_character_by_name_or_id(character_input)
    if not char:
        await ctx.send("❌ Character not found. Use character name or ID (e.g., `!wa 123` or `!wa Character Name`).")
        return

    now = datetime.now(timezone.utc).isoformat()

    try:
        claims_cur.execute("""
            INSERT INTO wishlists (user_id, roster_character_id, created_at)
            VALUES (?, ?, ?)
        """, (ctx.author.id, char["id"], now))
        claims_conn.commit()
    except sqlite3.IntegrityError:
        await ctx.send("⚠️ That character is already in your wishlist.")
        return

    await ctx.send(
        f"💖 Added **{char['name']}** ({char['anime']}) [ID: {char['id']}] to your wishlist!"
    )
    
@bot.command(aliases=["wr"])
async def wishremove(ctx, *, character_input: str):
    char = find_character_by_name_or_id(character_input)
    if not char:
        await ctx.send("❌ Character not found. Use character name or ID (e.g., `!wr 123` or `!wr Character Name`).")
        return

    claims_cur.execute("""
        DELETE FROM wishlists
        WHERE user_id = ? AND roster_character_id = ?
    """, (ctx.author.id, char["id"]))
    claims_conn.commit()

    if claims_cur.rowcount == 0:
        await ctx.send("⚠️ That character is not in your wishlist.")
        return

    await ctx.send(
        f"🗑️ Removed **{char['name']}** [ID: {char['id']}] from your wishlist."
    ) 
 
@bot.command(aliases=["wl"])
async def wishlist(ctx, user: discord.User = None):
    import asyncio

    PER_PAGE = 20

    target = user or ctx.author

    # ---------- FETCH WISHLIST IDS ----------
    claims_cur.execute("""
        SELECT roster_character_id
        FROM wishlists
        WHERE user_id = ?
        ORDER BY created_at ASC
    """, (target.id,))
    ids = [row[0] for row in claims_cur.fetchall()]

    if not ids:
        if target == ctx.author:
            await ctx.send("📭 Your wishlist is empty.")
        else:
            await ctx.send(f"📭 **{target.display_name}** has an empty wishlist.")
        return

    # ---------- FETCH CHARACTER INFO ----------
    placeholders = ",".join("?" for _ in ids)
    roster_cur.execute(f"""
        SELECT Id, Name, Source
        FROM Characters
        WHERE Id IN ({placeholders})
    """, ids)
    rows = roster_cur.fetchall()

    info_map = {cid: (name, source) for cid, name, source in rows}

    # ---------- SORT BY ANIME → NAME ----------
    sortable = []
    missing = 0

    for cid in ids:
        if cid in info_map:
            name, source = info_map[cid]
            sortable.append((
                source.casefold(),
                name.casefold(),
                cid,
                name,
                source
            ))
        else:
            missing += 1

    sortable.sort()

    entries = [
        f"• **{name}** ({source}) `[ID: {cid}]`"
        for _, _, cid, name, source in sortable
    ]

    total = len(entries)
    pages = (total - 1) // PER_PAGE + 1
    index = 0

    # ---------- EMBED BUILDER ----------
    def make_embed(page: int):
        start = page * PER_PAGE
        end = start + PER_PAGE
        chunk = entries[start:end]

        embed = discord.Embed(
            title=f"💖 {target.display_name}'s Wishlist",
            description="\n".join(chunk),
            color=discord.Color.pink()
        )

        embed.set_footer(
            text=f"Page {page + 1}/{pages} • {total}/150 entries"
        )

        if missing:
            embed.add_field(
                name="⚠️ Missing characters",
                value=f"{missing} entry(s) no longer exist in the roster.",
                inline=False
            )

        return embed

    # ---------- SEND FIRST PAGE ----------
    msg = await ctx.send(embed=make_embed(index))

    if pages == 1:
        return

    # ---------- REACTIONS ----------
    await msg.add_reaction("⬅️")
    await msg.add_reaction("➡️")
    await msg.add_reaction("⏹️")

    def check(reaction, u):
        return (
            u == ctx.author
            and reaction.message.id == msg.id
            and str(reaction.emoji) in ("⬅️", "➡️", "⏹️")
        )

    # ---------- PAGINATION LOOP ----------
    while True:
        try:
            reaction, u = await bot.wait_for(
                "reaction_add",
                timeout=90,
                check=check
            )
        except asyncio.TimeoutError:
            break

        emoji = str(reaction.emoji)

        if emoji == "⏹️":
            break
        elif emoji == "➡️":
            index = (index + 1) % pages
        elif emoji == "⬅️":
            index = (index - 1) % pages

        await msg.edit(embed=make_embed(index))

        try:
            await msg.remove_reaction(reaction.emoji, u)
        except discord.Forbidden:
            pass

    try:
        await msg.clear_reactions()
    except discord.Forbidden:
        pass

@bot.command()
async def buy(ctx, tier: str):
    tier = tier.upper()
    if tier not in PACK_COSTS:
        await ctx.send("❌ Invalid pack tier. Use: C, B, A, S, S+")
        return

    cost = PACK_COSTS[tier]

    # ---------- CHECK AFFORDABILITY ----------
    if not can_afford_pack(ctx.author.id, tier):
        await ctx.send("❌ You don’t have enough materials to buy this pack.")
        return

    # ---------- BUILD COST TEXT ----------
    lines = []

    if "legendary_token" in cost:
        lines.append(f"🪙 **Legendary Tokens**: {cost['legendary_token']}")

    elif "mythical_token" in cost:
        lines.append(f"🌠 **Mythical Tokens**: {cost['mythical_token']}")

    else:
        lines.append(f"💰 **Gold**: {cost['gold']:,}")
        for rarity, amt in cost["shards"].items():
            lines.append(f"🧩 **{rarity} Shards**: {amt}")

    embed = discord.Embed(
        title=f"📦 Buy {tier} Pack?",
        description="\n".join(lines) + "\n\nReact with ✅ to confirm or ❌ to cancel.",
        color=discord.Color.gold()
    )
    
    # ---------- PACK IMAGE ----------
    pack_img = PACK_IMAGES.get(tier)
    if pack_img:
        embed.set_image(url=pack_img)

    msg = await ctx.send(embed=embed)
    await msg.add_reaction("✅")
    await msg.add_reaction("❌")

    def check(reaction, user):
        return (
            user == ctx.author
            and reaction.message.id == msg.id
            and str(reaction.emoji) in ("✅", "❌")
        )

    try:
        reaction, _ = await bot.wait_for("reaction_add", timeout=30, check=check)
    except asyncio.TimeoutError:
        await ctx.send("⏱️ Purchase timed out.")
        return

    if str(reaction.emoji) == "❌":
        await ctx.send("❌ Purchase cancelled.")
        return

    # ---------- DEDUCT COST ----------
    deduct_pack_cost(ctx.author.id, tier)

    # ---------- OPEN PACK ----------
    if tier not in PACK_ODDS:
        await ctx.send("❌ Invalid pack tier.")
        return

    odds = PACK_ODDS[tier]
    cards = []

    # Roll filler
    for _ in range(4):
        rarity = roll_from_odds(odds["filler"])
        card = spawn_card_for_user(ctx.author.id, rarity)
        if card:
            cards.append(card)

    # Roll pog
    for _ in range(1):
        rarity = roll_from_odds(odds["pog"])
        card = spawn_card_for_user(ctx.author.id, rarity)
        if card:
            cards.append(card)

    if not cards:
        await ctx.send("❌ Pack failed to open.")
        return

    index = 0
    total = len(cards)

    pack_msg = await ctx.send(
        embed=make_pack_card_embed(cards[index], index, total)
    )

    if total == 1:
        return

    await pack_msg.add_reaction("⬅️")
    await pack_msg.add_reaction("➡️")
    await pack_msg.add_reaction("⏹️")

    def pack_check(reaction, user):
        return (
            user == ctx.author
            and reaction.message.id == pack_msg.id
            and str(reaction.emoji) in ("⬅️", "➡️", "⏹️")
        )

    while True:
        try:
            reaction, user = await bot.wait_for(
                "reaction_add",
                timeout=60,
                check=pack_check
            )
        except asyncio.TimeoutError:
            break

        emoji = str(reaction.emoji)

        if emoji == "⏹️":
            break
        elif emoji == "➡️":
            index = (index + 1) % total
        elif emoji == "⬅️":
            index = (index - 1) % total

        await pack_msg.edit(
            embed=make_pack_card_embed(cards[index], index, total)
        )

        try:
            await pack_msg.remove_reaction(reaction.emoji, user)
        except discord.Forbidden:
            pass

    try:
        await pack_msg.clear_reactions()
    except discord.Forbidden:
        pass

@bot.command()
async def shop(ctx):
    embed = discord.Embed(
        title="🛒 Shop",
        description=(
            "View item prices and rates.\n"
            "**Each pack contains 5 cards** (4 filler + 1 Chase card)."
        ),
        color=discord.Color.gold()
    )
    
    DIVIDER = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    def cost_line(cost: dict) -> str:
        parts = []
        if "gold" in cost:
            parts.append(f"{cost['gold']:,} Gold")
        if "shards" in cost:
            for rarity, amt in cost["shards"].items():
                parts.append(f"{amt} {rarity} Shards")
        if "legendary_token" in cost:
            parts.append(f"{cost['legendary_token']} Legendary Tokens")
        if "mythical_token" in cost:
            parts.append(f"{cost['mythical_token']} Mythical Token")
        return " + ".join(parts)

    def odds_text(odds):
        return "\n".join(f"• {r} — {p}%" for r, p in odds)

    for tier in ["C", "B", "A", "S", "S+"]:
        odds = PACK_ODDS[tier]
        cost = PACK_COSTS[tier]

        embed.add_field(
            name=f"📦 {tier} Pack",
            value=(
                f"`{cost_line(cost)}\n`"
                f"**Card Rates:**\n"
                f"{odds_text(odds['pog'])}\n\n"
                f"{DIVIDER}"
            ),
            inline=False
        )

    embed.set_footer(text="Use !buy <pack tier> to purchase a pack")

    await ctx.send(embed=embed)

# =========================
# TEAM MANAGEMENT COMMANDS
# =========================

@bot.command(aliases=["tank"])
async def tankadd(ctx, card_input: str):
    """Add a tank card to your team"""
    await add_to_team(ctx, card_input, "tank", "Tank")

@bot.command(aliases=["dps1"])
async def dps1add(ctx, card_input: str):
    """Add a DPS card to slot 1"""
    await add_to_team(ctx, card_input, "dps1", "DPS")

@bot.command(aliases=["dps2"])
async def dps2add(ctx, card_input: str):
    """Add a DPS card to slot 2"""
    await add_to_team(ctx, card_input, "dps2", "DPS")

@bot.command(aliases=["support"])
async def supportadd(ctx, card_input: str):
    """Add a support card to your team"""
    await add_to_team(ctx, card_input, "support", "Support")

async def add_to_team(ctx, card_input: str, slot: str, required_role: str):
    """Helper function to add a card to team slot"""
    
    # Parse card ID (supports both numeric and alphanumeric codes)
    card_id = parse_card_id(card_input)
    if card_id is None:
        await ctx.send(f"❌ Invalid card ID format: `{card_input}`. Use alphanumeric codes (minimum 3 characters).")
        return
    
    # Check if card exists and user owns it
    claims_cur.execute("""
        SELECT id, role, user_id, name, rarity, power
        FROM claimed_cards
        WHERE id = ? AND active = 1
    """, (card_id,))
    
    card_data = claims_cur.fetchone()
    
    if not card_data:
        card_code = int_to_code(card_id) if card_id else card_input
        await ctx.send(f"❌ No active card found with ID `{card_code}`.")
        return
    
    card_db_id, role, owner_id, name, rarity, power = card_data
    
    # Check ownership
    if owner_id != ctx.author.id:
        await ctx.send("❌ You can only add cards you own to your team.")
        return
    
    # Check role compatibility
    if role != required_role:
        await ctx.send(f"❌ This card is a **{role}**, but the **{slot}** slot requires a **{required_role}**.")
        return
    
    # Get current team (global, not per-server)
    team = get_or_create_team(ctx.author.id)
    
    if not team:
        team = (None, None, None, None)
    
    # Check if card is already in another slot
    if card_id in team:
        await ctx.send(f"❌ This card is already in your team!")
        return
    
    # Determine column name
    column_map = {
        "tank": "tank_card_id",
        "dps1": "dps1_card_id", 
        "dps2": "dps2_card_id",
        "support": "support_card_id"
    }
    
    column = column_map.get(slot)
    if not column:
        await ctx.send("❌ Invalid slot.")
        return
    
    # Update the team
    claims_cur.execute(f"""
        UPDATE teams
        SET {column} = ?
        WHERE user_id = ?
    """, (card_id, ctx.author.id))
    claims_conn.commit()
    
    # Get emoji based on rarity
    emoji = RARITY_EMOJI.get(rarity.upper(), "✨")
    
    await ctx.send(f"✅ Added **{name}** {emoji} (Power: {float(power):.1f}) to your **{slot}** slot!")

@bot.command()
async def tankremove(ctx):
    """Remove tank from team"""
    await remove_from_team(ctx, "tank")

@bot.command()
async def dps1remove(ctx):
    """Remove DPS1 from team"""
    await remove_from_team(ctx, "dps1")

@bot.command()
async def dps2remove(ctx):
    """Remove DPS2 from team"""
    await remove_from_team(ctx, "dps2")

@bot.command()
async def supportremove(ctx):
    """Remove support from team"""
    await remove_from_team(ctx, "support")

async def remove_from_team(ctx, slot: str):
    """Helper function to remove card from team slot"""
    
    column_map = {
        "tank": "tank_card_id",
        "dps1": "dps1_card_id",
        "dps2": "dps2_card_id", 
        "support": "support_card_id"
    }
    
    column = column_map.get(slot)
    if not column:
        await ctx.send("❌ Invalid slot.")
        return
    
    # Get the card being removed (global team)
    claims_cur.execute(f"""
        SELECT {column}
        FROM teams
        WHERE user_id = ?
    """, (ctx.author.id,))
    
    result = claims_cur.fetchone()
    
    if not result or not result[0]:
        await ctx.send(f"❌ You don't have a card in the **{slot}** slot.")
        return
    
    card_id = result[0]
    
    # Get card details for message
    claims_cur.execute("""
        SELECT name, rarity, power
        FROM claimed_cards
        WHERE id = ?
    """, (card_id,))
    
    card_data = claims_cur.fetchone()
    card_name = card_data[0] if card_data else "Unknown"
    card_rarity = card_data[1] if card_data else "F"
    card_power = float(card_data[2]) if card_data else 0.0
    
    # Remove from team
    claims_cur.execute(f"""
        UPDATE teams
        SET {column} = NULL
        WHERE user_id = ?
    """, (ctx.author.id,))
    claims_conn.commit()
    
    emoji = RARITY_EMOJI.get(card_rarity.upper(), "✨")
    
    await ctx.send(f"🗑️ Removed **{card_name}** {emoji} (Power: {card_power:.1f}) from your **{slot}** slot!")

@bot.command(aliases=["team"])
async def teamview(ctx, user: discord.Member = None):
    """View your or another user's team"""
    
    target = user or ctx.author
    
    # Get team (global, not per-server)
    claims_cur.execute("""
        SELECT tank_card_id, dps1_card_id, dps2_card_id, support_card_id
        FROM teams
        WHERE user_id = ?
    """, (target.id,))
    
    team = claims_cur.fetchone()
    
    if not team or all(slot is None for slot in team):
        if target == ctx.author:
            await ctx.send("📭 You don't have a team set up yet!")
        else:
            await ctx.send(f"📭 **{target.display_name}** doesn't have a team set up yet!")
        return
    
    tank_id, dps1_id, dps2_id, support_id = team
    
    # Calculate team stats FIRST
    total_power = 0
    active_cards = [tank_id, dps1_id, dps2_id, support_id]
    active_cards = [cid for cid in active_cards if cid is not None]
    card_count = len(active_cards)
    
    if active_cards:
        placeholders = ",".join("?" for _ in active_cards)
        claims_cur.execute(f"""
            SELECT SUM(power), COUNT(*)
            FROM claimed_cards
            WHERE id IN ({placeholders})
        """, active_cards)
        
        result = claims_cur.fetchone()
        total_power = float(result[0]) if result and result[0] else 0
        card_count = result[1] if result else 0
    
    # Create embed with power in title
    embed = discord.Embed(
        title=f"👥 {target.display_name}'s Team | ⚔️ Power: {total_power:.1f} | 🃏 Cards: {card_count}/4",
        color=discord.Color.blue()
    )
    
    # Function to get card details for display
    def get_card_display(card_id, slot_name):
        if not card_id:
            return f"❌ **{slot_name}:** Empty"
        
        claims_cur.execute("""
            SELECT name, rarity, role, power, hp, atk, def, luck
            FROM claimed_cards
            WHERE id = ? AND active = 1
        """, (card_id,))
        
        card = claims_cur.fetchone()
        if not card:
            return f"❌ **{slot_name}:** Card not found"
        
        name, rarity, role, power, hp, atk, defense, luck = card
        emoji = RARITY_EMOJI.get(rarity.upper(), "✨")
        role_emoji = ROLE_EMOJI.get(role, "🎭")
        
        return (
            f"{role_emoji} **{slot_name}:** {emoji} **{name}**\n"
            f"   ⚔️ Power: **{float(power):.1f}** | "
            f"💗 {hp} | ⚔️ {atk} | 🛡️ {defense} | 🍀 {float(luck):.2f}\n"
            f"   🆔 ID: `{int_to_code(card_id)}`"
        )
    
    # Add each slot to embed
    if tank_id:
        embed.add_field(
            name="🧱 Tank",
            value=get_card_display(tank_id, "Tank"),
            inline=False
        )
    else:
        embed.add_field(
            name="🧱 Tank",
            value="❌ **Tank:** Empty",
            inline=False
        )
    
    embed.add_field(
        name="⚔️ DPS Slots",
        value=(
            f"{get_card_display(dps1_id, 'DPS 1')}\n\n"
            f"{get_card_display(dps2_id, 'DPS 2')}"
        ),
        inline=False
    )
    
    embed.add_field(
        name="🩹 Support",
        value=get_card_display(support_id, "Support"),
        inline=False
    )
    
    await ctx.send(embed=embed)

@bot.command()
async def teamclear(ctx):
    """Clear your entire team"""
    
    # Check if user has a team (global, not per-server)
    claims_cur.execute("""
        SELECT tank_card_id, dps1_card_id, dps2_card_id, support_card_id
        FROM teams
        WHERE user_id = ?
    """, (ctx.author.id,))
    
    team = claims_cur.fetchone()
    
    if not team or all(slot is None for slot in team):
        await ctx.send("❌ You don't have a team to clear!")
        return
    
    # Clear the team
    claims_cur.execute("""
        UPDATE teams
        SET tank_card_id = NULL,
            dps1_card_id = NULL,
            dps2_card_id = NULL,
            support_card_id = NULL
        WHERE user_id = ?
    """, (ctx.author.id,))
    claims_conn.commit()
    
    await ctx.send("🧹 Cleared your entire team!")

# Helper function that was referenced earlier (add this near your other helper functions)
# =========================
# QUEST CONFIG - DURATION BASED
# =========================
QUEST_CONFIG = {
    "Common": {
        "power_required": 200,
        "base_success_rate": 1.00,  # 100%
        "stat_bonus_cap": 0.00,     # No bonus needed - auto 100%
        "duration_minutes": 10,      # Takes 5 minutes to complete
        "shard_rewards": {"Common": (8, 20)},
        "gold_rewards": (100, 500),
        "token_rewards": {}
    },
    "Uncommon": {
        "power_required": 250,
        "base_success_rate": 1.00,  # 100%
        "stat_bonus_cap": 0.00,     # No bonus needed - auto 100%
        "duration_minutes": 20,     # Takes 10 minutes to complete
        "shard_rewards": {"Uncommon": (8, 20)},
        "gold_rewards": (250, 1000),
        "token_rewards": {}
    },
    "Rare": {
        "power_required": 280,
        "base_success_rate": 1.00,  # 100%
        "stat_bonus_cap": 0.00,     # No bonus needed - auto 100%
        "duration_minutes": 30,     # Takes 15 minutes to complete
        "shard_rewards": {"Rare": (8, 20)},
        "gold_rewards": (750, 2500),
        "token_rewards": {}
    },
    "Epic": {
        "power_required": 320,
        "base_success_rate": 0.80,  # 80%
        "stat_bonus_cap": 0.05,     # +5% per role, max +20% total
        "duration_minutes": 90,     # Takes 30 minutes to complete
        "shard_rewards": {"Epic": (8, 20)},
        "gold_rewards": (2000, 5000),
        "token_rewards": {}
    },
    "Legendary": {
        "power_required": 345,
        "base_success_rate": 0.65,  # 65%
        "stat_bonus_cap": 0.05,     # +5% per role, max +20% total
        "duration_minutes": 240,     # Takes 60 minutes to complete
        "gold_rewards": (4000, 10000),
        "token_rewards": {"Legendary": (1, 3)}  # 1-3 Legendary tokens
    },
    "Mythical": {
        "power_required": 375,
        "base_success_rate": 0.55,  # 55%
        "stat_bonus_cap": 0.05,     # +5% per role, max +20% total
        "duration_minutes": 480,    # Takes 120 minutes (2 hours) to complete
        "gold_rewards": (7500, 20000),
        "token_rewards": {"Mythical": (1, 1)}  # 1 Mythical token
    }
}

# Stat thresholds for bonus calculation
# Each role can contribute up to 5% bonus (max 20% total)
STAT_THRESHOLDS = {
    "Tank": {
        "hp_min": 5000, "hp_max": 10000,
        "def_min": 500, "def_max": 2500,
        "hp_weight": 0.6,  # HP contributes 60% to tank bonus
        "def_weight": 0.4   # DEF contributes 40% to tank bonus
    },
    "DPS": {
        "atk_min": 500, "atk_max": 2500,
        "atk_weight": 1.0   # Only ATK matters for DPS bonus
    },
    "Support": {
        "luck_min": 1.0, "luck_max": 10.0,
        "luck_weight": 1.0   # Only LUCK matters for support bonus
    }
}

# =========================
# QUEST SYSTEM - SIMPLE USER-BASED
# =========================

@bot.command(aliases=["quests"])
async def questboard(ctx):
    """View available quests and their requirements"""
    
    guild_id = ctx.guild.id if ctx.guild else 0
    
    # Get user's team power (global team)
    team_power = 0
    team = None
    
    try:
        claims_cur.execute("""
            SELECT tank_card_id, dps1_card_id, dps2_card_id, support_card_id
            FROM teams
            WHERE user_id = ?
        """, (ctx.author.id,))
        
        team = claims_cur.fetchone()
        
        if team:
            tank_id, dps1_id, dps2_id, support_id = team
            active_cards = [cid for cid in [tank_id, dps1_id, dps2_id, support_id] if cid is not None]
            
            if active_cards:
                placeholders = ",".join("?" for _ in active_cards)
                claims_cur.execute(f"""
                    SELECT SUM(power)
                    FROM claimed_cards
                    WHERE id IN ({placeholders})
                """, active_cards)
                
                result = claims_cur.fetchone()
                team_power = float(result[0]) if result and result[0] else 0
    except Exception as e:
        print(f"Error calculating team power: {e}")
        team_power = 0
    
    # Check if user has an active quest
    quest_active = False
    quest_data = None
    
    try:
        claims_cur.execute("""
            SELECT tier, start_time, end_time, completed, collected
            FROM user_quests
            WHERE user_id = ? AND guild_id = ?
            AND (completed = FALSE OR collected = FALSE)
        """, (ctx.author.id, guild_id))
        
        quest_data = claims_cur.fetchone()
        
        if quest_data:
            # Check if quest is still active (not completed OR completed but not collected)
            tier, start_str, end_str, completed, collected = quest_data
            quest_active = not completed or (completed and not collected)
    except Exception as e:
        print(f"Error checking quest status: {e}")
        quest_active = False
    
    embed = discord.Embed(
        title="📜 Quest Board",
        color=discord.Color.dark_gold()
    )
    
    if quest_active and quest_data:
        tier, start_str, end_str, completed, collected = quest_data
        
        try:
            end_time = datetime.fromisoformat(end_str)
            now = datetime.now(timezone.utc)
            
            if completed or now >= end_time:
                time_left = "**READY TO COLLECT**"
            else:
                remaining = end_time - now
                hours = int(remaining.total_seconds() // 3600)
                minutes = int((remaining.total_seconds() % 3600) // 60)
                time_left = f"{hours}h {minutes}m" if hours > 0 else f"{minutes}m"
            
            embed.description = (
                f"⚔️ **Your Team Power:** {team_power:.1f}\n"
                f"📋 **Current Quest:** {tier}\n"
                f"⏰ **Time Remaining:** {time_left}\n\n"
                f"Use `!queststatus` for details!"
            )
        except Exception as e:
            embed.description = (
                f"⚔️ **Your Team Power:** {team_power:.1f}\n"
                f"📋 **Current Quest:** {tier}\n\n"
                f"Use `!queststatus` for details!"
            )
    else:
        embed.description = (
            f"⚔️ **Your Team Power:** {team_power:.1f}\n\n"
            f"Select a quest with `!quest <tier>`"
        )
    
    # Show all quest tiers
    for tier, config in QUEST_CONFIG.items():
        power_req = config["power_required"]
        base_rate = config["base_success_rate"] * 100
        max_rate = (config["base_success_rate"] + (config["stat_bonus_cap"] * 4)) * 100
        duration = config["duration_minutes"]
        
        can_attempt = team_power >= power_req and not quest_active
        status = "✅" if can_attempt else "❌"
        
        # Build rewards text
        rewards_text = []
        if "shard_rewards" in config:
            for shard_tier, (min_shards, max_shards) in config["shard_rewards"].items():
                rewards_text.append(f"{shard_tier} Shards")
        
        if "token_rewards" in config and config["token_rewards"]:
            for token_type, (min_tokens, max_tokens) in config["token_rewards"].items():
                rewards_text.append(f"{token_type} Token{'s' if max_tokens > 1 else ''}")
        
        rewards_text.append("Gold")
        
        embed.add_field(
            name=f"{status} {tier} Quest",
            value=(
                f"**Power:** {power_req:,}+\n"
                f"**Time:** {duration} min\n"
                f"**Success:** {base_rate:.0f}% → **{max_rate:.0f}%**\n"
                f"**Rewards:** {', '.join(rewards_text)}\n"
                f"`!quest {tier.lower()}`"
            ),
            inline=True
        )
    
    await ctx.send(embed=embed)

@bot.command()
async def quest(ctx, tier: str):
    """Start a quest of specified tier (1 quest per user)"""
    
    tier = tier.capitalize()
    if tier not in QUEST_CONFIG:
        await ctx.send(f"❌ Invalid quest tier. Available: {', '.join(QUEST_CONFIG.keys())}")
        return
    
    config = QUEST_CONFIG[tier]
    guild_id = ctx.guild.id if ctx.guild else 0
    
    # DELETE OLD COLLECTED QUESTS FIRST
    claims_cur.execute("""
        DELETE FROM user_quests
        WHERE user_id = ? AND guild_id = ? AND completed = TRUE AND collected = TRUE
    """, (ctx.author.id, guild_id))
    claims_conn.commit()
    
    # Check if user already has an ACTIVE quest (not completed or not collected)
    claims_cur.execute("""
        SELECT tier, completed, collected
        FROM user_quests
        WHERE user_id = ? AND guild_id = ?
        AND (completed = FALSE OR collected = FALSE)
    """, (ctx.author.id, guild_id))
    
    existing_quest = claims_cur.fetchone()
    
    if existing_quest:
        existing_tier, completed, collected = existing_quest
        if not completed:
            await ctx.send(f"❌ You already have an active {existing_tier} quest! Use `!queststatus` to check it.")
            return
        
    # Check if user has a complete team (global team)
    team = get_or_create_team(ctx.author.id)
    
    if not team or all(slot is None for slot in team):
        await ctx.send("❌ You need a complete team to start quests! Use `!teamview` to see your team.")
        return
    
    tank_id, dps1_id, dps2_id, support_id = team
    
    # Check if team is complete
    if None in [tank_id, dps1_id, dps2_id, support_id]:
        await ctx.send("❌ Your team must be complete (Tank + 2 DPS + Support) to start quests!")
        return
    
    # Calculate team power and success rate
    team_power = 0
    card_stats = {}
    card_ids = []
    
    for slot_name, card_id in [("Tank", tank_id), ("DPS1", dps1_id), ("DPS2", dps2_id), ("Support", support_id)]:
        if card_id:
            claims_cur.execute("""
                SELECT power, role, hp, atk, def, luck, name, rarity
                FROM claimed_cards
                WHERE id = ? AND active = 1
            """, (card_id,))
            
            card = claims_cur.fetchone()
            if card:
                power, role, hp, atk, defense, luck, name, rarity = card
                team_power += float(power)
                card_ids.append(card_id)
                card_stats[slot_name] = {
                    "id": card_id,
                    "name": name,
                    "rarity": rarity,
                    "role": role,
                    "hp": hp,
                    "atk": atk,
                    "def": defense,
                    "luck": float(luck),
                    "power": float(power)
                }
    
    # Check power requirement
    if team_power < config["power_required"]:
        await ctx.send(f"❌ Your team power ({team_power:.1f}) is too low for {tier} quests! Required: {config['power_required']}")
        return
    
    # Calculate success rate with stat bonuses
    role_bonuses = {}
    total_bonus = 0
    
    # Tank bonus
    if "Tank" in card_stats:
        tank = card_stats["Tank"]
        tank_hp = tank["hp"]
        tank_def = tank["def"]
        
        hp_norm = min(1.0, max(0.0, (tank_hp - STAT_THRESHOLDS["Tank"]["hp_min"]) / 
                               (STAT_THRESHOLDS["Tank"]["hp_max"] - STAT_THRESHOLDS["Tank"]["hp_min"])))
        def_norm = min(1.0, max(0.0, (tank_def - STAT_THRESHOLDS["Tank"]["def_min"]) / 
                                (STAT_THRESHOLDS["Tank"]["def_max"] - STAT_THRESHOLDS["Tank"]["def_min"])))
        
        tank_bonus = (hp_norm * STAT_THRESHOLDS["Tank"]["hp_weight"] + 
                     def_norm * STAT_THRESHOLDS["Tank"]["def_weight"])
        role_bonuses["Tank"] = tank_bonus * config["stat_bonus_cap"]
        total_bonus += role_bonuses["Tank"]
    
    # DPS bonuses
    for slot in ["DPS1", "DPS2"]:
        if slot in card_stats:
            dps_atk = card_stats[slot]["atk"]
            atk_norm = min(1.0, max(0.0, (dps_atk - STAT_THRESHOLDS["DPS"]["atk_min"]) / 
                                   (STAT_THRESHOLDS["DPS"]["atk_max"] - STAT_THRESHOLDS["DPS"]["atk_min"])))
            slot_bonus = atk_norm * config["stat_bonus_cap"]
            role_bonuses[slot] = slot_bonus
            total_bonus += slot_bonus
    
    # Support bonus
    if "Support" in card_stats:
        support_luck = card_stats["Support"]["luck"]
        luck_norm = min(1.0, max(0.0, (support_luck - STAT_THRESHOLDS["Support"]["luck_min"]) / 
                               (STAT_THRESHOLDS["Support"]["luck_max"] - STAT_THRESHOLDS["Support"]["luck_min"])))
        support_bonus = luck_norm * config["stat_bonus_cap"]
        role_bonuses["Support"] = support_bonus
        total_bonus += support_bonus
    
    # Calculate final success rate with caps
    success_rate = config["base_success_rate"] + total_bonus
    
    if tier == "Epic":
        success_rate = min(1.00, success_rate)
    elif tier == "Legendary":
        success_rate = min(0.85, success_rate)
    elif tier == "Mythical":
        success_rate = min(0.75, success_rate)
    else:
        success_rate = 1.00  # Common, Uncommon, Rare are always 100%
    
    # Set quest timing
    start_time = datetime.now(timezone.utc)
    end_time = start_time + timedelta(minutes=config["duration_minutes"])
    
    # Store the quest (overwrites any existing)
    card_ids_json = json.dumps(card_ids)
    
    claims_cur.execute("""
        INSERT OR REPLACE INTO user_quests (
            user_id, guild_id, tier, team_power, success_rate, 
            start_time, end_time, completed, collected, card_ids
        ) VALUES (?, ?, ?, ?, ?, ?, ?, FALSE, FALSE, ?)
    """, (
        ctx.author.id, guild_id, tier, team_power, success_rate,
        start_time.isoformat(), end_time.isoformat(), card_ids_json
    ))
    claims_conn.commit()
    
    # Create embed showing quest started
    embed = discord.Embed(
        title=f"🚀 {tier} Quest Started!",
        color=discord.Color.green()
    )
    
    embed.add_field(
        name="📊 Quest Details",
        value=(
            f"**Team Power:** {team_power:.1f} ⚔️\n"
            f"**Success Chance:** {success_rate*100:.1f}%\n"
            f"**Duration:** {config['duration_minutes']} minutes\n"
            f"**Completion:** <t:{int(end_time.timestamp())}:R>"
        ),
        inline=False
    )
    
    # Show team members
    team_details = []
    for slot_name in ["Tank", "DPS1", "DPS2", "Support"]:
        if slot_name in card_stats:
            card = card_stats[slot_name]
            emoji = RARITY_EMOJI.get(card["rarity"].upper(), "✨")
            role_emoji = ROLE_EMOJI.get(card["role"], "🎭")
            bonus = role_bonuses.get(slot_name, 0) * 100
            
            if slot_name.startswith("DPS"):
                slot_display = f"{slot_name.replace('DPS', 'DPS ')}"
            else:
                slot_display = slot_name
            
            team_details.append(
                f"{role_emoji} **{slot_display}:** {emoji} {card['name']} "
                f"(+{bonus:.1f}% bonus)"
            )
    
    embed.add_field(
        name="👥 Team Members",
        value="\n".join(team_details),
        inline=False
    )
    
    embed.add_field(
        name="📋 Commands",
        value=(
            "`!queststatus` - Check your quest progress\n"
            "`!cancelquest` - Cancel your current quest"
        ),
        inline=False
    )
    
    await ctx.send(embed=embed)

@bot.command()
async def queststatus(ctx, user: discord.Member = None):
    """Check your or another user's quest status"""
    
    target = user or ctx.author
    guild_id = ctx.guild.id if ctx.guild else 0
    now = datetime.now(timezone.utc)
    
    # Get ONLY active quests (not completed or not collected)
    claims_cur.execute("""
        SELECT tier, team_power, success_rate, start_time, end_time, 
               completed, collected, card_ids
        FROM user_quests
        WHERE user_id = ? AND guild_id = ? 
        AND (completed = FALSE OR collected = FALSE)
    """, (target.id, guild_id))
    
    quest_data = claims_cur.fetchone()
    
    embed = discord.Embed(
        title=f"⏰ Quest Status - {target.display_name}",
        color=discord.Color.blue()
    )
    
    if not quest_data:
        embed.description = "📭 No active quest.\nUse `!questboard` to start one!"
    else:
        (tier, team_power, success_rate, start_str, end_str, 
         completed, collected, card_ids_json) = quest_data
        
        end_time = datetime.fromisoformat(end_str)
        remaining = end_time - now
        
        if completed:
            if collected:
                status = "✅ **COLLECTED**"
                time_info = "Rewards have been collected"
            else:
                status = "🎁 **READY TO COLLECT**"
                time_info = "Quest completed - use `!collectquest`"
        else:
            if remaining.total_seconds() <= 0:
                status = "⏰ **READY TO COLLECT**"
                time_info = "Quest finished - use `!collectquest`"
            else:
                status = "⏳ **IN PROGRESS**"
                hours = int(remaining.total_seconds() // 3600)
                minutes = int((remaining.total_seconds() % 3600) // 60)
                seconds = int(remaining.total_seconds() % 60)
                time_info = f"Time remaining: **{hours}h {minutes}m {seconds}s**"
        
        embed.add_field(
            name=f"{tier} Quest - {status}",
            value=(
                f"**Team Power:** {team_power:.1f} ⚔️\n"
                f"**Success Chance:** {success_rate*100:.1f}%\n"
                f"**Started:** <t:{int(datetime.fromisoformat(start_str).timestamp())}:R>\n"
                f"**{time_info}**"
            ),
            inline=False
        )
        
        if not completed and remaining.total_seconds() > 0:
            embed.set_footer(text=f"Will complete at: {end_time.strftime('%H:%M:%S')} UTC")
    
    await ctx.send(embed=embed)

@bot.command()
async def collectquest(ctx):
    """Collect rewards from a completed quest"""
    try:
        user_id = ctx.author.id
        guild_id = ctx.guild.id if ctx.guild else 0
        now = datetime.now(timezone.utc)
        
        # Get active quest (not collected yet)
        claims_cur.execute("""
            SELECT id, tier, success_rate, end_time, completed, collected
            FROM user_quests
            WHERE user_id = ? AND guild_id = ?
            AND collected = FALSE
        """, (user_id, guild_id))
        
        quest = claims_cur.fetchone()
        
        if not quest:
            await ctx.send("❌ You don't have any quest ready to collect!")
            return
        
        quest_id, tier, success_rate, end_str, completed, collected = quest
        
        # Debug: print quest info
        print(f"DEBUG collectquest: Quest ID={quest_id}, Tier={repr(tier)}, Success Rate={success_rate}, Completed={completed}, Collected={collected}")
        
        # Parse end_time - handle both string and datetime formats
        if isinstance(end_str, str):
            end_time = datetime.fromisoformat(end_str)
        else:
            end_time = end_str
        
        # Convert completed/collected to boolean if they're stored as integers
        completed = bool(completed) if isinstance(completed, int) else completed
        collected = bool(collected) if isinstance(collected, int) else collected
        
        # Validate quest tier
        if tier not in QUEST_CONFIG:
            await ctx.send(f"❌ Invalid quest tier: {tier}. Please contact an administrator.")
            return
        
        # Check if quest is finished
        if not completed and now < end_time:
            remaining = end_time - now
            minutes = int(remaining.total_seconds() // 60)
            seconds = int(remaining.total_seconds() % 60)
            await ctx.send(f"⏰ Quest is still in progress! Wait {minutes}m {seconds}s more.")
            return
        
        # Get quest config
        config = QUEST_CONFIG[tier]
        print(f"DEBUG collectquest: Using config for tier '{tier}': gold_rewards={config.get('gold_rewards')}, shard_rewards={config.get('shard_rewards')}")
        success = False
        rewards_given = False
        
        # Roll for success if not already completed
        if not completed:
            success = random.random() <= success_rate
            
            # Give rewards if successful
            if success:
                rewards_given = True
                # Gold
                if "gold_rewards" in config:
                    gold = random.randint(*config["gold_rewards"])
                    add_gold(user_id, gold)
                
                # Shards
                if "shard_rewards" in config:
                    for rarity, (lo, hi) in config["shard_rewards"].items():
                        amt = random.randint(lo, hi)
                        # Debug: print what we're trying to add
                        print(f"DEBUG: Attempting to add {amt} shards of rarity '{rarity}' (type: {type(rarity)})")
                        try:
                            add_shards(user_id, rarity, amt)
                        except ValueError as e:
                            # Re-raise with more context
                            raise ValueError(f"Error adding {amt} {rarity} shards for user {user_id}: {str(e)}") from e
                
                # Tokens
                if "token_rewards" in config:
                    for token, (lo, hi) in config["token_rewards"].items():
                        amt = random.randint(lo, hi)
                        if amt > 0:
                            add_tokens(user_id, token, amt)
        else:
            # Quest already completed - assume success and give rewards
            # (in case rewards weren't given when quest was completed)
            success = True
            rewards_given = True
            
            # Give rewards
            # Gold
            if "gold_rewards" in config:
                gold = random.randint(*config["gold_rewards"])
                add_gold(user_id, gold)
            
            # Shards
            if "shard_rewards" in config:
                for rarity, (lo, hi) in config["shard_rewards"].items():
                    amt = random.randint(lo, hi)
                    # Debug: print what we're trying to add
                    print(f"DEBUG: Attempting to add {amt} shards of rarity '{rarity}' (type: {type(rarity)})")
                    try:
                        add_shards(user_id, rarity, amt)
                    except ValueError as e:
                        # Re-raise with more context
                        raise ValueError(f"Error adding {amt} {rarity} shards for user {user_id}: {str(e)}") from e
            
            # Tokens
            if "token_rewards" in config:
                for token, (lo, hi) in config["token_rewards"].items():
                    amt = random.randint(lo, hi)
                    if amt > 0:
                        add_tokens(user_id, token, amt)
        
        # Mark as collected
        claims_cur.execute("""
            UPDATE user_quests
            SET completed = TRUE,
                collected = TRUE
            WHERE id = ?
        """, (quest_id,))
        claims_conn.commit()
        
        # Send result message
        if success:
            await ctx.send(f"✅ {tier} quest completed successfully! Rewards have been added to your inventory.")
        else:
            await ctx.send(f"❌ {tier} quest failed! No rewards earned.")
    
    except Exception as e:
        await ctx.send(f"❌ An error occurred while collecting quest rewards: {str(e)}")
        import traceback
        print(f"Error in collectquest: {traceback.format_exc()}")

@bot.command()
@is_bot_owner()
async def completequest(ctx, member: discord.Member):
    """Force complete a quest for a user (Admin only)"""
    try:
        user_id = member.id
        guild_id = ctx.guild.id if ctx.guild else 0
        
        # Get active quest
        claims_cur.execute("""
            SELECT id, tier, success_rate, end_time, completed, collected
            FROM user_quests
            WHERE user_id = ? AND guild_id = ?
            AND collected = FALSE
        """, (user_id, guild_id))
        
        quest = claims_cur.fetchone()
        
        if not quest:
            await ctx.send(f"❌ {member.mention} doesn't have any active quest ready to collect!")
            return
        
        quest_id, tier, success_rate, end_str, completed, collected = quest
        
        # Validate quest tier
        if tier not in QUEST_CONFIG:
            await ctx.send(f"❌ Invalid quest tier: {tier}. Please contact a developer.")
            return
        
        # Get quest config
        config = QUEST_CONFIG[tier]
        
        # Force success and give rewards
        success = True
        
        # Gold
        gold_given = 0
        if "gold_rewards" in config:
            gold_given = random.randint(*config["gold_rewards"])
            add_gold(user_id, gold_given)
        
        # Shards
        shards_given = {}
        if "shard_rewards" in config:
            for rarity, (lo, hi) in config["shard_rewards"].items():
                amt = random.randint(lo, hi)
                add_shards(user_id, rarity, amt)
                shards_given[rarity] = amt
        
        # Tokens
        tokens_given = {}
        if "token_rewards" in config:
            for token, (lo, hi) in config["token_rewards"].items():
                amt = random.randint(lo, hi)
                if amt > 0:
                    add_tokens(user_id, token, amt)
                    tokens_given[token] = amt
        
        # Mark as collected
        claims_cur.execute("""
            UPDATE user_quests
            SET completed = TRUE,
                collected = TRUE
            WHERE id = ?
        """, (quest_id,))
        claims_conn.commit()
        
        # Build reward message
        reward_parts = []
        if gold_given > 0:
            reward_parts.append(f"💰 **{gold_given:,} Gold**")
        for rarity, amt in shards_given.items():
            reward_parts.append(f"🧩 **{amt} {rarity} Shards**")
        for token, amt in tokens_given.items():
            reward_parts.append(f"🔮 **{amt} {token} Tokens**")
        
        rewards_text = "\n".join(reward_parts) if reward_parts else "No rewards"
        
        embed = discord.Embed(
            title=f"✅ Quest Force Completed",
            description=f"**{tier} Quest** for {member.mention} has been force completed!",
            color=discord.Color.green()
        )
        embed.add_field(
            name="Rewards Given",
            value=rewards_text,
            inline=False
        )
        
        await ctx.send(embed=embed)
    
    except Exception as e:
        await ctx.send(f"❌ An error occurred while force completing quest: {str(e)}")
        import traceback
        print(f"Error in completequest: {traceback.format_exc()}")

@bot.command()
async def cancelquest(ctx):
    """Cancel your current active quest"""
    guild_id = ctx.guild.id if ctx.guild else 0
    user_id = ctx.author.id
    
    claims_cur.execute("""
        DELETE FROM user_quests
        WHERE user_id = ? AND guild_id = ?
        AND completed = FALSE
    """, (user_id, guild_id))
    claims_conn.commit()
    
    if claims_cur.rowcount > 0:
        await ctx.send("✅ Quest cancelled successfully!")
    else:
        await ctx.send("❌ You don't have an active quest to cancel!")

init_databases()
migrate_claimed_cards_softwipe()
migrate_claimed_cards_stats()
migrate_card_tags()
migrate_card_tags_single()
migrate_user_shards()
load_guild_configs_into_memory()
migrate_user_gold()
migrate_user_daily()
migrate_user_favorite()
migrate_user_keys()
migrate_user_tokens()
migrate_trading()
migrate_teams()
migrate_quests()

run_bot()

