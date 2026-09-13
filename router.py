import hashlib
import psycopg2
from psycopg2.extras import RealDictCursor

# 1. THE TOPOLOGY: Keys are aligned with get_target_shard ("shard_0" -> "shard_3")
# Use 127.0.0.1 to avoid Windows IPv6 (::1) routing issues
SHARDS = {
    "shard_0": {"host": "127.0.0.1", "port": 5432},
    "shard_1": {"host": "127.0.0.1", "port": 5433},
    "shard_2": {"host": "127.0.0.1", "port": 5434},
    "shard_3": {"host": "127.0.0.1", "port": 5435},
}

DB_USER = "admin"
DB_PASS = "admin@shardmaster"
DB_NAME = "shard_db"


# 2. CONNECTION HELPER: Open a raw TCP socket connection to a specific shard
def get_connection(shard_name: str):
    node = SHARDS[shard_name]
    conn = psycopg2.connect(
        host=node["host"],
        port=node["port"],
        user=DB_USER,
        password=DB_PASS,
        dbname=DB_NAME
    )
    return conn


# 3. SCHEMA INITIALIZATION: Ensure the 'users' table exists on ALL 4 shards
def init_cluster():
    print("[*] Initializing 'users' table on all 4 database shards...")
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS users (
        user_id VARCHAR(64) PRIMARY KEY,
        username VARCHAR(100) NOT NULL,
        email VARCHAR(255) NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );
    """
    for shard_name in SHARDS.keys():
        conn = get_connection(shard_name)
        with conn.cursor() as cur:
            cur.execute(create_table_sql)
        conn.commit()
        conn.close()
        print(f"  ✔ Initialized {shard_name}")


# 4. THE ROUTING HASH FUNCTION
def get_target_shard(user_id: str) -> str:
    # Hash the string user_id using MD5 -> convert to large integer
    hash_digest = hashlib.md5(user_id.encode("utf-8")).hexdigest()
    hash_int = int(hash_digest, 16)
    
    # Calculate shard index (0, 1, 2, or 3)
    shard_index = hash_int % len(SHARDS)
    return f"shard_{shard_index}"


# 5. WRITE PATH: INSERT a user into the correct shard
def insert_user(user_id: str, username: str, email: str):
    target_shard = get_target_shard(user_id)
    
    conn = get_connection(target_shard)
    with conn.cursor() as cur:
        # Idempotent write: if user exists, update username and email
        insert_sql = """
        INSERT INTO users (user_id, username, email)
        VALUES (%s, %s, %s)
        ON CONFLICT (user_id) 
        DO UPDATE SET 
            username = EXCLUDED.username,
            email = EXCLUDED.email,
            updated_at = CURRENT_TIMESTAMP;
        """
        cur.execute(insert_sql, (user_id, username, email))
    conn.commit()
    conn.close()
    print(f"-> [INSERT] User '{user_id}' routed and saved to {target_shard}")


# 6. READ PATH: SELECT a user from the correct shard
def get_user(user_id: str):
    target_shard = get_target_shard(user_id)
    
    conn = get_connection(target_shard)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM users WHERE user_id = %s;", (user_id,))
        user = cur.fetchone()
    conn.close()
    
    if user:
        print(f"<- [READ] Found '{user_id}' on {target_shard}: {user['email']}")
        return user
    else:
        print(f"<- [READ] User '{user_id}' not found on {target_shard}")
        return None


# 7. INSPECTION: Check row counts directly on each physical PostgreSQL shard
def print_cluster_status():
    print("\n================ CLUSTER DATA AUDIT ================")
    total_rows = 0
    for shard_name in SHARDS.keys():
        conn = get_connection(shard_name)
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM users;")
            count = cur.fetchone()[0]
            total_rows += count
            print(f"  {shard_name} (Port {SHARDS[shard_name]['port']}): {count} rows")
        conn.close()
    print(f"Total Rows Distributed Across Cluster: {total_rows}")
    print("====================================================\n")


# --- MAIN TEST EXECUTION ---
if __name__ == "__main__":
    # 1. Create the tables across all nodes
    init_cluster()

    # 2. Insert a batch of 20 users
    print("\n[*] Inserting 20 test users through the routing engine...")
    for i in range(1, 21):
        user_id = f"user_{i}"
        insert_user(user_id, f"User_{i}", f"user_{i}@example.com")

    # 3. Inspect where they actually landed on disk
    print_cluster_status()

    # 4. Perform targeted point lookups
    print("[*] Performing targeted read queries...")
    get_user("user_5")
    get_user("user_12")
    get_user("user_999")  # Doesn't exist