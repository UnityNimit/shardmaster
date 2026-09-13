import hashlib

# 1. OUR 4 PHYSICAL DATABASES (Represented simply as 4 Python dictionaries)
databases = {
    "shard_0": {},
    "shard_1": {},
    "shard_2": {},
    "shard_3": {}
}

# 2. THE ROUTER (The math that picks the shard)
def get_shard(user_id: str) -> str:
    # Convert the user's name into a big number using an algorithm called MD5
    hash_object = hashlib.md5(user_id.encode())
    big_number = int(hash_object.hexdigest(), 16)
    
    # Use modulo (%) to get a number between 0 and 3
    shard_number = big_number % 4
    
    return f"shard_{shard_number}"

# 3. SAVING DATA (INSERT)
def save_user(user_id: str, email: str):
    target_shard = get_shard(user_id)
    databases[target_shard][user_id] = email
    print(f"-> Saved {user_id} into {target_shard}")

# 4. FETCHING DATA (SELECT)
def get_user(user_id: str):
    target_shard = get_shard(user_id)
    return databases[target_shard].get(user_id, "Not Found")


# --- LET'S TEST IT ---
save_user("alice", "alice@gmail.com")
save_user("bob", "bob@gmail.com")
save_user("charlie", "charlie@gmail.com")
save_user("david", "david@gmail.com")

print("\n--- WHAT IS INSIDE OUR SHARDS? ---")
for shard_name, contents in databases.items():
    print(f"{shard_name}: {contents}")