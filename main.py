import hashlib

databases = {
    "shard_0": {},
    "shard_1": {},
    "shard_2": {},
    "shard_3": {}
}

def get_shard(user_id: str) -> str:
    hash_object = hashlib.md5(user_id.encode())
    big_number = int(hash_object.hexdigest(), 16)
    
    shard_number = big_number % 4
    
    return f"shard_{shard_number}"

def save_user(user_id: str, email: str):
    target_shard = get_shard(user_id)
    databases[target_shard][user_id] = email
    print(f"-> Saved {user_id} into {target_shard}")

def get_user(user_id: str):
    target_shard = get_shard(user_id)
    return databases[target_shard].get(user_id, "Not Found")


save_user("Nimit", "N@gmail.com")
save_user("Bimit", "b@gmail.com")
save_user("Cimit", "c@gmail.com")
save_user("Dimit", "d@gmail.com")

print("\nPrinting shards")
for shard_name, contents in databases.items():
    print(f"{shard_name}: {contents}")