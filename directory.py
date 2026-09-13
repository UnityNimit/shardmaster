import zlib
import json
import os

TOTAL_BUCKETS = 1024
DIRECTORY_FILE = "shard_directory.json"

class ShardDirectory:
    def __init__(self):
        self.total_buckets = TOTAL_BUCKETS
        # The list of physical database nodes available
        self.nodes = {
            "shard_0": {"host": "127.0.0.1", "port": 5432},
            "shard_1": {"host": "127.0.0.1", "port": 5433},
            "shard_2": {"host": "127.0.0.1", "port": 5434},
            "shard_3": {"host": "127.0.0.1", "port": 5435},
        }
        # In-memory lookup map: bucket_id (int) -> shard_name (str)
        self.bucket_map = {}
        
        # Load from disk if exists, otherwise generate default distribution
        if os.path.exists(DIRECTORY_FILE):
            self.load_from_disk()
        else:
            self._generate_default_allocation()
            self.save_to_disk()

    def _generate_default_allocation(self):
        """Evenly assigns 1024 buckets across available physical shards."""
        shard_names = sorted(list(self.nodes.keys()))
        num_shards = len(shard_names)
        buckets_per_shard = self.total_buckets // num_shards

        for bucket_id in range(self.total_buckets):
            shard_idx = min(bucket_id // buckets_per_shard, num_shards - 1)
            self.bucket_map[bucket_id] = shard_names[shard_idx]
        
        print(f"[*] Generated default allocation: {buckets_per_shard} buckets per shard.")

    def get_bucket(self, user_id: str) -> int:
        """
        Deterministic hash function using CRC32 (faster than MD5, uniform distribution).
        Guarantees that user_id maps to the EXACT same bucket forever.
        """
        # zlib.crc32 returns an unsigned 32-bit integer (0 to 4,294,967,295)
        checksum = zlib.crc32(user_id.encode('utf-8'))
        return checksum % self.total_buckets

    def get_shard_for_user(self, user_id: str) -> tuple[str, dict]:
        """Returns (shard_name, connection_details) for a given user_id."""
        bucket_id = self.get_bucket(user_id)
        shard_name = self.bucket_map[bucket_id]
        return shard_name, self.nodes[shard_name]

    def update_bucket_assignment(self, bucket_id: int, new_shard: str):
        """Used during rebalancing to atomically point a bucket to a new physical shard."""
        if new_shard not in self.nodes:
            raise ValueError(f"Target shard {new_shard} does not exist in cluster topology.")
        self.bucket_map[bucket_id] = new_shard

    def save_to_disk(self):
        """Persists the routing table to disk."""
        data = {
            "nodes": self.nodes,
            # JSON keys must be strings
            "bucket_map": {str(k): v for k, v in self.bucket_map.items()}
        }
        with open(DIRECTORY_FILE, "w") as f:
            json.dump(data, f, indent=2)
        print(f"[✔] Shard directory saved to {DIRECTORY_FILE}")

    def load_from_disk(self):
        """Loads the routing table from disk."""
        with open(DIRECTORY_FILE, "r") as f:
            data = json.load(f)
        self.nodes = data["nodes"]
        self.bucket_map = {int(k): v for k, v in data["bucket_map"].items()}
        print(f"[✔] Loaded shard directory from {DIRECTORY_FILE}")


# Quick verification test
if __name__ == "__main__":
    directory = ShardDirectory()
    
    # Test bucket mappings
    test_users = ["alice", "bob", "charlie", "david", "user_100"]
    print("\n--- BUCKET TO SHARD MAPPING TEST ---")
    for user in test_users:
        bucket = directory.get_bucket(user)
        shard, conn = directory.get_shard_for_user(user)
        print(f"User: {user:<10} -> Bucket: {bucket:<4} -> Physical Node: {shard} (Port {conn['port']})")