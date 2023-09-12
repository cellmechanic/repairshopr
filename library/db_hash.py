"""hashing functions for database"""
import hashlib
import json


def compute_hash(data):
    """Compute a hash for the data."""
    return hashlib.md5(json.dumps(data, sort_keys=True).encode()).hexdigest()
