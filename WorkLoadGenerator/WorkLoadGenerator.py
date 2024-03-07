import hashlib
import random

def generate_random_hash():
    random_data = str(random.getrandbits(256)).encode('utf-8')
    hash_value = hashlib.sha256(random_data).hexdigest()
    return hash_value


def generate_work():
    return generate_random_hash()

if __name__ =="__main__":
    while True:
        print("Random SHA-256 hash:", generate_work())