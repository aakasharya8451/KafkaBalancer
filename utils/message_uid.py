import hashlib
import random
import time


def generate_message():
    random_data = str(random.getrandbits(256)).encode('utf-8')
    hash_value = hashlib.sha256(random_data).hexdigest()
    return hash_value


def generate_user_id():
    unique_identifier = str(hash(time.time()))
    hash_object = hashlib.sha256(unique_identifier.encode())
    hex_dig = hash_object.hexdigest()
    user_id = hex_dig[:7]
    return user_id


if __name__ == "__main__":
    while True:
        print("User: ", generate_user_id(),
              "Random SHA-256 hash:", generate_message())
