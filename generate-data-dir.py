import argparse
from pathlib import Path
import random
import time
from hashlib import sha1
import json


def random_node_id():
    return random.randbytes(8).hex()


def random_task_name():
    id = random.randbytes(random.randint(1, 20)).hex()
    return f"task-{id}"


def random_filename():
    word = random.choice(["wow", "cool", "neat"])
    id = random.randint(0, 999)
    ext = random.choice(["", ".txt", ".jpg", ".png", ".flac", ".wav"])
    return f"{word}-{id}{ext}"


def random_version():
    return ".".join([str(random.randint(0, 20)) for _ in range(3)])


def random_data():
    return random.randbytes(random.randint(0, 256))


def random_labels():
    meta = {}
    for _ in range(random.randint(0, 3)):
        k = random.randbytes(random.randint(1, 10)).hex()
        v = random.randbytes(random.randint(1, 20)).hex()
        meta[k] = v
    return meta


def add_random_item(data_dir):
    timestamp = time.time_ns()
    data = random_data()
    shasum = sha1(data).hexdigest()

    meta = {
        "timestamp": timestamp,
        "shasum": shasum,
        "labels": random_labels()
    }

    meta["labels"]["filename"] = random_filename()

    dir = Path(data_dir, f"node-{random_node_id()}/uploads/{random_task_name()}/{random_version()}/{timestamp}-{shasum}")
    Path(dir, ".partial").mkdir(parents=True, exist_ok=True)
    
    Path(dir, ".partial", "data").write_bytes(data)
    Path(dir, ".partial", "meta").write_text(json.dumps(meta, separators=(",", ":")))

    Path(dir, ".partial", "data").rename(Path(dir, "data"))
    Path(dir, ".partial", "meta").rename(Path(dir, "meta"))
    Path(dir, ".partial").rmdir()



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default="test-data")
    parser.add_argument("numitems", type=int)
    args = parser.parse_args()

    for _ in range(args.numitems):
        add_random_item(args.data_dir)
        

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
