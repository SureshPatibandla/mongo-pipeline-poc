# batch/connect_atlas.py
# Minimal MongoDB Atlas connector that exports collections to local JSON files.
# I've installed 'pip install pymongo bson' and then replacd the credentials with place holders for safety purposes

import json
from pymongo import MongoClient
from bson.json_util import dumps

MONGO_URI = "mongodb+srv://<user>:<password>@atlas-poc-finance-metrics.xxxxxx.mongodb.net/"
DB = "acct_poc"

COLLECTIONS = ["accounts", "journal_entries", "close_tasks"]

def main():
    client = MongoClient(MONGO_URI, tls=True)
    db = client[DB]
    for coll in COLLECTIONS:
        docs = list(db[coll].find({}))
        with open(f"{coll}.json", "w") as f:
            f.write(dumps(docs))
        print(f"Exported {len(docs)} docs to {coll}.json")
    client.close()

if __name__ == "__main__":
    main()
