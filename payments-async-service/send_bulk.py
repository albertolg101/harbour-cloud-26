import csv
import json
import sys
import os
import time
from urllib import request

API = os.getenv("API_URL", "http://127.0.0.1:8088")


def main():
    csv_file = sys.argv[1] if len(sys.argv) > 1 else "../payments.csv"
    store_id = sys.argv[2] if len(sys.argv) > 2 else "store-async-1"

    payments = []
    with open(csv_file, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            payments.append({
                "coffeeType": row["coffee_type"],
                "price": float(row["price"]),
                "currency": row["currency"],
                "loyaltyCardId": row["loyalty_card_id"],
            })

    body = json.dumps({"storeId": store_id, "payments": payments}).encode("utf-8")
    req = request.Request(f"{API}/requests", data=body,
                          headers={"Content-Type": "application/json"}, method="POST")
    with request.urlopen(req) as resp:
        created = json.loads(resp.read())

    request_id = created["requestId"]
    print(f"submitted {created['total']} payments")
    print(f"request id : {request_id}")
    print(f"shard split: {created['shardDistribution']}")

    while True:
        with request.urlopen(f"{API}/requests/{request_id}") as resp:
            status = json.loads(resp.read())
        print(f"  status={status['status']} done={status['done']} "
              f"pending={status['pending']} failed={status['failed']}")
        if status["status"] == "DONE":
            break
        time.sleep(1)
    print("finished.")


if __name__ == "__main__":
    main()
