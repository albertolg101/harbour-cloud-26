import csv
import sys
import time
import uuid
import hashlib
import json
import urllib.request
import urllib.error

BASE_URL = "http://localhost:9091"
MAX_RETRIES = 5
INITIAL_BACKOFF = 1


def generate_idempotency_key(row):
    raw = f"{row['store_id']}|{row['coffee_type']}|{row['price']}|{row['currency']}|{row['loyalty_card_id']}"
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, raw))


def send_payment(row):
    url = f"{BASE_URL}/api/v1/payments"
    idempotency_key = generate_idempotency_key(row)

    body = json.dumps({
        "coffeeType": row["coffee_type"],
        "price": float(row["price"]),
        "currency": row["currency"],
        "loyaltyCardId": row["loyalty_card_id"]
    }).encode("utf-8")

    headers = {
        "Content-Type": "application/json",
        "Store-Id": row["store_id"],
        "Idempotency-Key": idempotency_key
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(url, data=body, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=10) as resp:
                status = resp.status
                response_body = json.loads(resp.read().decode("utf-8"))
                if status == 201:
                    print(f"[CREATED] {row['coffee_type']} {row['price']} {row['currency']} -> paymentId={response_body['paymentId']}")
                elif status == 200:
                    print(f"[DUPLICATE] {row['coffee_type']} {row['price']} {row['currency']} -> already registered paymentId={response_body['paymentId']}")
                return True

        except urllib.error.HTTPError as e:
            if e.code == 400:
                error_body = e.read().decode("utf-8")
                print(f"[REJECTED] {row['coffee_type']} {row['price']} {row['currency']} -> {error_body}")
                return False
            if e.code >= 500:
                backoff = INITIAL_BACKOFF * (2 ** (attempt - 1))
                print(f"[RETRY {attempt}/{MAX_RETRIES}] Server error {e.code}, retrying in {backoff}s...")
                time.sleep(backoff)
                continue
            print(f"[ERROR] HTTP {e.code}: {e.read().decode('utf-8')}")
            return False

        except (urllib.error.URLError, TimeoutError, OSError) as e:
            backoff = INITIAL_BACKOFF * (2 ** (attempt - 1))
            print(f"[RETRY {attempt}/{MAX_RETRIES}] Connection error: {e}, retrying in {backoff}s...")
            time.sleep(backoff)
            continue

    print(f"[FAILED] {row['coffee_type']} {row['price']} {row['currency']} -> exhausted all {MAX_RETRIES} retries")
    return False


def main():
    csv_file = sys.argv[1] if len(sys.argv) > 1 else "payments.csv"

    with open(csv_file, newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    print(f"Sending {len(rows)} payments from {csv_file} to {BASE_URL}")
    print("-" * 60)

    success = 0
    failed = 0

    for row in rows:
        if send_payment(row):
            success += 1
        else:
            failed += 1

    print("-" * 60)
    print(f"Done: {success} succeeded, {failed} failed out of {len(rows)} total")


if __name__ == "__main__":
    main()
