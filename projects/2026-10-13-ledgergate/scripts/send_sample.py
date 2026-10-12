"""Send one locally signed synthetic success event to a LedgerGate server."""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request

from ledgergate import sign_payload


def main() -> int:
    if len(sys.argv) != 2 or not sys.argv[1].startswith("http://127.0.0.1:"):
        raise SystemExit("usage: python -m scripts.send_sample http://127.0.0.1:4242/webhook")
    secret_text = os.environ.get("WEBHOOK_SECRET", "")
    if len(secret_text.encode()) < 16:
        raise SystemExit("WEBHOOK_SECRET must contain at least 16 bytes")
    now = int(time.time())
    raw = json.dumps(
        {
            "id": f"evt_manual_{now}",
            "type": "payment_intent.succeeded",
            "created": now,
            "data": {
                "object": {
                    "id": f"pi_manual_{now}",
                    "version": 2,
                    "status": "succeeded",
                    "amount": 2500,
                    "currency": "usd",
                }
            },
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    request = urllib.request.Request(
        sys.argv[1],
        data=raw,
        headers={
            "Content-Type": "application/json",
            "Stripe-Signature": sign_payload(raw, secret_text.encode(), now),
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=3) as response:
            print(response.read().decode())
            return 0
    except urllib.error.URLError as exc:
        raise SystemExit(f"request failed: {exc.reason}") from exc


if __name__ == "__main__":
    raise SystemExit(main())
