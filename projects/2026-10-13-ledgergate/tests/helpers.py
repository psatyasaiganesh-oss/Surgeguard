import json

from ledgergate.security import sign_payload


SECRET = b"test_webhook_secret_at_least_32b"
NOW = 1_800_000_000


def make_payload(
    event_id="evt_test_0001",
    event_type="payment_intent.succeeded",
    payment_id="pi_test_0001",
    version=2,
    status="succeeded",
    amount=1200,
    currency="usd",
    created=NOW,
):
    return json.dumps(
        {
            "id": event_id,
            "type": event_type,
            "created": created,
            "data": {"object": {"id": payment_id, "version": version, "status": status, "amount": amount, "currency": currency}},
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def signed(raw, secret=SECRET, timestamp=NOW):
    return sign_payload(raw, secret, timestamp)
