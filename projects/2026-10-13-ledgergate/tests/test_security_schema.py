import hashlib
import hmac
import json
import unittest

from ledgergate.schema import MAX_PAYLOAD_BYTES, SchemaError, parse_event
from ledgergate.security import SignatureError, sign_payload, verify_signature

from tests.helpers import NOW, SECRET, make_payload, signed


class SignatureTests(unittest.TestCase):
    def test_valid_signature_and_multiple_rotation_secrets(self):
        raw = make_payload()
        self.assertEqual(verify_signature(raw, signed(raw), [b"old_secret_that_is_long_enough", SECRET], now=NOW), NOW)

    def test_matches_documented_hmac_construction(self):
        raw = b'{"hello":"world"}'
        header = sign_payload(raw, SECRET, NOW)
        expected = hmac.new(SECRET, str(NOW).encode() + b"." + raw, hashlib.sha256).hexdigest()
        self.assertEqual(header, f"t={NOW},v1={expected}")

    def test_rejects_tamper_wrong_secret_old_and_future_replay(self):
        raw = make_payload()
        cases = [
            (raw + b" ", signed(raw), NOW),
            (raw, signed(raw, b"another_secret_long_enough_123"), NOW),
            (raw, signed(raw, timestamp=NOW - 301), NOW),
            (raw, signed(raw, timestamp=NOW + 301), NOW),
        ]
        for payload, header, current in cases:
            with self.subTest(header=header), self.assertRaises(SignatureError):
                verify_signature(payload, header, [SECRET], now=current)

    def test_accepts_boundary_and_one_of_multiple_v1_values(self):
        raw = make_payload()
        valid = signed(raw).split("v1=", 1)[1]
        header = f"noise=x,t={NOW},v1={'0' * 64},v1={valid}"
        self.assertEqual(verify_signature(raw, header, [SECRET], now=NOW + 300), NOW)

    def test_rejects_malformed_headers(self):
        raw = make_payload()
        headers = ["", "v1=" + "0" * 64, f"t={NOW}", f"t={NOW},t={NOW},v1={'0'*64}", f"t=-1,v1={'0'*64}", f"t={NOW},v1=xyz", "x", "x" * 4097]
        for header in headers:
            with self.subTest(header=header[:20]), self.assertRaises(SignatureError):
                verify_signature(raw, header, [SECRET], now=NOW)

    def test_configuration_validation(self):
        raw = make_payload()
        for secrets in ([], [b"short"], [SECRET] * 9):
            with self.subTest(secrets=len(secrets)), self.assertRaises(ValueError):
                verify_signature(raw, signed(raw), secrets, now=NOW)
        with self.assertRaises(ValueError):
            verify_signature(raw, signed(raw), [SECRET], now=NOW, tolerance_seconds=0)


class SchemaTests(unittest.TestCase):
    def test_valid_event_and_effect_key(self):
        event = parse_event(make_payload())
        self.assertEqual(event.payment_id, "pi_test_0001")
        self.assertEqual(event.effect_key, "payment_intent.succeeded:pi_test_0001:v2")

    def test_rejects_invalid_json_and_oversize(self):
        for raw in (b"", b"not-json", b"[]", b"x" * (MAX_PAYLOAD_BYTES + 1)):
            with self.subTest(size=len(raw)), self.assertRaises(SchemaError):
                parse_event(raw)

    def test_rejects_duplicate_json_fields(self):
        raw = make_payload().replace(b'"created":1800000000', b'"created":1,"created":1800000000')
        with self.assertRaisesRegex(SchemaError, "duplicate JSON field"):
            parse_event(raw)

    def test_rejects_extra_fields_and_type_status_disagreement(self):
        data = json.loads(make_payload())
        data["extra"] = True
        with self.assertRaises(SchemaError):
            parse_event(json.dumps(data).encode())
        with self.assertRaises(SchemaError):
            parse_event(make_payload(status="failed"))

    def test_rejects_bad_identifiers_amount_version_currency_and_created(self):
        cases = [
            {"event_id": "bad"},
            {"payment_id": "bad"},
            {"version": True},
            {"version": 0},
            {"amount": -1},
            {"amount": True},
            {"currency": "USD"},
            {"created": -1},
        ]
        for values in cases:
            with self.subTest(values=values), self.assertRaises(SchemaError):
                parse_event(make_payload(**values))
