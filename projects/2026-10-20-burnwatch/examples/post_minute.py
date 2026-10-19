"""Send one synthetic closed-minute report to the loopback collector, never print the key."""

import argparse
import json
import os
import sys
import urllib.error
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--minute", type=int, required=True)
    parser.add_argument("--total", type=int, default=1000)
    parser.add_argument("--bad", type=int, default=0)
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    key = os.environ.get("BURNWATCH_API_KEY")
    if not key:
        print(json.dumps({"error": "BURNWATCH_API_KEY is required"}), file=sys.stderr)
        return 1
    body = json.dumps({"batch_id": f"example-{args.minute}", "buckets": [
        {"minute": args.minute, "total": args.total, "bad": args.bad}], "watermark": args.minute + 60}).encode()
    request = urllib.request.Request(f"http://127.0.0.1:{args.port}/v1/buckets", body,
                                     {"Content-Type": "application/json", "Authorization": "Bearer " + key})
    # Never forward a local producer credential through an environment HTTP proxy.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(request, timeout=5) as response:
            print(response.read().decode())
        return 0
    except urllib.error.HTTPError as exc:
        print(exc.read().decode(), file=sys.stderr)
        return 1
    except (urllib.error.URLError, OSError, ValueError):
        print(json.dumps({"error": "collector request failed"}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
