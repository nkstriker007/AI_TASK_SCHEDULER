"""Live demo: one typed request through the running stack, with the task graph updating in place.

    make up-llm                                       # Redis + atsd + API + 3 Groq workers
    python3 scripts/demo.py                           # the default open-ended request
    python3 scripts/demo.py "Research AMD and Intel, compare them and write a short report"
    python3 scripts/demo.py --plan examples/plans/expand_canned.json   # hand-written plan, no planner

Standard library only. Shows the plan the planner returned, then each task's state and attempt
as atsd runs it (spliced tasks appear when the expand task commits), then the final report.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request

DEFAULT = "Research the top 3 AI chip companies and write a short report comparing them"
COLORS = {"COMPLETED": "32", "RUNNING": "36", "QUEUED": "34", "READY": "34", "RETRYING": "33", "FAILED": "31",
          "CANCELLED": "31", "WAITING": "90"}


def call(api: str, method: str, path: str, body=None):
    req = urllib.request.Request(api + path, method=method, data=json.dumps(body).encode() if body is not None else None,
                                 headers={"content-type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=180) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def paint(state: str, tty: bool) -> str:
    return f"\033[{COLORS.get(state, '0')}m{state:<10}\033[0m" if tty else f"{state:<10}"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("request", nargs="?", default=DEFAULT)
    ap.add_argument("--plan", help="submit this plan file's plan to POST /plans instead of planning")
    ap.add_argument("--api", default="http://localhost:8000")
    a = ap.parse_args()
    tty = sys.stdout.isatty()

    t0 = time.monotonic()
    if a.plan:
        doc = json.load(open(a.plan))
        print(f"submitting {a.plan} ...")
        code, body = call(a.api, "POST", "/plans", doc.get("plan", doc))
    else:
        print(f"request: {a.request}\nplanning with Groq ...")
        code, body = call(a.api, "POST", "/requests", {"text": a.request})
    if code != 202:
        print(f"HTTP {code}: {body}")
        return 1
    sub = json.loads(body)
    rid = sub["request_id"]
    print(f"\n{rid}: {sub['plan']['summary']}  (planned in {time.monotonic() - t0:.1f}s)")
    for t in sub["plan"]["tasks"]:
        deps = ", ".join(t["depends_on"]) or "-"
        print(f"  {t['id']:<4} {t['type']:<9} after {deps:<10} {t['title']}")
    print()

    lines = 0
    while True:
        st = json.loads(call(a.api, "GET", f"/requests/{rid}")[1])
        rows = [f"  {t['id']:<8} {paint(t['state'], tty)} attempt {t['attempt']}  {t['title'] or ''}"[:110]
                for t in st["tasks"]]
        header = f"{st['status']:<10} {time.monotonic() - t0:5.1f}s"
        if tty and lines:
            sys.stdout.write(f"\033[{lines}F\033[J")  # redraw in place
        print("\n".join([header, *rows]), flush=True)
        lines = len(rows) + 1
        if st["status"] in ("SUCCEEDED", "FAILED", "REJECTED"):
            break
        time.sleep(1)

    usage = st.get("usage") or {}
    print(f"\n{st['status']} in {time.monotonic() - t0:.1f}s; {usage.get('llm_calls', 0)} LLM calls, "
          f"{usage.get('tokens_in', 0)} tokens in, {usage.get('tokens_out', 0)} out")
    if st["status"] != "SUCCEEDED":
        print(f"error: {st.get('error')}")
        return 1
    print("\n" + "=" * 80 + "\n" + call(a.api, "GET", f"/requests/{rid}/report")[1])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
