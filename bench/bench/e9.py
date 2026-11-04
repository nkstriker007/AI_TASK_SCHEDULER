"""E9: is the workflow's answer as good as one normal LLM call? Accuracy on public datasets.

    make up-llm                                     # the stack the workflow mode runs on
    python -m bench.e9                              # 10 questions per dataset -> bench/results/e9.csv
    python -m bench.e9 --dataset parallelqa --per-dataset 20
    python -m bench.e9 --budget-tokens 150000       # stop before the day's Groq budget runs out

Datasets (bench/datasets/, see its README for sources and licenses), each with a reference answer:

  parallelqa             113 questions: math over facts about 2-5 entities (LLMCompiler)
  hotpotqa_comparison    200 questions comparing two entities (HotpotQA)
  movie_recommendation   100 questions: which of 4 options is most like 4 given movies (BIG-bench)

Every question runs in two modes on the same model:

  workflow   the planner turns the question into a plan, which runs on the stack (atsd + LLM workers)
  single     one LLM call answers the question

Both get the same question text with the same answer-format instruction ("end with 'Final answer:
...'"). The workflow's final task also returns the answer as a structured field (`final_answer`,
D58), which is used when present (`extracted` = "field"); otherwise the answer is read from that
line ("line"), and if there is none, one small extraction call reads it ("llm"), counted against
that mode. Scoring follows LLMCompiler's evaluation: a numeric
reference is matched within 10 %, anything else by exact match after normalising case,
punctuation, articles and spaces (`correct`); token-F1 is reported too (`f1`, as in HotpotQA). For
the movie set, an answer that names exactly one of the options is scored as that option.

Modes alternate which runs first, so rate-limit and ordering effects fall on both. Rows are written
after every question; a rerun skips (dataset, id, mode) rows that already succeeded, so a run that
stops at the token budget or a crash continues where it left off.
"""

from __future__ import annotations

import argparse
import asyncio
import collections
import csv
import json
import re
import string
import time
from pathlib import Path
from typing import Any

from worker.executors.llm import DEFAULT_EXECUTOR_MODEL, make_client

from . import live
from .sim import RESULTS_DIR, write_csv

DATASETS_DIR = Path(__file__).resolve().parents[1] / "datasets"
DATASETS = ("parallelqa", "hotpotqa_comparison", "movie_recommendation")
MODES = ("workflow", "single")
FIELDS = ("dataset", "id", "branch", "mode", "status", "correct", "f1", "latency_s", "planner_s", "tasks",
          "llm_calls", "tokens_in", "tokens_out", "cost_usd", "retries", "extracted", "prediction", "gold",
          "request_id", "error")

ANSWER_FORMAT = ("Work it out carefully from your own knowledge. End your answer with one last line of the form "
                 "'Final answer: <answer>', where <answer> is only the short answer: a name, yes or no, or a number "
                 "without units.")
MOVIE_FORMAT = "The answer must be exactly one of the options, copied as written."
SINGLE_PROMPT = "You are a careful analyst. Answer the user's question completely in one response."
EXTRACT_PROMPT = ("You read an answer to a question and return only its final short answer (a name, yes or no, or a "
                  "number without units), with no other words. If there is no answer, return NONE.")


# ------------------------------------------------------------------------------- data


def load(name: str, datasets_dir: Path = DATASETS_DIR) -> list[dict[str, Any]]:
    with (datasets_dir / f"{name}.jsonl").open() as f:
        return [json.loads(line) for line in f if line.strip()]


def sample(rows: list[dict[str, Any]], n: int) -> list[dict[str, Any]]:
    """The first n questions, taken round-robin over branch counts so every width is covered.
    Deterministic: the files are already shuffled samples, so file order is the draw."""
    by_branch: dict[int, list[dict]] = collections.defaultdict(list)
    for r in rows:
        by_branch[int(r.get("branch") or 0)].append(r)
    out: list[dict] = []
    queues = [collections.deque(by_branch[b]) for b in sorted(by_branch)]
    while len(out) < n and any(queues):
        for q in queues:
            if q and len(out) < n:
                out.append(q.popleft())
    return out


def request_text(item: dict[str, Any], dataset: str) -> str:
    parts = [item["question"].strip(), ANSWER_FORMAT]
    if dataset == "movie_recommendation":
        parts.append(MOVIE_FORMAT)
    return "\n\n".join(parts)


# ------------------------------------------------------------------------------- scoring


def normalize(s: str) -> str:
    """HotpotQA / LLMCompiler normalisation: lower case, no punctuation, no articles, single spaces."""
    s = "".join(ch for ch in s.lower() if ch not in set(string.punctuation))
    s = re.sub(r"\b(a|an|the)\b", " ", s)
    return " ".join(s.split())


_NUMBER = re.compile(r"-?\d[\d,]*\.?\d*(?:[eE]-?\d+)?|-?\.\d+")


def as_number(s: str) -> float | None:
    try:
        return float(s.replace(",", "").strip())
    except ValueError:
        return None


def first_number(s: str) -> float | None:
    m = _NUMBER.search(s.replace("−", "-"))
    return as_number(m.group(0)) if m else None


def f1(prediction: str, gold: str) -> float:
    p, g = normalize(prediction).split(), normalize(gold).split()
    if not p or not g:
        return float(p == g)
    common = collections.Counter(p) & collections.Counter(g)
    same = sum(common.values())
    if same == 0:
        return 0.0
    precision, recall = same / len(p), same / len(g)
    return 2 * precision * recall / (precision + recall)


def correct(prediction: str | None, gold: str, options: list[str] | None = None) -> bool:
    """LLMCompiler's compare_answer, made robust to a short answer with units or words around a number."""
    if not prediction:
        return False
    g = as_number(gold)
    if g is not None:
        p = first_number(prediction)
        if p is None:
            return False
        if g == 0:
            return abs(p) < 1e-9
        lo, hi = sorted((g * 0.9, g * 1.1))
        return lo < p < hi
    if options:
        named = [o for o in options if normalize(o) and normalize(o) in normalize(prediction)]
        if len(named) == 1:
            return normalize(named[0]) == normalize(gold)
    return normalize(prediction) == normalize(gold)


_FINAL = re.compile(r"final answer\s*[:：]\s*(.+)", re.IGNORECASE)


def final_line(text: str) -> str | None:
    """The answer after the last 'Final answer:' marker, without Markdown decoration."""
    found = _FINAL.findall(text or "")
    if not found:
        return None
    ans = found[-1].strip().strip("*_`").strip()
    ans = re.sub(r"^\*\*|\*\*$", "", ans).strip()
    return ans.rstrip(".").strip() or None


# ------------------------------------------------------------------------------- modes


async def _chat(client: Any, system: str, user: str) -> tuple[str, int, int, int, int]:
    """One call with 429 waits (as atsd would, D45). Returns text, tokens in/out, calls, retries."""
    retries = 0
    for attempt in range(1, 9):
        try:
            r = await client.chat.completions.create(
                model=DEFAULT_EXECUTOR_MODEL, reasoning_effort="low", timeout=300,
                messages=[{"role": "system", "content": system}, {"role": "user", "content": user}])
            u = r.usage
            return r.choices[0].message.content or "", u.prompt_tokens, u.completion_tokens, attempt, retries
        except Exception as exc:
            if type(exc).__name__ != "RateLimitError":
                raise
            retries += 1
            await asyncio.sleep(max(live._retry_after(exc, 2.0), min(32, 2 ** attempt)))
    raise RuntimeError("rate limited 8 times")


async def answer_of(text: str, question: str) -> tuple[str | None, str, tuple[int, int, int]]:
    """(answer, how it was found, extraction tokens in/out and calls)."""
    ans = final_line(text)
    if ans:
        return ans, "line", (0, 0, 0)
    async with make_client() as client:
        out, tin, tout, calls, _ = await _chat(client, EXTRACT_PROMPT, f"Question:\n{question}\n\nAnswer:\n{text[-4000:]}")
    out = final_line(out) or (out.strip().splitlines() or [""])[0].strip()
    return (None if not out or out.upper() == "NONE" else out), "llm", (tin, tout, calls)


async def run_single(request: str) -> dict[str, Any]:
    t0 = time.monotonic()
    async with make_client() as client:
        text, tin, tout, calls, retries = await _chat(client, SINGLE_PROMPT, request)
    return {"status": "SUCCEEDED", "seconds": time.monotonic() - t0, "planner_s": 0.0, "tasks": 0, "text": text,
            "tokens_in": tin, "tokens_out": tout, "llm_calls": calls, "retries": retries, "request_id": ""}


def run_workflow(request: str, api: str) -> dict[str, Any]:
    t0 = time.monotonic()
    try:
        planned = live.plan(request)
    except Exception as exc:  # the planner rejected the request or the provider failed: the workflow has no answer
        return {"status": "PLAN_FAILED", "seconds": time.monotonic() - t0, "planner_s": time.monotonic() - t0,
                "tasks": 0, "text": "", "tokens_in": 0, "tokens_out": 0, "llm_calls": 0, "retries": 0,
                "request_id": "", "error": f"{type(exc).__name__}: {exc}"[:300]}
    run = live.run_on_stack(planned.plan, api)
    return {"status": run.status, "seconds": planned.seconds + run.seconds, "planner_s": planned.seconds,
            "tasks": len(planned.plan.tasks), "text": live.output_text(run.output) if run.output is not None else "",
            "final_answer": run.output.get("final_answer") if isinstance(run.output, dict) else None,
            "tokens_in": planned.tokens_in + run.tokens_in, "tokens_out": planned.tokens_out + run.tokens_out,
            "llm_calls": planned.calls + run.llm_calls, "retries": run.retries, "request_id": run.request_id}


# ------------------------------------------------------------------------------- loop


def score_row(dataset: str, item: dict[str, Any], mode: str, r: dict[str, Any], answer: str | None, how: str,
              extra: tuple[int, int, int]) -> dict[str, Any]:
    tin, tout = r["tokens_in"] + extra[0], r["tokens_out"] + extra[1]
    ok = r["status"] == "SUCCEEDED" and correct(answer, item["answer"], item.get("options"))
    return {"dataset": dataset, "id": item["id"], "branch": item.get("branch", ""), "mode": mode,
            "status": r["status"], "correct": int(ok), "f1": round(f1(answer or "", item["answer"]), 3),
            "latency_s": round(r["seconds"], 2), "planner_s": round(r.get("planner_s", 0.0), 2), "tasks": r["tasks"],
            "llm_calls": r["llm_calls"] + extra[2], "tokens_in": tin, "tokens_out": tout,
            "cost_usd": round(live.cost(tin, tout), 6), "retries": r["retries"], "extracted": how,
            "prediction": (answer or "")[:200], "gold": item["answer"], "request_id": r.get("request_id", ""),
            "error": r.get("error", "")}


def done_keys(path: Path) -> tuple[list[dict], set[tuple[str, str, str]]]:
    if not path.exists():
        return [], set()
    with path.open() as f:
        rows = list(csv.DictReader(f))
    return rows, {(r["dataset"], r["id"], r["mode"]) for r in rows if r["status"] == "SUCCEEDED"}


def run(datasets: list[str], per_dataset: int, out: Path, api: str, budget_tokens: int, cooldown: float,
        modes: tuple[str, ...] = MODES, single=run_single, workflow=run_workflow, extract=None,
        datasets_dir: Path = DATASETS_DIR, sleep=time.sleep) -> list[dict]:
    """The experiment loop; `single`, `workflow` and `extract` are injectable for tests."""
    extract = extract or answer_of
    old, done = done_keys(out)
    rows = [r for r in old if (r["dataset"], r["id"], r["mode"]) in done]  # failed rows are retried
    used = 0
    k = 0
    for name in datasets:
        for item in sample(load(name, datasets_dir), per_dataset):
            todo = [m for m in modes if (name, item["id"], m) not in done]
            if not todo:
                continue
            if used >= budget_tokens:
                print(f"token budget reached ({used:,} >= {budget_tokens:,}); rerun later to continue", flush=True)
                write_csv(rows, out, FIELDS)
                return rows
            request = request_text(item, name)
            order = todo if k % 2 == 0 else list(reversed(todo))  # alternate which mode runs first
            k += 1
            for mode in order:
                if cooldown > 0:
                    sleep(cooldown)
                r = workflow(request, api) if mode == "workflow" else asyncio.run(single(request))
                if r["status"] == "SUCCEEDED" and isinstance(r.get("final_answer"), str) and r["final_answer"].strip():
                    answer, how, extra = r["final_answer"].strip(), "field", (0, 0, 0)  # structured (D58)
                elif r["status"] == "SUCCEEDED":
                    answer, how, extra = asyncio.run(extract(r["text"], item["question"]))
                else:
                    answer, how, extra = None, "-", (0, 0, 0)
                row = score_row(name, item, mode, r, answer, how, extra)
                rows.append(row)
                used += row["tokens_in"] + row["tokens_out"]
                print(f"{name:20s} {item['id']:28s} {mode:8s} {row['status']:9s} correct={row['correct']} "
                      f"{row['latency_s']:6.1f}s tokens={row['tokens_in'] + row['tokens_out']:6d}  "
                      f"answer={row['prediction'][:40]!r} gold={item['answer'][:30]!r}", flush=True)
            write_csv(rows, out, FIELDS)  # after every question
    write_csv(rows, out, FIELDS)
    return rows


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dataset", action="append", choices=DATASETS, help="repeatable; default all three")
    ap.add_argument("--per-dataset", type=int, default=10, help="questions per dataset (default 10)")
    ap.add_argument("--budget-tokens", type=int, default=150_000,
                    help="stop starting new questions after this many tokens (Groq free tier: 200k/day for gpt-oss-120b)")
    ap.add_argument("--cooldown", type=float, default=5.0, help="seconds before each mode (lets the tokens/min window drain)")
    ap.add_argument("--mode", action="append", choices=MODES, help="repeatable; default both")
    ap.add_argument("--api", default=live.API)
    ap.add_argument("--out", type=Path, default=RESULTS_DIR / "e9.csv")
    a = ap.parse_args(argv)
    rows = run(a.dataset or list(DATASETS), a.per_dataset, a.out, a.api, a.budget_tokens, a.cooldown,
               tuple(a.mode or MODES))
    print(f"wrote {a.out} ({len(rows)} rows)")
    for name in a.dataset or DATASETS:
        for mode in a.mode or MODES:
            rs = [r for r in rows if r["dataset"] == name and r["mode"] == mode]
            if rs:
                acc = sum(int(r["correct"]) for r in rs) / len(rs)
                print(f"  {name:20s} {mode:8s} accuracy {100 * acc:5.1f} % ({len(rs)} questions)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
