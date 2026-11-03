# Demo script (about 5 minutes)

What the video shows, in order, with the commands to run and what to point out. Record the
terminal at a large font size; split the screen between the demo terminal and `docker compose
logs -f atsd` if you have room.

## Before recording

```bash
make build
ATSD_LLM_TIMEOUT_MS=40000 make up-llm   # Redis + atsd + API + 3 Groq workers (GROQ_API_KEY in .env);
                                        # 40 s attempt deadline so a killed worker's task is retried quickly on camera
curl -s localhost:8000/healthz     # {"ok":true}
```

Wait a minute after any earlier runs, so Groq's tokens-per-minute window is empty (8,000 tokens/min
on the on_demand tier). Have `bench/results/` open in a file browser for the plots.

## 1. The idea (30 s)

"A typed request becomes a graph of tasks that runs in parallel. A Python planner asks an LLM for the
graph and validates it; a C++ scheduler runs it on workers over Redis, retries failures and can grow the
graph while it runs." Show the diagram at the top of `docs/REPORT.md`.

## 2. A request with known items (60 s)

```bash
python3 scripts/demo.py "Research Apple and NVIDIA, summarize recent developments for each, compare them and write a short report"
```

Point out: the plan the planner returned (research tasks have no dependencies, so they run at the
same time); the two research tasks RUNNING together; compare waits for both; the report at the end
with token counts.

## 3. Dynamic expansion: the items are unknown (90 s)

```bash
python3 scripts/demo.py "Research the top 3 AI chip companies and write a short report comparing them"
```

Point out: the plan has an `expand` task and no per-company tasks, because the companies are not in
the request. When t1 completes, `t1.1 … t1.3` (one per company, named) and `t1.join` appear: atsd
spliced the subplan into the running graph. In the atsd log: `graph_expanded`, then the new tasks
dispatched, then `join_completed`. The compare task receives all three results.

## 4. Fault tolerance (60 s)

In a second terminal, while a request runs:

```bash
python3 scripts/demo.py "Research AMD, Intel and Qualcomm, compare them and write a report" &
docker compose ps worker                       # pick a worker
docker kill ai_task_scheduler-worker-1         # kill one mid-task
```

Point out: the killed worker's task stays RUNNING until its deadline (40 s here), then atsd logs `attempt_lost`
and `retry_scheduled`, and another worker runs attempt 2. The request still succeeds; results are
committed with `SET NX`, so there is never a duplicate. (`make chaos` runs this at scale on sim tasks:
6 requests, 3 workers killed, 0 duplicate commits.) Restart the worker with `ATSD_LLM_TIMEOUT_MS=40000 make up-llm`.

## 5. Measurements (60 s)

Open `bench/results/` and walk through, one sentence each:

- `e1_overhead.png`: the real runtime is within 2–6 % of the simulator.
- `e2_scaling.png`: speedup follows the graph's critical-path bound; a chain never speeds up.
- `e3_efficiency.png`: 3–6 ms overhead per step; not worth distributing below ~3 ms tasks.
- `e4_policy.png`: critical-path priority saves ~5 % when workers are scarce, ties when they are not.
- `e7_requests.png` and `e8_expansion.png`: real requests on Groq. On this rate-limited tier one large call is the fastest answer; the plan buys per-task retries, inspectable steps and runtime discovery of unknown items, and the splice itself takes 1 ms.

## 6. Close (15 s)

"`make test` runs every test, `make bench` regenerates every plot, and `docs/REPORT.md` has the
results and limitations."

```bash
make down
```
