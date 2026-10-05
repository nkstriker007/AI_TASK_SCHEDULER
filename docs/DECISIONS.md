# Decision log

D1–D14 and D16 are recorded in the design doc (section 14). This file records the decisions made during the build.

| ID | Decision | Rationale |
|---|---|---|
| D15 | **Research data source: LLM-only knowledge (proposed, confirm by the Day 1 gate).** Research executors answer from model knowledge; web search is not used. | Groq has no built-in web-search tool on the structured-output models, so search would need a separate provider and API key. LLM-only keeps cost and rate limits predictable for E7. Report results as "model knowledge as of training cutoff", not as current facts. Revisit if E7 answers are too stale. |
| D17 | **LLM provider: Groq** (`groq` Python SDK, `GROQ_API_KEY`). Planner default model `openai/gpt-oss-120b`, overridable with `ATS_PLANNER_MODEL`. | Team choice. `gpt-oss-120b` supports Groq's strict `json_schema` mode, which guarantees the plan's shape. |
| D18 | **Strict-mode schema is a relaxed copy.** `llm.LLM_PLAN_SCHEMA` makes every field required and nullable (Groq strict rules), leaves out length/pattern/count limits and `sim_subplan`, and the output is normalized (nulls dropped) and then run through the full validator. | Strict mode doesn't guarantee the finer constraints. The validator stays the only judge of validity (design doc 4.4). A test checks the LLM schema against the strict rules. |
| D19 | **Plan-level extensions frozen with contract #1:** `hints.expected_fanout`, `hints.sim_subplan`, and the `Subplan`/`SubplanTask` models (local IDs `s1…`, no nested expand, ≤ 20 tasks). | The design doc's Day 1 joint hour includes the expand type and subplan model, and freezing them now avoids a contract change on Day 6. See `docs/contract.md`. |
