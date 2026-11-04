# Datasets for E9 (accuracy against a single LLM call)

Fixed samples, committed so every run asks the same questions. One JSON object per line:
`id`, `question`, `answer` (the reference answer), `branch` (how many entities the question needs
looked up independently), plus `options` for the movie set.

| file | questions | from | license |
|---|---|---|---|
| `parallelqa.jsonl` | all 113 | ParallelQA, introduced by LLMCompiler (Kim et al., ICML 2024): math over factual attributes of 2–5 entities | MIT, see `LICENSE-LLMCompiler-MIT` |
| `hotpotqa_comparison.jsonl` | 200 of 1,487 (random, seed 2026) | the comparison subset of HotpotQA (Yang et al., EMNLP 2018) as packaged by LLMCompiler | **CC BY-SA 4.0** (HotpotQA); this file is shared under the same license |
| `movie_recommendation.jsonl` | 100 of 500 (random, seed 2026) | BIG-bench `movie_recommendation` as formatted by LLMCompiler | BIG-bench is Apache 2.0; LLMCompiler's formatting is MIT |

Source files: `datasets/` in https://github.com/SqueezeAILab/LLMCompiler (MIT). HotpotQA:
https://hotpotqa.github.io. BIG-bench: https://github.com/google/BIG-bench.

Notes:

- LLMCompiler's agents answered with a Wikipedia search tool. This project answers from model
  knowledge only (D15), so the scores are comparable between this project's own modes, not to the
  paper's numbers.
- Some ParallelQA answers depend on facts that change (populations, rankings). A closed-book model
  can disagree with the stored answer; both modes are affected the same way, and numbers are scored
  with a tolerance.
