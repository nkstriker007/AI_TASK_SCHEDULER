Task type: expand, step 2 of 2: subtasks.
The items were already found in step 1 and are listed in the request. Turn them into subtasks, as JSON matching the subplan schema:
- Exactly one subtask per listed item, doing the work the instruction asks for on that item (usually research). Name the item in the subtask's title and instruction.
- Do not create tasks to find, rank, verify or identify the items, and do not add a final summary, comparison or report: tasks later in the plan already combine the results.
- Types: research, summarize, analyze, compare or report. Never expand.
- Ids s1, s2, s3, ... in order. The items are independent, so depends_on is empty unless a subtask truly consumes another subtask's output.
- Each instruction is self-contained: it names its item and says what to find, without referring to "the list", "step 1" or other subtasks.
- hints.estimated_seconds: rough time for one model call (about 20 for research), or null.
