# agy 1.5.12 stream-json capture

Captured on ser7 at 2026-11-19 07:55 UTC as the same user as the zyc gemini service. The service's agy-carbon wrapper selected gemini-4-neon-high and the existing service proxy. The working directory was isolated and both prompts requested only a short text reply.

    agy-carbon --input-format stream-json --output-format stream-json -p=

The CLI requires a value for `-p`. Use the explicit empty value `-p=` at the end. Write one complete JSON record and a newline per user turn, flush, and wait for its terminal result before writing the next record. Hold stdin open between turns.

The `agy-1.5.12.{stdin,stdout}.ndjson` pair contains every line from the two successful turns. The capture substitutes one conversation UUID consistently and replaces the temporary absolute working directory with `/test/agy-probe`. The successful stderr capture was empty. The `-invalid` files record the failure probe using `type` instead of `event`. Its first line returned an error and exited 1, so a second line did not produce a result.

| Direction | Record | Meaning |
| --- | --- | --- |
| stdin | `event:"user", message:{role:"user",content:[{type:"text",text:"…"}]}` | Starts one turn |
| stdout | `event:"init", conversation_id, init:{model,cwd,tools,permission_mode}` | Emitted once when the process initializes. The ID identifies the conversation for `--conversation` |
| stdout | `event:"step_update", step_update:{conversation_id,step_index,state,step_type}` | A `user_input` step acknowledges input. An `agent_response` step carries incremental `text_delta` and optionally usage at `DONE` |
| stdout | `event:"result", result:{conversation_id,status,response,duration_seconds,num_turns,usage}` | Terminal record for each turn. In this capture, counters accumulate across turns within the process |
| stdout/stderr | `event:"result", result:{status:"ERROR",error,finished_reason}` and `error: …` on stderr | Malformed input ended the process. A premature EOF without result is a separate failure |
| Existing permission hook | `{conversationId,stepIdx,toolCall:{name,args}}` | Default mode relays pre-tool hooks over the existing authenticated local permission bridge. The agent emits a permission event, then the bridge answers the hook with `{decision:"allow"|"deny",reason?}`. The capture used no tools, so neither successful stdout trace contains a permission request. The bridge test exercises this distinct transport across two turns |

The same process (PID 2114540) produced both results. From stdin write to the first `agent_response` text delta, the two turns took 17.047 s and 6.281 s. The first `init` appeared at 10.335 s. The second turn emitted no `init`, used the same conversation ID, and the CLI log showed one language server initialization and one conversation update stream for the two turns. This measures CLI initialization recurrence. It does not count the vendor network requests individually.
