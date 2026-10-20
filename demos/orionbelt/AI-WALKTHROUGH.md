# Ask the same question, inspect the same definition

Start the demo, then add the `orionbelt_demo` server to your client using
`claude.mcp.json` or `codex.config.toml` as a **merge example**. Keep your existing
servers. Both clients use `http://127.0.0.1:19000/mcp`; neither needs warehouse
credentials. A client running remotely cannot reach your computer's loopback.

Use this instruction in either client:

> Use only the local orionbelt_demo MCP server for this exercise. First discover
> its tools, describe the loaded commerce model, and explain the metrics you use.
> Execute structured semantic queries using the model's business names. State
> the dimensions, filters, currency and date attribution alongside every answer.
> Never invent results, sum distinct counts across groups, average group averages,
> or replace a governed metric with your own SQL formula. If a question is
> ambiguous, state the assumption or ask for clarification. Show the tool result
> and query object so I can reproduce it in Metabase.

Try these prompts beside the dashboard:

| Prompt | Expected answer |
|---|---|
| What are revenue, refunds and net revenue for all orders? | EUR 850, 175 and 675 |
| Show net revenue by region. | North 405; South 210; West 60 |
| Show North's February revenue, refunds and net revenue. | EUR 250, 30 and 220 |
| How many customers bought in each month, and overall? | January 4; February 4; overall 6, not 8 |
| What is our average order value? Explain how it is calculated. | EUR 106.25 = 850 / 8; do not average regional averages |
| Show West's refunds and refund rate. | EUR 0 and 0%; absence of refund rows means zero |
| Compare January 31 through February 1, including both dates. | Revenue 200; refunds 30; net revenue 170 |
| What is net revenue for region Missing? | No matching region rows; do not invent a region |

For the last prompt, request a **Region breakdown** to distinguish an empty
grouped result from the SQL aggregate behavior of a zero-row input.

The MCP `execute_query` tool accepts a `query_json` string. For example:

```json
{
  "select": {"dimensions": ["Region"], "measures": ["Net Revenue"]},
  "where": [
    {"field": "Order Date", "op": "gte", "value": "2026-02-01"},
    {"field": "Order Date", "op": "lte", "value": "2026-02-28"}
  ],
  "orderBy": [{"field": "Region", "direction": "asc"}]
}
```

Also try `explain_artefact` and `get_lineage` after reading their discovered
schemas. They let an agent explain where Net Revenue comes from instead of
guessing a formula. Refunds in this demo follow the **original order date**;
asking for cash refunds paid during a month cannot be answered by this model.

`demo.py verify` executes the real upstream MCP tools deterministically and also
uses Metabase's native MCP endpoint. It needs no LLM API key and verifies the
data paths, not natural-language reasoning. An LLM can still choose the wrong
metric, time range or grain. These explicit prompts and inspectable tool calls
make those choices reviewable; model-provider evaluations are a separate step.

To connect interactively to Metabase's own MCP, use the existing
[agent setup guide](../../docs/agents/setup.md), targeting this demo's port 3002.
The automated verifier uses its isolated, gitignored demo API key.
