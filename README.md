# free-model-aggregator-01

The best free AI model aggregator: one stdlib-only Node 22 server that speaks
the OpenAI Chat Completions API and the Anthropic Messages API (JSON and SSE),
routes every request to the best free model that can serve it across ten
simulated free-tier providers, fails over when a provider refuses or runs out
of budget, and exits 0 when the workload is done.

## Run

```sh
node main.ts --list            # print the catalog the entry found, exit 0
node main.ts                   # serve (needs MAKEFUN_PROMPT_FILE + MAKEFUN_ENV_URL)
```

The server reads the brief at `$MAKEFUN_PROMPT_FILE` (provider directory,
quality table, workload manifest), probes each provider's catalog route in the
provider's own wire, builds one catalog, listens on `0.0.0.0` at a free port
(never 8080), POSTs `$MAKEFUN_ENV_URL/register`
`{"protocol_version": 1, "port": <port>, "catalog": <catalog>}`, serves the
client, polls `GET $MAKEFUN_ENV_URL/work` every 100 ms, and exits 0 when the
env says `{"done": true}`.

## Routing

For each request, in arrival order (the manifest's row):

1. **Candidates** — every free model on a provider whose catalog probe
   answered, meeting the request's requirements (tools / vision / JSON mode /
   minimum context) from the sealed quality table's row where one exists, the
   listing's own fields where it does not.
2. **JSON-mode wire rule** — a request that carries `response_format`
   `json_object`/`json_schema` is never sent down an Anthropic-wire provider:
   that wire cannot carry `response_format`, so the echo would break. It goes
   to an OpenAI-wire provider (the format passes through) or a Gemini-wire
   provider (`generationConfig.responseMimeType: "application/json"`).
3. **Order** — highest `rating_milli` first; ties prefer a provider whose wire
   matches the client's wire (no translation means no translation risk), then
   fewest calls so far, then the brief's provider order.
4. **Failover** — a 402, 429, 5xx or unreachable provider is recorded as a
   refusal and the request is retried on the next candidate, in order. After
   2 consecutive refusals a provider is demoted to the end of the order until
   it serves a request again. A 400/401 (a bug on our side, not provider
   state) skips that candidate without counting a refusal.
5. **One provider per request** — the first successful upstream call answers
   the client; the entry never calls a second provider for a served request.

## The three wires

| Client \ upstream | OpenAI | Anthropic | Gemini |
| --- | --- | --- | --- |
| OpenAI | passthrough (+ `stream_options.include_usage` on streams, so the usage chunk exists) | request translated (system, images, tools, tool choice, stop); answer rebuilt as a completion | `generateContent` / `streamGenerateContent`, `systemInstruction`, `functionDeclarations`, `generationConfig` |
| Anthropic | request rewritten to Chat Completions (the starter's translation); answer rebuilt as a Messages object / event stream | passthrough | same Gemini request shape, answer rebuilt as Messages |

All six paths carry the client's fields faithfully (system, user text, image
data, tool definitions, `max_tokens`, stop sequences, JSON mode) so the
provider's deterministic `ANSWER ECHO` comes back whole, and all six map the
provider's usage and stop reason back into the client's shape
(`end_turn`/`stop`/`STOP` are one family; streamed Anthropic usage compares
output tokens only, streamed OpenAI usage needs the `include_usage` chunk).

## Determinism

No wall clock and no unseeded randomness anywhere in routing or translation,
so a replay leaves identical state. Listed models use fixed timestamps.

## Grade-local (practice trials, process driver)

| Variant | t0 | t1 | t2 | mean score | mean upstream_calls |
| --- | --- | --- | --- | --- | --- |
| **A greedy (this entry)** | **99** | **99** | **99** | **99** | **28.33** |
| B spread | 99 | 99 | 99 | 99 | 30.00 |
| C no-gemini | 92 | 92 | 92 | 92 | 30.00 |
| D demote-after-1 | 99 | 99 | 99 | 99 | 28.67 |

Per-component for A on every trial: quality 99, served 100, wire 100,
failover 100, catalog 100. See `VARIANTS.md` for why A wins and what still
beats it.
