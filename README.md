# Free Model Aggregator

One long-running program that serves every request with the best free model
it can get, over ten simulated free-tier providers.

The client speaks either the OpenAI Chat Completions API or the Anthropic
Messages API — JSON bodies or server-sent events — and this server answers
both, by routing each request upstream to whichever free model is rated
highest for exactly what that request needs.

## How it runs

```sh
node main.ts
```

The trial hands the process two environment variables:

- `MAKEFUN_PROMPT_FILE` — the brief: three JSON documents (the provider
  directory, the sealed quality table, and the request manifest).
- `MAKEFUN_ENV_URL` — the env: the simulated providers live under their base
  paths there, and the register/work endpoints live there too.

On startup the server reads the brief, asks every provider's catalog route
for its models in that provider's own wire, builds one catalog from the
answers, registers it with the env, and then answers requests until the env
says the work is done, at which point it exits 0.

```sh
node main.ts --list
```

prints the catalog as JSON and exits, without starting the server. It works
with no brief at all, in which case the catalog is empty.

## How a request is routed

1. **Needs first.** The request's own body says what it needs: tools, an
   image, a JSON response format, or a long prompt (over two thousand
   characters of input text means it needs a 200,000-token window). Only
   free models whose sealed capabilities meet every need are considered.
2. **Best rating wins.** Candidates are ordered by the sealed
   `rating_milli` for that provider and model. Ties break toward the wire
   that matches the client's own API, then toward the provider with the
   fewest calls so far, then toward the brief's provider order.
3. **Refusals demote.** A provider that refuses twice in a row — a 402, a
   429, a 5xx, or unreachable — is pushed to the back of the order until it
   serves a request again. A 400 or 401 is the request's fault, not the
   provider's, so it never demotes.
4. **Failover is free.** If a call fails, the next best fitting model is
   tried, until one answers or the list runs out.

Two routing rules that matter more than they look:

- A JSON-mode request never rides an Anthropic-wire provider: that wire has
  no response format to carry, so its answer could never pass the JSON
  check.
- A streamed call to an OpenAI-wire provider asks for
  `stream_options: { include_usage: true }`, or the streamed usage check has
  nothing to read.

## What it translates

The client and the provider rarely speak the same wire, so the server
translates both ways:

| Client | Provider | Request | Answer |
| --- | --- | --- | --- |
| OpenAI | OpenAI | as-is | as-is |
| OpenAI | Anthropic | Chat Completions → Messages | Messages → Chat Completions |
| OpenAI | Gemini | Chat Completions → generateContent | generateContent → Chat Completions |
| Anthropic | OpenAI | Messages → Chat Completions | Chat Completions → Messages |
| Anthropic | Anthropic | as-is | as-is |
| Anthropic | Gemini | Messages → generateContent | generateContent → Messages |

Streams are translated frame by frame: OpenAI chunks to Anthropic events,
Anthropic events to OpenAI chunks, Gemini frames to either. Usage is always
reported exactly as the provider reported it, because the checks compare it
token for token.

`GET /v1/models` lists exactly the registered free models, in the OpenAI
list shape by default and in the Anthropic list shape when the request
carries an `x-api-key` header.

## What it never does

- It never reads the sealed per-trial numbers — budgets, failing call
  numbers, outage points — and never hard-codes a provider, a model, or a
  rating. Every routing input comes from the brief at runtime.
- It never uses a clock or unseeded randomness, so a replay of a trial sees
  the same requests and leaves the same state.
- It never picks a paid model. The sealed table's free flag decides, and the
  catalog reports free and paid models separately.

## Layout

- `main.ts` — the whole server: brief reading, catalog probing, routing,
  the four request translators, the four response translators, the four
  stream translators, the HTTP server, and the register/poll bootstrap.
- `package.json` — Node 22, ES module, no dependencies.
- `VARIANTS.md` — the routing variants that were tried and what they scored.
- `LICENSE` — MIT.

## Grade-local

```sh
node cli/makefun.mjs grade-local --kit . --bot <path-to-this-repo> --driver process --instances 3
```

run from inside the competition kit directory. The shipped variant scores
99/99/99 across the three practice trials (quality 99, served 100, wire
100, failover 100, catalog 100), with a mean of about 28 upstream calls per
trial. Quality 100 is unreachable on trial 0: seventeen requests want the
top-rated free model and its budget is twelve, so four of them ride one
tier down.
