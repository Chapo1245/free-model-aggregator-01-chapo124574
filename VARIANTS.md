# Variants

All variants share one wire layer (all six client×upstream wire paths, the
catalog probe, the JSON-mode wire rule) and differ only in the routing
policy — the `POLICY` constant at the top of `main.ts`. Each was graded with
the kit's `grade-local --driver process --instances 3` on all three practice
trials. Scores below are the sealed scorer's `scores.json` per trial.

## A — greedy (SHIPPED)

```ts
{ mode: "greedy", demoteAfter: 2, spreadWindow: 25, useGemini: true }
```

Rank candidates by `rating_milli` descending, wire-match tie-break, demote a
provider to the end after 2 consecutive refusals.

| trial | score | quality | served | wire | failover | catalog | upstream_calls |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 99 | 99 | 100 | 100 | 100 | 100 | 26 |
| 1 | 99 | 99 | 100 | 100 | 100 | 100 | 30 |
| 2 | 99 | 99 | 100 | 100 | 100 | 100 | 29 |
| **mean** | **99** | **99** | **100** | **100** | **100** | **100** | **28.33** |

Kept run: `/home/hatch/.makefun/trials/free-model-aggregator-01/01a1d7b4-338d-7599-b034-542ee047e68d`
(20/20 valid, 233/233 checks on trial 0). The quality 99 (not 100) is the
cost of refusals the policy cannot foresee: on trial 0, openrouter refuses
call #2 and #10 (`fail_calls`) and exhausts its budget of 12, so four
requests fall back one tier (1475 → 1470, once 1475 → 1465). No policy can
avoid a first-call refusal without giving up more quality elsewhere — routing
r03 away from openrouter's 1475 to save a later refusal would cost the same
or more on r03 itself.

## B — spread

```ts
{ mode: "spread", demoteAfter: 2, spreadWindow: 25, useGemini: true }
```

Weight-1 requests spread over providers within 25 milli of the best rating,
fewest-calls-first; weight-2 requests use strict greedy (they are only six
and the top providers can absorb them).

| trial | score | quality | served | wire | failover | catalog | upstream_calls |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 99 | 99 | 100 | 100 | 100 | 100 | 31 |
| 1 | 99 | 99 | 100 | 100 | 100 | 100 | 28 |
| 2 | 99 | 99 | 100 | 100 | 100 | 100 | 31 |
| **mean** | **99** | **99** | **100** | **100** | **100** | **100** | **30.00** |

Same score as A, more calls (mean 30 vs 28.33). Spreading touches more
providers' early `fail_calls` slots, so it pays more refusals than it saves.
Rejected: same score, worse tie-break.

## C — greedy, no Gemini wire

```ts
{ mode: "greedy", demoteAfter: 2, spreadWindow: 25, useGemini: false }
```

Vision requests fall back to sambanova's gemma-4-31b (1452) and zai's
glm-4.6v-flash (1432) instead of gemini-3.8-flash (1458).

| trial | score | quality | served | wire | failover | catalog | upstream_calls |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 92 | 92 | 95 | 93 | 83 | 100 | 29 |
| 1 | 92 | 91 | 95 | 93 | 87 | 100 | 30 |
| 2 | 92 | 91 | 95 | 93 | 87 | 100 | 31 |
| **mean** | **92** | **91.33** | **95** | **93** | **85.67** | **100** | **30.00** |

The Gemini wire is load-bearing: sambanova's gemma-4-31b has `tools: false`,
so for tools+vision requests (r01/r02/r06) the only non-Gemini candidate is
zai's glm-4.6v-flash — one refusal there leaves the request unservable
(trial 0: r02 503 after a single zai 429). Rejected: −7 score.

## D — greedy, demote after 1 refusal

```ts
{ mode: "greedy", demoteAfter: 1, spreadWindow: 25, useGemini: true }
```

Demote a provider after a single refusal instead of two, hoping to skip the
paired `fail_calls` (e.g. zai [2,3], cloudflare [4,5]).

| trial | score | quality | served | wire | failover | catalog | upstream_calls |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 99 | 99 | 100 | 100 | 100 | 100 | 27 |
| 1 | 99 | 99 | 100 | 100 | 100 | 100 | 28 |
| 2 | 99 | 99 | 100 | 100 | 100 | 100 | 31 |
| **mean** | **99** | **99** | **100** | **100** | **100** | **100** | **28.67** |

Same score as A, one more call in total (86 vs 85). Demoting after one
refusal also skips providers that would have succeeded next (an isolated
`fail_calls` entry), costing a fallback. Rejected: same score, worse
tie-break.

## Verdict

**A wins**: the highest score (99, tied with B and D) with the fewest
upstream calls (85 total across the three trials vs 86 for D and 90 for B),
which is the leaderboard tie-break. The remaining gap to 100 is the
unavoidable first-call refusals in each trial's draw — quality 99 is the
practical ceiling for a policy that cannot see the sealed `fail_calls`.
