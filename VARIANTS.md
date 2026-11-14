# Variants tried

Four routing variants were measured with grade-local (process driver, three
practice trials). Only one ships. The scores below are the per-trial
quality / served / wire / failover / catalog columns; the shipped variant
was re-measured after a from-scratch rewrite and reproduced its numbers
exactly.

## A — Greedy, demote after two refusals (SHIPPED)

The routing in `main.ts`: the highest sealed rating for the request's exact
needs wins; a provider that refuses twice in a row is pushed to the back
until it serves again; a failed call retries on the next best fitting model;
JSON-mode requests never ride an Anthropic-wire provider; streamed calls to
an OpenAI-wire provider ask for usage in the stream.

- Trial 0: 99 / 100 / 100 / 100 / 100, 26 upstream calls
- Trial 1: 99 / 100 / 100 / 100 / 100, 30 upstream calls
- Trial 2: 99 / 100 / 100 / 100 / 100, 29 upstream calls
- Mean: score 99, 28.33 upstream calls

Quality 99 is the ceiling on these trials, not a miss: seventeen requests
want the top-rated free model and its budget is twelve, so four of them ride
one tier down. No routing choice can recover those points.

## B — Spread the load

Same needs filter and failover, but the tie-break prefers the provider with
the fewest calls so far *before* the sealed rating, so calls spread across
providers instead of piling onto the best one.

- Mean: score 99, 30.00 upstream calls

Same quality as A — the spread rarely changes which model answers — but
almost two extra upstream calls per trial, so it loses the tie-break. The
rating-first order of A is strictly better.

## C — No Gemini wire

Same as A, but the Gemini wire is never used: Gemini's models are left out
of the candidate list entirely.

- Mean: score 92

The drop is the tools-plus-vision requests. Without Gemini's 1458-rated
flash model, those requests fall to the next vision-capable free model and
lose several rating points each. The Gemini wire is load-bearing for this
workload; dropping it costs seven quality points.

## D — Demote after one refusal

Same as A, but a provider is pushed to the back after a single refusal
instead of two consecutive ones.

- Mean: score 99, 28.67 upstream calls

Same quality, but a third of a call more per trial on average. A single
refusal is usually a budget edge on one model, not a dead provider, so
waiting for the second refusal before demoting avoids a needless failover.
Two in a row is the right threshold.

## Why A wins

A and D tie on every score column; A wins on the tie-break (28.33 vs 28.67
upstream calls). B ties on score too and loses the same way (30.00). C is
simply worse. So A ships: greedy by sealed rating, demote after two
consecutive refusals, JSON never down an Anthropic wire, usage requested on
streamed OpenAI calls.
