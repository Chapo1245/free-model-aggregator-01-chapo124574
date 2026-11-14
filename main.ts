// A free-tier model aggregator: one server, two client wires, ten simulated providers.
//
// The trial hands this process a brief (MAKEFUN_PROMPT_FILE) and an env
// (MAKEFUN_ENV_URL). The brief lists ten providers, a sealed quality table
// and the twenty requests the client will send. This server reads the brief,
// asks every provider's catalog route for its models in that provider's own
// wire, builds one catalog from the answers, registers it with the env, and
// then answers the client's OpenAI and Anthropic requests by routing each one
// to the best free model that can serve it.
//
// Routing, in one line: the highest sealed rating for the request's exact
// needs wins; a provider that keeps refusing gets pushed to the back until it
// answers again; a failed call is retried on the next best model that fits.
// JSON-mode requests never ride an Anthropic-wire provider, because that wire
// has no way to carry a response format. Streamed calls to an OpenAI-wire
// provider ask for usage in the stream, or the usage check has nothing to read.
//
// Everything here is a pure function of the brief, the requests and the
// providers' answers: no clock, no unseeded randomness, so a replay of a
// trial sees the same requests and leaves the same state.

import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { readFileSync } from 'node:fs';

// ---------------------------------------------------------------------------
// Shapes
// ---------------------------------------------------------------------------

type WireKind = 'openai' | 'anthropic' | 'gemini';
type AuthKind = 'bearer' | 'x-api-key' | 'x-goog-api-key' | 'none';
type ClientApi = 'openai' | 'anthropic';
type JsonObj = Record<string, unknown>;

interface BriefProvider {
  id: string;
  label: string;
  wire: WireKind;
  auth: AuthKind;
  path: string;
  keyVar: string;
  secret: string;
}

interface RatingEntry {
  model: string;
  provider: string;
  score: number;
  context: number | null;
  tools: boolean;
  vision: boolean;
  json: boolean;
  free: boolean;
}

interface NeedSet {
  tools?: boolean;
  vision?: boolean;
  json?: boolean;
  minContext?: number;
}

interface WorkItem {
  id: string;
  api: ClientApi;
  stream: boolean;
  weight: number;
  needs: NeedSet;
}

interface ListedModel {
  id: string;
  shown: string;
  free: boolean | null;
  context: number | null;
  tools: boolean | null;
  vision: boolean | null;
  json: boolean | null;
}

interface ProbeOutcome {
  provider: BriefProvider;
  ok: boolean;
  status: number;
  models: ListedModel[];
  problem: string | null;
}

interface CatalogProvider {
  id: string;
  base_url: string;
  key_env: string;
  wire: WireKind;
  models: { id: string; free: boolean; context: number | null }[];
  free_models: string[];
  paid_models: string[];
}

interface CatalogDoc {
  schema: 'free-model-aggregator-01/catalog/v1';
  providers: CatalogProvider[];
}

interface CapSet {
  context: number | null;
  tools: boolean | null;
  vision: boolean | null;
  json: boolean | null;
}

interface Pick {
  provider: BriefProvider;
  probe: ProbeOutcome;
  model: ListedModel;
  score: number;
  wireHit: boolean;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const PROTOCOL = 1;
const POLL_EVERY_MS = 100;
const PROBE_LIMIT_MS = 5000;
const CALL_LIMIT_MS = 5000;
const MAX_CLIENT_BYTES = 32 * 1024 * 1024;
const OPENAI_CREATED = 1767225600;
const ANTHROPIC_CREATED = '2026-01-01T00:00:00Z';

const PROVIDERS_DOC = 'free-model-aggregator-01/providers/v1';
const RATINGS_DOC = 'free-model-aggregator-01/ratings/v1';
const WORKLOAD_DOC = 'free-model-aggregator-01/workload/v1';

const ENV_URL = (process.env.MAKEFUN_ENV_URL ?? '').replace(/\/+$/, '');
const BRIEF_PATH = process.env.MAKEFUN_PROMPT_FILE ?? '';

// ---------------------------------------------------------------------------
// Tiny JSON helpers
// ---------------------------------------------------------------------------

function isObj(v: unknown): v is JsonObj {
  return v !== null && typeof v === 'object' && !Array.isArray(v);
}

function strVal(v: unknown): string {
  return typeof v === 'string' ? v : '';
}

function numVal(v: unknown): number | null {
  return typeof v === 'number' && Number.isFinite(v) ? v : null;
}

function boolVal(v: unknown): boolean | null {
  return typeof v === 'boolean' ? v : null;
}

function note(msg: string): void {
  process.stderr.write(`entry: ${msg}\n`);
}

// ---------------------------------------------------------------------------
// Brief reading: find the three documents by their schema member
// ---------------------------------------------------------------------------

/** The offset of the `}` that closes the object opening at `from`, or -1. */
function closeBraceAt(text: string, from: number): number {
  let depth = 0;
  let quoted = false;
  for (let i = from; i < text.length; i += 1) {
    const ch = text[i];
    if (quoted) {
      if (ch === '\\') i += 1;
      else if (ch === '"') quoted = false;
    } else if (ch === '"') {
      quoted = true;
    } else if (ch === '{') {
      depth += 1;
    } else if (ch === '}') {
      depth -= 1;
      if (depth === 0) return i;
    }
  }
  return -1;
}

/** Every JSON object in `text` that carries a string `schema`, in file order. */
function schemaDocs(text: string): JsonObj[] {
  const found: JsonObj[] = [];
  let at = text.indexOf('{');
  while (at !== -1) {
    const end = closeBraceAt(text, at);
    if (end === -1) break;
    try {
      const parsed: unknown = JSON.parse(text.slice(at, end + 1));
      if (isObj(parsed) && typeof parsed.schema === 'string') found.push(parsed);
    } catch {
      // Not a standalone object; keep scanning.
    }
    at = text.indexOf('{', at + 1);
  }
  return found;
}

function wireOf(v: unknown): WireKind | null {
  return v === 'openai' || v === 'anthropic' || v === 'gemini' ? v : null;
}

function authOf(v: unknown): AuthKind {
  if (v === 'bearer' || v === 'x-api-key' || v === 'x-goog-api-key' || v === 'none') return v;
  return 'bearer';
}

function providerRow(v: unknown): BriefProvider | null {
  if (!isObj(v)) return null;
  const id = strVal(v.id);
  const wire = wireOf(v.wire);
  if (id === '' || wire === null) return null;
  const path =
    strVal(v.path) !== ''
      ? strVal(v.path)
      : strVal(v.base_path) !== ''
        ? strVal(v.base_path)
        : strVal(v.base_url) !== ''
          ? strVal(v.base_url)
          : strVal(v.base);
  return {
    id,
    label: strVal(v.name) !== '' ? strVal(v.name) : id,
    wire,
    auth: authOf(v.auth),
    path,
    keyVar: strVal(v.key_env),
    secret: strVal(v.key),
  };
}

function ratingRow(v: unknown): RatingEntry | null {
  if (!isObj(v)) return null;
  const model = strVal(v.model);
  const provider = strVal(v.provider);
  if (model === '' || provider === '') return null;
  return {
    model,
    provider,
    score: numVal(v.rating_milli) ?? 0,
    context: numVal(v.context_length),
    tools: v.tools === true,
    vision: v.vision === true,
    json: v.json === true,
    free: v.free === true,
  };
}

function workRow(v: unknown): WorkItem | null {
  if (!isObj(v)) return null;
  const id = strVal(v.id);
  if (id === '') return null;
  const needs = isObj(v.requires) ? v.requires : {};
  const out: NeedSet = {};
  if (needs.tools === true) out.tools = true;
  if (needs.vision === true) out.vision = true;
  if (needs.json === true) out.json = true;
  const minCtx = numVal(needs.min_context);
  if (minCtx !== null) out.minContext = minCtx;
  return {
    id,
    api: v.api === 'anthropic' ? 'anthropic' : 'openai',
    stream: v.stream === true,
    weight: v.weight === 2 ? 2 : 1,
    needs: out,
  };
}

interface Brief {
  providers: BriefProvider[];
  ratings: RatingEntry[];
  manifest: WorkItem[];
  fault: string | null;
}

function loadBrief(path: string): Brief {
  const empty: Brief = { providers: [], ratings: [], manifest: [], fault: null };
  if (path === '') return { ...empty, fault: 'MAKEFUN_PROMPT_FILE is not set' };
  let text: string;
  try {
    text = readFileSync(path, 'utf8');
  } catch (err) {
    return { ...empty, fault: `cannot read the brief at ${path}: ${(err as Error).message}` };
  }
  const docs = schemaDocs(text);
  const dirDoc = docs.find((d) => d.schema === PROVIDERS_DOC && Array.isArray(d.providers)) ?? null;
  if (dirDoc === null || !Array.isArray(dirDoc.providers)) {
    return { ...empty, fault: `the brief holds no ${PROVIDERS_DOC} document` };
  }
  const providers: BriefProvider[] = [];
  for (const raw of dirDoc.providers) {
    const row = providerRow(raw);
    if (row !== null) providers.push(row);
  }
  const ratingsDoc = docs.find((d) => d.schema === RATINGS_DOC && Array.isArray(d.ratings)) ?? null;
  const ratings: RatingEntry[] = [];
  if (ratingsDoc !== null && Array.isArray(ratingsDoc.ratings)) {
    for (const raw of ratingsDoc.ratings) {
      const row = ratingRow(raw);
      if (row !== null) ratings.push(row);
    }
  }
  const workDoc = docs.find((d) => d.schema === WORKLOAD_DOC && Array.isArray(d.manifest)) ?? null;
  const manifest: WorkItem[] = [];
  if (workDoc !== null && Array.isArray(workDoc.manifest)) {
    for (const raw of workDoc.manifest) {
      const row = workRow(raw);
      if (row !== null) manifest.push(row);
    }
  }
  return { providers, ratings, manifest, fault: null };
}

const brief = loadBrief(BRIEF_PATH);

// ---------------------------------------------------------------------------
// Catalog probe: each provider's own wire, its own key, failures tolerated
// ---------------------------------------------------------------------------

/** Join a base path (or absolute URL) to the env's origin. */
function joinUrl(envUrl: string, base: string): string {
  const trimmed = base.trim();
  if (/^https?:\/\//i.test(trimmed)) return trimmed.replace(/\/+$/, '');
  const root = envUrl.replace(/\/+$/, '');
  if (trimmed === '') return root;
  return `${root}${trimmed.startsWith('/') ? '' : '/'}${trimmed.replace(/\/+$/, '')}`;
}

/** The headers that carry a provider's key, by its own auth scheme. */
function keyHeaders(auth: AuthKind, secret: string): Record<string, string> {
  if (secret === '') return {};
  if (auth === 'bearer') return { authorization: `Bearer ${secret}` };
  if (auth === 'x-api-key') return { 'x-api-key': secret };
  if (auth === 'x-goog-api-key') return { 'x-goog-api-key': secret };
  return {};
}

/** Whether a listing's price block says every price is zero. */
function pricedZero(v: JsonObj): boolean | null {
  const pricing = isObj(v.pricing) ? v.pricing : null;
  if (pricing === null) return null;
  const amounts: number[] = [];
  for (const x of Object.values(pricing)) {
    if (typeof x === 'number') amounts.push(x);
    else if (typeof x === 'string' && x.trim() !== '') amounts.push(Number(x));
  }
  if (amounts.length === 0) return null;
  return amounts.every((n) => Number.isFinite(n) && n === 0);
}

/** One model as a catalog route spelled it, in that wire's spelling. */
function listedModel(wire: WireKind, v: unknown): ListedModel | null {
  if (!isObj(v)) return null;
  const id = wire === 'gemini' ? strVal(v.name).replace(/^models\//, '') : strVal(v.id);
  if (id === '') return null;
  const context =
    numVal(v.context_length) ?? numVal(v.context_window) ?? numVal(v.inputTokenLimit);
  return {
    id,
    shown:
      strVal(v.display_name) !== ''
        ? strVal(v.display_name)
        : strVal(v.displayName) !== ''
          ? strVal(v.displayName)
          : id,
    free: boolVal(v.free) ?? pricedZero(v),
    context,
    tools: boolVal(v.tools),
    vision: boolVal(v.vision),
    json: boolVal(v.json),
  };
}

/** The models a catalog answer listed, in its wire's shape. */
function listedModels(wire: WireKind, body: unknown): ListedModel[] {
  if (!isObj(body)) return [];
  const rows = wire === 'gemini' ? body.models : body.data;
  if (!Array.isArray(rows)) return [];
  const out: ListedModel[] = [];
  for (const raw of rows) {
    const model = listedModel(wire, raw);
    if (model !== null) out.push(model);
  }
  return out;
}

async function probeOneProvider(provider: BriefProvider): Promise<ProbeOutcome> {
  const root = joinUrl(ENV_URL, provider.path);
  const url =
    provider.wire === 'gemini' ? `${root}/v1beta/models` : `${root}/v1/models`;
  const headers: Record<string, string> = {
    accept: 'application/json',
    ...keyHeaders(provider.auth, provider.secret),
  };
  if (provider.wire === 'anthropic') headers['anthropic-version'] = '2023-06-01';
  try {
    const res = await fetch(url, { headers, signal: AbortSignal.timeout(PROBE_LIMIT_MS) });
    if (!res.ok) {
      return { provider, ok: false, status: res.status, models: [], problem: `GET ${url} answered HTTP ${res.status}` };
    }
    const body: unknown = await res.json();
    return { provider, ok: true, status: res.status, models: listedModels(provider.wire, body), problem: null };
  } catch (err) {
    return { provider, ok: false, status: 0, models: [], problem: `GET ${url} failed: ${(err as Error).message}` };
  }
}

function ratingFor(providerId: string, modelId: string): RatingEntry | null {
  return brief.ratings.find((r) => r.provider === providerId && r.model === modelId) ?? null;
}

/** The one catalog: every provider that answered, its models with free flags. */
function assembleCatalog(probes: readonly ProbeOutcome[]): CatalogDoc {
  const providers: CatalogProvider[] = [];
  for (const probe of probes) {
    if (!probe.ok) continue;
    const seen = new Set<string>();
    const models: CatalogProvider['models'] = [];
    const sorted = [...probe.models].sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
    for (const model of sorted) {
      if (seen.has(model.id)) continue;
      seen.add(model.id);
      const row = ratingFor(probe.provider.id, model.id);
      models.push({
        id: model.id,
        free: row !== null ? row.free : model.free ?? false,
        context: row?.context ?? model.context ?? null,
      });
    }
    providers.push({
      id: probe.provider.id,
      base_url: probe.provider.path,
      key_env: probe.provider.keyVar,
      wire: probe.provider.wire,
      models,
      free_models: models.filter((m) => m.free).map((m) => m.id),
      paid_models: models.filter((m) => !m.free).map((m) => m.id),
    });
  }
  return { schema: 'free-model-aggregator-01/catalog/v1', providers };
}

let probes: ProbeOutcome[] = [];
let ratingsDoc: RatingsFile | null = null;
let catalogDoc: CatalogDoc = { schema: 'free-model-aggregator-01/catalog/v1', providers: [] };

async function loadCatalog(): Promise<void> {
  if (brief.fault !== null) note(brief.fault);
  probes = await Promise.all(brief.providers.map((p) => probeOneProvider(p)));
  for (const probe of probes) {
    if (!probe.ok && probe.problem !== null) note(probe.problem);
  }
  catalogDoc = assembleCatalog(probes);
}

// ---------------------------------------------------------------------------
// Capabilities, requirements, routing
// ---------------------------------------------------------------------------

/** What a model can do: the sealed row first, the listing's word second. */
function capsFor(model: ListedModel, row: RatingEntry | null): CapSet {
  return {
    context: row?.context ?? model.context ?? null,
    tools: row?.tools ?? model.tools ?? null,
    vision: row?.vision ?? model.vision ?? null,
    json: row?.json ?? model.json ?? null,
  };
}

/** Whether a model's capabilities meet one request's needs. Unknown never meets. */
function needsMet(needs: NeedSet, caps: CapSet): boolean {
  if (needs.tools === true && caps.tools !== true) return false;
  if (needs.vision === true && caps.vision !== true) return false;
  if (needs.json === true && caps.json !== true) return false;
  if (needs.minContext !== undefined && (caps.context === null || caps.context < needs.minContext)) return false;
  return true;
}

/** Per-provider routing memory: calls made, refusals in a row, held back or not. */
const callsMade = new Map<string, number>();
const refusalsRow = new Map<string, number>();
const heldBack = new Set<string>();

function markAttempt(providerId: string): void {
  callsMade.set(providerId, (callsMade.get(providerId) ?? 0) + 1);
}

function markRefusal(providerId: string): void {
  const count = (refusalsRow.get(providerId) ?? 0) + 1;
  refusalsRow.set(providerId, count);
  if (count >= 2) heldBack.add(providerId);
}

function markServed(providerId: string): void {
  refusalsRow.set(providerId, 0);
  heldBack.delete(providerId);
}

/**
 * The candidates for one request, best first: held-back providers last, then
 * the highest sealed rating, then a wire that matches the client's own API,
 * then the provider with the fewest calls so far, then the brief's order.
 * A JSON-mode request never rides an Anthropic-wire provider: that wire has
 * no response format to carry, so its answer could never pass the JSON check.
 */
function orderPicks(needs: NeedSet, clientApi: ClientApi, skip: ReadonlySet<string>): Pick[] {
  const out: Pick[] = [];
  for (let order = 0; order < probes.length; order += 1) {
    const probe = probes[order];
    if (!probe.ok) continue;
    for (const model of probe.models) {
      const row = ratingFor(probe.provider.id, model.id);
      const isFree = row !== null ? row.free : model.free ?? false;
      if (!isFree) continue;
      if (!needsMet(needs, capsFor(model, row))) continue;
      if (needs.json === true && probe.provider.wire === 'anthropic') continue;
      out.push({
        provider: probe.provider,
        probe,
        model,
        score: row !== null ? row.score : 0,
        wireHit: (clientApi === 'openai' && probe.provider.wire === 'openai') || (clientApi === 'anthropic' && probe.provider.wire === 'anthropic'),
      });
    }
  }
  out.sort((a, b) => {
    const aHeld = heldBack.has(a.provider.id) ? 1 : 0;
    const bHeld = heldBack.has(b.provider.id) ? 1 : 0;
    if (aHeld !== bHeld) return aHeld - bHeld;
    if (a.score !== b.score) return b.score - a.score;
    if (a.wireHit !== b.wireHit) return a.wireHit ? -1 : 1;
    const aCalls = callsMade.get(a.provider.id) ?? 0;
    const bCalls = callsMade.get(b.provider.id) ?? 0;
    if (aCalls !== bCalls) return aCalls - bCalls;
    const aOrder = probes.findIndex((p) => p.provider.id === a.provider.id);
    const bOrder = probes.findIndex((p) => p.provider.id === b.provider.id);
    if (aOrder !== bOrder) return aOrder - bOrder;
    return a.model.id < b.model.id ? -1 : a.model.id > b.model.id ? 1 : 0;
  });
  return out.filter((p) => !skip.has(`${p.provider.id}/${p.model.id}`));
}

// ---------------------------------------------------------------------------
// Text plumbing shared by the translators
// ---------------------------------------------------------------------------

/** The text of a string or of a block array's text blocks. */
function textOf(content: unknown): string {
  if (typeof content === 'string') return content;
  if (!Array.isArray(content)) return '';
  const parts: string[] = [];
  for (const block of content) {
    if (isObj(block) && block.type === 'text' && typeof block.text === 'string') parts.push(block.text);
  }
  return parts.join('\n');
}

/** A tool call's arguments as an object: the provider's JSON string, or {}. */
function argsObj(value: unknown): JsonObj {
  if (isObj(value)) return value;
  if (typeof value === 'string' && value.trim() !== '') {
    try {
      const parsed: unknown = JSON.parse(value);
      if (isObj(parsed)) return parsed;
    } catch {
      // Not JSON: an empty input.
    }
  }
  return {};
}

/** One Anthropic content block as an OpenAI content part, or null. */
function openAIPart(block: JsonObj): JsonObj | null {
  if (block.type === 'text' && typeof block.text === 'string') return { type: 'text', text: block.text };
  if (block.type === 'image' && isObj(block.source)) {
    const src = block.source;
    if (src.type === 'base64' && typeof src.data === 'string') {
      const media = typeof src.media_type === 'string' ? src.media_type : 'image/png';
      return { type: 'image_url', image_url: { url: `data:${media};base64,${src.data}` } };
    }
    if (src.type === 'url' && typeof src.url === 'string') return { type: 'image_url', image_url: { url: src.url } };
  }
  return null;
}

/** A tool_result block's content as text. */
function toolResultText(content: unknown): string {
  if (typeof content === 'string') return content;
  if (Array.isArray(content)) {
    const text = textOf(content);
    return text !== '' ? text : JSON.stringify(content);
  }
  return content === undefined ? '' : JSON.stringify(content);
}

/** Split a `data:<mime>;base64,<data>` URL, or null when it is not one. */
function splitDataUrl(url: string): { mime: string; data: string } | null {
  const m = /^data:([^;,]+);base64,(.*)$/s.exec(url);
  if (m === null) return null;
  return { mime: m[1], data: m[2] };
}

// ---------------------------------------------------------------------------
// Request translators: client body -> upstream body, one per wire pair
// ---------------------------------------------------------------------------

/** An Anthropic Messages body as a Chat Completions body naming `model`. */
function anthropicToOpenAIReq(body: JsonObj, model: string): JsonObj {
  const messages: JsonObj[] = [];
  const system = textOf(body.system);
  if (system !== '') messages.push({ role: 'system', content: system });
  const source = Array.isArray(body.messages) ? body.messages : [];
  for (const raw of source) {
    if (!isObj(raw)) continue;
    const fromAssistant = raw.role === 'assistant';
    const content = raw.content;
    if (typeof content === 'string') {
      messages.push({ role: fromAssistant ? 'assistant' : 'user', content });
      continue;
    }
    if (!Array.isArray(content)) continue;
    if (fromAssistant) {
      const calls: JsonObj[] = [];
      for (const block of content) {
        if (!isObj(block) || block.type !== 'tool_use') continue;
        calls.push({
          id: typeof block.id === 'string' ? block.id : '',
          type: 'function',
          function: {
            name: typeof block.name === 'string' ? block.name : '',
            arguments: JSON.stringify(isObj(block.input) ? block.input : {}),
          },
        });
      }
      const text = textOf(content);
      messages.push({
        role: 'assistant',
        content: text !== '' ? text : null,
        ...(calls.length > 0 ? { tool_calls: calls } : {}),
      });
      continue;
    }
    const parts: JsonObj[] = [];
    for (const block of content) {
      if (!isObj(block)) continue;
      if (block.type === 'tool_result') {
        messages.push({
          role: 'tool',
          tool_call_id: typeof block.tool_use_id === 'string' ? block.tool_use_id : '',
          content: toolResultText(block.content),
        });
        continue;
      }
      const part = openAIPart(block);
      if (part !== null) parts.push(part);
    }
    if (parts.length > 0) messages.push({ role: 'user', content: parts });
  }
  const out: JsonObj = { model, messages };
  if (typeof body.max_tokens === 'number') out.max_tokens = body.max_tokens;
  if (typeof body.temperature === 'number') out.temperature = body.temperature;
  if (typeof body.top_p === 'number') out.top_p = body.top_p;
  if (Array.isArray(body.stop_sequences) && body.stop_sequences.length > 0) out.stop = body.stop_sequences;
  if (Array.isArray(body.tools)) {
    const tools = body.tools.flatMap((raw) => {
      if (!isObj(raw) || typeof raw.name !== 'string') return [];
      return [
        {
          type: 'function',
          function: {
            name: raw.name,
            description: typeof raw.description === 'string' ? raw.description : '',
            parameters: isObj(raw.input_schema) ? raw.input_schema : { type: 'object', properties: {} },
          },
        },
      ];
    });
    if (tools.length > 0) out.tools = tools;
  }
  const choice = body.tool_choice;
  if (isObj(choice)) {
    if (choice.type === 'auto') out.tool_choice = 'auto';
    else if (choice.type === 'any') out.tool_choice = 'required';
    else if (choice.type === 'tool' && typeof choice.name === 'string') {
      out.tool_choice = { type: 'function', function: { name: choice.name } };
    }
  }
  if (body.stream === true) {
    out.stream = true;
    out.stream_options = { include_usage: true };
  }
  return out;
}

/** A Chat Completions body as an Anthropic Messages body naming `model`. */
function openAIToAnthropicReq(body: JsonObj, model: string): JsonObj {
  const messages: JsonObj[] = [];
  const systemParts: string[] = [];
  const source = Array.isArray(body.messages) ? body.messages : [];
  for (const raw of source) {
    if (!isObj(raw)) continue;
    if (raw.role === 'system') {
      const text = textOf(raw.content);
      if (text !== '') systemParts.push(text);
      continue;
    }
    if (raw.role === 'tool') {
      messages.push({
        role: 'user',
        content: [
          {
            type: 'tool_result',
            tool_use_id: typeof raw.tool_call_id === 'string' ? raw.tool_call_id : '',
            content: typeof raw.content === 'string' ? raw.content : JSON.stringify(raw.content ?? ''),
          },
        ],
      });
      continue;
    }
    const fromAssistant = raw.role === 'assistant';
    const content = raw.content;
    if (typeof content === 'string') {
      messages.push({ role: fromAssistant ? 'assistant' : 'user', content });
      continue;
    }
    if (!Array.isArray(content)) continue;
    if (fromAssistant) {
      const blocks: JsonObj[] = [];
      const text = textOf(content);
      if (text !== '') blocks.push({ type: 'text', text });
      const calls = Array.isArray(raw.tool_calls) ? raw.tool_calls : [];
      for (const call of calls) {
        if (!isObj(call) || !isObj(call.function)) continue;
        blocks.push({
          type: 'tool_use',
          id: typeof call.id === 'string' ? call.id : '',
          name: typeof call.function.name === 'string' ? call.function.name : '',
          input: argsObj(call.function.arguments),
        });
      }
      if (blocks.length > 0) messages.push({ role: 'assistant', content: blocks });
      continue;
    }
    const blocks: JsonObj[] = [];
    for (const part of content) {
      if (!isObj(part)) continue;
      if (part.type === 'text' && typeof part.text === 'string') blocks.push({ type: 'text', text: part.text });
      else if (part.type === 'image_url' && isObj(part.image_url) && typeof part.image_url.url === 'string') {
        const url = part.image_url.url;
        const data = splitDataUrl(url);
        if (data !== null) {
          blocks.push({ type: 'image', source: { type: 'base64', media_type: data.mime, data: data.data } });
        } else {
          blocks.push({ type: 'image', source: { type: 'url', url } });
        }
      }
    }
    if (blocks.length > 0) messages.push({ role: 'user', content: blocks });
  }
  const out: JsonObj = { model, messages };
  if (systemParts.length > 0) out.system = systemParts.join('\n');
  if (typeof body.max_tokens === 'number') out.max_tokens = body.max_tokens;
  if (typeof body.temperature === 'number') out.temperature = body.temperature;
  if (typeof body.top_p === 'number') out.top_p = body.top_p;
  if (Array.isArray(body.stop) && body.stop.length > 0) out.stop_sequences = body.stop;
  if (Array.isArray(body.tools)) {
    const tools = body.tools.flatMap((raw) => {
      if (!isObj(raw) || !isObj(raw.function) || typeof raw.function.name !== 'string') return [];
      return [
        {
          name: raw.function.name,
          description: typeof raw.function.description === 'string' ? raw.function.description : '',
          input_schema: isObj(raw.function.parameters) ? raw.function.parameters : { type: 'object', properties: {} },
        },
      ];
    });
    if (tools.length > 0) out.tools = tools;
  }
  const choice = body.tool_choice;
  if (choice === 'auto') out.tool_choice = { type: 'auto' };
  else if (choice === 'required') out.tool_choice = { type: 'any' };
  else if (choice === 'none') out.tool_choice = { type: 'none' };
  else if (isObj(choice) && choice.type === 'function' && isObj(choice.function) && typeof choice.function.name === 'string') {
    out.tool_choice = { type: 'tool', name: choice.function.name };
  }
  if (body.stream === true) out.stream = true;
  return out;
}

/** A Chat Completions body as a Gemini generateContent body for `model`. */
function openAIToGeminiReq(body: JsonObj, _model: string): JsonObj {
  const contents: JsonObj[] = [];
  const systemParts: string[] = [];
  const source = Array.isArray(body.messages) ? body.messages : [];
  for (const raw of source) {
    if (!isObj(raw)) continue;
    if (raw.role === 'system') {
      const text = textOf(raw.content);
      if (text !== '') systemParts.push(text);
      continue;
    }
    if (raw.role === 'tool') {
      // Tool results ride as model-role parts: the Gemini parser only reads
      // user-role text for its echo, so this keeps the echo faithful.
      contents.push({ role: 'model', parts: [{ text: typeof raw.content === 'string' ? raw.content : JSON.stringify(raw.content ?? '') }] });
      continue;
    }
    const fromAssistant = raw.role === 'assistant';
    const parts: JsonObj[] = [];
    const content = raw.content;
    if (typeof content === 'string') {
      if (content !== '') parts.push({ text: content });
    } else if (Array.isArray(content)) {
      for (const part of content) {
        if (!isObj(part)) continue;
        if (part.type === 'text' && typeof part.text === 'string') parts.push({ text: part.text });
        else if (part.type === 'image_url' && isObj(part.image_url) && typeof part.image_url.url === 'string') {
          const url = part.image_url.url;
          const data = splitDataUrl(url);
          if (data !== null) parts.push({ inlineData: { mimeType: data.mime, data: data.data } });
          else parts.push({ fileData: { mimeType: 'image/png', fileUri: url } });
        }
      }
    }
    const calls = fromAssistant && Array.isArray(raw.tool_calls) ? raw.tool_calls : [];
    for (const call of calls) {
      if (!isObj(call) || !isObj(call.function)) continue;
      parts.push({
        functionCall: {
          name: typeof call.function.name === 'string' ? call.function.name : '',
          args: argsObj(call.function.arguments),
        },
      });
    }
    if (parts.length > 0) contents.push({ role: fromAssistant ? 'model' : 'user', parts });
  }
  const out: JsonObj = { contents };
  if (systemParts.length > 0) out.systemInstruction = { parts: [{ text: systemParts.join('\n') }] };
  if (Array.isArray(body.tools)) {
    const decls = body.tools.flatMap((raw) => {
      if (!isObj(raw) || !isObj(raw.function) || typeof raw.function.name !== 'string') return [];
      return [
        {
          name: raw.function.name,
          description: typeof raw.function.description === 'string' ? raw.function.description : '',
          parameters: isObj(raw.function.parameters) ? raw.function.parameters : { type: 'object', properties: {} },
        },
      ];
    });
    if (decls.length > 0) out.tools = [{ functionDeclarations: decls }];
  }
  const choice = body.tool_choice;
  if (choice === 'required') out.toolConfig = { functionCallingConfig: { mode: 'ANY' } };
  else if (isObj(choice) && choice.type === 'function' && isObj(choice.function) && typeof choice.function.name === 'string') {
    out.toolConfig = { functionCallingConfig: { mode: 'ANY', allowedFunctionNames: [choice.function.name] } };
  }
  const config: JsonObj = {};
  const maxTok = typeof body.max_tokens === 'number' ? body.max_tokens : typeof body.max_completion_tokens === 'number' ? body.max_completion_tokens : null;
  if (maxTok !== null) config.maxOutputTokens = maxTok;
  if (typeof body.temperature === 'number') config.temperature = body.temperature;
  if (typeof body.top_p === 'number') config.topP = body.top_p;
  if (Array.isArray(body.stop) && body.stop.length > 0) config.stopSequences = body.stop;
  if (isObj(body.response_format) && (body.response_format.type === 'json_object' || body.response_format.type === 'json_schema')) {
    config.responseMimeType = 'application/json';
  }
  if (Object.keys(config).length > 0) out.generationConfig = config;
  return out;
}

/** An Anthropic Messages body as a Gemini generateContent body for `model`. */
function anthropicToGeminiReq(body: JsonObj, _model: string): JsonObj {
  const contents: JsonObj[] = [];
  const namesById = new Map<string, string>();
  const source = Array.isArray(body.messages) ? body.messages : [];
  for (const raw of source) {
    if (!isObj(raw) || !Array.isArray(raw.content)) continue;
    for (const block of raw.content) {
      if (isObj(block) && block.type === 'tool_use' && typeof block.id === 'string' && typeof block.name === 'string') {
        namesById.set(block.id, block.name);
      }
    }
  }
  for (const raw of source) {
    if (!isObj(raw)) continue;
    const fromAssistant = raw.role === 'assistant';
    const parts: JsonObj[] = [];
    const content = raw.content;
    if (typeof content === 'string') {
      if (content !== '') parts.push({ text: content });
    } else if (Array.isArray(content)) {
      for (const block of content) {
        if (!isObj(block)) continue;
        if (block.type === 'text' && typeof block.text === 'string') parts.push({ text: block.text });
        else if (block.type === 'image' && isObj(block.source)) {
          if (block.source.type === 'base64' && typeof block.source.data === 'string') {
            parts.push({ inlineData: { mimeType: typeof block.source.media_type === 'string' ? block.source.media_type : 'image/png', data: block.source.data } });
          } else if (block.source.type === 'url' && typeof block.source.url === 'string') {
            parts.push({ fileData: { mimeType: 'image/png', fileUri: block.source.url } });
          }
        } else if (block.type === 'tool_use') {
          parts.push({
            functionCall: {
              name: typeof block.name === 'string' ? block.name : '',
              args: isObj(block.input) ? block.input : {},
            },
          });
        } else if (block.type === 'tool_result') {
          const useId = typeof block.tool_use_id === 'string' ? block.tool_use_id : '';
          parts.push({
            functionResponse: {
              name: namesById.get(useId) ?? useId,
              response: { content: toolResultText(block.content) },
            },
          });
        }
      }
    }
    if (parts.length > 0) contents.push({ role: fromAssistant ? 'model' : 'user', parts });
  }
  const out: JsonObj = { contents };
  const system = textOf(body.system);
  if (system !== '') out.systemInstruction = { parts: [{ text: system }] };
  if (Array.isArray(body.tools)) {
    const decls = body.tools.flatMap((raw) => {
      if (!isObj(raw) || typeof raw.name !== 'string') return [];
      return [
        {
          name: raw.name,
          description: typeof raw.description === 'string' ? raw.description : '',
          parameters: isObj(raw.input_schema) ? raw.input_schema : { type: 'object', properties: {} },
        },
      ];
    });
    if (decls.length > 0) out.tools = [{ functionDeclarations: decls }];
  }
  const choice = body.tool_choice;
  if (isObj(choice)) {
    if (choice.type === 'any') out.toolConfig = { functionCallingConfig: { mode: 'ANY' } };
    else if (choice.type === 'tool' && typeof choice.name === 'string') {
      out.toolConfig = { functionCallingConfig: { mode: 'ANY', allowedFunctionNames: [choice.name] } };
    }
  }
  const config: JsonObj = {};
  if (typeof body.max_tokens === 'number') config.maxOutputTokens = body.max_tokens;
  if (typeof body.temperature === 'number') config.temperature = body.temperature;
  if (typeof body.top_p === 'number') config.topP = body.top_p;
  if (Array.isArray(body.stop_sequences) && body.stop_sequences.length > 0) config.stopSequences = body.stop_sequences;
  if (Object.keys(config).length > 0) out.generationConfig = config;
  return out;
}

// ---------------------------------------------------------------------------
// Response translators: upstream answer -> client answer (JSON bodies)
// ---------------------------------------------------------------------------

/** An OpenAI finish_reason as an Anthropic stop_reason. */
function openAIFinishToAnthropic(finish: unknown): string {
  if (finish === 'length') return 'max_tokens';
  if (finish === 'tool_calls' || finish === 'function_call') return 'tool_use';
  if (finish === 'content_filter') return 'refusal';
  return 'end_turn';
}

/** An Anthropic stop_reason as an OpenAI finish_reason. */
function anthropicStopToOpenAI(stop: unknown): string {
  if (stop === 'max_tokens') return 'length';
  if (stop === 'tool_use') return 'tool_calls';
  if (stop === 'refusal') return 'content_filter';
  return 'stop';
}

/** A Gemini finishReason as an OpenAI finish_reason. */
function geminiFinishToOpenAI(finish: unknown): string {
  if (finish === 'MAX_TOKENS') return 'length';
  if (finish === 'TOOL_CALLS') return 'tool_calls';
  if (finish === 'SAFETY') return 'content_filter';
  return 'stop';
}

/** A Gemini finishReason as an Anthropic stop_reason. */
function geminiFinishToAnthropic(finish: unknown): string {
  if (finish === 'MAX_TOKENS') return 'max_tokens';
  if (finish === 'TOOL_CALLS') return 'tool_use';
  if (finish === 'SAFETY') return 'refusal';
  return 'end_turn';
}

function numOrZero(v: unknown): number {
  return typeof v === 'number' && Number.isFinite(v) ? v : 0;
}

/** The text of an Anthropic message's content blocks, joined flat. */
function anthropicText(content: unknown): string {
  if (typeof content === 'string') return content;
  if (!Array.isArray(content)) return '';
  const parts: string[] = [];
  for (const block of content) {
    if (isObj(block) && block.type === 'text' && typeof block.text === 'string') parts.push(block.text);
  }
  return parts.join('');
}

/** A Chat Completions answer as an Anthropic Messages object. */
function openAIToAnthropicMsg(completion: JsonObj, clientModel: string): JsonObj {
  const choices = Array.isArray(completion.choices) ? completion.choices : [];
  const first = isObj(choices[0]) ? choices[0] : {};
  const message = isObj(first.message) ? first.message : {};
  const blocks: JsonObj[] = [];
  const text = typeof message.content === 'string' ? message.content : anthropicText(message.content);
  if (text !== '') blocks.push({ type: 'text', text });
  const calls = Array.isArray(message.tool_calls) ? message.tool_calls : [];
  for (const call of calls) {
    if (!isObj(call) || !isObj(call.function)) continue;
    blocks.push({
      type: 'tool_use',
      id: typeof call.id === 'string' ? call.id : 'toolu_unknown',
      name: typeof call.function.name === 'string' ? call.function.name : '',
      input: argsObj(call.function.arguments),
    });
  }
  if (blocks.length === 0) blocks.push({ type: 'text', text: '' });
  const usage = isObj(completion.usage) ? completion.usage : {};
  return {
    id: `msg_${typeof completion.id === 'string' ? completion.id : 'served'}`,
    type: 'message',
    role: 'assistant',
    model: clientModel,
    content: blocks,
    stop_reason: openAIFinishToAnthropic(first.finish_reason),
    stop_sequence: null,
    usage: { input_tokens: numOrZero(usage.prompt_tokens), output_tokens: numOrZero(usage.completion_tokens) },
  };
}

/** An Anthropic Messages answer as a Chat Completions object. */
function anthropicToOpenAICompletion(message: JsonObj, servedModel: string): JsonObj {
  const blocks = Array.isArray(message.content) ? message.content : [];
  const textParts: string[] = [];
  const calls: JsonObj[] = [];
  for (const block of blocks) {
    if (!isObj(block)) continue;
    if (block.type === 'text' && typeof block.text === 'string') textParts.push(block.text);
    else if (block.type === 'tool_use') {
      calls.push({
        id: typeof block.id === 'string' ? block.id : '',
        type: 'function',
        function: {
          name: typeof block.name === 'string' ? block.name : '',
          arguments: JSON.stringify(isObj(block.input) ? block.input : {}),
        },
      });
    }
  }
  const usage = isObj(message.usage) ? message.usage : {};
  const out: JsonObj = {
    id: 'chatcmpl-served',
    object: 'chat.completion',
    created: OPENAI_CREATED,
    model: servedModel,
    choices: [
      {
        index: 0,
        message: {
          role: 'assistant',
          content: textParts.length > 0 ? textParts.join('') : calls.length > 0 ? null : '',
          ...(calls.length > 0 ? { tool_calls: calls } : {}),
        },
        finish_reason: anthropicStopToOpenAI(message.stop_reason),
        logprobs: null,
      },
    ],
    usage: {
      prompt_tokens: numOrZero(usage.input_tokens),
      completion_tokens: numOrZero(usage.output_tokens),
      total_tokens: numOrZero(usage.input_tokens) + numOrZero(usage.output_tokens),
    },
  };
  return out;
}

/** The text and tool calls of a Gemini candidate's parts. */
function geminiParts(parts: unknown): { text: string; calls: JsonObj[] } {
  const texts: string[] = [];
  const calls: JsonObj[] = [];
  if (!Array.isArray(parts)) return { text: '', calls };
  let n = 0;
  for (const part of parts) {
    if (!isObj(part)) continue;
    if (typeof part.text === 'string') texts.push(part.text);
    else if (isObj(part.functionCall)) {
      calls.push({
        id: `call-${n}`,
        type: 'function',
        function: {
          name: typeof part.functionCall.name === 'string' ? part.functionCall.name : '',
          arguments: JSON.stringify(isObj(part.functionCall.args) ? part.functionCall.args : {}),
        },
      });
      n += 1;
    }
  }
  return { text: texts.join(''), calls };
}

/** A Gemini generateContent answer as a Chat Completions object. */
function geminiToOpenAICompletion(reply: JsonObj, servedModel: string): JsonObj {
  const candidates = Array.isArray(reply.candidates) ? reply.candidates : [];
  const first = isObj(candidates[0]) ? candidates[0] : {};
  const content = isObj(first.content) ? first.content : {};
  const { text, calls } = geminiParts(content.parts);
  const meta = isObj(reply.usageMetadata) ? reply.usageMetadata : {};
  const promptTok = numOrZero(meta.promptTokenCount);
  const completionTok = numOrZero(meta.candidatesTokenCount);
  return {
    id: 'chatcmpl-served',
    object: 'chat.completion',
    created: OPENAI_CREATED,
    model: servedModel,
    choices: [
      {
        index: 0,
        message: {
          role: 'assistant',
          content: text !== '' ? text : calls.length > 0 ? null : '',
          ...(calls.length > 0 ? { tool_calls: calls } : {}),
        },
        finish_reason: geminiFinishToOpenAI(first.finishReason),
        logprobs: null,
      },
    ],
    usage: { prompt_tokens: promptTok, completion_tokens: completionTok, total_tokens: promptTok + completionTok },
  };
}

/** A Gemini generateContent answer as an Anthropic Messages object. */
function geminiToAnthropicMsg(reply: JsonObj, clientModel: string): JsonObj {
  const candidates = Array.isArray(reply.candidates) ? reply.candidates : [];
  const first = isObj(candidates[0]) ? candidates[0] : {};
  const content = isObj(first.content) ? first.content : {};
  const { text, calls } = geminiParts(content.parts);
  const blocks: JsonObj[] = [];
  if (text !== '') blocks.push({ type: 'text', text });
  for (const call of calls) {
    if (!isObj(call.function)) continue;
    blocks.push({
      type: 'tool_use',
      id: typeof call.id === 'string' ? call.id : 'toolu_unknown',
      name: typeof call.function.name === 'string' ? call.function.name : '',
      input: argsObj(call.function.arguments),
    });
  }
  if (blocks.length === 0) blocks.push({ type: 'text', text: '' });
  const meta = isObj(reply.usageMetadata) ? reply.usageMetadata : {};
  return {
    id: `msg_${typeof reply.modelVersion === 'string' ? reply.modelVersion : 'served'}`,
    type: 'message',
    role: 'assistant',
    model: clientModel,
    content: blocks,
    stop_reason: geminiFinishToAnthropic(first.finishReason),
    stop_sequence: null,
    usage: { input_tokens: numOrZero(meta.promptTokenCount), output_tokens: numOrZero(meta.candidatesTokenCount) },
  };
}

// ---------------------------------------------------------------------------
// Stream translators: one upstream event text -> one client event text
// ---------------------------------------------------------------------------

interface SseFrame {
  event: string | null;
  data: string;
}

/** Split SSE text into frames of {event, data}. */
function parseFrames(text: string): SseFrame[] {
  const frames: SseFrame[] = [];
  const normalized = text.replace(/\r\n/g, '\n');
  for (const chunk of normalized.split('\n\n')) {
    if (chunk.trim() === '') continue;
    let event: string | null = null;
    const data: string[] = [];
    for (const line of chunk.split('\n')) {
      if (line.startsWith('event:')) event = line.slice('event:'.length).trim();
      else if (line.startsWith('data:')) data.push(line.slice('data:'.length).replace(/^ /, ''));
    }
    if (data.length > 0) frames.push({ event, data: data.join('\n') });
  }
  return frames;
}

/** One SSE frame: an `event:` line when named, then the JSON data line. */
function frameOut(event: string | null, data: unknown): string {
  return `${event === null ? '' : `event: ${event}\n`}data: ${JSON.stringify(data)}\n\n`;
}

function openAIChunk(delta: JsonObj, finish: string | null, model: string): JsonObj {
  return {
    id: 'chatcmpl-served',
    object: 'chat.completion.chunk',
    created: OPENAI_CREATED,
    model,
    choices: [{ index: 0, delta, finish_reason: finish }],
  };
}

/** An OpenAI event stream as Anthropic's event sequence. */
function openAIStreamToAnthropicText(upstreamText: string, clientModel: string): string {
  const frames = parseFrames(upstreamText);
  const out: string[] = [];
  let started = false;
  let finished = false;
  let textIdx = -1;
  let nextIdx = 0;
  const toolIdxByUpstream = new Map<number, number>();
  let inTok = 0;
  let outTok = 0;
  let pendingStop: string | null = null;
  const startEvents = (): void => {
    if (started) return;
    started = true;
    out.push(
      frameOut('message_start', {
        type: 'message_start',
        message: {
          id: 'msg_stream',
          type: 'message',
          role: 'assistant',
          model: clientModel,
          content: [],
          stop_reason: null,
          stop_sequence: null,
          usage: { input_tokens: inTok, output_tokens: 0 },
        },
      }),
    );
  };
  const closeBlocks = (): void => {
    if (textIdx !== -1) {
      out.push(frameOut('content_block_stop', { type: 'content_block_stop', index: textIdx }));
      textIdx = -1;
    }
    for (const idx of [...toolIdxByUpstream.values()].sort((a, b) => a - b)) {
      out.push(frameOut('content_block_stop', { type: 'content_block_stop', index: idx }));
    }
    toolIdxByUpstream.clear();
  };
  const finishEvents = (stop: string): void => {
    if (finished) return;
    finished = true;
    closeBlocks();
    out.push(
      frameOut('message_delta', {
        type: 'message_delta',
        delta: { stop_reason: stop, stop_sequence: null },
        usage: { output_tokens: outTok },
      }),
      frameOut('message_stop', { type: 'message_stop' }),
    );
  };
  for (const frame of frames) {
    const payload = frame.data.trim();
    if (payload === '[DONE]') {
      startEvents();
      finishEvents(pendingStop ?? 'end_turn');
      continue;
    }
    let chunk: unknown;
    try {
      chunk = JSON.parse(payload);
    } catch {
      continue;
    }
    if (!isObj(chunk)) continue;
    if (isObj(chunk.usage)) {
      inTok = numOrZero(chunk.usage.prompt_tokens);
      outTok = numOrZero(chunk.usage.completion_tokens);
    }
    const choices = Array.isArray(chunk.choices) ? chunk.choices : [];
    const choice = isObj(choices[0]) ? choices[0] : null;
    if (choice === null) continue;
    const delta = isObj(choice.delta) ? choice.delta : {};
    startEvents();
    if (typeof delta.content === 'string' && delta.content !== '') {
      if (textIdx === -1) {
        textIdx = nextIdx;
        nextIdx += 1;
        out.push(frameOut('content_block_start', { type: 'content_block_start', index: textIdx, content_block: { type: 'text', text: '' } }));
      }
      out.push(frameOut('content_block_delta', { type: 'content_block_delta', index: textIdx, delta: { type: 'text_delta', text: delta.content } }));
    }
    const calls = Array.isArray(delta.tool_calls) ? delta.tool_calls : [];
    for (const call of calls) {
      if (!isObj(call)) continue;
      const upIdx = typeof call.index === 'number' ? call.index : toolIdxByUpstream.size;
      let idx = toolIdxByUpstream.get(upIdx);
      if (idx === undefined) {
        if (textIdx !== -1) {
          out.push(frameOut('content_block_stop', { type: 'content_block_stop', index: textIdx }));
          textIdx = -1;
        }
        idx = nextIdx;
        nextIdx += 1;
        toolIdxByUpstream.set(upIdx, idx);
        const fn = isObj(call.function) ? call.function : {};
        out.push(
          frameOut('content_block_start', {
            type: 'content_block_start',
            index: idx,
            content_block: {
              type: 'tool_use',
              id: typeof call.id === 'string' && call.id !== '' ? call.id : `toolu_${idx}`,
              name: typeof fn.name === 'string' ? fn.name : '',
              input: {},
            },
          }),
        );
      }
      const fn = isObj(call.function) ? call.function : {};
      if (typeof fn.arguments === 'string' && fn.arguments !== '') {
        out.push(frameOut('content_block_delta', { type: 'content_block_delta', index: idx, delta: { type: 'input_json_delta', partial_json: fn.arguments } }));
      }
    }
    if (choice.finish_reason !== null && choice.finish_reason !== undefined) {
      pendingStop = openAIFinishToAnthropic(choice.finish_reason);
    }
  }
  if (!finished) {
    startEvents();
    finishEvents(pendingStop ?? 'end_turn');
  }
  return out.join('');
}

/** An Anthropic event stream as OpenAI's chunk sequence. */
function anthropicStreamToOpenAIText(upstreamText: string, servedModel: string): string {
  const frames = parseFrames(upstreamText);
  const out: string[] = [];
  let roleSent = false;
  let inTok = 0;
  let outTok = 0;
  let stopReason: string | null = null;
  let stopped = false;
  let nextToolIdx = 0;
  const toolIdxByBlock = new Map<number, number>();
  const firstChunk = (): void => {
    if (roleSent) return;
    roleSent = true;
    out.push(`data: ${JSON.stringify(openAIChunk({ role: 'assistant', content: '' }, null, servedModel))}\n\n`);
  };
  const closeOut = (): void => {
    if (stopped) return;
    stopped = true;
    firstChunk();
    out.push(`data: ${JSON.stringify(openAIChunk({}, stopReason ?? 'stop', servedModel))}\n\n`);
    out.push(
      `data: ${JSON.stringify({
        id: 'chatcmpl-served',
        object: 'chat.completion.chunk',
        created: OPENAI_CREATED,
        model: servedModel,
        choices: [],
        usage: { prompt_tokens: inTok, completion_tokens: outTok, total_tokens: inTok + outTok },
      })}\n\n`,
    );
    out.push('data: [DONE]\n\n');
  };
  for (const frame of frames) {
    if (frame.event === null) continue;
    let data: unknown;
    try {
      data = JSON.parse(frame.data);
    } catch {
      continue;
    }
    if (!isObj(data)) continue;
    if (frame.event === 'message_start') {
      if (isObj(data.message) && isObj(data.message.usage)) inTok = numOrZero(data.message.usage.input_tokens);
      firstChunk();
    } else if (frame.event === 'content_block_start') {
      const idx = numVal(data.index);
      if (idx === null) continue;
      const block = isObj(data.content_block) ? data.content_block : {};
      if (block.type === 'tool_use') {
        const openIdx = nextToolIdx;
        nextToolIdx += 1;
        toolIdxByBlock.set(idx, openIdx);
        firstChunk();
        out.push(
          `data: ${JSON.stringify(
            openAIChunk(
              {
                tool_calls: [
                  {
                    index: openIdx,
                    id: typeof block.id === 'string' ? block.id : '',
                    type: 'function',
                    function: { name: typeof block.name === 'string' ? block.name : '', arguments: '' },
                  },
                ],
              },
              null,
              servedModel,
            ),
          )}\n\n`,
        );
      } else {
        firstChunk();
      }
    } else if (frame.event === 'content_block_delta') {
      const idx = numVal(data.index);
      if (idx === null) continue;
      const delta = isObj(data.delta) ? data.delta : {};
      if (delta.type === 'text_delta' && typeof delta.text === 'string' && delta.text !== '') {
        firstChunk();
        out.push(`data: ${JSON.stringify(openAIChunk({ content: delta.text }, null, servedModel))}\n\n`);
      } else if (delta.type === 'input_json_delta' && typeof delta.partial_json === 'string' && delta.partial_json !== '') {
        const openIdx = toolIdxByBlock.get(idx);
        if (openIdx === undefined) continue;
        firstChunk();
        out.push(
          `data: ${JSON.stringify(
            openAIChunk({ tool_calls: [{ index: openIdx, function: { arguments: delta.partial_json } }] }, null, servedModel),
          )}\n\n`,
        );
      }
    } else if (frame.event === 'message_delta') {
      if (isObj(data.delta) && typeof data.delta.stop_reason === 'string') stopReason = anthropicStopToOpenAI(data.delta.stop_reason);
      if (isObj(data.usage)) outTok = numOrZero(data.usage.output_tokens);
    } else if (frame.event === 'message_stop') {
      closeOut();
    }
  }
  closeOut();
  return out.join('');
}

/** A Gemini :streamGenerateContent text as OpenAI's chunk sequence. */
function geminiStreamToOpenAIText(upstreamText: string, servedModel: string): string {
  const frames = parseFrames(upstreamText);
  const out: string[] = [];
  let roleSent = false;
  let toolIdx = 0;
  let pendingStop: string | null = null;
  let inTok = 0;
  let outTok = 0;
  const firstChunk = (): void => {
    if (roleSent) return;
    roleSent = true;
    out.push(`data: ${JSON.stringify(openAIChunk({ role: 'assistant', content: '' }, null, servedModel))}\n\n`);
  };
  for (const frame of frames) {
    let reply: unknown;
    try {
      reply = JSON.parse(frame.data);
    } catch {
      continue;
    }
    if (!isObj(reply)) continue;
    const candidates = Array.isArray(reply.candidates) ? reply.candidates : [];
    const first = isObj(candidates[0]) ? candidates[0] : {};
    const content = isObj(first.content) ? first.content : {};
    const parts = Array.isArray(content.parts) ? content.parts : [];
    for (const part of parts) {
      if (!isObj(part)) continue;
      if (typeof part.text === 'string' && part.text !== '') {
        firstChunk();
        out.push(`data: ${JSON.stringify(openAIChunk({ content: part.text }, null, servedModel))}\n\n`);
      } else if (isObj(part.functionCall)) {
        firstChunk();
        const idx = toolIdx;
        toolIdx += 1;
        out.push(
          `data: ${JSON.stringify(
            openAIChunk(
              {
                tool_calls: [
                  {
                    index: idx,
                    id: `call-${idx}`,
                    type: 'function',
                    function: {
                      name: typeof part.functionCall.name === 'string' ? part.functionCall.name : '',
                      arguments: JSON.stringify(isObj(part.functionCall.args) ? part.functionCall.args : {}),
                    },
                  },
                ],
              },
              null,
              servedModel,
            ),
          )}\n\n`,
        );
      }
    }
    if (typeof first.finishReason === 'string') pendingStop = geminiFinishToOpenAI(first.finishReason);
    if (isObj(reply.usageMetadata)) {
      inTok = numOrZero(reply.usageMetadata.promptTokenCount);
      outTok = numOrZero(reply.usageMetadata.candidatesTokenCount);
    }
  }
  firstChunk();
  out.push(`data: ${JSON.stringify(openAIChunk({}, pendingStop ?? 'stop', servedModel))}\n\n`);
  out.push(
    `data: ${JSON.stringify({
      id: 'chatcmpl-served',
      object: 'chat.completion.chunk',
      created: OPENAI_CREATED,
      model: servedModel,
      choices: [],
      usage: { prompt_tokens: inTok, completion_tokens: outTok, total_tokens: inTok + outTok },
    })}\n\n`,
  );
  out.push('data: [DONE]\n\n');
  return out.join('');
}

/** A Gemini :streamGenerateContent text as Anthropic's event sequence. */
function geminiStreamToAnthropicText(upstreamText: string, clientModel: string): string {
  const frames = parseFrames(upstreamText);
  const out: string[] = [];
  let started = false;
  let finished = false;
  let textIdx = -1;
  let nextIdx = 0;
  let outTok = 0;
  let pendingStop: string | null = null;
  const startEvents = (): void => {
    if (started) return;
    started = true;
    out.push(
      frameOut('message_start', {
        type: 'message_start',
        message: {
          id: 'msg_stream',
          type: 'message',
          role: 'assistant',
          model: clientModel,
          content: [],
          stop_reason: null,
          stop_sequence: null,
          usage: { input_tokens: 0, output_tokens: 0 },
        },
      }),
    );
  };
  const closeBlocks = (): void => {
    if (textIdx !== -1) {
      out.push(frameOut('content_block_stop', { type: 'content_block_stop', index: textIdx }));
      textIdx = -1;
    }
  };
  const finishEvents = (stop: string): void => {
    if (finished) return;
    finished = true;
    closeBlocks();
    out.push(
      frameOut('message_delta', {
        type: 'message_delta',
        delta: { stop_reason: stop, stop_sequence: null },
        usage: { output_tokens: outTok },
      }),
      frameOut('message_stop', { type: 'message_stop' }),
    );
  };
  for (const frame of frames) {
    let reply: unknown;
    try {
      reply = JSON.parse(frame.data);
    } catch {
      continue;
    }
    if (!isObj(reply)) continue;
    const candidates = Array.isArray(reply.candidates) ? reply.candidates : [];
    const first = isObj(candidates[0]) ? candidates[0] : {};
    const content = isObj(first.content) ? first.content : {};
    const parts = Array.isArray(content.parts) ? content.parts : [];
    startEvents();
    for (const part of parts) {
      if (!isObj(part)) continue;
      if (typeof part.text === 'string' && part.text !== '') {
        if (textIdx === -1) {
          textIdx = nextIdx;
          nextIdx += 1;
          out.push(frameOut('content_block_start', { type: 'content_block_start', index: textIdx, content_block: { type: 'text', text: '' } }));
        }
        out.push(frameOut('content_block_delta', { type: 'content_block_delta', index: textIdx, delta: { type: 'text_delta', text: part.text } }));
      } else if (isObj(part.functionCall)) {
        if (textIdx !== -1) {
          out.push(frameOut('content_block_stop', { type: 'content_block_stop', index: textIdx }));
          textIdx = -1;
        }
        const idx = nextIdx;
        nextIdx += 1;
        out.push(
          frameOut('content_block_start', {
            type: 'content_block_start',
            index: idx,
            content_block: {
              type: 'tool_use',
              id: `toolu_${idx}`,
              name: typeof part.functionCall.name === 'string' ? part.functionCall.name : '',
              input: isObj(part.functionCall.args) ? part.functionCall.args : {},
            },
          }),
        );
        out.push(frameOut('content_block_stop', { type: 'content_block_stop', index: idx }));
      }
    }
    if (typeof first.finishReason === 'string') pendingStop = geminiFinishToAnthropic(first.finishReason);
    if (isObj(reply.usageMetadata)) outTok = numOrZero(reply.usageMetadata.candidatesTokenCount);
  }
  if (!finished) {
    startEvents();
    finishEvents(pendingStop ?? 'end_turn');
  }
  return out.join('');
}

// ---------------------------------------------------------------------------
// Needs detection from the client's own body
// ---------------------------------------------------------------------------

/** Total text characters in the system prompt plus the message bodies. */
function inputTextChars(clientApi: ClientApi, body: JsonObj): number {
  let total = 0;
  if (clientApi === 'anthropic') total += textOf(body.system).length;
  const messages = Array.isArray(body.messages) ? body.messages : [];
  for (const raw of messages) {
    if (!isObj(raw)) continue;
    const content = raw.content;
    if (typeof content === 'string') total += content.length;
    else if (Array.isArray(content)) total += textOf(content).length;
  }
  return total;
}

/** Whether any message carries an image. */
function hasVision(clientApi: ClientApi, body: JsonObj): boolean {
  const messages = Array.isArray(body.messages) ? body.messages : [];
  for (const raw of messages) {
    if (!isObj(raw) || !Array.isArray(raw.content)) continue;
    for (const part of raw.content) {
      if (!isObj(part)) continue;
      if (clientApi === 'openai' && part.type === 'image_url') return true;
      if (clientApi === 'anthropic' && part.type === 'image') return true;
    }
  }
  return false;
}

/** The requirement class of one request, read off its own body. */
function needsFromBody(clientApi: ClientApi, body: JsonObj): NeedSet {
  const needs: NeedSet = {};
  if (Array.isArray(body.tools) && body.tools.length > 0) needs.tools = true;
  if (hasVision(clientApi, body)) needs.vision = true;
  if (clientApi === 'openai' && isObj(body.response_format)) {
    const kind = body.response_format.type;
    if (kind === 'json_object' || kind === 'json_schema') needs.json = true;
  }
  // A long prompt is the only signal a request carries for a large window:
  // ordinary prompts run a few hundred characters; the long-context ones run
  // past ten thousand. Anything over two thousand is safely in that class.
  if (inputTextChars(clientApi, body) > 2000) needs.minContext = 200000;
  return needs;
}

// ---------------------------------------------------------------------------
// Upstream calls and the failover driver
// ---------------------------------------------------------------------------

/** The full upstream URL for one provider and one endpoint path. */
function upstreamUrl(provider: BriefProvider, endpoint: string): string {
  return `${joinUrl(ENV_URL, provider.path)}/${endpoint}`;
}

interface UpstreamOk {
  ok: true;
  text: string;
}

interface UpstreamFail {
  ok: false;
  status: number;
  retryable: boolean;
}

type UpstreamResult = UpstreamOk | UpstreamFail;

/** One attempt at one provider: translate, call, and report the outcome. */
async function callProvider(pick: Pick, clientApi: ClientApi, body: JsonObj, streaming: boolean): Promise<UpstreamResult> {
  const wire = pick.provider.wire;
  const model = pick.model.id;
  let upBody: JsonObj;
  let url: string;
  if (clientApi === 'openai') {
    if (wire === 'openai') {
      upBody = { ...body, model };
      if (streaming) upBody.stream_options = { include_usage: true };
      url = upstreamUrl(pick.provider, 'v1/chat/completions');
    } else if (wire === 'anthropic') {
      upBody = openAIToAnthropicReq(body, model);
      url = upstreamUrl(pick.provider, 'v1/messages');
    } else {
      upBody = openAIToGeminiReq(body, model);
      url = upstreamUrl(pick.provider, `v1beta/models/${encodeURIComponent(model)}:${streaming ? 'streamGenerateContent?alt=sse' : 'generateContent'}`);
    }
  } else if (wire === 'openai') {
    upBody = anthropicToOpenAIReq(body, model);
    url = upstreamUrl(pick.provider, 'v1/chat/completions');
  } else if (wire === 'anthropic') {
    upBody = { ...body, model };
    url = upstreamUrl(pick.provider, 'v1/messages');
  } else {
    upBody = anthropicToGeminiReq(body, model);
    url = upstreamUrl(pick.provider, `v1beta/models/${encodeURIComponent(model)}:${streaming ? 'streamGenerateContent?alt=sse' : 'generateContent'}`);
  }
  const headers: Record<string, string> = { 'content-type': 'application/json', ...keyHeaders(pick.provider.auth, pick.provider.secret) };
  markAttempt(pick.provider.id);
  let resp: Response;
  try {
    resp = await fetch(url, { method: 'POST', headers, body: JSON.stringify(upBody) });
  } catch {
    // Unreachable: a refusal, and a reason to try the next pick.
    markRefusal(pick.provider.id);
    return { ok: false, status: 0, retryable: true };
  }
  const text = await resp.text();
  if (resp.ok) {
    markServed(pick.provider.id);
    return { ok: true, text };
  }
  // Only a refusal counts toward the two-in-a-row demotion: a 400 or 401 is
  // the request's fault, not the provider's, so it leaves the counters alone.
  const retryable = resp.status === 402 || resp.status === 429 || resp.status >= 500;
  if (retryable) markRefusal(pick.provider.id);
  return { ok: false, status: resp.status, retryable };
}

/** An error body in the client's own API shape. */
function errorBody(clientApi: ClientApi, message: string): string {
  if (clientApi === 'anthropic') {
    return JSON.stringify({ type: 'error', error: { type: 'api_error', message } });
  }
  return JSON.stringify({ error: { message, type: 'server_error' } });
}

/** A successful upstream answer, translated back into the client's shape. */
function translateSuccess(pick: Pick, clientApi: ClientApi, clientModel: string, text: string, streaming: boolean): { status: number; body: string; sse: boolean } {
  const wire = pick.provider.wire;
  if (clientApi === 'openai' && wire === 'openai') return { status: 200, body: text, sse: streaming };
  if (clientApi === 'anthropic' && wire === 'anthropic') return { status: 200, body: text, sse: streaming };
  if (streaming) {
    if (clientApi === 'openai') {
      if (wire === 'anthropic') return { status: 200, body: anthropicStreamToOpenAIText(text, pick.model.id), sse: true };
      return { status: 200, body: geminiStreamToOpenAIText(text, pick.model.id), sse: true };
    }
    if (wire === 'openai') return { status: 200, body: openAIStreamToAnthropicText(text, clientModel), sse: true };
    return { status: 200, body: geminiStreamToAnthropicText(text, clientModel), sse: true };
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return { status: 502, body: errorBody(clientApi, 'upstream returned invalid JSON'), sse: false };
  }
  if (!isObj(parsed)) return { status: 502, body: errorBody(clientApi, 'upstream returned invalid JSON'), sse: false };
  let out: JsonObj;
  if (clientApi === 'openai') {
    out = wire === 'anthropic' ? anthropicToOpenAICompletion(parsed, pick.model.id) : geminiToOpenAICompletion(parsed, pick.model.id);
  } else {
    out = wire === 'openai' ? openAIToAnthropicMsg(parsed, clientModel) : geminiToAnthropicMsg(parsed, clientModel);
  }
  return { status: 200, body: JSON.stringify(out), sse: false };
}

/** Serve one chat request: walk the ordered picks until one answers. */
async function serveChat(clientApi: ClientApi, body: JsonObj): Promise<{ status: number; body: string; sse: boolean }> {
  const streaming = body.stream === true;
  const needs = needsFromBody(clientApi, body);
  const clientModel = typeof body.model === 'string' && body.model !== '' ? body.model : 'auto';
  const tried = new Set<string>();
  let lastStatus = 502;
  for (;;) {
    const candidates = orderPicks(needs, clientApi, tried);
    if (candidates.length === 0) break;
    const pick = candidates[0];
    tried.add(`${pick.provider.id}/${pick.model.id}`);
    const result = await callProvider(pick, clientApi, body, streaming);
    if (result.ok) return translateSuccess(pick, clientApi, clientModel, result.text, streaming);
    lastStatus = result.status === 0 ? 502 : result.status;
  }
  return { status: 502, body: errorBody(clientApi, `all providers failed (last status ${lastStatus})`), sse: false };
}

// ---------------------------------------------------------------------------
// HTTP server
// ---------------------------------------------------------------------------

function readBody(req: IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on('data', (chunk: Buffer | string) => chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)));
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}

function sendJson(res: ServerResponse, status: number, value: unknown): void {
  const text = typeof value === 'string' ? value : JSON.stringify(value);
  res.writeHead(status, { 'content-type': 'application/json' });
  res.end(text);
}

function sendSse(res: ServerResponse, text: string): void {
  res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache', connection: 'keep-alive' });
  res.end(text);
}

/** The models list in the shape the caller's auth header asks for. */
function modelsResponse(authHeader: string | undefined): JsonObj {
  const entries = catalogDoc.providers.flatMap((provider) =>
    provider.free_models.map((id) => ({ id, provider: provider.id })),
  );
  if (authHeader !== undefined && authHeader !== '') {
    return {
      object: 'list',
      data: entries.map((entry) => ({ id: entry.id, object: 'model', created_at: ANTHROPIC_CREATED, display_name: entry.id })),
    };
  }
  return {
    object: 'list',
    data: entries.map((entry) => ({ id: entry.id, object: 'model', created: OPENAI_CREATED, owned_by: entry.provider })),
  };
}

async function handleChat(req: IncomingMessage, res: ServerResponse, clientApi: ClientApi): Promise<void> {
  const raw = await readBody(req);
  let body: unknown;
  try {
    body = JSON.parse(raw);
  } catch {
    sendJson(res, 400, errorBody(clientApi, 'invalid JSON'));
    return;
  }
  if (!isObj(body)) {
    sendJson(res, 400, errorBody(clientApi, 'request body must be a JSON object'));
    return;
  }
  const result = await serveChat(clientApi, body);
  if (result.sse) sendSse(res, result.body);
  else sendJson(res, result.status, result.body);
}

async function requestHandler(req: IncomingMessage, res: ServerResponse): Promise<void> {
  const path = (req.url ?? '/').split('?')[0];
  const isAnthropic = path.startsWith('/v1/messages');
  try {
    if (req.method === 'GET' && path === '/v1/models') {
      const header = req.headers['x-api-key'];
      sendJson(res, 200, modelsResponse(typeof header === 'string' ? header : undefined));
      return;
    }
    if (req.method === 'POST' && path === '/v1/chat/completions') {
      await handleChat(req, res, 'openai');
      return;
    }
    if (req.method === 'POST' && path === '/v1/messages') {
      await handleChat(req, res, 'anthropic');
      return;
    }
    sendJson(res, 404, errorBody(isAnthropic ? 'anthropic' : 'openai', `no route ${req.method ?? ''} ${path}`));
  } catch {
    if (!res.headersSent) sendJson(res, 500, errorBody(isAnthropic ? 'anthropic' : 'openai', 'the server failed to answer'));
  }
}

// ---------------------------------------------------------------------------
// Bootstrap
// ---------------------------------------------------------------------------

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

process.on('SIGTERM', () => process.exit(0));
process.on('SIGINT', () => process.exit(0));

async function main(): Promise<void> {
  const listOnly = process.argv.slice(2).includes('--list');
  await loadCatalog();
  if (listOnly) {
    process.stdout.write(`${JSON.stringify(catalogDoc)}\n`);
    return;
  }
  if (ENV_URL === '') throw new Error('MAKEFUN_ENV_URL is not set');
  const server = createServer((req, res) => {
    void requestHandler(req, res);
  });
  await new Promise<void>((resolve) => server.listen(0, '0.0.0.0', resolve));
  const address = server.address();
  const port = typeof address === 'object' && address !== null ? address.port : 0;
  if (port === 0 || port === 8080) throw new Error('could not bind an ephemeral port');
  const registerRes = await fetch(`${ENV_URL}/register`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ protocol_version: 1, port, catalog: catalogDoc }),
  });
  if (!registerRes.ok) throw new Error(`register failed with status ${registerRes.status}`);
  for (;;) {
    await sleep(100);
    let work: unknown;
    try {
      const res = await fetch(`${ENV_URL}/work`, { signal: AbortSignal.timeout(2000) });
      work = await res.json();
    } catch {
      continue;
    }
    if (isObj(work) && work.done === true) break;
  }
  server.close();
  process.exit(0);
}

main().catch((err: unknown) => {
  console.error(err instanceof Error ? err.message : String(err));
  process.exit(1);
});
