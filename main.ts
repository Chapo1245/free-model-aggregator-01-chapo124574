#!/usr/bin/env node
/**
 * free-model-aggregator-01 -- the best free AI model aggregator.
 *
 * One stdlib-only Node 22 server. It speaks the OpenAI Chat Completions API
 * and the Anthropic Messages API (JSON and SSE), routes every request to the
 * best free model that can serve it across ten simulated free-tier providers,
 * fails over when a provider refuses or runs out of budget, registers its
 * catalog at POST $MAKEFUN_ENV_URL/register, and exits 0 when the env says
 * the workload is done.
 *
 * Routing policy (see POLICY below; VARIANTS.md for the variants graded):
 *   - rank candidates by the sealed quality rating for the exact requirement
 *     class of the request (tools / vision / json / min_context / plain);
 *   - never send a JSON-mode request down an Anthropic-wire provider
 *     (that wire cannot carry response_format, so the echo would break);
 *   - prefer a provider whose wire matches the client's wire (no translation
 *     means no translation risk) when ratings tie;
 *   - after 2 consecutive refusals a provider is demoted to the end of the
 *     order until it serves a request again.
 *
 * Determinism: no wall clock and no unseeded randomness anywhere in routing
 * or translation, so a replay leaves identical state.
 */
import { readFileSync } from "node:fs";
import { createServer } from "node:http";
import type { IncomingMessage, ServerResponse } from "node:http";

const PROTOCOL_VERSION = 1;
const POLL_INTERVAL_MS = 100;
const PROBE_TIMEOUT_MS = 5000;
const UPSTREAM_TIMEOUT_MS = 25000;
const MAX_BODY_BYTES = 32 * 1024 * 1024;
const FIXED_CREATED = 1767225600;
const FIXED_ANTHROPIC_CREATED_AT = "2026-01-01T00:00:00Z";

// Routing policy. Variants graded in VARIANTS.md change only these values.
const POLICY = {
  mode: "greedy", // "greedy" | "spread"
  demoteAfter: 2, // consecutive refusals before a provider is demoted
  spreadWindow: 25, // spread mode: rating milli within this of the best
  useGemini: true, // false: never route to a gemini-wire provider
};

type Wire = "openai" | "anthropic" | "gemini";
type AuthScheme = "bearer" | "x-api-key" | "x-goog-api-key" | "none";
type Api = "openai" | "anthropic";

const PROVIDERS_SCHEMA = "free-model-aggregator-01/providers/v1";
const RATINGS_SCHEMA = "free-model-aggregator-01/ratings/v1";
const WORKLOAD_SCHEMA = "free-model-aggregator-01/workload/v1";

/** One provider as the brief lists it. `base` is the base path (or absolute URL) its routes hang off. */
interface ProviderDoc {
  id: string;
  name: string;
  wire: Wire;
  auth: AuthScheme;
  base: string;
  key_env: string;
  key: string;
}

/** One row of the sealed quality table (the brief's copy). */
interface RatingRow {
  model: string;
  provider: string;
  rating_milli: number;
  context_length: number | null;
  tools: boolean;
  vision: boolean;
  json: boolean;
  free: boolean;
}

interface WorkloadRequest {
  id: string;
  api: Api;
  stream: boolean;
  weight: 1 | 2;
  requires: { tools?: boolean; vision?: boolean; json?: boolean; min_context?: number };
}

interface Brief {
  directory: { schema: string; providers: ProviderDoc[] };
  ratings: { schema: string; ratings: RatingRow[]; best: { id: string; rating_milli: number }[] } | null;
  workload: { schema: string; manifest: WorkloadRequest[] } | null;
  error: string | null;
}

interface ListingModel {
  id: string;
  display_name: string;
  free: boolean | null;
  context_length: number | null;
  tools: boolean | null;
  vision: boolean | null;
  json: boolean | null;
}

interface Probe {
  provider: ProviderDoc;
  ok: boolean;
  error: string;
  models: ListingModel[];
}

interface CatalogModel {
  id: string;
  free: boolean;
  context_length: number | null;
}

interface CatalogProvider {
  id: string;
  base_url: string;
  key_env: string;
  wire: string;
  models: CatalogModel[];
}

interface Catalog {
  providers: CatalogProvider[];
}

interface Requirements {
  tools: boolean;
  vision: boolean;
  json: boolean;
  minContext: number;
  weight: number;
}

interface Candidate {
  provider: ProviderDoc;
  modelId: string;
  rating: number;
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

// ---------------------------------------------------------------------------
// brief parsing (scan the prompt file for JSON documents with a "schema" key)
// ---------------------------------------------------------------------------

function objectEnd(text: string, start: number): number {
  let depth = 0;
  let inString = false;
  let escaped = false;
  for (let i = start; i < text.length; i++) {
    const ch = text[i];
    if (inString) {
      if (escaped) escaped = false;
      else if (ch === "\\") escaped = true;
      else if (ch === '"') inString = false;
      continue;
    }
    if (ch === '"') inString = true;
    else if (ch === "{") depth++;
    else if (ch === "}") {
      depth--;
      if (depth === 0) return i;
    }
  }
  return -1;
}

function scanDocuments(text: string): any[] {
  const docs: any[] = [];
  let cursor = 0;
  for (;;) {
    const start = text.indexOf("{", cursor);
    if (start === -1) break;
    const end = objectEnd(text, start);
    if (end === -1) break;
    try {
      const value = JSON.parse(text.slice(start, end + 1));
      if (value && typeof value === "object" && typeof value.schema === "string") {
        docs.push(value);
      }
    } catch {
      // not a document; keep scanning
    }
    cursor = start + 1;
  }
  return docs;
}

function isRecord(v: unknown): v is Record<string, unknown> {
  return v !== null && typeof v === "object" && !Array.isArray(v);
}

function stringOf(v: unknown): string {
  return typeof v === "string" ? v : "";
}

function numberOrNull(v: unknown): number | null {
  return typeof v === "number" && Number.isFinite(v) ? v : null;
}

function boolOrNull(v: unknown): boolean | null {
  return typeof v === "boolean" ? v : null;
}

function providerOf(v: unknown): ProviderDoc | null {
  if (!isRecord(v)) return null;
  const id = stringOf(v.id);
  const wire = v.wire === "openai" || v.wire === "anthropic" || v.wire === "gemini" ? v.wire : null;
  if (id === "" || wire === null) return null;
  const auth =
    v.auth === "bearer" || v.auth === "x-api-key" || v.auth === "x-goog-api-key" || v.auth === "none"
      ? v.auth
      : "bearer";
  // The brief's directory writes `path` (/p/<id>, joined to $MAKEFUN_ENV_URL); a directory that spells an
  // absolute base instead may use base_path, base_url or base. Take the first non-empty, `path` first.
  const base =
    stringOf(v.path) !== ""
      ? stringOf(v.path)
      : stringOf(v.base_path) !== ""
        ? stringOf(v.base_path)
        : stringOf(v.base_url) !== ""
          ? stringOf(v.base_url)
          : stringOf(v.base);
  return {
    id,
    name: stringOf(v.name) !== "" ? stringOf(v.name) : id,
    wire,
    auth,
    base,
    key_env: stringOf(v.key_env),
    key: stringOf(v.key),
  };
}

function ratingOf(v: unknown): RatingRow | null {
  if (!isRecord(v)) return null;
  const model = stringOf(v.model);
  const provider = stringOf(v.provider);
  if (model === "" || provider === "") return null;
  return {
    model,
    provider,
    rating_milli: typeof v.rating_milli === "number" && Number.isFinite(v.rating_milli) ? v.rating_milli : 0,
    context_length: numberOrNull(v.context_length),
    tools: v.tools === true,
    vision: v.vision === true,
    json: v.json === true,
    free: v.free === true,
  };
}

function workloadRequestOf(v: unknown): WorkloadRequest | null {
  if (!isRecord(v)) return null;
  const id = stringOf(v.id);
  if (id === "") return null;
  const requires = isRecord(v.requires) ? v.requires : {};
  return {
    id,
    api: v.api === "anthropic" ? "anthropic" : "openai",
    stream: v.stream === true,
    weight: v.weight === 2 ? 2 : 1,
    requires: {
      tools: requires.tools === true,
      vision: requires.vision === true,
      json: requires.json === true,
      ...(typeof requires.min_context === "number" && Number.isFinite(requires.min_context)
        ? { min_context: requires.min_context }
        : {}),
    },
  };
}

/** The brief at `path`, its three documents found by schema. */
function readBrief(path: string): Brief {
  const directory = { schema: PROVIDERS_SCHEMA, providers: [] as ProviderDoc[] };
  if (path === "") {
    return { directory, ratings: null, workload: null, error: "MAKEFUN_PROMPT_FILE is not set" };
  }
  let text: string;
  try {
    text = readFileSync(path, "utf8");
  } catch (err) {
    return { directory, ratings: null, workload: null, error: `cannot read ${path}: ${(err as Error).message}` };
  }
  const docs = scanDocuments(text);
  const directoryDoc = docs.find((d) => d.schema === PROVIDERS_SCHEMA && Array.isArray(d.providers)) ?? null;
  if (directoryDoc === null || !Array.isArray(directoryDoc.providers)) {
    return { directory, ratings: null, workload: null, error: `the brief at ${path} holds no ${PROVIDERS_SCHEMA} document` };
  }
  const providers = directoryDoc.providers.flatMap((v: unknown) => {
    const p = providerOf(v);
    return p === null ? [] : [p];
  });
  const ratingsDoc = docs.find((d) => d.schema === RATINGS_SCHEMA && Array.isArray(d.ratings)) ?? null;
  const ratingRows = ratingsDoc !== null && Array.isArray(ratingsDoc.ratings) ? ratingsDoc.ratings : null;
  const ratings: Brief["ratings"] =
    ratingsDoc === null || ratingRows === null
      ? null
      : {
          schema: RATINGS_SCHEMA,
          ratings: ratingRows.flatMap((v: unknown) => {
            const r = ratingOf(v);
            return r === null ? [] : [r];
          }),
          best: Array.isArray(ratingsDoc.best)
            ? ratingsDoc.best.flatMap((v: unknown) => {
                if (!isRecord(v) || stringOf(v.id) === "") return [];
                return [{ id: stringOf(v.id), rating_milli: numberOrNull(v.rating_milli) ?? 0 }];
              })
            : [],
        };
  const workloadDoc = docs.find((d) => d.schema === WORKLOAD_SCHEMA && Array.isArray(d.manifest)) ?? null;
  const workload: Brief["workload"] =
    workloadDoc === null || !Array.isArray(workloadDoc.manifest)
      ? null
      : {
          schema: WORKLOAD_SCHEMA,
          manifest: workloadDoc.manifest.flatMap((v: unknown) => {
            const w = workloadRequestOf(v);
            return w === null ? [] : [w];
          }),
        };
  return { directory: { schema: PROVIDERS_SCHEMA, providers }, ratings, workload, error: null };
}

/** (provider, model) -> rating row, the sealed quality table. */
function ratingRows(ctx: Brief | null): Map<string, RatingRow> {
  const out = new Map<string, RatingRow>();
  if (!ctx?.ratings) return out;
  for (const r of ctx.ratings.ratings) {
    out.set(r.provider + "\0" + r.model, r);
  }
  return out;
}

/** The workload manifest, in order. */
function workloadRequests(ctx: Brief | null): WorkloadRequest[] {
  return ctx?.workload?.manifest ?? [];
}

function joinBase(envUrl: string, base: string): string {
  const b = base.trim();
  if (/^https?:\/\//i.test(b)) return b.replace(/\/+$/, "");
  const prefix = envUrl.replace(/\/+$/, "");
  if (b === "") return prefix;
  return `${prefix}${b.startsWith("/") ? "" : "/"}${b.replace(/\/+$/, "")}`;
}

/** The headers a provider wants its key in. The key comes from the brief itself. */
function authHeaders(auth: AuthScheme, key: string): Record<string, string> {
  if (key === "") return {};
  if (auth === "bearer") return { authorization: `Bearer ${key}` };
  if (auth === "x-api-key") return { "x-api-key": key };
  if (auth === "x-goog-api-key") return { "x-goog-api-key": key };
  return {};
}

/** A price field of a listing: true when every price is zero, null when the listing says nothing. */
function zeroPriced(v: Record<string, unknown>): boolean | null {
  const pricing = isRecord(v.pricing) ? v.pricing : null;
  if (pricing === null) return null;
  const values = Object.values(pricing).flatMap((x) => {
    if (typeof x === "number") return [x];
    if (typeof x === "string" && x.trim() !== "") return [Number(x)];
    return [];
  });
  if (values.length === 0) return null;
  return values.every((n) => Number.isFinite(n) && n === 0);
}

function parseListingModels(wire: Wire, body: any): ListingModel[] {
  const out: ListingModel[] = [];
  const modelIn = (item: any): ListingModel | null => {
    if (!item || typeof item !== "object") return null;
    const id =
      wire === "gemini"
        ? String(item.name ?? "").replace(/^models\//, "")
        : typeof item.id === "string"
          ? item.id
          : "";
    if (!id) return null;
    const display =
      typeof item.display_name === "string" && item.display_name !== ""
        ? item.display_name
        : typeof item.displayName === "string" && item.displayName !== ""
          ? item.displayName
          : id;
    return {
      id,
      display_name: display,
      free: boolOrNull(item.free) ?? zeroPriced(item),
      context_length:
        numberOrNull(item.context_length) ?? numberOrNull(item.context_window) ?? numberOrNull(item.inputTokenLimit),
      tools: boolOrNull(item.tools),
      vision: boolOrNull(item.vision),
      json: boolOrNull(item.json),
    };
  };
  const data = wire === "gemini" ? body?.models : body?.data;
  if (!Array.isArray(data)) return out;
  for (const item of data) {
    const m = modelIn(item);
    if (m !== null) out.push(m);
  }
  return out;
}

async function probeProvider(envUrl: string, provider: ProviderDoc): Promise<Probe> {
  const base = joinBase(envUrl, provider.base);
  const url = base + (provider.wire === "gemini" ? "/v1beta/models" : "/v1/models");
  const headers: Record<string, string> = { accept: "application/json", ...authHeaders(provider.auth, provider.key) };
  if (provider.wire === "anthropic") headers["anthropic-version"] = "2023-06-01";
  try {
    const res = await fetch(url, {
      headers,
      signal: AbortSignal.timeout(PROBE_TIMEOUT_MS),
    });
    if (res.status !== 200) {
      return { provider, ok: false, error: "HTTP " + res.status, models: [] };
    }
    const body = await res.json();
    const models = parseListingModels(provider.wire, body);
    const seen = new Set<string>();
    const uniq = models.filter((m) => {
      if (seen.has(m.id)) return false;
      seen.add(m.id);
      return true;
    });
    uniq.sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
    return { provider, ok: true, error: "", models: uniq };
  } catch (err) {
    return { provider, ok: false, error: String(err).slice(0, 120), models: [] };
  }
}

function buildCatalog(probes: Probe[], rows: Map<string, RatingRow>): Catalog {
  const providers: CatalogProvider[] = [];
  for (const probe of probes) {
    if (!probe.ok) continue;
    const p = probe.provider;
    const models: CatalogModel[] = [];
    for (const m of probe.models) {
      const row = rows.get(p.id + "\0" + m.id);
      let free: boolean;
      let ctxLen: number | null;
      if (row) {
        free = row.free;
        ctxLen = row.context_length;
      } else {
        free = m.free === true;
        ctxLen = m.context_length;
      }
      models.push({ id: m.id, free, context_length: ctxLen });
    }
    providers.push({
      id: p.id,
      base_url: p.base,
      key_env: p.key_env,
      wire: p.wire,
      models,
    });
  }
  return { providers };
}

/** The free models across the providers that answered, in the brief's provider order. */
function servedModels(probes: Probe[], rows: Map<string, RatingRow>): { id: string; provider: string; display_name: string }[] {
  const out: { id: string; provider: string; display_name: string }[] = [];
  for (const probe of probes) {
    if (!probe.ok) continue;
    for (const model of [...probe.models].sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0))) {
      const row = rows.get(probe.provider.id + "\0" + model.id);
      const free = row ? row.free : model.free === true;
      if (!free) continue;
      out.push({ id: model.id, provider: probe.provider.id, display_name: model.display_name });
    }
  }
  return out;
}

function freeModelIds(catalog: Catalog): string[] {
  const out: string[] = [];
  const seen = new Set<string>();
  for (const p of catalog.providers) {
    for (const m of p.models) {
      if (m.free && !seen.has(m.id)) {
        seen.add(m.id);
        out.push(m.id);
      }
    }
  }
  return out;
}

// ---------------------------------------------------------------------------
// capabilities and requirements
// ---------------------------------------------------------------------------

function capabilities(model: ListingModel, row: RatingRow | undefined): [boolean, boolean, boolean, number] {
  if (row) return [!!row.tools, !!row.vision, !!row.json, row.context_length || 0];
  return [
    model.tools === true,
    model.vision === true,
    model.json === true,
    typeof model.context_length === "number" ? model.context_length : 0,
  ];
}

function requirementsFromManifest(ctx: Brief | null, index: number): Requirements {
  const reqs = workloadRequests(ctx);
  if (index >= 0 && index < reqs.length) {
    const item = reqs[index];
    const requires = item?.requires ?? {};
    if (item && typeof item === "object") {
      return {
        tools: !!requires.tools,
        vision: !!requires.vision,
        json: !!requires.json,
        minContext: requires.min_context || 0,
        weight: item.weight || 1,
      };
    }
  }
  return { tools: false, vision: false, json: false, minContext: 0, weight: 1 };
}

function requirementsFromBody(api: Api, body: any): Requirements {
  let tools = false;
  let vision = false;
  if (Array.isArray(body?.tools) && body.tools.length > 0) tools = true;
  for (const msg of body?.messages ?? []) {
    const content = msg?.content;
    if (Array.isArray(content)) {
      for (const block of content) {
        if (block?.type === "image_url" || block?.type === "image") vision = true;
      }
    }
  }
  let jsonMode = false;
  if (api === "openai") {
    const fmt = body?.response_format;
    jsonMode = fmt?.type === "json_object" || fmt?.type === "json_schema";
  }
  return { tools, vision, json: jsonMode, minContext: 0, weight: 1 };
}

function meetsRequirements(caps: [boolean, boolean, boolean, number], req: Requirements): boolean {
  const [tools, vision, json, ctxLen] = caps;
  if (req.tools && !tools) return false;
  if (req.vision && !vision) return false;
  if (req.json && !json) return false;
  if (req.minContext && ctxLen < req.minContext) return false;
  return true;
}

// ---------------------------------------------------------------------------
// routing
// ---------------------------------------------------------------------------

class Router {
  calls = new Map<string, number>();
  refusals = new Map<string, number>();
  providerIndex = new Map<string, number>();
  private probes: Probe[];
  private rows: Map<string, RatingRow>;

  constructor(probes: Probe[], rows: Map<string, RatingRow>) {
    this.probes = probes;
    this.rows = rows;
    probes.forEach((probe, i) => {
      this.calls.set(probe.provider.id, 0);
      this.refusals.set(probe.provider.id, 0);
      this.providerIndex.set(probe.provider.id, i);
    });
  }

  candidates(req: Requirements, clientWire: Api, jsonMode: boolean): Candidate[] {
    const out: Candidate[] = [];
    for (const probe of this.probes) {
      if (!probe.ok) continue;
      const provider = probe.provider;
      const wire = provider.wire;
      if (!POLICY.useGemini && wire === "gemini") continue;
      if (jsonMode && wire === "anthropic") continue; // cannot carry response_format
      for (const model of probe.models) {
        const row = this.rows.get(provider.id + "\0" + model.id);
        if (row) {
          if (!row.free) continue;
        } else if (!model.free) {
          continue;
        }
        if (!meetsRequirements(capabilities(model, row), req)) continue;
        out.push({
          provider,
          modelId: model.id,
          rating: row ? row.rating_milli || 0 : 0,
        });
      }
    }
    return this.order(out, req.weight, clientWire);
  }

  private order(cands: Candidate[], weight: number, clientWire: Api): Candidate[] {
    const demoted = (c: Candidate) =>
      (this.refusals.get(c.provider.id) ?? 0) >= POLICY.demoteAfter ? 1 : 0;
    const wireMismatch = (c: Candidate) => (c.provider.wire === clientWire ? 0 : 1);
    const idx = (c: Candidate) => this.providerIndex.get(c.provider.id) ?? 0;
    const calls = (c: Candidate) => this.calls.get(c.provider.id) ?? 0;

    if (POLICY.mode === "spread" && weight === 1 && cands.length > 0) {
      const best = Math.max(...cands.map((c) => c.rating));
      const near = cands.filter((c) => c.rating >= best - POLICY.spreadWindow);
      const far = cands.filter((c) => c.rating < best - POLICY.spreadWindow);
      near.sort(
        (a, b) =>
          demoted(a) - demoted(b) ||
          calls(a) - calls(b) ||
          b.rating - a.rating ||
          wireMismatch(a) - wireMismatch(b) ||
          idx(a) - idx(b),
      );
      far.sort(
        (a, b) =>
          demoted(a) - demoted(b) ||
          b.rating - a.rating ||
          wireMismatch(a) - wireMismatch(b) ||
          calls(a) - calls(b) ||
          idx(a) - idx(b),
      );
      return [...near, ...far];
    }
    return [...cands].sort(
      (a, b) =>
        demoted(a) - demoted(b) ||
        b.rating - a.rating ||
        wireMismatch(a) - wireMismatch(b) ||
        calls(a) - calls(b) ||
        idx(a) - idx(b),
    );
  }

  noteAttempt(providerId: string): void {
    this.calls.set(providerId, (this.calls.get(providerId) ?? 0) + 1);
  }

  noteSuccess(providerId: string): void {
    this.refusals.set(providerId, 0);
  }

  noteRefusal(providerId: string): void {
    this.refusals.set(providerId, (this.refusals.get(providerId) ?? 0) + 1);
  }
}


// ---------------------------------------------------------------------------
// message helpers (shared by the request translations)
// ---------------------------------------------------------------------------

function blocksText(value: any): string {
  if (typeof value === "string") return value;
  if (Array.isArray(value)) {
    return value
      .map((block) => {
        if (block && typeof block === "object" && block.type === "text") return block.text ?? "";
        if (block && typeof block === "object" && typeof block.text === "string") return block.text;
        return "";
      })
      .join("\n");
  }
  return "";
}

function openaiContentText(content: any): string {
  if (typeof content === "string") return content;
  if (Array.isArray(content)) {
    return content
      .map((b) => (b?.type === "text" ? b.text ?? "" : ""))
      .join("");
  }
  return "";
}

function anthropicImageToOpenai(source: any): any {
  if (!source || typeof source !== "object") return null;
  if (source.type === "base64") {
    return {
      type: "image_url",
      image_url: { url: `data:${source.media_type ?? ""};base64,${source.data ?? ""}` },
    };
  }
  if (source.type === "url") {
    return { type: "image_url", image_url: { url: source.url ?? "" } };
  }
  return null;
}

function openaiImageToGemini(block: any): any {
  const url = block?.image_url?.url ?? "";
  if (url.startsWith("data:")) {
    const comma = url.indexOf(",");
    const header = comma >= 0 ? url.slice(5, comma) : "";
    const mime = header.split(";")[0] || "image/png";
    return { inlineData: { mimeType: mime, data: comma >= 0 ? url.slice(comma + 1) : "" } };
  }
  return { fileData: { fileUri: url } };
}

function anthropicImageToGemini(block: any): any {
  const source = block?.source ?? {};
  if (source.type === "base64") {
    return {
      inlineData: { mimeType: source.media_type ?? "image/png", data: source.data ?? "" },
    };
  }
  return { fileData: { fileUri: source.url ?? "" } };
}

// ---------------------------------------------------------------------------
// request translation: client body -> provider wire body
// ---------------------------------------------------------------------------

function openaiToAnthropicRequest(body: any, modelId: string): any {
  const out: any = { model: modelId };
  const systemParts: string[] = [];
  const messages: any[] = [];
  for (const msg of body?.messages ?? []) {
    const role = msg?.role;
    const content = msg?.content;
    if (role === "system") {
      systemParts.push(openaiContentText(content));
      continue;
    }
    if (role === "tool") {
      messages.push({
        role: "user",
        content: [
          {
            type: "tool_result",
            tool_use_id: msg.tool_call_id ?? "",
            content: openaiContentText(content),
          },
        ],
      });
      continue;
    }
    const blocks: any[] = [];
    if (typeof content === "string") {
      if (content) blocks.push({ type: "text", text: content });
    } else if (Array.isArray(content)) {
      for (const block of content) {
        if (!block || typeof block !== "object") continue;
        if (block.type === "text") {
          blocks.push({ type: "text", text: block.text ?? "" });
        } else if (block.type === "image_url") {
          const url = block.image_url?.url ?? "";
          if (url.startsWith("data:")) {
            const comma = url.indexOf(",");
            const header = comma >= 0 ? url.slice(5, comma) : "";
            const mime = header.split(";")[0] || "image/png";
            blocks.push({
              type: "image",
              source: {
                type: "base64",
                media_type: mime,
                data: comma >= 0 ? url.slice(comma + 1) : "",
              },
            });
          } else {
            blocks.push({ type: "image", source: { type: "url", url } });
          }
        }
      }
    }
    for (const call of msg?.tool_calls ?? []) {
      const fn = call?.function ?? {};
      let args: any = {};
      try {
        args = JSON.parse(fn.arguments ?? "{}");
      } catch {
        args = {};
      }
      blocks.push({ type: "tool_use", id: call.id ?? "", name: fn.name ?? "", input: args });
    }
    messages.push({ role: role === "user" ? "user" : "assistant", content: blocks });
  }
  if (systemParts.length > 0) out.system = systemParts.join("\n");
  out.messages = messages;
  const tools = body?.tools;
  if (Array.isArray(tools) && tools.length > 0) {
    out.tools = tools.map((tool: any) => ({
      name: tool?.function?.name ?? "",
      description: tool?.function?.description ?? "",
      input_schema: tool?.function?.parameters ?? { type: "object" },
    }));
  }
  const choice = body?.tool_choice;
  if (choice === "auto") out.tool_choice = { type: "auto" };
  else if (choice === "required") out.tool_choice = { type: "any" };
  else if (choice === "none") out.tool_choice = { type: "none" };
  else if (choice && typeof choice === "object" && choice.type === "function") {
    out.tool_choice = { type: "tool", name: choice.function?.name ?? "" };
  }
  if (typeof body?.max_tokens === "number") out.max_tokens = body.max_tokens;
  else if (typeof body?.max_completion_tokens === "number") out.max_tokens = body.max_completion_tokens;
  if (typeof body?.temperature === "number") out.temperature = body.temperature;
  if (typeof body?.top_p === "number") out.top_p = body.top_p;
  if (body?.stop !== undefined && body.stop !== null) {
    out.stop_sequences = Array.isArray(body.stop) ? body.stop : [body.stop];
  }
  if (body?.stream) out.stream = true;
  return out;
}

/** A tool_result block's content as text: its text blocks, or its JSON when it holds something else. */
function toolResultText(content: unknown): string {
  if (typeof content === "string") return content;
  if (Array.isArray(content)) {
    const text = blocksText(content);
    return text !== "" ? text : JSON.stringify(content);
  }
  return content === undefined ? "" : JSON.stringify(content);
}

function anthropicToOpenaiRequest(body: any, modelId: string, stream: boolean): any {
  const messages: any[] = [];
  const system = blocksText(body?.system);
  if (system !== "") messages.push({ role: "system", content: system });
  const source = Array.isArray(body?.messages) ? body.messages : [];
  for (const raw of source) {
    if (!raw || typeof raw !== "object") continue;
    const assistant = raw.role === "assistant";
    const content = raw.content;
    if (typeof content === "string") {
      messages.push({ role: assistant ? "assistant" : "user", content });
      continue;
    }
    if (!Array.isArray(content)) continue;
    if (assistant) {
      const toolCalls: any[] = [];
      for (const block of content) {
        if (!block || typeof block !== "object" || block.type !== "tool_use") continue;
        toolCalls.push({
          id: typeof block.id === "string" ? block.id : "",
          type: "function",
          function: {
            name: typeof block.name === "string" ? block.name : "",
            arguments: JSON.stringify(block.input && typeof block.input === "object" ? block.input : {}),
          },
        });
      }
      const text = blocksText(content);
      messages.push({
        role: "assistant",
        content: text !== "" ? text : null,
        ...(toolCalls.length > 0 ? { tool_calls: toolCalls } : {}),
      });
      continue;
    }
    const parts: any[] = [];
    for (const block of content) {
      if (!block || typeof block !== "object") continue;
      if (block.type === "tool_result") {
        messages.push({
          role: "tool",
          tool_call_id: typeof block.tool_use_id === "string" ? block.tool_use_id : "",
          content: toolResultText(block.content),
        });
        continue;
      }
      const part = block.type === "image" ? anthropicImageToOpenai(block.source) : null;
      if (block.type === "text" && typeof block.text === "string") {
        parts.push({ type: "text", text: block.text });
      } else if (block.type === "image" && part !== null) {
        parts.push(part);
      }
    }
    if (parts.length > 0) messages.push({ role: "user", content: parts });
  }

  const out: any = { model: modelId, messages };
  if (typeof body?.max_tokens === "number") out.max_tokens = body.max_tokens;
  if (typeof body?.temperature === "number") out.temperature = body.temperature;
  if (typeof body?.top_p === "number") out.top_p = body.top_p;
  if (Array.isArray(body?.stop_sequences) && body.stop_sequences.length > 0) {
    out.stop = body.stop_sequences;
  }
  if (Array.isArray(body?.tools)) {
    const tools = body.tools.flatMap((raw: any) => {
      if (!raw || typeof raw !== "object" || typeof raw.name !== "string") return [];
      return [
        {
          type: "function",
          function: {
            name: raw.name,
            description: typeof raw.description === "string" ? raw.description : "",
            parameters:
              raw.input_schema && typeof raw.input_schema === "object"
                ? raw.input_schema
                : { type: "object", properties: {} },
          },
        },
      ];
    });
    if (tools.length > 0) out.tools = tools;
  }
  const choice = body?.tool_choice;
  if (choice && typeof choice === "object") {
    if (choice.type === "auto") out.tool_choice = "auto";
    else if (choice.type === "any") out.tool_choice = "required";
    else if (choice.type === "tool" && typeof choice.name === "string") {
      out.tool_choice = { type: "function", function: { name: choice.name } };
    }
  }
  if (body?.stream === true) {
    out.stream = true;
    out.stream_options = { include_usage: true };
  }
  return out;
}

function openaiMessageToGemini(msg: any): any {
  const role = msg?.role;
  const content = msg?.content;
  const parts: any[] = [];
  if (typeof content === "string") {
    if (content) parts.push({ text: content });
  } else if (Array.isArray(content)) {
    for (const block of content) {
      if (!block || typeof block !== "object") continue;
      if (block.type === "text") parts.push({ text: block.text ?? "" });
      else if (block.type === "image_url") parts.push(openaiImageToGemini(block));
    }
  }
  if (role === "tool") return { role: "user", parts };
  if (role === "assistant") return { role: "model", parts };
  return { role: "user", parts };
}

function anthropicMessageToGemini(msg: any): any {
  const role = msg?.role;
  const content = msg?.content;
  const parts: any[] = [];
  if (typeof content === "string") {
    parts.push({ text: content });
  } else if (Array.isArray(content)) {
    for (const block of content) {
      if (!block || typeof block !== "object") continue;
      if (block.type === "text") parts.push({ text: block.text ?? "" });
      else if (block.type === "image") parts.push(anthropicImageToGemini(block));
      else if (block.type === "tool_use") parts.push({ text: "tool call " + String(block.name ?? "") });
      else if (block.type === "tool_result") parts.push({ text: blocksText(block.content) });
    }
  }
  return { role: role === "assistant" ? "model" : "user", parts };
}

function buildGeminiRequest(
  clientApi: Api,
  body: any,
  modelId: string,
  stream: boolean,
): [string, any] {
  const contents: any[] = [];
  let systemText = "";
  const gen: any = {};
  let tools: any = null;
  if (clientApi === "openai") {
    for (const msg of body?.messages ?? []) {
      if (msg?.role === "system") {
        const text = openaiContentText(msg.content);
        if (text) systemText = systemText ? systemText + "\n" + text : text;
        continue;
      }
      contents.push(openaiMessageToGemini(msg));
    }
    if (typeof body?.max_tokens === "number") gen.maxOutputTokens = body.max_tokens;
    else if (typeof body?.max_completion_tokens === "number") gen.maxOutputTokens = body.max_completion_tokens;
    if (body?.stop !== undefined && body.stop !== null) {
      gen.stopSequences = Array.isArray(body.stop) ? body.stop : [body.stop];
    }
    const fmt = body?.response_format;
    if (fmt && typeof fmt === "object" && (fmt.type === "json_object" || fmt.type === "json_schema")) {
      gen.responseMimeType = "application/json";
    }
    if (typeof body?.temperature === "number") gen.temperature = body.temperature;
    if (typeof body?.top_p === "number") gen.topP = body.top_p;
    const src = body?.tools;
    if (Array.isArray(src) && src.length > 0) {
      tools = [
        {
          functionDeclarations: src.map((tool: any) => ({
            name: tool?.function?.name ?? "",
            description: tool?.function?.description ?? "",
            parameters: tool?.function?.parameters ?? { type: "object" },
          })),
        },
      ];
    }
  } else {
    if (body?.system !== undefined && body.system !== null) {
      systemText = blocksText(body.system);
    }
    for (const msg of body?.messages ?? []) {
      contents.push(anthropicMessageToGemini(msg));
    }
    if (typeof body?.max_tokens === "number") gen.maxOutputTokens = body.max_tokens;
    if (Array.isArray(body?.stop_sequences) && body.stop_sequences.length > 0) {
      gen.stopSequences = body.stop_sequences;
    }
    if (typeof body?.temperature === "number") gen.temperature = body.temperature;
    if (typeof body?.top_p === "number") gen.topP = body.top_p;
    const src = body?.tools;
    if (Array.isArray(src) && src.length > 0) {
      tools = [
        {
          functionDeclarations: src.map((tool: any) => ({
            name: tool?.name ?? "",
            description: tool?.description ?? "",
            parameters: tool?.input_schema ?? { type: "object" },
          })),
        },
      ];
    }
  }
  const payload: any = { contents };
  if (systemText) payload.systemInstruction = { parts: [{ text: systemText }] };
  if (Object.keys(gen).length > 0) payload.generationConfig = gen;
  if (tools) payload.tools = tools;
  const action = stream ? ":streamGenerateContent?alt=sse" : ":generateContent";
  return ["/v1beta/models/" + modelId + action, payload];
}


// ---------------------------------------------------------------------------
// SSE frame parsing
// ---------------------------------------------------------------------------

function splitSseFrames(text: string): Array<Array<[string, string]>> {
  const frames: Array<Array<[string, string]>> = [];
  let current: Array<[string, string]> = [];
  for (const rawLine of text.split("\n")) {
    const line = rawLine.endsWith("\r") ? rawLine.slice(0, -1) : rawLine;
    if (line === "") {
      if (current.length > 0) {
        frames.push(current);
        current = [];
      }
      continue;
    }
    if (line.startsWith(":")) continue;
    const colon = line.indexOf(":");
    if (colon >= 0) {
      current.push([line.slice(0, colon), line.slice(colon + 1).replace(/^ /, "")]);
    } else {
      current.push([line, ""]);
    }
  }
  if (current.length > 0) frames.push(current);
  return frames;
}

function frameData(frame: Array<[string, string]>): string {
  return frame
    .filter(([k]) => k === "data")
    .map(([, v]) => v)
    .join("\n");
}

function frameEvent(frame: Array<[string, string]>): string {
  for (const [k, v] of frame) if (k === "event") return v;
  return "";
}

function splitTextPieces(text: string): string[] {
  const out: string[] = [];
  for (let i = 0; i < text.length; i += 13) out.push(text.slice(i, i + 13));
  return out.length > 0 ? out : [""];
}

// ---------------------------------------------------------------------------
// upstream answer parsing (all three wires, JSON and SSE)
// ---------------------------------------------------------------------------

interface ParsedAnswer {
  text: string;
  input: number;
  output: number;
  stop: string; // already normalised to the stop family of the wire it came from
  id: string; // the upstream answer's id, when it has one
}

function parseOpenaiJson(raw: string): ParsedAnswer {
  const body = JSON.parse(raw);
  const choice = (body?.choices ?? [])[0] ?? {};
  const message = choice.message ?? {};
  let text = "";
  if (typeof message.content === "string") text = message.content;
  else if (Array.isArray(message.content)) {
    text = message.content
      .map((b: any) => (b?.type === "text" ? b.text ?? "" : ""))
      .join("");
  }
  const usage = body?.usage ?? {};
  return {
    text,
    input: usage.prompt_tokens || 0,
    output: usage.completion_tokens || 0,
    stop: choice.finish_reason ?? "stop",
    id: typeof body?.id === "string" ? body.id : "aggregate",
  };
}

function parseOpenaiSse(raw: string): ParsedAnswer {
  let text = "";
  let input = 0;
  let output = 0;
  let finish = "stop";
  for (const frame of splitSseFrames(raw)) {
    const data = frameData(frame);
    if (data === "[DONE]") break;
    let chunk: any;
    try {
      chunk = JSON.parse(data);
    } catch {
      continue;
    }
    if (chunk?.usage && typeof chunk.usage === "object") {
      input = chunk.usage.prompt_tokens || 0;
      output = chunk.usage.completion_tokens || 0;
    }
    const choices = chunk?.choices ?? [];
    if (choices.length === 0) continue;
    const delta = choices[0].delta ?? {};
    if (typeof delta.content === "string") text += delta.content;
    if (choices[0].finish_reason) finish = choices[0].finish_reason;
  }
  return { text, input, output, stop: finish, id: "aggregate" };
}

function parseAnthropicJson(raw: string): ParsedAnswer {
  const body = JSON.parse(raw);
  let text = "";
  for (const block of body?.content ?? []) {
    if (block?.type === "text") text += block.text ?? "";
  }
  const usage = body?.usage ?? {};
  return {
    text,
    input: usage.input_tokens || 0,
    output: usage.output_tokens || 0,
    stop: body?.stop_reason ?? "end_turn",
    id: typeof body?.id === "string" ? body.id : "aggregate",
  };
}

function parseAnthropicSse(raw: string): ParsedAnswer {
  let text = "";
  let input = 0;
  let output = 0;
  let stop = "end_turn";
  for (const frame of splitSseFrames(raw)) {
    const event = frameEvent(frame);
    const data = frameData(frame);
    if (!data) continue;
    let payload: any;
    try {
      payload = JSON.parse(data);
    } catch {
      continue;
    }
    if (event === "message_start") {
      input = payload?.message?.usage?.input_tokens || 0;
    } else if (event === "content_block_delta") {
      if (payload?.delta?.type === "text_delta") text += payload.delta.text ?? "";
    } else if (event === "message_delta") {
      if (payload?.delta?.stop_reason) stop = payload.delta.stop_reason;
      output = payload?.usage?.output_tokens || 0;
    }
  }
  return { text, input, output, stop, id: "aggregate" };
}

function geminiCandidateText(candidate: any): string {
  const parts = candidate?.content?.parts ?? [];
  return parts
    .map((p: any) => (typeof p?.text === "string" ? p.text : ""))
    .join("");
}

function parseGeminiJson(raw: string): ParsedAnswer {
  const body = JSON.parse(raw);
  const candidate = (body?.candidates ?? [])[0] ?? {};
  const usage = body?.usageMetadata ?? {};
  return {
    text: geminiCandidateText(candidate),
    input: usage.promptTokenCount || 0,
    output: usage.candidatesTokenCount || 0,
    stop: candidate.finishReason ?? "STOP",
    id: "aggregate",
  };
}

function parseGeminiSse(raw: string): ParsedAnswer {
  let text = "";
  let input = 0;
  let output = 0;
  let finish = "STOP";
  for (const frame of splitSseFrames(raw)) {
    const data = frameData(frame);
    if (!data) continue;
    let payload: any;
    try {
      payload = JSON.parse(data);
    } catch {
      continue;
    }
    const candidates = payload?.candidates ?? [];
    if (candidates.length > 0) {
      text += geminiCandidateText(candidates[0]);
      if (candidates[0].finishReason) finish = candidates[0].finishReason;
    }
    if (payload?.usageMetadata && typeof payload.usageMetadata === "object") {
      input = payload.usageMetadata.promptTokenCount || 0;
      output = payload.usageMetadata.candidatesTokenCount || 0;
    }
  }
  return { text, input, output, stop: finish, id: "aggregate" };
}

// ---------------------------------------------------------------------------
// stop-reason mapping between wires (all sim answers use stop family 0)
// ---------------------------------------------------------------------------

function openaiFinishFromAnthropic(stop: string): string {
  return (
    { end_turn: "stop", max_tokens: "length", tool_use: "tool_calls", stop_sequence: "stop", refusal: "content_filter" } as Record<string, string>
  )[stop] ?? "stop";
}

function openaiFinishFromGemini(finish: string): string {
  return (
    { STOP: "stop", MAX_TOKENS: "length", SAFETY: "content_filter" } as Record<string, string>
  )[finish] ?? "stop";
}

function anthropicStopFromOpenai(finish: string): string {
  return (
    { stop: "end_turn", length: "max_tokens", tool_calls: "tool_use", content_filter: "refusal" } as Record<string, string>
  )[finish] ?? "end_turn";
}

function anthropicStopFromGemini(finish: string): string {
  return (
    { STOP: "end_turn", MAX_TOKENS: "max_tokens", SAFETY: "refusal" } as Record<string, string>
  )[finish] ?? "end_turn";
}

// ---------------------------------------------------------------------------
// response assembly in the client's wire
// ---------------------------------------------------------------------------

function openaiCompletion(text: string, modelId: string, input: number, output: number, finish: string): string {
  return JSON.stringify({
    id: "chatcmpl-sim",
    object: "chat.completion",
    created: FIXED_CREATED,
    model: modelId,
    choices: [
      {
        index: 0,
        message: { role: "assistant", content: text },
        finish_reason: finish,
      },
    ],
    usage: { prompt_tokens: input, completion_tokens: output, total_tokens: input + output },
  });
}

function openaiSse(text: string, modelId: string, input: number, output: number, finish: string): string {
  const frames: any[] = [
    {
      id: "chatcmpl-sim",
      object: "chat.completion.chunk",
      created: FIXED_CREATED,
      model: modelId,
      choices: [{ index: 0, delta: { role: "assistant", content: "" }, finish_reason: null }],
    },
  ];
  for (const piece of splitTextPieces(text)) {
    frames.push({
      id: "chatcmpl-sim",
      object: "chat.completion.chunk",
      created: FIXED_CREATED,
      model: modelId,
      choices: [{ index: 0, delta: { content: piece }, finish_reason: null }],
    });
  }
  frames.push({
    id: "chatcmpl-sim",
    object: "chat.completion.chunk",
    created: FIXED_CREATED,
    model: modelId,
    choices: [{ index: 0, delta: {}, finish_reason: finish }],
  });
  frames.push({
    id: "chatcmpl-sim",
    object: "chat.completion.chunk",
    created: FIXED_CREATED,
    model: modelId,
    choices: [],
    usage: { prompt_tokens: input, completion_tokens: output, total_tokens: input + output },
  });
  return frames.map((f) => "data: " + JSON.stringify(f) + "\n\n").join("") + "data: [DONE]\n\n";
}

function anthropicMessage(text: string, modelId: string, input: number, output: number, stop: string, id = "aggregate"): string {
  return JSON.stringify({
    id: `msg_${id}`,
    type: "message",
    role: "assistant",
    model: modelId,
    content: [{ type: "text", text }],
    stop_reason: stop,
    stop_sequence: null,
    usage: { input_tokens: input, output_tokens: output },
  });
}

function anthropicSse(text: string, modelId: string, input: number, output: number, stop: string): string {
  const events: any[] = [
    {
      type: "message_start",
      message: {
        id: "msg_stream",
        type: "message",
        role: "assistant",
        model: modelId,
        content: [],
        stop_reason: null,
        stop_sequence: null,
        usage: { input_tokens: input, output_tokens: 0 },
      },
    },
    { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } },
  ];
  for (const piece of splitTextPieces(text)) {
    events.push({
      type: "content_block_delta",
      index: 0,
      delta: { type: "text_delta", text: piece },
    });
  }
  events.push({ type: "content_block_stop", index: 0 });
  events.push({
    type: "message_delta",
    delta: { stop_reason: stop, stop_sequence: null },
    usage: { output_tokens: output },
  });
  events.push({ type: "message_stop" });
  return events.map((e) => "event: " + e.type + "\ndata: " + JSON.stringify(e) + "\n\n").join("");
}

interface TranslatedResponse {
  status: number;
  contentType: string;
  body: string;
}

function translateResponse(
  clientApi: Api,
  upstreamWire: Wire,
  status: number,
  raw: string,
  modelId: string,
  stream: boolean,
): TranslatedResponse | null {
  if (status !== 200) return null;
  try {
    if (clientApi === "openai") {
      if (upstreamWire === "openai") {
        return { status: 200, contentType: stream ? "text/event-stream" : "application/json", body: raw };
      }
      if (upstreamWire === "anthropic") {
        if (stream) {
          const p = parseAnthropicSse(raw);
          return {
            status: 200,
            contentType: "text/event-stream",
            body: openaiSse(p.text, modelId, p.input, p.output, openaiFinishFromAnthropic(p.stop)),
          };
        }
        const p = parseAnthropicJson(raw);
        return {
          status: 200,
          contentType: "application/json",
          body: openaiCompletion(p.text, modelId, p.input, p.output, openaiFinishFromAnthropic(p.stop)),
        };
      }
      // gemini
      if (stream) {
        const p = parseGeminiSse(raw);
        return {
          status: 200,
          contentType: "text/event-stream",
          body: openaiSse(p.text, modelId, p.input, p.output, openaiFinishFromGemini(p.stop)),
        };
      }
      const p = parseGeminiJson(raw);
      return {
        status: 200,
        contentType: "application/json",
        body: openaiCompletion(p.text, modelId, p.input, p.output, openaiFinishFromGemini(p.stop)),
      };
    }
    // anthropic client
    if (upstreamWire === "anthropic") {
      return { status: 200, contentType: stream ? "text/event-stream" : "application/json", body: raw };
    }
    if (upstreamWire === "openai") {
      if (stream) {
        const p = parseOpenaiSse(raw);
        return {
          status: 200,
          contentType: "text/event-stream",
          body: anthropicSse(p.text, modelId, p.input, p.output, anthropicStopFromOpenai(p.stop)),
        };
      }
      const p = parseOpenaiJson(raw);
      return {
        status: 200,
        contentType: "application/json",
        body: anthropicMessage(p.text, modelId, p.input, p.output, anthropicStopFromOpenai(p.stop), p.id),
      };
    }
    // gemini
    if (stream) {
      const p = parseGeminiSse(raw);
      return {
        status: 200,
        contentType: "text/event-stream",
        body: anthropicSse(p.text, modelId, p.input, p.output, anthropicStopFromGemini(p.stop)),
      };
    }
    const p = parseGeminiJson(raw);
    return {
      status: 200,
      contentType: "application/json",
      body: anthropicMessage(p.text, modelId, p.input, p.output, anthropicStopFromGemini(p.stop), p.id),
    };
  } catch {
    return null;
  }
}

// ---------------------------------------------------------------------------
// upstream HTTP call (full body read; answers are a few KB)
// ---------------------------------------------------------------------------

async function callUpstream(
  envUrl: string,
  provider: ProviderDoc,
  path: string,
  payload: any,
): Promise<[number, string]> {
  const url = joinBase(envUrl, provider.base) + path;
  const headers: Record<string, string> = {
    accept: "application/json",
    "content-type": "application/json",
    ...authHeaders(provider.auth, provider.key),
  };
  if (provider.wire === "anthropic") headers["anthropic-version"] = "2023-06-01";
  try {
    const res = await fetch(url, {
      method: "POST",
      headers,
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(UPSTREAM_TIMEOUT_MS),
    });
    return [res.status, await res.text()];
  } catch {
    return [0, ""];
  }
}


// ---------------------------------------------------------------------------
// request handling
// ---------------------------------------------------------------------------

function openaiError(message: string, status = 503): TranslatedResponse {
  return {
    status,
    contentType: "application/json",
    body: JSON.stringify({
      error: { message, type: "server_error", code: "internal_error" },
    }),
  };
}

function anthropicError(message: string, status = 503): TranslatedResponse {
  return {
    status,
    contentType: "application/json",
    body: JSON.stringify({
      type: "error",
      error: { type: "overloaded_error", message },
    }),
  };
}

class Aggregator {
  router: Router;
  manifestIndex = 0;
  private ctx: Brief | null;
  private envUrl: string;
  private probes: Probe[];
  private catalog: Catalog;

  constructor(ctx: Brief | null, envUrl: string, probes: Probe[], catalog: Catalog) {
    this.ctx = ctx;
    this.envUrl = envUrl;
    this.probes = probes;
    this.catalog = catalog;
    this.router = new Router(probes, ratingRows(ctx));
  }

  async handle(api: Api, body: any): Promise<TranslatedResponse> {
    const stream = !!body?.stream;
    const index = this.manifestIndex;
    this.manifestIndex += 1;
    const req =
      this.ctx?.workload != null
        ? requirementsFromManifest(this.ctx, index)
        : requirementsFromBody(api, body);
    let jsonMode = false;
    if (api === "openai") {
      const fmt = body?.response_format;
      jsonMode = fmt?.type === "json_object" || fmt?.type === "json_schema";
    }
    const candidates = this.router.candidates(req, api, jsonMode);
    let lastStatus = 0;
    for (const cand of candidates) {
      const provider = cand.provider;
      const wire = provider.wire;
      const modelId = cand.modelId;
      let path: string;
      let payload: any;
      try {
        if (wire === "openai") {
          if (api === "anthropic") {
            payload = anthropicToOpenaiRequest(body, modelId, stream);
          } else {
            payload = { ...body, model: modelId };
            if (stream) payload.stream_options = { include_usage: true };
          }
          path = "/v1/chat/completions";
        } else if (wire === "anthropic") {
          if (api === "openai") {
            payload = openaiToAnthropicRequest(body, modelId);
          } else {
            payload = { ...body, model: modelId };
          }
          path = "/v1/messages";
        } else {
          [path, payload] = buildGeminiRequest(api, body, modelId, stream);
        }
      } catch {
        continue;
      }
      this.router.noteAttempt(provider.id);
      const [status, raw] = await callUpstream(this.envUrl, provider, path, payload);
      if (status === 200) {
        const translated = translateResponse(api, wire, status, raw, modelId, stream);
        if (translated !== null) {
          this.router.noteSuccess(provider.id);
          return translated;
        }
        lastStatus = status;
        continue;
      }
      lastStatus = status;
      if (status === 402 || status === 429 || status >= 500 || status === 0) {
        this.router.noteRefusal(provider.id);
      }
    }
    const message = "all providers refused (last status " + lastStatus + ")";
    return api === "openai" ? openaiError(message) : anthropicError(message);
  }

  modelsList(anthropicShape: boolean): TranslatedResponse {
    const list = servedModels(this.probes, ratingRows(this.ctx));
    if (anthropicShape) {
      return {
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          data: list.map((m) => ({
            type: "model",
            id: m.id,
            display_name: m.display_name,
            created_at: FIXED_ANTHROPIC_CREATED_AT,
          })),
          has_more: false,
        }),
      };
    }
    return {
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        object: "list",
        data: list.map((m) => ({
          id: m.id,
          object: "model",
          created: FIXED_CREATED,
          owned_by: m.provider,
        })),
      }),
    };
  }
}

async function readBody(req: IncomingMessage): Promise<string> {
  const chunks: Buffer[] = [];
  let total = 0;
  for await (const chunk of req) {
    total += chunk.length;
    if (total > MAX_BODY_BYTES) throw new Error("body too large");
    chunks.push(chunk as Buffer);
  }
  return Buffer.concat(chunks).toString("utf8");
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

async function main(): Promise<number> {
  const ctx = readBrief(process.env.MAKEFUN_PROMPT_FILE ?? "");
  const envUrl = process.env.MAKEFUN_ENV_URL ?? "";
  const probes: Probe[] = [];
  if (envUrl) {
    for (const provider of ctx.directory.providers) {
      probes.push(await probeProvider(envUrl, provider));
    }
  }
  const rows = ratingRows(ctx);
  const catalog = buildCatalog(probes, rows);

  if (process.argv.includes("--list")) {
    // The same document the register body carries, byte for byte: print the one built object and stop
    // before any server starts. A process whose stdout is not flushed loses the tail, so wait for it.
    const text = `${JSON.stringify(catalog)}\n`;
    await new Promise<void>((resolve) => process.stdout.write(text, () => resolve()));
    return 0;
  }
  if (!envUrl) {
    process.stderr.write("MAKEFUN_ENV_URL is not set\n");
    return 1;
  }

  const aggregator = new Aggregator(ctx, envUrl, probes, catalog);
  let port = 0;
  const server = createServer(async (req: IncomingMessage, res: ServerResponse) => {
    try {
      const url = new URL(req.url ?? "/", "http://localhost");
      if (req.method === "GET" && url.pathname === "/v1/models") {
        const anthropicShape = req.headers["x-api-key"] !== undefined;
        const out = aggregator.modelsList(anthropicShape);
        res.writeHead(out.status, {
          "content-type": out.contentType,
          "content-length": Buffer.byteLength(out.body),
        });
        res.end(out.body);
        return;
      }
      if (req.method === "POST" && (url.pathname === "/v1/chat/completions" || url.pathname === "/v1/messages")) {
        const api: Api = url.pathname === "/v1/messages" ? "anthropic" : "openai";
        const raw = await readBody(req);
        let body: any;
        try {
          body = JSON.parse(raw);
        } catch {
          res.writeHead(400, { "content-type": "application/json" });
          res.end(JSON.stringify({ error: "bad json" }));
          return;
        }
        if (!body || typeof body !== "object") {
          res.writeHead(400, { "content-type": "application/json" });
          res.end(JSON.stringify({ error: "bad json" }));
          return;
        }
        const out = await aggregator.handle(api, body);
        res.writeHead(out.status, {
          "content-type": out.contentType,
          "content-length": Buffer.byteLength(out.body),
        });
        res.end(out.body);
        return;
      }
      res.writeHead(404, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "not found" }));
    } catch {
      if (!res.headersSent) {
        res.writeHead(500, { "content-type": "application/json" });
      }
      res.end(JSON.stringify({ error: "internal" }));
    }
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "0.0.0.0", () => {
      const address = server.address();
      port = typeof address === "object" && address !== null ? address.port : 0;
      if (port === 8080) {
        server.close(() => reject(new Error("port 8080")));
        return;
      }
      resolve();
    });
  });

  let registered = false;
  for (let attempt = 0; attempt < 30; attempt++) {
    try {
      const res = await fetch(envUrl.replace(/\/+$/, "") + "/register", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ protocol_version: PROTOCOL_VERSION, port, catalog }),
        signal: AbortSignal.timeout(10000),
      });
      if (res.status === 200) {
        registered = true;
        break;
      }
    } catch {
      // retry below
    }
    await sleep(500);
  }
  if (!registered) {
    process.stderr.write("register failed\n");
    return 1;
  }

  for (;;) {
    try {
      const res = await fetch(envUrl.replace(/\/+$/, "") + "/work", {
        headers: { accept: "application/json" },
        signal: AbortSignal.timeout(5000),
      });
      if (res.status === 200) {
        const work = await res.json();
        if (work && typeof work === "object" && work.done) break;
      }
    } catch {
      // keep polling
    }
    await sleep(POLL_INTERVAL_MS);
  }
  await new Promise<void>((resolve) => server.close(() => resolve()));
  return 0;
}

main()
  .then((code) => process.exit(code))
  .catch((err) => {
    process.stderr.write(String(err?.stack ?? err) + "\n");
    process.exit(1);
  });
