// Client for the server's read API (/api/v1/query/*, see README.md).

export interface Stream {
  agent_id: string
  target: string
  protocol: string
  flow_id: number
  last_ts: string
  cycles: number
  reach_ratio: number
  last_reached: boolean
  last_path_len: number
  last_target_ip: string
  last_path_hash: string
}

export interface TopoNode {
  ttl: number
  ip: string
  name: string
  asn: number
  as_name: string
  cycles: number
  rtt_p50_us: number
  destination: boolean
  loop_cycles: number
  first_ts: string
  last_ts: string
}

export interface TopoEdge {
  from_ttl: number
  from_ip: string
  to_ttl: number
  to_ip: string
  cycles: number
  last_ts: string
}

export interface Bucket {
  t: string
  cycles: number
  reached_cycles: number
  loop_cycles: number
  max_path_len: number
  rtt_p50_us: number
}

export interface Topology {
  bucket_ms: number
  truncated: boolean
  nodes: TopoNode[]
  edges: TopoEdge[]
  timeline: Bucket[]
}

export interface MPLSLabel {
  label: number
  tc: number
  s: boolean
  ttl: number
}

export type HopStatus = 'reply' | 'ttl-expired' | 'unreachable' | 'no-reply'

export interface Hop {
  ttl: number
  status: HopStatus
  ip?: string
  rtt_us?: number
  code?: number
  asn?: number
  as_name?: string
  name?: string
  mpls?: MPLSLabel[]
}

export interface Cycle {
  agent_id: string
  seq: number
  ts: string
  target: string
  target_ip: string
  af: number
  protocol: string
  flow_id: number
  reached: boolean
  path_len: number
  path_hash: string
  loops: { ip: string; ttls: number[] }[]
  hops: Hop[]
}

export interface PathChange {
  ts: string
  agent_id: string
  target: string
  protocol: string
  flow_id: number
  seq: number
  reason: 'router' | 'distance' | 'target'
  ttls: number[]
  from_ips: string[]
  to_ips: string[]
  prev_seq: number
  prev_path_len: number
  path_len: number
}

export interface Outage {
  agent_id: string
  target: string
  protocol: string
  flow_id: number
  start_seq: number
  start_ts: string
  end_seq: number
  end_ts: string | null
  duration_ms: number | null
  cycles: number
  last_hop_ttl: number
  last_hop_ip: string
}

export interface LoopEvent {
  ts: string
  agent_id: string
  target: string
  protocol: string
  flow_id: number
  seq: number
  loop_ip: string
  ttls: number[]
}

/** One traced path: what the stream list links to. */
export interface StreamKey {
  agent: string
  target: string
  protocol: string
}

export interface TimeRange {
  from: number // Unix ms
  to: number
}

export class APIError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message)
  }
}

/**
 * The read API's base URL, relative to the page: the UI is served at
 * <prefix>/ui/ and the API at <prefix>/api/v1/query/, so this works behind a
 * reverse proxy that mounts hopcorder under any prefix.
 */
export function apiBase(page: string): URL {
  // '.' drops the file name, query and hash: http://h/p/ui/#/x -> http://h/p/ui/
  return new URL('../api/v1/query/', new URL('.', page))
}

export function queryURL(base: URL, endpoint: string, params: Record<string, string | number | undefined>): string {
  const u = new URL(endpoint, base)
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== '') u.searchParams.set(k, String(v))
  }
  return u.href
}

export class Client {
  constructor(
    private readonly base: URL,
    private readonly fetcher: typeof fetch = (...a) => fetch(...a),
  ) {}

  private async get<T>(endpoint: string, params: Record<string, string | number | undefined>, signal?: AbortSignal): Promise<T> {
    const resp = await this.fetcher(queryURL(this.base, endpoint, params), { signal, headers: { Accept: 'application/json' } })
    let body: unknown
    try {
      body = await resp.json()
    } catch {
      throw new APIError(`${endpoint}: HTTP ${resp.status}`, resp.status)
    }
    if (!resp.ok) {
      const msg = (body as { error?: string } | null)?.error ?? `HTTP ${resp.status}`
      throw new APIError(`${endpoint}: ${msg}`, resp.status)
    }
    return body as T
  }

  private async rows<T>(endpoint: string, params: Record<string, string | number | undefined>, signal?: AbortSignal): Promise<T[]> {
    return (await this.get<{ rows: T[] }>(endpoint, params, signal)).rows
  }

  private static stream(s: StreamKey, r: TimeRange) {
    return { agent: s.agent, target: s.target, protocol: s.protocol, from: Math.floor(r.from), to: Math.ceil(r.to) }
  }

  targets(r: TimeRange, signal?: AbortSignal): Promise<Stream[]> {
    return this.rows('targets', { from: Math.floor(r.from), to: Math.ceil(r.to), limit: 10000 }, signal)
  }

  topology(s: StreamKey, r: TimeRange, signal?: AbortSignal): Promise<Topology> {
    return this.get('topology', Client.stream(s, r), signal)
  }

  pathChanges(s: StreamKey, r: TimeRange, signal?: AbortSignal): Promise<PathChange[]> {
    return this.rows('path-changes', { ...Client.stream(s, r), limit: 10000 }, signal)
  }

  outages(s: StreamKey, r: TimeRange, signal?: AbortSignal): Promise<Outage[]> {
    return this.rows('outages', { ...Client.stream(s, r), limit: 10000 }, signal)
  }

  loops(s: StreamKey, r: TimeRange, signal?: AbortSignal): Promise<LoopEvent[]> {
    return this.rows('loops', { ...Client.stream(s, r), limit: 10000 }, signal)
  }

  /** The last cycle at or before `at` within the range, else the first after it. */
  async cycleAt(s: StreamKey, r: TimeRange, at: number, signal?: AbortSignal): Promise<Cycle | null> {
    const at1 = Math.min(Math.floor(at) + 1, Math.ceil(r.to))
    if (at1 > r.from) {
      const before = await this.rows<Cycle>('cycles', { ...Client.stream(s, { from: r.from, to: at1 }), limit: 1 }, signal)
      if (before[0]) return before[0]
    }
    return this.cycleAfter(s, r, Math.max(r.from, at) - 1, signal)
  }

  /** The first cycle after `ts` within the range. */
  async cycleAfter(s: StreamKey, r: TimeRange, ts: number, signal?: AbortSignal): Promise<Cycle | null> {
    const from = Math.floor(ts) + 1
    if (from >= r.to) return null
    const rows = await this.rows<Cycle>('cycles', { ...Client.stream(s, { from, to: r.to }), order: 'asc', limit: 1 }, signal)
    return rows[0] ?? null
  }

  /** The last cycle before `ts` within the range. */
  async cycleBefore(s: StreamKey, r: TimeRange, ts: number, signal?: AbortSignal): Promise<Cycle | null> {
    const to = Math.ceil(ts)
    if (to <= r.from) return null
    const rows = await this.rows<Cycle>('cycles', { ...Client.stream(s, { from: r.from, to }), limit: 1 }, signal)
    return rows[0] ?? null
  }
}
