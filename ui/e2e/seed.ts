// Two hours of cycles for one stream, sent through the server's ingest API:
// a stable path with a rate-limited hop, a route change, an outage and a
// loop. AS numbers and addresses are documentation values; e2e/asn.tsv maps
// them to AS names.

export const TARGET = 'demo.example'
export const DEST = '203.0.113.10'
export const STEP_MS = 5000
export const CYCLES = 1440

type Spec = (string | null)[] // address per TTL; null = no reply; '=' prefix = destination

interface Hop {
  ttl: number
  status: 'reply' | 'ttl-expired' | 'no-reply'
  ip?: string
  rtt_us?: number
}

const before = ['192.168.1.1', '100.64.0.1', '198.51.100.1', '198.51.100.9', '198.51.100.130', `=${DEST}`]
const after = ['192.168.1.1', '100.64.0.1', '198.51.100.1', '198.51.100.9', '192.0.2.17', `=${DEST}`]
const outage = ['192.168.1.1', '100.64.0.1', '198.51.100.1', '198.51.100.9', null, null]
const loop = ['192.168.1.1', '100.64.0.1', '198.51.100.1', '198.51.100.9', '192.0.2.17', '198.51.100.9', '192.0.2.17', '198.51.100.9', null, null]

/** What cycle i (0 = oldest) looks like. */
export function specFor(i: number): Spec {
  const fromEnd = CYCLES - i
  let base: Spec
  if (fromEnd <= 20 * 12 && fromEnd > 20 * 12 - 3) base = loop // 3 cycles, 20 min ago
  else if (fromEnd <= 35 * 12 && fromEnd > 30 * 12) base = outage // 35 to 30 min ago
  else if (fromEnd <= 60 * 12) base = after // the route changed an hour ago
  else base = before
  const spec = [...base]
  // TTL 4 is rate limited: silent in 3 of 5 cycles of the normal paths.
  if ((base === before || base === after) && i % 5 < 3) spec[3] = null
  return spec
}

export function record(agent: string, seq: number, ts: number, spec: Spec) {
  const hops: Hop[] = spec.map((s, k) => {
    const ttl = k + 1
    if (s === null) return { ttl, status: 'no-reply' }
    if (s.startsWith('=')) return { ttl, status: 'reply', ip: s.slice(1), rtt_us: 18000 + ((seq * 37) % 4000) }
    return { ttl, status: 'ttl-expired', ip: s, rtt_us: 800 + ttl * 2500 + ((seq * 13) % 900) }
  })
  const seen = new Map<string, number[]>()
  for (const h of hops) if (h.status === 'ttl-expired' && h.ip) seen.set(h.ip, [...(seen.get(h.ip) ?? []), h.ttl])
  const loops = [...seen].filter(([, t]) => t.length > 1).map(([ip, ttls]) => ({ ip, ttls }))
  const answered = hops.filter((h) => h.ip).map((h) => h.ip)
  return {
    schema: 1,
    agent_id: agent,
    seq,
    ts: new Date(ts).toISOString(),
    target: TARGET,
    target_ip: DEST,
    af: 4,
    protocol: 'icmp',
    flow_id: 33434,
    reached: hops.some((h) => h.status === 'reply'),
    path_len: hops.length,
    path_hash: answered.join('>').slice(0, 16).padEnd(16, '0'),
    loops,
    hops,
  }
}

/** Ingests the stream; the last cycle is at `end`. */
export async function seed(baseURL: string, agent: string, end: number): Promise<void> {
  const lines: string[] = []
  for (let i = 0; i < CYCLES; i++) {
    const ts = end - (CYCLES - 1 - i) * STEP_MS
    lines.push(JSON.stringify(record(agent, i + 1, ts, specFor(i))))
  }
  const resp = await fetch(new URL('api/v1/ingest', baseURL), {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-ndjson' },
    body: `${lines.join('\n')}\n`,
  })
  const body = (await resp.json()) as { accepted?: number }
  if (!resp.ok || body.accepted !== CYCLES) throw new Error(`ingest: ${resp.status} ${JSON.stringify(body)}`)
}
