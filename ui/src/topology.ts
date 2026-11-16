// The hop graph: every address that answered at a TTL is a node in that
// TTL's column; an edge joins consecutive answering hops of a cycle (dashed
// when silent TTLs lie between them). Pure functions, so they can be tested
// without a browser.

import type { Cycle, PathChange, Topology, TopoNode } from './api'

export const COLUMN_WIDTH = 150
export const ROW_HEIGHT = 56

export const nodeId = (ttl: number, ip: string): string => `${ttl}|${ip}`
export const edgeId = (from: string, to: string): string => `${from}>${to}`

export interface GraphNode {
  id: string
  ttl: number
  ip: string
  name: string
  asn: number
  asName: string
  cycles: number
  /** Share of the range's cycles in which this address answered at this TTL. */
  share: number
  destination: boolean
  looped: boolean
  rttUs: number
  x: number
  y: number
}

export interface GraphEdge {
  id: string
  source: string
  target: string
  cycles: number
  share: number
  /** TTLs the edge spans; more than 1 means silent hops in between. */
  span: number
}

export interface Layout {
  nodes: GraphNode[]
  edges: GraphEdge[]
  maxTTL: number
  totalCycles: number
}

/**
 * Places nodes in TTL columns, ordering each column by the barycenter of its
 * predecessors (weighted by cycles) to keep edges from crossing, with the
 * busiest addresses first where there is no predecessor.
 */
export function layoutTopology(topo: Topology): Layout {
  const totalCycles = topo.timeline.reduce((n, b) => n + b.cycles, 0)
  const denom = Math.max(totalCycles, 1)
  const byTTL = new Map<number, TopoNode[]>()
  for (const n of topo.nodes) {
    const col = byTTL.get(n.ttl)
    if (col) col.push(n)
    else byTTL.set(n.ttl, [n])
  }
  const incoming = new Map<string, { from: string; cycles: number }[]>()
  for (const e of topo.edges) {
    const to = nodeId(e.to_ttl, e.to_ip)
    const list = incoming.get(to)
    const item = { from: nodeId(e.from_ttl, e.from_ip), cycles: e.cycles }
    if (list) list.push(item)
    else incoming.set(to, [item])
  }

  const rank = new Map<string, number>() // position within its column
  const nodes: GraphNode[] = []
  const ttls = [...byTTL.keys()].sort((a, b) => a - b)
  for (const ttl of ttls) {
    const col = byTTL.get(ttl) ?? []
    const scored = col.map((n, i) => {
      const id = nodeId(n.ttl, n.ip)
      let w = 0
      let sum = 0
      for (const e of incoming.get(id) ?? []) {
        const r = rank.get(e.from)
        if (r !== undefined) {
          w += e.cycles
          sum += r * e.cycles
        }
      }
      return { n, id, bary: w > 0 ? sum / w : Number.POSITIVE_INFINITY, order: i }
    })
    // Without predecessors, keep the server's order (busiest first).
    scored.sort((a, b) => a.bary - b.bary || b.n.cycles - a.n.cycles || a.order - b.order)
    const offset = (scored.length - 1) / 2
    scored.forEach((s, i) => {
      rank.set(s.id, i - offset)
      nodes.push({
        id: s.id,
        ttl: s.n.ttl,
        ip: s.n.ip,
        name: s.n.name,
        asn: s.n.asn,
        asName: s.n.as_name,
        cycles: s.n.cycles,
        share: s.n.cycles / denom,
        destination: s.n.destination,
        looped: s.n.loop_cycles > 0,
        rttUs: s.n.rtt_p50_us,
        x: (s.n.ttl - 1) * COLUMN_WIDTH,
        y: (i - offset) * ROW_HEIGHT,
      })
    })
  }

  const known = new Set(nodes.map((n) => n.id))
  const edges: GraphEdge[] = []
  for (const e of topo.edges) {
    const source = nodeId(e.from_ttl, e.from_ip)
    const target = nodeId(e.to_ttl, e.to_ip)
    if (!known.has(source) || !known.has(target)) continue // dropped by the node cap
    edges.push({ id: edgeId(source, target), source, target, cycles: e.cycles, share: e.cycles / denom, span: e.to_ttl - e.from_ttl })
  }
  return { nodes, edges, maxTTL: ttls.at(-1) ?? 0, totalCycles }
}

export interface Highlight {
  /** The selected cycle's answering hops and the edges between them. */
  nodes: Set<string>
  edges: Set<string>
  /** Hops of the selected cycle that belong to a loop. */
  loopNodes: Set<string>
  /** At a route change: the previous route's hops and the new ones, at the changed TTLs. */
  changedFrom: Set<string>
  changedTo: Set<string>
  /** TTLs of the selected cycle that got no answer. */
  silentTTLs: number[]
}

export function emptyHighlight(): Highlight {
  return { nodes: new Set(), edges: new Set(), loopNodes: new Set(), changedFrom: new Set(), changedTo: new Set(), silentTTLs: [] }
}

export function highlightCycle(cycle: Cycle | null, change: PathChange | null): Highlight {
  const h = emptyHighlight()
  if (!cycle) return h
  let prev: string | undefined
  for (const hop of cycle.hops) {
    if (hop.status === 'no-reply' || !hop.ip) {
      h.silentTTLs.push(hop.ttl)
      continue
    }
    const id = nodeId(hop.ttl, hop.ip)
    h.nodes.add(id)
    if (prev) h.edges.add(edgeId(prev, id))
    prev = id
  }
  for (const l of cycle.loops) {
    for (const ttl of l.ttls) h.loopNodes.add(nodeId(ttl, l.ip))
  }
  if (change && change.seq === cycle.seq) {
    change.ttls.forEach((ttl, i) => {
      const from = change.from_ips[i]
      const to = change.to_ips[i]
      if (from) h.changedFrom.add(nodeId(ttl, from))
      if (to) h.changedTo.add(nodeId(ttl, to))
    })
  }
  return h
}

/** The route change recorded for this cycle, if any. */
export function changeAt(changes: PathChange[], cycle: Cycle | null): PathChange | null {
  if (!cycle) return null
  return changes.find((c) => c.seq === cycle.seq && c.agent_id === cycle.agent_id) ?? null
}

/** Node radius: 6 to 22 px by the share of cycles. */
export function nodeSize(share: number): number {
  return 6 + 16 * Math.sqrt(Math.min(Math.max(share, 0), 1))
}

/** Edge width: 1 to 6 px by the share of cycles. */
export function edgeWidth(share: number): number {
  return 1 + 5 * Math.sqrt(Math.min(Math.max(share, 0), 1))
}

/**
 * Colour slots for origin ASes: the busiest ASes of the graph get their own
 * slot, the rest share the last one; addresses without an AS (private,
 * unannounced) get none (-1).
 */
export function asSlots(nodes: GraphNode[], slots: number): Map<number, number> {
  const weight = new Map<number, number>()
  for (const n of nodes) if (n.asn) weight.set(n.asn, (weight.get(n.asn) ?? 0) + n.cycles)
  const ranked = [...weight.entries()].sort((a, b) => b[1] - a[1] || a[0] - b[0])
  const out = new Map<number, number>()
  ranked.forEach(([asn], i) => out.set(asn, Math.min(i, slots - 1)))
  return out
}
