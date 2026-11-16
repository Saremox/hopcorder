import { describe, expect, it } from 'vitest'
import type { Cycle, PathChange, Topology, TopoEdge, TopoNode } from '../src/api'
import { COLUMN_WIDTH, ROW_HEIGHT, asSlots, changeAt, edgeId, highlightCycle, layoutTopology, nodeId } from '../src/topology'

const node = (ttl: number, ip: string, cycles: number, extra: Partial<TopoNode> = {}): TopoNode => ({
  ttl, ip, name: '', asn: 0, as_name: '', cycles, rtt_p50_us: 1000, destination: false, loop_cycles: 0, first_ts: '', last_ts: '', ...extra,
})
const edge = (from_ttl: number, from_ip: string, to_ttl: number, to_ip: string, cycles: number): TopoEdge => ({
  from_ttl, from_ip, to_ttl, to_ip, cycles, last_ts: '',
})

// Two routes after TTL 1: a (busy) and b, which cross over at TTL 3.
const topo: Topology = {
  bucket_ms: 60000,
  truncated: false,
  timeline: [{ t: '2026-11-16T00:00:00Z', cycles: 100, reached_cycles: 90, loop_cycles: 0, max_path_len: 4, rtt_p50_us: 1 }],
  nodes: [
    node(1, 'gw', 100),
    node(2, 'a2', 70),
    node(2, 'b2', 30),
    node(3, 'b3', 60), // busier, but follows b2
    node(3, 'a3', 40),
    node(4, 'dst', 90, { destination: true, loop_cycles: 2 }),
    node(9, 'orphan', 1),
  ],
  edges: [
    edge(1, 'gw', 2, 'a2', 70),
    edge(1, 'gw', 2, 'b2', 30),
    edge(2, 'a2', 3, 'a3', 40),
    edge(2, 'a2', 3, 'b3', 30),
    edge(2, 'b2', 3, 'b3', 30),
    edge(3, 'a3', 4, 'dst', 40),
    edge(1, 'gw', 4, 'dst', 50), // TTLs 2 and 3 silent
    edge(3, 'b3', 4, 'gone', 5), // node dropped by the cap
  ],
}

describe('layoutTopology', () => {
  const l = layoutTopology(topo)
  const at = (id: string) => l.nodes.find((n) => n.id === id)!

  it('puts TTLs in columns', () => {
    expect(at(nodeId(1, 'gw')).x).toBe(0)
    expect(at(nodeId(4, 'dst')).x).toBe(3 * COLUMN_WIDTH)
    expect(l.maxTTL).toBe(9)
    expect(l.totalCycles).toBe(100)
  })

  it('orders a column by its predecessors, then by traffic', () => {
    expect(at(nodeId(2, 'a2')).y).toBe(-ROW_HEIGHT / 2) // busiest first
    expect(at(nodeId(2, 'b2')).y).toBe(ROW_HEIGHT / 2)
    // a3 hangs off a2 only; b3 off a2 and b2, so it goes below despite more traffic.
    expect(at(nodeId(3, 'a3')).y).toBeLessThan(at(nodeId(3, 'b3')).y)
  })

  it('keeps shares, flags and spans', () => {
    const d = at(nodeId(4, 'dst'))
    expect(d.share).toBeCloseTo(0.9)
    expect(d.destination && d.looped).toBe(true)
    const skip = l.edges.find((e) => e.id === edgeId(nodeId(1, 'gw'), nodeId(4, 'dst')))
    expect(skip?.span).toBe(3)
    expect(l.edges.some((e) => e.target.endsWith('gone'))).toBe(false)
  })

  it('handles an empty range', () => {
    expect(layoutTopology({ ...topo, nodes: [], edges: [], timeline: [] })).toEqual({ nodes: [], edges: [], maxTTL: 0, totalCycles: 0 })
  })
})

const cycle: Cycle = {
  agent_id: 'lab', seq: 7, ts: '2026-11-16T00:00:35Z', target: 'dc1', target_ip: '192.0.2.9', af: 4, protocol: 'icmp', flow_id: 1,
  reached: false, path_len: 6, path_hash: 'x',
  loops: [{ ip: 'r1', ttls: [2, 4] }],
  hops: [
    { ttl: 1, status: 'ttl-expired', ip: 'gw' },
    { ttl: 2, status: 'ttl-expired', ip: 'r1' },
    { ttl: 3, status: 'no-reply' },
    { ttl: 4, status: 'ttl-expired', ip: 'r1' },
    { ttl: 5, status: 'no-reply' },
    { ttl: 6, status: 'no-reply' },
  ],
}
const change: PathChange = {
  ts: cycle.ts, agent_id: 'lab', target: 'dc1', protocol: 'icmp', flow_id: 1, seq: 7, reason: 'router',
  ttls: [2, 3], from_ips: ['a2', 'a3'], to_ips: ['r1', ''], prev_seq: 6, prev_path_len: 4, path_len: 6,
}

describe('highlightCycle', () => {
  it('marks the path, skipping silent TTLs', () => {
    const h = highlightCycle(cycle, null)
    expect([...h.nodes]).toEqual([nodeId(1, 'gw'), nodeId(2, 'r1'), nodeId(4, 'r1')])
    expect([...h.edges]).toEqual([edgeId(nodeId(1, 'gw'), nodeId(2, 'r1')), edgeId(nodeId(2, 'r1'), nodeId(4, 'r1'))])
    expect(h.silentTTLs).toEqual([3, 5, 6])
    expect([...h.loopNodes]).toEqual([nodeId(2, 'r1'), nodeId(4, 'r1')])
  })

  it('marks a route change at this cycle only', () => {
    const h = highlightCycle(cycle, change)
    expect([...h.changedFrom]).toEqual([nodeId(2, 'a2'), nodeId(3, 'a3')])
    expect([...h.changedTo]).toEqual([nodeId(2, 'r1')])
    expect(highlightCycle(cycle, { ...change, seq: 8 }).changedFrom.size).toBe(0)
    expect(changeAt([change], cycle)).toBe(change)
    expect(changeAt([{ ...change, seq: 3 }], cycle)).toBeNull()
  })

  it('is empty without a cycle', () => {
    expect(highlightCycle(null, change).nodes.size).toBe(0)
  })
})

describe('asSlots', () => {
  it('gives the busiest ASes their own colour and the rest the last', () => {
    const l = layoutTopology({
      ...topo,
      nodes: [node(1, 'x', 50, { asn: 1 }), node(2, 'y', 80, { asn: 2 }), node(3, 'z', 10, { asn: 3 }), node(4, 'p', 99)],
      edges: [],
    })
    expect([...asSlots(l.nodes, 2)]).toEqual([
      [2, 0],
      [1, 1],
      [3, 1],
    ])
  })
})
