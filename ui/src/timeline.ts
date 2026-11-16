// The reachability timeline above the hop graph: unreachable share and
// destination RTT per bucket, outage windows, and route changes to step
// between. Pure functions.

import type { Outage, PathChange, Topology, TimeRange } from './api'
import { toMs } from './format'

export interface TimelineSeries {
  /** Bucket starts, Unix ms. */
  times: number[]
  /** Share of cycles per bucket that did not reach the destination, 0..100. */
  unreachablePct: number[]
  /** Median destination RTT per bucket in ms; null where nothing was reached. */
  rttMs: (number | null)[]
  loopCycles: number[]
}

export function timelineSeries(topo: Topology): TimelineSeries {
  const s: TimelineSeries = { times: [], unreachablePct: [], rttMs: [], loopCycles: [] }
  for (const b of topo.timeline) {
    s.times.push(toMs(b.t))
    s.unreachablePct.push(b.cycles ? Math.round((1000 * (b.cycles - b.reached_cycles)) / b.cycles) / 10 : 0)
    s.rttMs.push(b.reached_cycles && b.rtt_p50_us ? b.rtt_p50_us / 1000 : null)
    s.loopCycles.push(b.loop_cycles)
  }
  return s
}

/** Outage windows clipped to the range; an open one runs to the range's end. */
export function outageAreas(outages: Outage[], r: TimeRange): [number, number][] {
  const out: [number, number][] = []
  for (const o of outages) {
    const start = Math.max(toMs(o.start_ts), r.from)
    const end = Math.min(o.end_ts ? toMs(o.end_ts) : r.to, r.to)
    if (end > start) out.push([start, end])
  }
  return out.sort((a, b) => a[0] - b[0])
}

/** Route change times, ascending. */
export function changeTimes(changes: PathChange[]): number[] {
  return changes.map((c) => toMs(c.ts)).sort((a, b) => a - b)
}

/** The latest time in `times` (ascending) strictly before `at`. */
export function prevTime(times: number[], at: number): number | undefined {
  let best: number | undefined
  for (const t of times) {
    if (t >= at) break
    best = t
  }
  return best
}

/** The earliest time in `times` (ascending) strictly after `at`. */
export function nextTime(times: number[], at: number): number | undefined {
  return times.find((t) => t > at)
}
