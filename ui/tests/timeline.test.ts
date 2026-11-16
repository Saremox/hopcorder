import { describe, expect, it } from 'vitest'
import type { Outage, PathChange, Topology } from '../src/api'
import { escapeHTML, fmtDuration, fmtPct, fmtRTT } from '../src/format'
import { changeTimes, nextTime, outageAreas, prevTime, timelineSeries } from '../src/timeline'

describe('timelineSeries', () => {
  it('computes unreachable share and RTT per bucket', () => {
    const topo = {
      timeline: [
        { t: '2026-11-16T00:00:00.000Z', cycles: 12, reached_cycles: 12, loop_cycles: 0, max_path_len: 5, rtt_p50_us: 21500 },
        { t: '2026-11-16T00:01:00.000Z', cycles: 12, reached_cycles: 3, loop_cycles: 2, max_path_len: 9, rtt_p50_us: 30000 },
        { t: '2026-11-16T00:02:00.000Z', cycles: 12, reached_cycles: 0, loop_cycles: 0, max_path_len: 4, rtt_p50_us: 0 },
      ],
    } as Topology
    const s = timelineSeries(topo)
    expect(s.times).toEqual([Date.UTC(2026, 10, 16, 0, 0), Date.UTC(2026, 10, 16, 0, 1), Date.UTC(2026, 10, 16, 0, 2)])
    expect(s.unreachablePct).toEqual([0, 75, 100])
    expect(s.rttMs).toEqual([21.5, 30, null])
    expect(s.loopCycles).toEqual([0, 2, 0])
  })
})

describe('events', () => {
  const r = { from: 1000, to: 10_000 }
  const o = (start: number, end: number | null) => ({ start_ts: new Date(start).toISOString(), end_ts: end === null ? null : new Date(end).toISOString() }) as Outage

  it('clips outages to the range and runs open ones to its end', () => {
    expect(outageAreas([o(9000, null), o(500, 2000), o(3000, 4000), o(11_000, 12_000)], r)).toEqual([
      [1000, 2000],
      [3000, 4000],
      [9000, 10_000],
    ])
  })

  it('steps between route changes', () => {
    const times = changeTimes([{ ts: new Date(3000).toISOString() }, { ts: new Date(1000).toISOString() }] as PathChange[])
    expect(times).toEqual([1000, 3000])
    expect(prevTime(times, 3000)).toBe(1000)
    expect(prevTime(times, 1000)).toBeUndefined()
    expect(nextTime(times, 1000)).toBe(3000)
    expect(nextTime(times, 3000)).toBeUndefined()
  })
})

describe('format', () => {
  it('escapes untrusted names for tooltips', () => {
    expect(escapeHTML(`<img src=x onerror="a('b')">&`)).toBe('&lt;img src=x onerror=&quot;a(&#39;b&#39;)&quot;&gt;&amp;')
  })
  it('formats durations, RTTs and shares', () => {
    expect([fmtDuration(4000), fmtDuration(300_000), fmtDuration(3_720_000), fmtDuration(200_000_000)]).toEqual(['4s', '5m', '1h 2m', '2d 7h'])
    expect([fmtRTT(0), fmtRTT(1234), fmtRTT(45_670), fmtRTT(234_000)]).toEqual(['–', '1.23 ms', '45.7 ms', '234 ms'])
    expect([fmtPct(1), fmtPct(0.956), fmtPct(0.0004), fmtPct(Number.NaN)]).toEqual(['100%', '95.6%', '0.04%', '–'])
  })
})
