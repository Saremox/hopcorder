import { describe, expect, it } from 'vitest'
import type { Stream } from '../src/api'
import { DEFAULT_RANGE, formatHash, parseHash, parseRange, resolveRange, streamLink, type ViewState } from '../src/state'

describe('hash state', () => {
  it('round-trips a stream view', () => {
    const s: ViewState = {
      view: 'stream',
      stream: { agent: 'lab 1', target: 'dc1.example', protocol: 'udp', flow: 33435 },
      range: { kind: 'absolute', from: 1763251200000, to: 1763337600000 },
      at: 1763300000123,
    }
    const h = formatHash(s)
    expect(h).toBe('#/stream?agent=lab+1&target=dc1.example&protocol=udp&flow_id=33435&range=1763251200000-1763337600000&at=1763300000123')
    expect(parseHash(h)).toEqual(s)
  })

  it('links each flow of a target to its own view', () => {
    // The review's two rows (round five): IPv4 and IPv6 of one name on two
    // flows. Their links used to be the same, without flow_id.
    const row = (flow_id: number, last_af: number): Stream => ({
      agent_id: 'lab', target: 'dual.example', protocol: 'icmp', flow_id, last_af, last_ts: '', cycles: 20,
      reach_ratio: 1, last_reached: true, last_path_len: 3, last_target_ip: '', last_path_hash: '',
    })
    const v4 = streamLink(row(33434, 4), DEFAULT_RANGE)
    const v6 = streamLink(row(33435, 6), DEFAULT_RANGE)
    expect(v4).toBe('#/stream?agent=lab&target=dual.example&protocol=icmp&flow_id=33434')
    expect(v6).toBe('#/stream?agent=lab&target=dual.example&protocol=icmp&flow_id=33435')
    expect(parseHash(v6).stream).toEqual({ agent: 'lab', target: 'dual.example', protocol: 'icmp', flow: 33435 })
    expect(streamLink(row(0, 4), DEFAULT_RANGE)).toContain('&flow_id=0')
  })

  it('keeps links without a flow_id, which name no single stream', () => {
    expect(parseHash('#/stream?agent=a&target=t&protocol=icmp').stream).toEqual({ agent: 'a', target: 't', protocol: 'icmp' })
    for (const bad of ['', 'x', '-1', '65536', '1e3', '0x10', '123456']) {
      expect(parseHash(`#/stream?agent=a&target=t&protocol=icmp&flow_id=${bad}`).stream?.flow).toBeUndefined()
    }
    expect(parseHash('#/stream?agent=a&target=t&protocol=icmp&flow_id=65535').stream?.flow).toBe(65535)
  })

  it('omits defaults', () => {
    expect(formatHash({ view: 'streams', range: DEFAULT_RANGE })).toBe('#/')
    expect(formatHash({ view: 'streams', range: { kind: 'relative', preset: '7d' } })).toBe('#/?range=7d')
    expect(formatHash({ view: 'stream', stream: { agent: 'a', target: 't', protocol: '' }, range: DEFAULT_RANGE })).toBe(
      '#/stream?agent=a&target=t',
    )
  })

  it('falls back to the list for anything else', () => {
    for (const h of ['', '#', '#/', '#/stream', '#/stream?agent=a', '#/nope?agent=a&target=t']) {
      expect(parseHash(h).view).toBe('streams')
    }
  })

  it('ignores bad ranges and moments', () => {
    expect(parseRange('2w')).toEqual(DEFAULT_RANGE)
    expect(parseRange('1763337600000-1763251200000')).toEqual(DEFAULT_RANGE) // from after to
    expect(parseHash('#/stream?agent=a&target=t&at=yesterday').at).toBeUndefined()
  })

  it('resolves relative ranges against now', () => {
    expect(resolveRange({ kind: 'relative', preset: '1h' }, 10_000_000)).toEqual({ from: 6_400_000, to: 10_000_000 })
    expect(resolveRange({ kind: 'absolute', from: 1, to: 2 }, 99)).toEqual({ from: 1, to: 2 })
  })
})
