import { describe, expect, it } from 'vitest'
import { DEFAULT_RANGE, formatHash, parseHash, parseRange, resolveRange, type ViewState } from '../src/state'

describe('hash state', () => {
  it('round-trips a stream view', () => {
    const s: ViewState = {
      view: 'stream',
      stream: { agent: 'lab 1', target: 'dc1.example', protocol: 'udp' },
      range: { kind: 'absolute', from: 1763251200000, to: 1763337600000 },
      at: 1763300000123,
    }
    const h = formatHash(s)
    expect(h).toBe('#/stream?agent=lab+1&target=dc1.example&protocol=udp&range=1763251200000-1763337600000&at=1763300000123')
    expect(parseHash(h)).toEqual(s)
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
