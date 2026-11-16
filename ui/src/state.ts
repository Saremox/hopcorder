// What the page shows, kept in the URL hash so every view can be bookmarked
// and shared: #/ for the stream list, #/stream?agent=…&target=…&protocol=…
// for one stream, with range=24h (relative to now) or range=<from>-<to>
// (Unix ms), and at=<Unix ms> for the selected moment.

import type { StreamKey, TimeRange } from './api'

export const PRESETS = {
  '1h': 3_600_000,
  '6h': 21_600_000,
  '24h': 86_400_000,
  '7d': 604_800_000,
  '30d': 2_592_000_000,
} as const

export type Preset = keyof typeof PRESETS

export type Range = { kind: 'relative'; preset: Preset } | { kind: 'absolute'; from: number; to: number }

export const DEFAULT_RANGE: Range = { kind: 'relative', preset: '24h' }

export interface ViewState {
  view: 'streams' | 'stream'
  stream?: StreamKey
  range: Range
  /** Selected moment; undefined follows the latest cycle. */
  at?: number
}

function isPreset(s: string): s is Preset {
  return Object.hasOwn(PRESETS, s)
}

export function parseRange(s: string | null): Range {
  if (!s) return DEFAULT_RANGE
  if (isPreset(s)) return { kind: 'relative', preset: s }
  const m = /^(\d{10,16})-(\d{10,16})$/.exec(s)
  if (m) {
    const from = Number(m[1])
    const to = Number(m[2])
    if (from < to) return { kind: 'absolute', from, to }
  }
  return DEFAULT_RANGE
}

export function formatRange(r: Range): string {
  return r.kind === 'relative' ? r.preset : `${r.from}-${r.to}`
}

export function resolveRange(r: Range, now: number): TimeRange {
  return r.kind === 'relative' ? { from: now - PRESETS[r.preset], to: now } : { from: r.from, to: r.to }
}

export function parseHash(hash: string): ViewState {
  const h = hash.replace(/^#/, '')
  const q = h.indexOf('?')
  const path = q < 0 ? h : h.slice(0, q)
  const params = new URLSearchParams(q < 0 ? '' : h.slice(q + 1))
  const range = parseRange(params.get('range'))
  const agent = params.get('agent')
  const target = params.get('target')
  if (path === '/stream' && agent && target) {
    const atRaw = params.get('at')
    const at = atRaw && /^\d{10,16}$/.test(atRaw) ? Number(atRaw) : undefined
    return { view: 'stream', stream: { agent, target, protocol: params.get('protocol') ?? '' }, range, at }
  }
  return { view: 'streams', range }
}

export function formatHash(s: ViewState): string {
  const p = new URLSearchParams()
  if (s.view === 'stream' && s.stream) {
    p.set('agent', s.stream.agent)
    p.set('target', s.stream.target)
    if (s.stream.protocol) p.set('protocol', s.stream.protocol)
  }
  if (formatRange(s.range) !== formatRange(DEFAULT_RANGE)) p.set('range', formatRange(s.range))
  if (s.view === 'stream' && s.at !== undefined) p.set('at', String(Math.round(s.at)))
  const qs = p.toString()
  return `#/${s.view === 'stream' ? 'stream' : ''}${qs ? `?${qs}` : ''}`
}
