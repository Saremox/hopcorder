// Display helpers. Everything that goes into an HTML string (chart tooltips)
// passes through escapeHTML: hop names come from reverse DNS and AS names
// from a downloaded database, neither of which is trusted.

const HTML_ESCAPES: Record<string, string> = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }

export function escapeHTML(s: string): string {
  return s.replace(/[&<>"']/g, (c) => HTML_ESCAPES[c] ?? c)
}

export function toMs(t: string | number): number {
  return typeof t === 'number' ? t : Date.parse(t)
}

const pad = (n: number, w = 2) => String(n).padStart(w, '0')

/** Local time, with the date when it is not today (relative to `now`). */
export function fmtTime(t: string | number, now: number = Date.now()): string {
  const d = new Date(toMs(t))
  const hms = `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
  const n = new Date(now)
  if (d.getFullYear() === n.getFullYear() && d.getMonth() === n.getMonth() && d.getDate() === n.getDate()) return hms
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${hms}`
}

export function fmtDuration(ms: number): string {
  if (!Number.isFinite(ms) || ms < 0) return '–'
  const s = Math.round(ms / 1000)
  if (s < 60) return `${s}s`
  const m = Math.floor(s / 60)
  if (m < 60) return s % 60 ? `${m}m ${s % 60}s` : `${m}m`
  const h = Math.floor(m / 60)
  if (h < 48) return m % 60 ? `${h}h ${m % 60}m` : `${h}h`
  return `${Math.floor(h / 24)}d ${h % 24}h`
}

export function fmtRTT(us: number | undefined | null): string {
  if (!us) return '–'
  const ms = us / 1000
  return ms < 10 ? `${ms.toFixed(2)} ms` : ms < 100 ? `${ms.toFixed(1)} ms` : `${Math.round(ms)} ms`
}

export function fmtPct(ratio: number): string {
  if (!Number.isFinite(ratio)) return '–'
  const p = ratio * 100
  return p === 0 || p === 100 ? `${p}%` : p < 1 ? `${p.toFixed(2)}%` : `${p.toFixed(1)}%`
}

/** The name a hop is shown by: its reverse DNS name, else its address. */
export function hopLabel(name: string | undefined, ip: string | undefined): string {
  return name || ip || '*'
}

export function asLabel(asn: number | undefined, asName: string | undefined): string {
  if (!asn) return ''
  return asName ? `AS${asn} ${asName}` : `AS${asn}`
}
