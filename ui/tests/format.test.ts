import { describe, expect, it } from 'vitest'
import { cutNote, fmtCount } from '../src/format'

describe('lists the server cut', () => {
  it('count with a plus', () => {
    expect(fmtCount(10000, true)).toBe('10000+')
    expect(fmtCount(3, false)).toBe('3')
  })

  it('say that only the newest are shown', () => {
    const lists = (cut: [boolean, boolean, boolean]) => [
      { what: 'route changes', rows: 3612, cut: cut[0] },
      { what: 'outages', rows: 12, cut: cut[1] },
      { what: 'loops', rows: 10000, cut: cut[2] },
    ]
    expect(cutNote(lists([false, false, false]))).toBe('')
    expect(cutNote(lists([false, false, true]))).toBe(
      'Only the newest 10000 loops of this range are listed and counted; there are more. Pick a shorter range for all of them.',
    )
    expect(cutNote(lists([true, false, true]))).toBe(
      'Only the newest 3612 route changes and 10000 loops of this range are listed and counted; there are more. Pick a shorter range for all of them.',
    )
    expect(cutNote(lists([true, true, true]))).toContain('3612 route changes, 12 outages and 10000 loops')
  })
})
