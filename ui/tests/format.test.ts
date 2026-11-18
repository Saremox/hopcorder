import { describe, expect, it } from 'vitest'
import { cutNote, fmtCount, unreachableName } from '../src/format'

// ICMPv6 numbers its destination unreachable codes differently from ICMPv4
// (review round five: an ICMPv6 address unreachable read "port
// unreachable", administratively prohibited "host unreachable").
describe('unreachable codes', () => {
  it('are named by the address family', () => {
    expect(unreachableName(4, 3)).toBe('port unreachable')
    expect(unreachableName(6, 3)).toBe('address unreachable')
    expect(unreachableName(4, 1)).toBe('host unreachable')
    expect(unreachableName(6, 1)).toBe('administratively prohibited')
    expect(unreachableName(6, 4)).toBe('port unreachable')
    expect(unreachableName(6, 0)).toBe('no route')
    expect(unreachableName(4, 13)).toBe('administratively prohibited')
    expect(unreachableName(6, 13)).toBe('code 13')
    expect(unreachableName(4, 7)).toBe('code 7')
  })
})

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
