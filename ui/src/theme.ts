// Colours come from the CSS custom properties in style.css, so the canvas
// charts follow the page's light or dark scheme.
import { onBeforeUnmount, ref, type Ref } from 'vue'

export interface Theme {
  text: string
  muted: string
  border: string
  grid: string
  panel: string
  accent: string
  ok: string
  bad: string
  warn: string
  loop: string
  as: string[]
}

export const AS_SLOTS = 8

export function readTheme(el: Element = document.documentElement): Theme {
  const css = getComputedStyle(el)
  const v = (name: string) => css.getPropertyValue(name).trim()
  return {
    text: v('--text'),
    muted: v('--muted'),
    border: v('--border'),
    grid: v('--grid'),
    panel: v('--panel'),
    accent: v('--accent'),
    ok: v('--ok'),
    bad: v('--bad'),
    warn: v('--warn'),
    loop: v('--loop'),
    as: Array.from({ length: AS_SLOTS }, (_, i) => v(`--as-${i}`)),
  }
}

/** The current theme, updated when the colour scheme changes. */
export function useTheme(): Ref<Theme> {
  const theme = ref(readTheme())
  const mq = window.matchMedia('(prefers-color-scheme: dark)')
  const update = () => {
    theme.value = readTheme()
  }
  mq.addEventListener('change', update)
  onBeforeUnmount(() => mq.removeEventListener('change', update))
  return theme
}
