<script setup lang="ts">
// The shell: header with the range picker, then the stream list or one
// stream. All state lives in the URL hash (state.ts).
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import { Client, apiBase } from './api'
import StreamList from './components/StreamList.vue'
import StreamView from './components/StreamView.vue'
import { PRESETS, formatHash, parseHash, type Preset, type Range, type ViewState } from './state'
import { useTheme } from './theme'

const client = new Client(apiBase(window.location.href))
const theme = useTheme()
const state = ref<ViewState>(parseHash(window.location.hash))
const now = ref(Date.now())

function navigate(next: ViewState, replace = false) {
  const hash = formatHash(next)
  if (replace) history.replaceState(null, '', hash)
  else if (hash !== window.location.hash) history.pushState(null, '', hash)
  state.value = next
}

function onHash() {
  state.value = parseHash(window.location.hash)
}

// Relative ranges follow the clock: refresh every 30 s while the page is
// visible.
let timer: ReturnType<typeof setInterval> | undefined
function tick() {
  if (state.value.range.kind === 'relative' && document.visibilityState === 'visible') now.value = Date.now()
}
onMounted(() => {
  window.addEventListener('hashchange', onHash)
  timer = setInterval(tick, 30_000)
  document.addEventListener('visibilitychange', tick)
})
onBeforeUnmount(() => {
  window.removeEventListener('hashchange', onHash)
  clearInterval(timer)
  document.removeEventListener('visibilitychange', tick)
})

function setRange(r: Range) {
  now.value = Date.now()
  navigate({ ...state.value, range: r, at: undefined })
}
function refresh() {
  now.value = Date.now()
}
const presets = Object.keys(PRESETS) as Preset[]
const absolute = computed(() => (state.value.range.kind === 'absolute' ? state.value.range : null))
const streamsLink = computed(() => formatHash({ view: 'streams', range: state.value.range }))
</script>

<template>
  <header class="top">
    <a class="brand" :href="streamsLink">hopcorder</a>
    <nav class="crumbs" aria-label="Breadcrumb">
      <a :href="streamsLink">Streams</a>
      <template v-if="state.view === 'stream' && state.stream">
        <span aria-hidden="true">›</span>
        <span class="mono">{{ state.stream.target }}</span>
      </template>
    </nav>
    <div class="range" role="group" aria-label="Time range">
      <button
        v-for="p in presets"
        :key="p"
        :class="{ active: state.range.kind === 'relative' && state.range.preset === p }"
        @click="setRange({ kind: 'relative', preset: p })"
      >
        {{ p }}
      </button>
      <span v-if="absolute" class="muted abs">{{ new Date(absolute.from).toLocaleString() }} – {{ new Date(absolute.to).toLocaleString() }}</span>
      <button title="Refresh" aria-label="Refresh" @click="refresh">⟳</button>
    </div>
  </header>
  <main>
    <StreamView
      v-if="state.view === 'stream' && state.stream"
      :client="client"
      :stream="state.stream"
      :range="state.range"
      :at="state.at"
      :now="now"
      :theme="theme"
      @seek="(at) => navigate({ ...state, at }, true)"
    />
    <StreamList v-else :client="client" :range="state.range" :now="now" />
  </main>
</template>
