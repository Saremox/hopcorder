<script setup lang="ts">
// One stream (agent, target, protocol) over the range: the timeline with the
// time scrubber, the hop graph, the selected cycle and the range's events.
import { computed, onBeforeUnmount, ref, shallowRef, watch } from 'vue'
import type { Client, Cycle, LoopEvent, Outage, PathChange, StreamKey, Topology } from '../api'
import { asLabel, fmtPct, fmtRTT, fmtTime, hopLabel, toMs } from '../format'
import { resolveRange, type Range } from '../state'
import type { Theme } from '../theme'
import { changeTimes, nextTime, outageAreas, prevTime, timelineSeries } from '../timeline'
import { changeAt, highlightCycle, layoutTopology, type GraphNode } from '../topology'
import CycleDetails from './CycleDetails.vue'
import EventList from './EventList.vue'
import HopGraph from './HopGraph.vue'
import Timeline from './Timeline.vue'

const props = defineProps<{ client: Client; stream: StreamKey; range: Range; at: number | undefined; now: number; theme: Theme }>()
const emit = defineEmits<{ seek: [at: number | undefined] }>()

const tr = computed(() => resolveRange(props.range, props.now))

const topo = shallowRef<Topology | null>(null)
const changes = shallowRef<PathChange[]>([])
const outages = shallowRef<Outage[]>([])
const loops = shallowRef<LoopEvent[]>([])
const cycle = shallowRef<Cycle | null>(null)
const loadingRange = ref(false)
const loadingCycle = ref(false)
const error = ref('')
const selectedNode = shallowRef<GraphNode | null>(null)

let rangeAbort: AbortController | undefined
async function loadRange() {
  rangeAbort?.abort()
  const ac = new AbortController()
  rangeAbort = ac
  loadingRange.value = true
  try {
    const [t, c, o, l] = await Promise.all([
      props.client.topology(props.stream, tr.value, ac.signal),
      props.client.pathChanges(props.stream, tr.value, ac.signal),
      props.client.outages(props.stream, tr.value, ac.signal),
      props.client.loops(props.stream, tr.value, ac.signal),
    ])
    topo.value = t
    changes.value = c
    outages.value = o
    loops.value = l
    error.value = ''
  } catch (e) {
    if (!ac.signal.aborted) error.value = e instanceof Error ? e.message : String(e)
  } finally {
    if (rangeAbort === ac) loadingRange.value = false
  }
}

let cycleAbort: AbortController | undefined
let cycleTimer: ReturnType<typeof setTimeout> | undefined
function loadCycle() {
  clearTimeout(cycleTimer)
  // Debounced: dragging the scrubber fires many seeks.
  cycleTimer = setTimeout(async () => {
    cycleAbort?.abort()
    const ac = new AbortController()
    cycleAbort = ac
    loadingCycle.value = true
    try {
      cycle.value = await props.client.cycleAt(props.stream, tr.value, props.at ?? tr.value.to, ac.signal)
    } catch (e) {
      if (!ac.signal.aborted) error.value = e instanceof Error ? e.message : String(e)
    } finally {
      if (cycleAbort === ac) loadingCycle.value = false
    }
  }, 120)
}

watch(() => [props.stream.agent, props.stream.target, props.stream.protocol, tr.value.from, tr.value.to], loadRange, { immediate: true })
watch(() => [props.stream.agent, props.stream.target, props.stream.protocol, props.at, props.at === undefined ? tr.value.to : 0], loadCycle, {
  immediate: true,
})
watch(
  () => [props.stream.agent, props.stream.target, props.stream.protocol],
  () => {
    selectedNode.value = null
  },
)
onBeforeUnmount(() => {
  rangeAbort?.abort()
  cycleAbort?.abort()
  clearTimeout(cycleTimer)
})

const layout = computed(() =>
  topo.value ? layoutTopology(topo.value) : { nodes: [], edges: [], maxTTL: 0, totalCycles: 0 },
)
const change = computed(() => changeAt(changes.value, cycle.value))
const highlight = computed(() => highlightCycle(cycle.value, change.value))
const series = computed(() =>
  topo.value ? timelineSeries(topo.value) : { times: [], unreachablePct: [], rttMs: [], loopCycles: [] },
)
const areas = computed(() => outageAreas(outages.value, tr.value))
const marks = computed(() => changeTimes(changes.value))

const summary = computed(() => {
  const b = topo.value?.timeline ?? []
  const cycles = b.reduce((n, x) => n + x.cycles, 0)
  const reached = b.reduce((n, x) => n + x.reached_cycles, 0)
  return { cycles, reach: cycles ? reached / cycles : NaN }
})

const cycleTs = computed(() => (cycle.value ? toMs(cycle.value.ts) : undefined))
/** Where the scrubber sits: the chosen moment, else the shown cycle. */
const scrubAt = computed(() => props.at ?? cycleTs.value ?? tr.value.to)

function seek(t: number | undefined) {
  emit('seek', t === undefined ? undefined : Math.round(t))
}
function prevChange() {
  const t = prevTime(marks.value, cycleTs.value ?? scrubAt.value)
  if (t !== undefined) seek(t)
}
function nextChange() {
  const t = nextTime(marks.value, cycleTs.value ?? scrubAt.value)
  if (t !== undefined) seek(t)
}
async function step(dir: -1 | 1) {
  const ts = cycleTs.value
  if (ts === undefined) return
  try {
    const c = dir < 0 ? await props.client.cycleBefore(props.stream, tr.value, ts) : await props.client.cycleAfter(props.stream, tr.value, ts)
    if (c) seek(toMs(c.ts))
  } catch (e) {
    error.value = e instanceof Error ? e.message : String(e)
  }
}
function onSlider(ev: Event) {
  seek(Number((ev.target as HTMLInputElement).value))
}
function onKey(ev: KeyboardEvent) {
  if (ev.key === 'ArrowLeft' || ev.key === 'ArrowRight') {
    ev.preventDefault()
    if (ev.shiftKey) {
      if (ev.key === 'ArrowLeft') prevChange()
      else nextChange()
    } else void step(ev.key === 'ArrowLeft' ? -1 : 1)
  }
}
</script>

<template>
  <div class="stream-view">
    <header class="stream-head">
      <h1>
        <span class="mono">{{ stream.target }}</span>
        <span class="proto">{{ stream.protocol || 'all protocols' }}</span>
        <span class="muted">from {{ stream.agent }}</span>
      </h1>
      <dl class="stats">
        <div><dt>Cycles</dt><dd>{{ summary.cycles }}</dd></div>
        <div><dt>Reached</dt><dd>{{ fmtPct(summary.reach) }}</dd></div>
        <div><dt>Route changes</dt><dd>{{ changes.length }}</dd></div>
        <div><dt>Outages</dt><dd>{{ outages.length }}</dd></div>
        <div><dt>Loops</dt><dd>{{ loops.length }}</dd></div>
      </dl>
    </header>

    <p v-if="error" class="error" role="alert">{{ error }}</p>
    <p v-if="topo?.truncated" class="warn-note">
      This range has more addresses than the graph shows; the busiest are drawn. Pick a shorter range for all of them.
    </p>

    <section class="panel">
      <Timeline
        :series="series"
        :bucket-ms="topo?.bucket_ms ?? 60000"
        :outages="areas"
        :changes="marks"
        :range="tr"
        :at="scrubAt"
        :theme="theme"
        @seek="seek"
      />
      <div class="scrubber" @keydown="onKey">
        <button title="Previous route change (Shift+←)" aria-label="Previous route change" @click="prevChange">⏮</button>
        <button title="Previous cycle (←)" aria-label="Previous cycle" @click="step(-1)">◀</button>
        <input
          type="range"
          aria-label="Selected moment"
          :min="tr.from"
          :max="tr.to"
          step="1000"
          :value="scrubAt"
          @input="onSlider"
        />
        <button title="Next cycle (→)" aria-label="Next cycle" @click="step(1)">▶</button>
        <button title="Next route change (Shift+→)" aria-label="Next route change" @click="nextChange">⏭</button>
        <button :class="['live', { on: at === undefined }]" title="Follow the latest cycle" @click="seek(undefined)">
          {{ at === undefined ? 'following latest' : 'latest' }}
        </button>
        <span class="muted at">{{ fmtTime(scrubAt) }}</span>
      </div>
    </section>

    <section class="panel graph-panel">
      <header class="panel-head">
        <h2>Path topology</h2>
        <span class="muted">{{ layout.nodes.length }} addresses over {{ layout.maxTTL }} TTLs{{ loadingRange ? ' · loading…' : '' }}</span>
      </header>
      <p v-if="!loadingRange && layout.nodes.length === 0" class="muted empty">No hop answered in this range.</p>
      <HopGraph v-else :layout="layout" :highlight="highlight" :theme="theme" @select="selectedNode = $event" />
      <div v-if="selectedNode" class="node-card">
        <strong>{{ hopLabel(selectedNode.name, selectedNode.ip) }}</strong>
        <span v-if="selectedNode.name" class="mono">{{ selectedNode.ip }}</span>
        <span>{{ asLabel(selectedNode.asn, selectedNode.asName) }}</span>
        <span>TTL {{ selectedNode.ttl }}</span>
        <span>{{ selectedNode.cycles }} cycles ({{ fmtPct(selectedNode.share) }})</span>
        <span>median {{ fmtRTT(selectedNode.rttUs) }}</span>
        <button class="close" aria-label="Close" @click="selectedNode = null">×</button>
      </div>
    </section>

    <div class="columns">
      <CycleDetails :cycle="cycle" :change="change" :highlight="highlight" :loading="loadingCycle" />
      <EventList :changes="changes" :outages="outages" :loops="loops" :selected-seq="cycle?.seq" @seek="seek" />
    </div>
  </div>
</template>
