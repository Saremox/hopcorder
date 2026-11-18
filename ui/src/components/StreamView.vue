<script setup lang="ts">
// One stream (agent, target, protocol, flow) over the range: the timeline with the
// time scrubber, the hop graph, the selected cycle and the range's events.
import { computed, onBeforeUnmount, ref, shallowRef, watch } from 'vue'
import type { Client, Cycle, LoopEvent, Outage, PathChange, StreamKey, Topology } from '../api'
import { asLabel, cutNote, fmtCount, fmtPct, fmtRTT, fmtTime, hopLabel, toMs } from '../format'
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
/** Which of the lists the server cut: they hold the range's newest events only. */
const cut = shallowRef({ changes: false, outages: false, loops: false })
const cycle = shallowRef<Cycle | null>(null)
const loadingRange = ref(false)
const loadingCycle = ref(false)
const rangeError = ref('')
const cycleError = ref('')
const selectedNode = shallowRef<GraphNode | null>(null)

const message = (e: unknown) => (e instanceof Error ? e.message : String(e))

// What is shown is of one view: the stream and the range as chosen (a
// relative range moves with the clock, and a refresh keeps what is shown
// until it arrives). Another view's data is never shown, not while this
// one loads and not after it failed: the error takes its place (review
// round seven: after a failed load the previous stream's or range's
// figures, graph and cycle stayed under the new header).
const key = () => [props.stream.agent, props.stream.target, props.stream.protocol, props.stream.flow]
const viewOf = () => JSON.stringify([...key(), props.range])
/** The view topo, changes, outages and loops are of ('' for none). */
const rangeView = ref('')
const rangeLoaded = computed(() => rangeView.value === viewOf())

function clearRange() {
  topo.value = null
  changes.value = []
  outages.value = []
  loops.value = []
  cut.value = { changes: false, outages: false, loops: false }
  rangeView.value = ''
  selectedNode.value = null
}

let rangeAbort: AbortController | undefined
async function loadRange() {
  rangeAbort?.abort()
  const ac = new AbortController()
  rangeAbort = ac
  const view = viewOf()
  if (rangeView.value !== view) {
    clearRange()
    rangeError.value = ''
  }
  loadingRange.value = true
  try {
    const [t, c, o, l] = await Promise.all([
      props.client.topology(props.stream, tr.value, ac.signal),
      props.client.pathChanges(props.stream, tr.value, ac.signal),
      props.client.outages(props.stream, tr.value, ac.signal),
      props.client.loops(props.stream, tr.value, ac.signal),
    ])
    if (ac.signal.aborted) return
    topo.value = t
    changes.value = c.rows
    outages.value = o.rows
    loops.value = l.rows
    cut.value = { changes: c.truncated, outages: o.truncated, loops: l.truncated }
    rangeView.value = view
    rangeError.value = ''
  } catch (e) {
    if (!ac.signal.aborted) {
      clearRange()
      rangeError.value = message(e)
    }
  } finally {
    if (rangeAbort === ac) loadingRange.value = false
  }
}

let cycleAbort: AbortController | undefined
let cycleTimer: ReturnType<typeof setTimeout> | undefined
/** The view cycle is of. */
let cycleView = ''
function loadCycle() {
  clearTimeout(cycleTimer)
  const view = viewOf()
  if (cycleView !== view) {
    // The request below waits: drop the other view's cycle, and its answer, now.
    cycleAbort?.abort()
    cycleAbort = undefined
    cycle.value = null
    cycleError.value = ''
    cycleView = view
    loadingCycle.value = true
  }
  // Debounced: dragging the scrubber fires many seeks.
  cycleTimer = setTimeout(async () => {
    cycleAbort?.abort()
    const ac = new AbortController()
    cycleAbort = ac
    loadingCycle.value = true
    try {
      const c = await props.client.cycleAt(props.stream, tr.value, props.at ?? tr.value.to, ac.signal)
      if (ac.signal.aborted) return
      cycle.value = c
      cycleError.value = ''
    } catch (e) {
      if (!ac.signal.aborted) {
        cycle.value = null
        cycleError.value = message(e)
      }
    } finally {
      if (cycleAbort === ac) loadingCycle.value = false
    }
  }, 120)
}

watch(() => [...key(), tr.value.from, tr.value.to], loadRange, { immediate: true })
watch(() => [...key(), props.at, props.at === undefined ? tr.value.to : 0], loadCycle, {
  immediate: true,
})
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

const error = computed(() => [rangeError.value, cycleError.value].filter((e) => e !== '').join('; '))
/** What the range's panels say while they have no data of this view. */
const rangeStatus = computed(() => (rangeLoaded.value ? '' : loadingRange.value ? 'loading…' : 'not loaded'))
const stat = (v: number | string) => (rangeLoaded.value ? v : '–')
const listNote = computed(() =>
  cutNote([
    { what: 'route changes', rows: changes.value.length, cut: cut.value.changes },
    { what: 'outages', rows: outages.value.length, cut: cut.value.outages },
    { what: 'loops', rows: loops.value.length, cut: cut.value.loops },
  ]),
)
const graphNote = computed(() =>
  [rangeLoaded.value ? `${layout.value.nodes.length} addresses over ${layout.value.maxTTL} TTLs` : '', loadingRange.value ? 'loading…' : '']
    .filter((s) => s !== '')
    .join(' · '),
)

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
    cycleError.value = message(e)
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
        <span class="proto">{{ stream.protocol }}</span>
        <span class="muted">{{ cycle ? `IPv${cycle.af} · ` : '' }}flow {{ stream.flow }} from {{ stream.agent }}</span>
      </h1>
      <dl class="stats">
        <div><dt>Cycles</dt><dd>{{ stat(summary.cycles) }}</dd></div>
        <div><dt>Reached</dt><dd>{{ stat(fmtPct(summary.reach)) }}</dd></div>
        <div><dt>Route changes</dt><dd>{{ stat(fmtCount(changes.length, cut.changes)) }}</dd></div>
        <div><dt>Outages</dt><dd>{{ stat(fmtCount(outages.length, cut.outages)) }}</dd></div>
        <div><dt>Loops</dt><dd>{{ stat(fmtCount(loops.length, cut.loops)) }}</dd></div>
      </dl>
    </header>

    <p v-if="error" class="error" role="alert">{{ error }}</p>
    <p v-if="topo?.truncated" class="warn-note">
      This range has more addresses than the graph shows; the busiest are drawn. Pick a shorter range for all of them.
    </p>
    <p v-if="listNote" class="warn-note list-note">{{ listNote }}</p>

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
        <span class="muted">{{ graphNote }}</span>
      </header>
      <p v-if="rangeStatus === 'not loaded'" class="muted empty">Not loaded.</p>
      <p v-else-if="rangeLoaded && layout.nodes.length === 0" class="muted empty">No hop answered in this range.</p>
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
      <CycleDetails :cycle="cycle" :change="change" :highlight="highlight" :loading="loadingCycle" :failed="cycleError !== ''" />
      <EventList :changes="changes" :outages="outages" :loops="loops" :cut="cut" :status="rangeStatus" :selected-seq="cycle?.seq" @seek="seek" />
    </div>
  </div>
</template>
