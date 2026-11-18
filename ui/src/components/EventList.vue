<script setup lang="ts">
// Route changes, outages and loops of the range; choosing one moves the
// selected moment there.
import { computed, ref } from 'vue'
import type { LoopEvent, Outage, PathChange } from '../api'
import { fmtCount, fmtDuration, fmtTime, toMs } from '../format'

// cut: the lists the server cut, which hold the range's newest events only.
// status, unless empty, says why there are no events to show (loading, or
// the load failed): the lists are then not the range's.
const props = defineProps<{
  changes: PathChange[]
  outages: Outage[]
  loops: LoopEvent[]
  cut: { changes: boolean; outages: boolean; loops: boolean }
  status: string
  selectedSeq: number | undefined
}>()
const emit = defineEmits<{ seek: [t: number] }>()

type Tab = 'changes' | 'outages' | 'loops'
const tab = ref<Tab>('changes')

const reasons = { router: 'router', distance: 'distance', target: 'target address' }

const items = computed(() => {
  switch (tab.value) {
    case 'changes':
      return props.changes.map((c) => ({
        key: `c${c.seq}`,
        t: toMs(c.ts),
        seq: c.seq,
        what: reasons[c.reason],
        detail: c.ttls.map((ttl, i) => `${ttl}: ${c.from_ips[i] || '*'} → ${c.to_ips[i] || '*'}`).join(', ') || `${c.prev_path_len} → ${c.path_len} hops`,
      }))
    case 'outages':
      return props.outages.map((o) => ({
        key: `o${o.start_seq}`,
        t: toMs(o.start_ts),
        seq: o.start_seq,
        what: o.end_ts ? fmtDuration(toMs(o.end_ts) - toMs(o.start_ts)) : 'ongoing',
        detail: `${o.cycles} cycles${o.last_hop_ip ? `, last answer TTL ${o.last_hop_ttl} ${o.last_hop_ip}` : ', no hop answered'}`,
      }))
    default:
      return props.loops.map((l) => ({
        key: `l${l.seq}-${l.loop_ip}`,
        t: toMs(l.ts),
        seq: l.seq,
        what: l.loop_ip,
        detail: `TTL ${l.ttls.join(', ')}`,
      }))
  }
})

const tabs = computed(() => [
  { id: 'changes' as const, label: 'Route changes', n: fmtCount(props.changes.length, props.cut.changes) },
  { id: 'outages' as const, label: 'Outages', n: fmtCount(props.outages.length, props.cut.outages) },
  { id: 'loops' as const, label: 'Loops', n: fmtCount(props.loops.length, props.cut.loops) },
])
</script>

<template>
  <section class="panel events">
    <div class="tabs" role="tablist">
      <button
        v-for="t in tabs"
        :key="t.id"
        role="tab"
        :aria-selected="tab === t.id"
        :class="{ active: tab === t.id }"
        @click="tab = t.id"
      >
        {{ t.label }} <span class="count">{{ status ? '–' : t.n }}</span>
      </button>
    </div>
    <p v-if="status" class="muted empty">{{ status }}</p>
    <ol v-else-if="items.length" class="event-list">
      <li v-for="it in items" :key="it.key">
        <button :class="{ current: it.seq === selectedSeq }" @click="emit('seek', it.t)">
          <span class="when">{{ fmtTime(it.t) }}</span>
          <span class="what">{{ it.what }}</span>
          <span class="detail mono">{{ it.detail }}</span>
        </button>
      </li>
    </ol>
    <p v-else class="muted empty">None in this range.</p>
    <p v-if="!status && cut[tab]" class="muted list-end">The newest {{ items.length }} only; the range has more.</p>
  </section>
</template>
