<script setup lang="ts">
// One cycle hop by hop, with loop hops, a route change at this cycle and
// silent TTLs marked.
import { computed } from 'vue'
import type { Cycle, PathChange } from '../api'
import { asLabel, fmtRTT, fmtTime } from '../format'
import { nodeId, type Highlight } from '../topology'

const props = defineProps<{ cycle: Cycle | null; change: PathChange | null; highlight: Highlight; loading: boolean; failed: boolean }>()

const UNREACHABLE: Record<number, string> = {
  0: 'net unreachable',
  1: 'host unreachable',
  2: 'protocol unreachable',
  3: 'port unreachable',
  4: 'fragmentation needed',
  9: 'net prohibited',
  10: 'host prohibited',
  13: 'administratively prohibited',
}

const rows = computed(() =>
  (props.cycle?.hops ?? []).map((h) => {
    const id = h.ip ? nodeId(h.ttl, h.ip) : ''
    let status: string = h.status
    if (h.status === 'unreachable' && h.code !== undefined) status = `unreachable (${UNREACHABLE[h.code] ?? `code ${h.code}`})`
    return {
      ...h,
      statusText: status,
      loop: id !== '' && props.highlight.loopNodes.has(id),
      changed: id !== '' && props.highlight.changedTo.has(id),
      silent: h.status === 'no-reply',
      mplsText: (h.mpls ?? []).map((l) => `${l.label}${l.s ? '' : '+'}`).join(' '),
    }
  }),
)

const loopText = computed(() => (props.cycle?.loops ?? []).map((l) => `${l.ip} at TTL ${l.ttls.join(', ')}`).join('; '))

const changeText = computed(() => {
  const c = props.change
  if (!c) return ''
  const hops = c.ttls.map((ttl, i) => `TTL ${ttl}: ${c.from_ips[i] || '*'} → ${c.to_ips[i] || '*'}`)
  const why = { router: 'a router changed', distance: 'the destination moved', target: 'the target address changed' }[c.reason]
  return `${why}${hops.length ? ` (${hops.join(', ')})` : ''}${c.prev_path_len !== c.path_len ? `, path length ${c.prev_path_len} → ${c.path_len}` : ''}`
})
</script>

<template>
  <section class="panel cycle" aria-live="polite">
    <header class="panel-head">
      <h2>Cycle</h2>
      <template v-if="cycle">
        <span class="mono">#{{ cycle.seq }}</span>
        <span>{{ fmtTime(cycle.ts) }}</span>
        <span :class="['badge', cycle.reached ? 'ok' : 'bad']">{{ cycle.reached ? 'reached' : 'not reached' }}</span>
        <span class="muted">{{ cycle.target_ip }} · {{ cycle.path_len }} hops</span>
      </template>
      <span v-else-if="loading" class="muted">loading…</span>
      <span v-else-if="failed" class="muted">not loaded</span>
      <span v-else class="muted">no cycle in this range</span>
    </header>
    <p v-if="change" class="change-note"><strong>Route change:</strong> {{ changeText }}</p>
    <p v-if="cycle && cycle.loops.length" class="loop-note">
      <strong>Loop:</strong> <span class="mono">{{ loopText }}</span>
    </p>
    <div class="table-scroll">
      <table v-if="cycle" class="hops">
        <thead>
          <tr>
            <th class="num">TTL</th>
            <th>Address</th>
            <th>Name</th>
            <th>AS</th>
            <th class="num">RTT</th>
            <th>Status</th>
            <th>MPLS</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="h in rows" :key="h.ttl" :class="{ loop: h.loop, changed: h.changed, silent: h.silent }">
            <td class="num">{{ h.ttl }}</td>
            <td class="mono">{{ h.ip || '*' }}</td>
            <td class="name">{{ h.name }}</td>
            <td>{{ asLabel(h.asn, h.as_name) }}</td>
            <td class="num">{{ fmtRTT(h.rtt_us) }}</td>
            <td>{{ h.statusText }}</td>
            <td class="mono">{{ h.mplsText }}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </section>
</template>
