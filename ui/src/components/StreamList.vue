<script setup lang="ts">
// Every stream that recorded cycles in the range.
import { computed, ref, shallowRef, watch } from 'vue'
import type { Client, Stream } from '../api'
import { fmtPct, fmtTime } from '../format'
import { formatHash, resolveRange, type Range } from '../state'

const props = defineProps<{ client: Client; range: Range; now: number }>()

const streams = shallowRef<Stream[]>([])
const loading = ref(false)
const error = ref('')
const filter = ref('')

let ac: AbortController | undefined
async function load() {
  ac?.abort()
  const mine = new AbortController()
  ac = mine
  loading.value = true
  try {
    streams.value = await props.client.targets(resolveRange(props.range, props.now), mine.signal)
    error.value = ''
  } catch (e) {
    if (!mine.signal.aborted) error.value = e instanceof Error ? e.message : String(e)
  } finally {
    if (ac === mine) loading.value = false
  }
}
watch(() => [props.range, props.now], load, { immediate: true })

const shown = computed(() => {
  const f = filter.value.trim().toLowerCase()
  if (!f) return streams.value
  return streams.value.filter((s) => `${s.agent_id} ${s.target} ${s.protocol} ${s.last_target_ip}`.toLowerCase().includes(f))
})

function link(s: Stream): string {
  return formatHash({ view: 'stream', stream: { agent: s.agent_id, target: s.target, protocol: s.protocol }, range: props.range })
}
</script>

<template>
  <section class="panel streams">
    <header class="panel-head">
      <h1>Streams</h1>
      <span class="muted">{{ loading ? 'loading…' : `${shown.length} of ${streams.length}` }}</span>
      <input v-model="filter" class="filter" type="search" placeholder="Filter by agent, target, protocol or address" aria-label="Filter streams" />
    </header>
    <p v-if="error" class="error" role="alert">{{ error }}</p>
    <div class="table-scroll">
      <table v-if="shown.length" class="stream-table">
        <thead>
          <tr>
            <th>Target</th>
            <th>Protocol</th>
            <th>Agent</th>
            <th>Reached</th>
            <th>Last cycle</th>
            <th class="num">Cycles</th>
            <th>Last seen</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="s in shown" :key="`${s.agent_id}|${s.target}|${s.protocol}|${s.flow_id}`">
            <td>
              <a :href="link(s)" class="mono">{{ s.target }}</a>
              <span v-if="s.last_target_ip !== s.target" class="muted mono addr">{{ s.last_target_ip }}</span>
            </td>
            <td>{{ s.protocol }}</td>
            <td>{{ s.agent_id }}</td>
            <td>
              <span class="reach"><span class="reach-bar" :style="{ width: `${Math.round(s.reach_ratio * 100)}%` }"></span></span>
              {{ fmtPct(s.reach_ratio) }}
            </td>
            <td>
              <span :class="['badge', s.last_reached ? 'ok' : 'bad']">{{ s.last_reached ? 'reached' : 'not reached' }}</span>
              <span class="muted">{{ s.last_path_len }} hops</span>
            </td>
            <td class="num">{{ s.cycles }}</td>
            <td>{{ fmtTime(s.last_ts, now) }}</td>
          </tr>
        </tbody>
      </table>
      <p v-else-if="!loading && !error" class="muted empty">
        No cycles recorded in this range. Point an agent's <code>outputs.http.url</code> at this server.
      </p>
    </div>
  </section>
</template>
