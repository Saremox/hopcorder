<script setup lang="ts">
// Every stream that recorded cycles in the range. With match (a stream link
// that names no single stream: no protocol or flow_id), only the streams it
// matches; if that is one, it is opened instead (only).
import { computed, ref, shallowRef, watch } from 'vue'
import { onlyStream, type Client, type Stream, type StreamKey } from '../api'
import { fmtPct, fmtTime } from '../format'
import { resolveRange, streamLink, type Range } from '../state'

const props = defineProps<{ client: Client; range: Range; now: number; match?: StreamKey }>()
const emit = defineEmits<{ only: [stream: StreamKey] }>()

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
    streams.value = await props.client.targets(resolveRange(props.range, props.now), mine.signal, props.match)
    error.value = ''
    const only = props.match && onlyStream(streams.value)
    if (only) emit('only', only)
  } catch (e) {
    if (!mine.signal.aborted) error.value = e instanceof Error ? e.message : String(e)
  } finally {
    if (ac === mine) loading.value = false
  }
}
watch(() => [props.range, props.now, props.match], load, { immediate: true })

const shown = computed(() => {
  const f = filter.value.trim().toLowerCase()
  if (!f) return streams.value
  return streams.value.filter((s) =>
    `${s.agent_id} ${s.target} ${s.protocol} IPv${s.last_af} ${s.flow_id} ${s.last_target_ip}`.toLowerCase().includes(f),
  )
})
</script>

<template>
  <section class="panel streams">
    <header class="panel-head">
      <h1>Streams</h1>
      <span class="muted">{{ loading ? 'loading…' : `${shown.length} of ${streams.length}` }}</span>
      <input v-model="filter" class="filter" type="search" placeholder="Filter by agent, target, protocol or address" aria-label="Filter streams" />
    </header>
    <p v-if="match && !loading && streams.length > 1" class="warn-note match-note">
      This link names no single stream of <span class="mono">{{ match.target }}</span> from {{ match.agent }}: pick one.
    </p>
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
              <a :href="streamLink(s, range)" class="mono">{{ s.target }}</a>
              <span v-if="s.last_target_ip !== s.target" class="muted mono addr">{{ s.last_target_ip }}</span>
            </td>
            <td>
              {{ s.protocol }}
              <span class="muted flow">{{ s.last_af ? `IPv${s.last_af} · ` : '' }}flow {{ s.flow_id }}</span>
            </td>
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
      <p v-else-if="!loading && !error && match" class="muted empty">
        No cycles of <span class="mono">{{ match.target }}</span> from {{ match.agent }} in this range.
      </p>
      <p v-else-if="!loading && !error" class="muted empty">
        No cycles recorded in this range. Point an agent's <code>outputs.http.url</code> at this server.
      </p>
    </div>
  </section>
</template>
