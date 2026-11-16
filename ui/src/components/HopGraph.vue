<script setup lang="ts">
// The path topology: addresses per TTL as nodes, transitions between
// consecutive answering hops as edges, with the selected cycle's path, its
// loops and a route change at it highlighted. Scroll to zoom, drag to pan.
import { computed, ref, watch } from 'vue'
import { useChart } from '../chart'
import type { Option } from '../echarts'
import { asLabel, escapeHTML, fmtPct, fmtRTT, hopLabel } from '../format'
import { AS_SLOTS, type Theme } from '../theme'
import { ROW_HEIGHT, asSlots, edgeWidth, nodeSize, type GraphNode, type Highlight, type Layout } from '../topology'

const props = defineProps<{ layout: Layout; highlight: Highlight; theme: Theme }>()
const emit = defineEmits<{ select: [node: GraphNode | null] }>()

const el = ref<HTMLElement | null>(null)
const chart = useChart(el, (c) => {
  c.on('click', (p) => {
    const d = p.data as { node?: GraphNode } | undefined
    emit('select', p.dataType === 'node' && d?.node ? d.node : null)
  })
  render(true)
})

const slots = computed(() => asSlots(props.layout.nodes, AS_SLOTS))

/** Tall enough for the busiest TTL column. */
const height = computed(() => {
  const ys = props.layout.nodes.map((n) => n.y)
  const rows = ys.length ? (Math.max(...ys) - Math.min(...ys)) / ROW_HEIGHT + 1 : 1
  return `${Math.min(Math.max(140 + rows * 70, 260), 640)}px`
})

/** The ASes in the legend, busiest first. */
const legend = computed(() => {
  const seen = new Map<number, { asn: number; name: string; slot: number }>()
  for (const n of props.layout.nodes) {
    const slot = slots.value.get(n.asn)
    if (slot !== undefined && slot < AS_SLOTS - 1 && !seen.has(n.asn)) seen.set(n.asn, { asn: n.asn, name: n.asName, slot })
  }
  const items = [...seen.values()].sort((a, b) => a.slot - b.slot)
  const other = [...slots.value.values()].some((s) => s === AS_SLOTS - 1)
  return { items, other }
})

function nodeColor(n: GraphNode, t: Theme): string {
  const slot = slots.value.get(n.asn)
  return slot === undefined ? t.muted : (t.as[slot] ?? t.muted)
}

function nodeTooltip(n: GraphNode): string {
  const lines = [
    `<b>${escapeHTML(hopLabel(n.name, n.ip))}</b>`,
    n.name ? escapeHTML(n.ip) : '',
    escapeHTML(asLabel(n.asn, n.asName)),
    `TTL ${n.ttl}${n.destination ? ' · destination' : ''}`,
    `answered in ${n.cycles} cycles (${fmtPct(n.share)})`,
    `median RTT ${fmtRTT(n.rttUs)}`,
    n.looped ? 'seen in a loop' : '',
  ]
  return lines.filter(Boolean).join('<br>')
}

function shortLabel(n: GraphNode): string {
  const s = hopLabel(n.name, n.ip)
  return s.length > 30 ? `${s.slice(0, 29)}…` : s
}

function build(): Option {
  const t = props.theme
  const h = props.highlight
  const selected = h.nodes.size > 0
  const minY = Math.min(0, ...props.layout.nodes.map((n) => n.y))

  const nodes: object[] = props.layout.nodes.map((n) => {
    const inPath = h.nodes.has(n.id)
    const loop = h.loopNodes.has(n.id)
    const changedTo = h.changedTo.has(n.id)
    const changedFrom = h.changedFrom.has(n.id)
    let borderColor = t.panel
    let borderWidth = 1
    let borderType: 'solid' | 'dashed' = 'solid'
    if (n.looped) [borderColor, borderWidth, borderType] = [t.loop, 1.5, 'dashed']
    if (inPath) [borderColor, borderWidth, borderType] = [t.accent, 3, 'solid']
    if (changedTo) [borderColor, borderWidth] = [t.warn, 4]
    if (changedFrom) [borderColor, borderWidth, borderType] = [t.warn, 3, 'dashed']
    if (loop) [borderColor, borderWidth] = [t.loop, 4]
    const dim = selected && !inPath && !changedFrom
    return {
      id: n.id,
      name: n.id,
      x: n.x,
      y: n.y,
      node: n,
      symbol: n.destination ? 'diamond' : 'circle',
      symbolSize: nodeSize(n.share) * (n.destination ? 1.3 : 1),
      itemStyle: { color: nodeColor(n, t), borderColor, borderWidth, borderType, opacity: dim ? 0.35 : 1 },
      label: {
        show: inPath || changedFrom || n.destination || n.looped || n.share >= 0.2,
        formatter: shortLabel(n),
        color: dim ? t.muted : t.text,
        fontWeight: inPath ? 'bold' : 'normal',
      },
      tooltip: { formatter: nodeTooltip(n) },
    }
  })
  // TTL headings as label-only nodes, so they pan and zoom with the graph.
  for (let ttl = 1; ttl <= props.layout.maxTTL; ttl++) {
    const x = props.layout.nodes.find((n) => n.ttl === ttl)?.x ?? (ttl - 1) * 150
    nodes.push({
      id: `ttl-${ttl}`,
      name: `ttl-${ttl}`,
      x,
      y: minY - ROW_HEIGHT,
      symbol: 'circle',
      symbolSize: 1,
      itemStyle: { color: 'rgba(0,0,0,0)' },
      label: { show: true, position: 'top', formatter: `TTL ${ttl}`, color: t.muted },
      tooltip: { show: false },
    })
  }

  const links = props.layout.edges.map((e) => {
    const inPath = h.edges.has(e.id)
    const loop = inPath && h.loopNodes.has(e.source) && h.loopNodes.has(e.target)
    return {
      source: e.source,
      target: e.target,
      lineStyle: {
        width: edgeWidth(e.share) + (inPath ? 2 : 0),
        type: e.span > 1 ? 'dashed' : 'solid',
        curveness: e.span > 1 ? 0.25 : 0,
        color: loop ? t.loop : inPath ? t.accent : t.muted,
        opacity: selected ? (inPath ? 1 : 0.2) : 0.55,
      },
      tooltip: {
        formatter: `${e.cycles} cycles (${fmtPct(e.share)})${e.span > 1 ? `<br>${e.span - 1} silent TTL${e.span > 2 ? 's' : ''} between` : ''}`,
      },
    }
  })

  return {
    animationDurationUpdate: 150,
    tooltip: { trigger: 'item', confine: true, backgroundColor: t.panel, borderColor: t.border, textStyle: { color: t.text } },
    series: [
      {
        id: 'graph',
        type: 'graph',
        layout: 'none',
        roam: true,
        scaleLimit: { min: 0.2, max: 6 },
        edgeSymbol: ['none', 'arrow'],
        edgeSymbolSize: [0, 7],
        label: { position: 'bottom', fontSize: 11, distance: 4 },
        emphasis: { focus: 'adjacency', lineStyle: { opacity: 1 } },
        data: nodes,
        links,
      },
    ],
  } as Option
}

function render(full: boolean) {
  const c = chart.value
  if (!c) return
  // A new layout resets zoom and pan; a new highlight keeps them.
  c.setOption(build(), full ? { notMerge: true } : { lazyUpdate: true })
}

watch(() => [props.layout, props.theme], () => render(true))
watch(() => props.highlight, () => render(false))

defineExpose({ resetView: () => render(true) })
</script>

<template>
  <div class="graph-wrap">
    <div ref="el" class="graph" :style="{ height }" role="img" aria-label="Hop graph of the selected stream"></div>
    <ul class="legend" aria-label="Origin AS colours">
      <li v-for="a in legend.items" :key="a.asn">
        <span class="swatch" :style="{ background: theme.as[a.slot] }"></span>{{ asLabel(a.asn, a.name) }}
      </li>
      <li v-if="legend.other"><span class="swatch" :style="{ background: theme.as[AS_SLOTS - 1] }"></span>other ASes</li>
      <li><span class="swatch" :style="{ background: theme.muted }"></span>no AS (private)</li>
      <li><span class="swatch ring" :style="{ borderColor: theme.accent }"></span>selected cycle</li>
      <li><span class="swatch ring" :style="{ borderColor: theme.warn }"></span>route change</li>
      <li><span class="swatch ring" :style="{ borderColor: theme.loop }"></span>loop (dashed: seen in a loop)</li>
      <li><span class="diamond"></span>destination</li>
      <li><span class="dash"></span>silent TTLs between</li>
    </ul>
  </div>
</template>
