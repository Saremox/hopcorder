<script setup lang="ts">
// Reachability over the range: unreachable share per bucket (bars) and the
// destination's median RTT (line), outage windows shaded, route changes as
// dashed lines, and the selected moment. Click or drag to move it.
import { ref, watch } from 'vue'
import type { TimeRange } from '../api'
import { useChart } from '../chart'
import type { ECharts, Option } from '../echarts'
import type { Theme } from '../theme'
import type { TimelineSeries } from '../timeline'

const props = defineProps<{
  series: TimelineSeries
  bucketMs: number
  outages: [number, number][]
  changes: number[]
  range: TimeRange
  at: number | undefined
  theme: Theme
}>()
const emit = defineEmits<{ seek: [t: number] }>()

const el = ref<HTMLElement | null>(null)
let dragging = false

function seekAt(c: ECharts, x: number, y: number) {
  if (!c.containPixel('grid', [x, y])) return
  const v = c.convertFromPixel({ gridIndex: 0 }, [x, y]) as number[]
  const t = v[0]
  if (t !== undefined && Number.isFinite(t)) emit('seek', Math.min(Math.max(t, props.range.from), props.range.to))
}

const chart = useChart(el, (c) => {
  const zr = c.getZr()
  zr.on('mousedown', (e) => {
    dragging = true
    seekAt(c, e.offsetX, e.offsetY)
  })
  zr.on('mousemove', (e) => {
    if (dragging) seekAt(c, e.offsetX, e.offsetY)
  })
  zr.on('mouseup', () => {
    dragging = false
  })
  zr.on('globalout', () => {
    dragging = false
  })
  render()
})

function build(): Option {
  const t = props.theme
  const s = props.series
  const half = props.bucketMs / 2
  const marks: object[] = props.changes.map((x) => ({
    xAxis: x,
    lineStyle: { color: t.warn, type: 'dashed', width: 1.5 },
    label: { show: false },
  }))
  if (props.at !== undefined) {
    marks.push({
      xAxis: props.at,
      lineStyle: { color: t.accent, type: 'solid', width: 2 },
      label: { show: false },
    })
  }
  return {
    animation: false,
    grid: { left: 52, right: 60, top: 18, bottom: 26 },
    tooltip: {
      trigger: 'axis',
      confine: true,
      backgroundColor: t.panel,
      borderColor: t.border,
      textStyle: { color: t.text },
      valueFormatter: (v: unknown) => (typeof v === 'number' ? String(Math.round(v * 100) / 100) : '–'),
    },
    xAxis: {
      type: 'time',
      min: props.range.from,
      max: props.range.to,
      axisLine: { lineStyle: { color: t.border } },
      axisLabel: { color: t.muted, hideOverlap: true },
      splitLine: { show: false },
    },
    yAxis: [
      {
        type: 'value',
        min: 0,
        max: 100,
        interval: 50,
        axisLabel: { color: t.muted, formatter: '{value}%' },
        splitLine: { lineStyle: { color: t.grid } },
      },
      {
        type: 'value',
        min: 0,
        axisLabel: { color: t.muted, formatter: '{value} ms' },
        splitLine: { show: false },
      },
    ],
    series: [
      {
        name: 'unreachable %',
        type: 'bar',
        yAxisIndex: 0,
        barMaxWidth: 14,
        barMinWidth: 1,
        itemStyle: { color: t.bad },
        data: s.times.map((x, i) => [x + half, s.unreachablePct[i] ?? 0]),
        markArea: {
          silent: true,
          itemStyle: { color: t.bad, opacity: 0.12 },
          data: props.outages.map(([a, b]) => [{ xAxis: a }, { xAxis: b }]),
        },
        markLine: { silent: true, symbol: 'none', animation: false, data: marks },
      },
      {
        name: 'RTT p50 ms',
        type: 'line',
        yAxisIndex: 1,
        showSymbol: false,
        connectNulls: false,
        lineStyle: { width: 1.5, color: t.accent },
        itemStyle: { color: t.accent },
        data: s.times.map((x, i) => [x + half, s.rttMs[i] ?? null]),
      },
    ],
  } as Option
}

function render() {
  chart.value?.setOption(build(), { notMerge: true })
}

watch(() => [props.series, props.outages, props.changes, props.range, props.at, props.theme], render)
</script>

<template>
  <div ref="el" class="timeline" role="img" aria-label="Reachability timeline; click to select a moment"></div>
</template>
