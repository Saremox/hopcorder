// An ECharts instance bound to a component's element: created on mount,
// resized with the element, disposed on unmount.
import { onBeforeUnmount, onMounted, shallowRef, type Ref, type ShallowRef } from 'vue'
import { echarts, type ECharts } from './echarts'

export function useChart(el: Ref<HTMLElement | null>, onInit?: (c: ECharts) => void): ShallowRef<ECharts | null> {
  const chart = shallowRef<ECharts | null>(null)
  let ro: ResizeObserver | undefined
  onMounted(() => {
    if (!el.value) return
    const c = echarts.init(el.value, undefined, { renderer: 'canvas' })
    chart.value = c
    ro = new ResizeObserver(() => c.resize())
    ro.observe(el.value)
    onInit?.(c)
  })
  onBeforeUnmount(() => {
    ro?.disconnect()
    chart.value?.dispose()
    chart.value = null
  })
  return chart
}
