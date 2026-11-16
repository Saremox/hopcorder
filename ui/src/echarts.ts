// ECharts with only what the UI draws, to keep the bundle small.
import { BarChart, GraphChart, LineChart } from 'echarts/charts'
import { GridComponent, MarkAreaComponent, MarkLineComponent, TooltipComponent } from 'echarts/components'
import * as echarts from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'

echarts.use([GraphChart, BarChart, LineChart, GridComponent, TooltipComponent, MarkLineComponent, MarkAreaComponent, CanvasRenderer])

export { echarts }
export type ECharts = ReturnType<typeof echarts.init>
export type Option = Parameters<ECharts['setOption']>[0]
