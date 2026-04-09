<template>
    <v-container class="mt-4">
        <v-row>
            <v-col cols="12">
                <div ref="timeseriesChart" style="width: 100%; height: 600px;"></div>
            </v-col>
        </v-row>
    </v-container>
</template>

<script setup lang="ts">
import { ref, watch, onMounted, onBeforeUnmount } from 'vue'
import * as echarts from 'echarts'
import moment from 'moment'

import { useMainStore } from '~/stores/main'
const mainStore = useMainStore();

// Props
interface Props {
    data: any | null
    selectedVariable: string
    startTime: number | null
    endTime: number | null
}

const props = withDefaults(defineProps<Props>(), {
    data: null,
    selectedVariable: 'temperature',
    startTime: null,
    endTime: null
})

// Emits
const emit = defineEmits<{
    'update-date-range': [{ fromDate: string; toDate: string }]
}>()

// Chart references
const timeseriesChart = ref<HTMLElement | null>(null)

// Chart instance
let tsChartInstance: echarts.ECharts | null = null

const tz = 'America/Vancouver'

/**
 * Filter data by time range
 */
function filterDataByTime(data: any, startTime: number | null, endTime: number | null) {
    if (!data || !startTime || !endTime) {
        return data
    }

    const filteredIndices: number[] = []
    for (let i = 0; i < data.time.length; i++) {
        const time = new Date(data.time[i]).getTime()
        if (time >= startTime && time <= endTime) {
            filteredIndices.push(i)
        }
    }

    return {
        sensor: filteredIndices.map(i => data.sensor[i]),
        model: filteredIndices.map(i => data.model[i]),
        time: filteredIndices.map(i => data.time[i])
    }
}

/**
 * Refresh all charts when data or visibility changes
 */
function refreshCharts() {
    plotTimeseries()
}

/**
 * Plot time series comparison
 */
function plotTimeseries() {
    if (!timeseriesChart.value) return
    if (!props.data) return

    // Filter data by time range
    const displayData = filterDataByTime(props.data, props.startTime, props.endTime)

    const seriesData: any[] = []
    const threshold = 1000 // Threshold for downsampling

    seriesData.push({
        name: 'Sensor',
        type: 'line',
        data: displayData.sensor.map((d: any, i: number) => [moment.utc(displayData.time[i]).tz(tz).format(), d]).filter((d: any) => d[1] !== null),
        smooth: true,
        lineStyle: { width: 2, color: mainStore.colors.observation.line, opacity: 0.75 },
        showSymbol: false,
        itemStyle: { color: mainStore.colors.observation.line, opacity: 0.75 },
        large: true,
        largeThreshold: threshold,
        sampling: 'lttb'
    })

    seriesData.push({
        name: 'Model',
        type: 'line',
        data: displayData.model.map((d: any, i: number) => [moment.utc(displayData.time[i]).tz(tz).format(), d]).filter((d: any) => d[1] !== null),
        smooth: true,
        lineStyle: { width: 2, color: mainStore.colors.model.line, opacity: 0.75 },
        showSymbol: false,
        symbol: "none",
        symbolSize: 0,
        showAllSymbol: false,
        itemStyle: { color: mainStore.colors.model.line, opacity: 0.75 },
        large: true,
        largeThreshold: threshold,
        sampling: 'lttb'
    })

    const option = {
        title: {
            text: `${props.selectedVariable} - Time Series`,
            textStyle: { color: '#e0e0e0' }
        },
        tooltip: { trigger: 'axis' },
        legend: {
            data: seriesData.map(s => s.name),
            orient: 'vertical',
            right: 10,
            top: 'middle',
            textStyle: { color: '#e0e0e0' },
            icon: 'rect'
        },
        toolbox: {
            feature: {
                dataZoom: { yAxisIndex: 'none' },
                restore: {},
                saveAsImage: {}
            }
        },
        xAxis: { type: 'time', name: 'Date', axisLabel: { color: '#e0e0e0' }, nameTextStyle: { color: '#e0e0e0' } },
        yAxis: {
            type: 'value',
            min: 'dataMin',
            max: 'dataMax',
            axisLabel: { color: '#e0e0e0', formatter: (value: any) => Number(value).toFixed(0) },
            name: mainStore.variables.find((v: any) => v.var === props.selectedVariable)?.unit ?? '',
            nameLocation: 'center',
            nameTextStyle: { color: '#e0e0e0' },
        },
        series: seriesData,
        grid: { left: 60, right: 120, top: 60, bottom: 60 }
    }

    console.log(mainStore.variables, props.selectedVariable);

    if (!tsChartInstance) {
        tsChartInstance = echarts.init(timeseriesChart.value as any)
        // Listen for datazoom events to sync back to parent
        tsChartInstance.on('datazoom', handleDataZoom)
        // Listen for restore events to reset date pickers
        tsChartInstance.on('restore', handleRestore)
    }
    tsChartInstance.setOption(option)
}

/**
 * Watch for data changes and re-render chart
 */
watch(() => props.data, () => {
    refreshCharts()
}, { deep: false })

/**
 * Watch for date range changes and re-render chart
 */
watch(() => [props.startTime, props.endTime], () => {
    refreshCharts()
}, { deep: false })

/**
 * Handle datazoom event - convert zoomed range back to dates and emit to parent
 */
function handleDataZoom(event: any) {
    if (!props.data || !props.data.time || props.data.time.length === 0) return
    // Get the filtered data that's currently displayed in the chart
    const displayData = filterDataByTime(props.data, props.startTime, props.endTime)
    if (!displayData || !displayData.time || displayData.time.length === 0) return
    // Get the start and end indices from the zoom event (percentages of filtered data)
    const startIndex = Math.floor(event.start * displayData.time.length / 100)
    const endIndex = Math.ceil(event.end * displayData.time.length / 100)

    // Get the dates from the filtered data at those indices
    const fromDateStr = event.batch[0].startValue
    const toDateStr = event.batch[0].endValue
    // Validate that we have valid date strings
    if (!fromDateStr || !toDateStr) return
    try {
        // Convert to YYYY-MM-DD format
        const fromDate = new Date(fromDateStr).toISOString().split('T')[0]
        const toDate = new Date(toDateStr).toISOString().split('T')[0]

        // Emit to parent
        emit('update-date-range', { fromDate, toDate })
    } catch (e) {
        console.warn('Failed to parse date from zoom event:', e)
    }
}

/**
 * Handle restore event - reset date pickers to full data range
 */
function handleRestore() {
    if (!props.data || !props.data.time || props.data.time.length === 0) return

    try {
        // Get the full data range
        const fromDateStr = props.data.time[0]
        const toDateStr = props.data.time[props.data.time.length - 1]

        // Convert to YYYY-MM-DD format
        const fromDate = new Date(fromDateStr).toISOString().split('T')[0]
        const toDate = new Date(toDateStr).toISOString().split('T')[0]

        // Emit to parent
        emit('update-date-range', { fromDate, toDate })
    } catch (e) {
        console.warn('Failed to parse date from restore event:', e)
    }
}

/**
 * Handle window resize
 */
function handleResize() {
    if (tsChartInstance) {
        tsChartInstance.resize()
    }
}

/**
 * Initialize on mount
 */
onMounted(() => {
    window.addEventListener('resize', handleResize)
    if (props.data) {
        refreshCharts()
    }
})

/**
 * Cleanup on unmount
 */
onBeforeUnmount(() => {
    window.removeEventListener('resize', handleResize)
    if (tsChartInstance) {
        tsChartInstance.off('datazoom', handleDataZoom)
        tsChartInstance.off('restore', handleRestore)
        tsChartInstance.dispose()
        tsChartInstance = null
    }
})
</script>

<style scoped></style>
