<template>
    <v-container class="mt-4">
        <v-row>
            <v-col cols="12">
                <div ref="scatterChart" style="width: 100%; height: 600px;"></div>
            </v-col>
        </v-row>
    </v-container>
</template>

<script setup lang="ts">
import { ref, watch, onMounted, onBeforeUnmount } from 'vue'
import * as echarts from 'echarts'

// Props
interface Props {
    data: any | null
    startTime: number | null
    endTime: number | null
}

const props = withDefaults(defineProps<Props>(), {
    data: null,
    startTime: null,
    endTime: null
})

// Chart references
const scatterChart = ref<HTMLElement | null>(null)

// Chart instance
let scatterChartInstance: echarts.ECharts | null = null

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


function alignAndBin(sensorData, modelData) {
    // 1. Create a map to hold the buckets
    // Key: Timestamp of the hour start (e.g., 2023-01-01 10:00:00)
    const buckets = new Map();

    // Helper to process a dataset
    // type: 'sensor' or 'model'
    const process = (data, type) => {
        data.forEach(point => {
            const timestamp = point[0]; // Assumes [time, value] format
            const value = point[1];

            // Round down to nearest Hour (3600000 ms)
            // Change this to 15 * 60 * 1000 for 15-min buckets
            const bucketTime = Math.floor(timestamp / 3600000) * 3600000;

            if (!buckets.has(bucketTime)) {
                buckets.set(bucketTime, {
                    sensorSum: 0, sensorCount: 0,
                    modelSum: 0, modelCount: 0
                });
            }

            const b = buckets.get(bucketTime);
            if (type === 'sensor') {
                if (value) {
                    b.sensorSum += value;
                    b.sensorCount++;
                }
            } else {
                if (value) {
                    b.modelSum += value;
                    b.modelCount++;
                }
            }
        });
    };

    // 2. Fill buckets
    process(sensorData, 'sensor');
    process(modelData, 'model');

    // 3. Flatten map to Array, keeping only complete buckets
    const alignedData = [];

    buckets.forEach((val, key) => {
        // ONLY keep buckets where we have BOTH sensor and model data
        if (val.sensorCount > 0 && val.modelCount > 0) {
            const sensorAvg = val.sensorSum / val.sensorCount;
            const modelAvg = val.modelSum / val.modelCount;

            alignedData.push({
                time: key,
                sensor: sensorAvg,
                model: modelAvg,
                diff: modelAvg - sensorAvg // The Error
            });
        }
    });

    // Sort by time so it plots correctly
    return alignedData.sort((a, b) => a.time - b.time);
}


/**
 * Plot heatmap comparison showing density of sensor vs model values
 */
function plotScatter() {
    if (!scatterChart.value) return
    if (!props.data) return

    // Filter data by time range if provided
    const displayData = filterDataByTime(props.data, props.startTime, props.endTime)

    // Align and bin data to ensure we are comparing the same time points
    const alignedData = alignAndBin(
        displayData.sensor.map((d: any, i: number) => [new Date(displayData.time[i]).getTime(), d]),
        displayData.model.map((d: any, i: number) => [new Date(displayData.time[i]).getTime(), d])
    )

    // Filter out points where either sensor or model is null
    const validPoints = alignedData.filter(p => p.sensor !== null && p.model !== null).map(p => ({ s: p.sensor, model: p.model }))

    if (validPoints.length === 0) {
        // Show empty state
        const option = {
            title: { text: 'Sensor vs Model' },
            xAxis: { type: 'category', name: 'Sensor' },
            yAxis: { type: 'category', name: 'Model' },
            series: []
        }
        if (!scatterChartInstance) {
            scatterChartInstance = echarts.init(scatterChart.value as any)
        }
        scatterChartInstance.setOption(option)
        return
    }

    // Calculate min/max for both axes
    const sensorValues = validPoints.map(p => p.s)
    const modelValues = validPoints.map(p => p.model)
    const sensorMin = Math.min(...sensorValues)
    const sensorMax = Math.max(...sensorValues)
    const modelMin = Math.min(...modelValues)
    const modelMax = Math.max(...modelValues)

    // Create bins for heatmap (30x30 grid)
    const binCount = 100
    const sensorBinWidth = (sensorMax - sensorMin) / binCount
    const modelBinWidth = (modelMax - modelMin) / binCount

    // Create 2D heatmap data with bin indices
    const heatmapData: Array<[number, number, number]> = []
    const binMatrix: Map<string, number> = new Map()

    // Count points in each bin
    validPoints.forEach(p => {
        const sensorBinIdx = Math.min(binCount - 1, Math.floor((p.s - sensorMin) / sensorBinWidth))
        const modelBinIdx = Math.min(binCount - 1, Math.floor((p.model - modelMin) / modelBinWidth))
        const key = `${sensorBinIdx},${modelBinIdx}`
        binMatrix.set(key, (binMatrix.get(key) || 0) + 1)
    })

    // Convert to ECharts heatmap format using bin indices
    binMatrix.forEach((count, key) => {
        const [sensorBinIdx, modelBinIdx] = key.split(',').map(Number)
        heatmapData.push([sensorBinIdx, modelBinIdx, count])
    })

    // Create category labels for axes
    const sensorLabels: string[] = []
    const modelLabels: string[] = []

    for (let i = 0; i < binCount; i++) {
        const sensorVal = sensorMin + i * sensorBinWidth + sensorBinWidth / 2
        const modelVal = modelMin + i * modelBinWidth + modelBinWidth / 2
        sensorLabels.push(sensorVal.toFixed(2))
        modelLabels.push(modelVal.toFixed(2))
    }

    // Calculate diagonal line endpoints for 1:1 agreement
    // Find the overlapping range where x=y makes sense
    const diagMin = Math.max(sensorMin, modelMin)
    const diagMax = Math.min(sensorMax, modelMax)

    // Convert data values to bin indices
    const diagStartSensorBinIdx = Math.floor((diagMin - sensorMin) / sensorBinWidth)
    const diagStartModelBinIdx = Math.floor((diagMin - modelMin) / modelBinWidth)
    const diagEndSensorBinIdx = Math.floor((diagMax - sensorMin) / sensorBinWidth)
    const diagEndModelBinIdx = Math.floor((diagMax - modelMin) / modelBinWidth)

    const option = {
        title: {
            text: 'Sensor vs Model Density Heatmap',
            textStyle: { color: '#e0e0e0' }
        },
        tooltip: { trigger: 'item' },
        xAxis: {
            type: 'category',
            name: 'Sensor',
            data: sensorLabels,
            axisLabel: {
                color: '#e0e0e0',
                formatter: (val: string, idx: number) => {
                    return idx % 3 === 0 ? val : ''
                }
            },
            nameTextStyle: { color: '#e0e0e0' }
        },
        yAxis: {
            type: 'category',
            name: 'Model',
            data: modelLabels,
            axisLabel: {
                color: '#e0e0e0',
                formatter: (val: string, idx: number) => {
                    return idx % 3 === 0 ? val : ''
                }
            },
            nameTextStyle: { color: '#e0e0e0' }
        },
        visualMap: {
            min: 1,
            max: Math.max(...Array.from(binMatrix.values())),
            splitNumber: 10,
            left: 'right',
            top: 'center',
            inRange: {
                color: ['#313695', '#4575b4', '#74add1', '#abd9e9', '#e0f3f8', '#ffffbf', '#fee090', '#fdae61', '#f46d43', '#d73027', '#a50026']
            },
            textStyle: {
                color: '#e0e0e0',
                formatter: (value: number) => {
                    return Math.round(value).toString()
                }
            }
        },
        series: [
            {
                name: 'Frequency',
                type: 'heatmap',
                data: heatmapData,
                emphasis: {
                    itemStyle: {
                        borderColor: '#333',
                        borderWidth: 1
                    }
                }
            },
            {
                name: 'Ideal Fit',
                type: 'line',
                data: [[diagStartSensorBinIdx, diagStartModelBinIdx], [diagEndSensorBinIdx, diagEndModelBinIdx]],
                lineStyle: { color: '#000', width: 2, type: 'dashed', opacity: 0.5 },
                symbol: 'none'
            }
        ],
        grid: { left: 60, right: 80, top: 60, bottom: 60 }
    }

    if (!scatterChartInstance) {
        scatterChartInstance = echarts.init(scatterChart.value as any)
    }
    scatterChartInstance.setOption(option)
}

/**
 * Watch for data or time range changes and re-render chart
 */
watch(
    () => [props.data, props.startTime, props.endTime],
    () => {
        plotScatter()
    },
    { deep: false }
)

/**
 * Handle window resize
 */
function handleResize() {
    if (scatterChartInstance) {
        scatterChartInstance.resize()
    }
}

/**
 * Initialize on mount
 */
onMounted(() => {
    window.addEventListener('resize', handleResize)
    if (props.data) {
        plotScatter()
    }
})

/**
 * Cleanup on unmount
 */
onBeforeUnmount(() => {
    window.removeEventListener('resize', handleResize)
    if (scatterChartInstance) {
        scatterChartInstance.dispose()
        scatterChartInstance = null
    }
})
</script>

<style scoped></style>
