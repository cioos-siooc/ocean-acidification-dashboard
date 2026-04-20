<template>
    <v-container fluid>
        <v-row>
            <v-col cols="12">
                <v-card class="pa-4">
                    <v-card-title>Climate Timeseries Extraction</v-card-title>
                    <!-- Loading overlay -->
                    <v-overlay :model-value="loading" class="align-center justify-center" persistent>
                        <v-progress-circular indeterminate color="primary" size="64"></v-progress-circular>
                    </v-overlay>

                    <!-- Chart Container -->
                    <div ref="chartRef" style="width: 100%; height: 600px;"></div>
                </v-card>
            </v-col>
        </v-row>

        <v-row>
            <v-col cols="12" class="text-center">
                <v-card>
                    <v-card-title>Series Descriptions</v-card-title>
                    <v-card-text>
                        <ul style="text-align: left;">
                            <li><strong>Mean:</strong> The mean value, smoothed over a 5-day window.</li>
                            <li><strong>Q1/Q3 Bounds:</strong> The range between the first and third quartiles, smoothed over a 5-day window.</li>
                            <li><strong>Min/Max Bounds:</strong> The light-grey shaded area between the minimum and maximum values observed, smoothed over a 5-day window.</li>
                        </ul>
                    </v-card-text>
                </v-card>
            </v-col>
        </v-row>
    </v-container>
</template>

<script setup>
import { ref, onMounted, onUnmounted } from 'vue';
import * as echarts from 'echarts';
import axios from 'axios';
// Note: v3-chart is a legacy file - the following import may need adjustment for Vuetify 4
// import { stack } from 'vuetify/components/VCalendar/modes/stack';

const config = useRuntimeConfig();
const apiBaseUrl = config.public.apiBaseUrl


// Chart reference and instance
const chartRef = ref(null);
let chartInstance = null;
const loading = ref(false);

// Sample coordinates and date
const params = ref({
    lat: 49.2,
    lon: -123.5,
    dt: new Date().toISOString()
});

/**
 * Fetch data from the API
 */
const fetchData = async () => {
    loading.value = true;
    try {
        // Replace with your actual API base URL
        const response = await axios.post(`${apiBaseUrl}/extract_climateTimeseries`, {
            lat: params.value.lat,
            lon: params.value.lon,
            dt: params.value.dt
        });
        updateChart(JSON.parse(response.data));
    } catch (error) {
        console.error("Error fetching climate data:", error);
    } finally {
        loading.value = false;
    }
};

/**
 * Initialize or update the ECharts plot with 5 series
 */
const updateChart = (data) => {
    if (!chartInstance && chartRef.value) {
        chartInstance = echarts.init(chartRef.value);
    }
    if (!chartInstance) return;

    const times = Object.values(data.requested_date)

    const mean = Object.values(data.mean);
    const median = Object.values(data.median);
    const q1 = Object.values(data.q1);
    const q3Diff = Object.values(data.q3).map((val, idx) => val - q1[idx]);
    const min = Object.values(data.min);
    const maxDiff = Object.values(data.max).map((val, idx) => val - min[idx]);
    
    // Find index for the "Now" vertical line (closest timestamp in the data)
    let closestIdx = 0;
    if (times.length > 0) {
        const targetMs = new Date(params.value.dt).getTime();
        let minDiff = Infinity;
        times.forEach((t, i) => {
            const diff = Math.abs(new Date(t).getTime() - targetMs);
            if (diff < minDiff) {
                minDiff = diff;
                closestIdx = i;
            }
        });
    }

    const option = {
        animation: false,
        title: { text: 'Climatology Window (10 Days)' },

      tooltip: {
        trigger: 'axis',
        axisPointer: {
          type: 'cross',
          animation: false,
          label: {
            backgroundColor: '#ccc',
            borderColor: '#aaa',
            borderWidth: 1,
            shadowBlur: 0,
            shadowOffsetX: 0,
            shadowOffsetY: 0,
            color: '#222'
          }
        },
        formatter: function (params) {
        //   return (
        //     params[2].name +
        //     '<br />' +
        //     ((params[2].value) * 1).toFixed(1)
        //   );
        }
      },

        legend: { data: ['Mean', 'Median', 'Q1', 'Q3', 'Min/Max Bounds'], top: 30 },
        grid: { left: '3%', right: '4%', bottom: '3%', containLabel: true },
        xAxis: {
            type: 'category',
            data: times,
            // axisLabel: {
            //     formatter: (value) => value.split('T')[1].substring(0, 5) // Show time for hourly data
            // }
        },
        yAxis: {
            type: 'value', min: 'dataMin',
            max: 'dataMax',
        },
        series: [
            {
                name: 'Min Bounds',
                type: 'line',
                data: min,
                lineStyle: { opacity: 0 },
                stack: 'minmax',
                symbol: 'none'
            },
            {
                name: 'Max Bounds',
                type: 'line',
                data: maxDiff,
                lineStyle: { opacity: 0 },
                areaStyle: { color: '#999', opacity: 0.5 },
                stack: 'minmax',
                symbol: 'none'
            },

            {
                name: 'Q1',
                type: 'line',
                data: q1,
                stack: 'range',
                lineStyle: { opacity: 0 },
                symbol: 'none'
            },
            {
                name: 'Q3',
                type: 'line',
                data: q3Diff,
                stack: 'range',
                lineStyle: { opacity: 0 },
                areaStyle: { color: '#666', opacity: 0.5 },
                symbol: 'none'
            },

            {
                name: 'Mean',
                type: 'line',
                data: mean,
                smooth: true,
                lineStyle: { color: '#333', width: 4 },
                symbol: 'none',
                markLine: {
                    symbol: ['none', 'none'],
                    label: { 
                        show: true, 
                        position: 'end', 
                        formatter: 'Now',
                        backgroundColor: '#fff',
                        padding: [2, 4],
                        borderRadius: 2,
                        borderWidth: 1,
                        borderColor: 'red'
                    },
                    data: [
                        {
                            xAxis: closestIdx,
                            lineStyle: {
                                color: 'red',
                                width: 1,
                                type: 'dashed'
                            },
                        }
                    ]
                },
            },

        ]
    };

    chartInstance.setOption(option);
};

// Handle window resize
const handleResize = () => {
    if (chartInstance) {
        chartInstance.resize();
    }
};

onMounted(() => {
    fetchData();
    window.addEventListener('resize', handleResize);
});

onUnmounted(() => {
    window.removeEventListener('resize', handleResize);
    if (chartInstance) {
        chartInstance.dispose();
    }
});
</script>

<style scoped>
/* Add any specific styles here */
</style>
