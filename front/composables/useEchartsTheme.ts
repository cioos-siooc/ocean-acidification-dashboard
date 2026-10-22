import * as echarts from 'echarts'
import { chart as c } from '@/config/palette'

let registered = false

/**
 * Register the app's ECharts theme (v2 "Instrument") under the name 'dark',
 * which every chart already passes to `echarts.init`.
 *
 * Registered directly rather than through ECharts' UMD dark.js, which fails in
 * Vite/ESM context (root.echarts is undefined when this===undefined).
 *
 * The chart chrome is deliberately quieter than the data: transparent ground
 * (the pane behind it is the surface), hairline grid, muted mono axis labels,
 * no point markers on lines.
 */
export function registerEchartsDarkTheme() {
  if (registered) return

  const axisLabel = { color: c.text, fontFamily: c.fontMono, fontSize: 11 }
  const axisCommon = () => ({
    axisLine: { lineStyle: { color: c.axis } },
    axisTick: { show: false },
    axisLabel,
    nameTextStyle: { color: c.muted, fontFamily: c.fontSans, fontSize: 11 },
    splitLine: { lineStyle: { color: c.grid } },
    splitArea: { areaStyle: { color: ['rgba(255,255,255,0.015)', 'rgba(255,255,255,0.03)'] } },
    minorSplitLine: { lineStyle: { color: c.grid } }
  })

  const colorPalette = [c.model, c.sensor, '#9ad08a', '#c9a0dc', '#e07a7a', '#7fc8c0', '#d9c27a', '#8a9bd8', '#b5b5b5']

  const theme: object = {
    darkMode: true,
    color: colorPalette,
    backgroundColor: 'transparent',
    textStyle: { color: c.text, fontFamily: c.fontSans },
    axisPointer: {
      lineStyle: { color: c.muted },
      crossStyle: { color: c.muted },
      label: { color: '#d4d9de', backgroundColor: c.tooltipBg, borderColor: c.tooltipBorder, borderWidth: 1, fontFamily: c.fontMono }
    },
    tooltip: {
      backgroundColor: c.tooltipBg,
      borderColor: c.tooltipBorder,
      borderWidth: 1,
      textStyle: { color: '#d4d9de', fontFamily: c.fontSans, fontSize: 12 },
      extraCssText: 'border-radius:2px;box-shadow:none;'
    },
    // One legend swatch style app-wide: a short filled bar. ECharts' default
    // `legendIcon: 'inherit'` draws the series' line + point symbol, which reads as
    // a per-point marker most of these series don't have. Setting `icon` here wins
    // over that inherit, so every chart picks it up without a per-chart override.
    legend: {
      textStyle: { color: c.text, fontSize: 11 },
      icon: 'rect',
      itemWidth: 12,
      itemHeight: 3,
      itemGap: 14
    },
    title: {
      textStyle: { color: '#d4d9de', fontWeight: 500, fontSize: 13 },
      subtextStyle: { color: c.muted }
    },
    toolbox: { iconStyle: { borderColor: c.muted } },
    dataZoom: {
      borderColor: c.axis,
      textStyle: { color: c.muted },
      brushStyle: { color: 'rgba(226,163,54,0.15)' },
      handleStyle: { color: '#191d22', borderColor: c.muted },
      moveHandleStyle: { color: c.muted, opacity: 0.3 },
      fillerColor: 'rgba(226,163,54,0.10)',
      emphasis: {
        handleStyle: { borderColor: c.model, color: '#20252b' },
        moveHandleStyle: { color: c.model, opacity: 0.6 }
      },
      dataBackground: {
        lineStyle: { color: c.axis, width: 1 },
        areaStyle: { color: c.axis }
      },
      selectedDataBackground: {
        lineStyle: { color: c.muted },
        areaStyle: { color: c.muted }
      }
    },
    visualMap: { textStyle: { color: c.text } },
    timeline: {
      lineStyle: { color: c.muted },
      label: { color: c.text },
      controlStyle: { color: c.muted, borderColor: c.muted }
    },
    calendar: {
      itemStyle: { color: 'transparent' },
      dayLabel: { color: c.text },
      monthLabel: { color: c.text },
      yearLabel: { color: c.text }
    },
    timeAxis: { ...axisCommon(), splitLine: { show: false } },
    logAxis: axisCommon(),
    valueAxis: axisCommon(),
    categoryAxis: { ...axisCommon(), splitLine: { show: false } },
    line: { symbol: 'circle', symbolSize: 4, showSymbol: false, lineStyle: { width: 1.5 } },
    graph: { color: colorPalette },
    gauge: { title: { color: c.text } },
    candlestick: {
      itemStyle: {
        color: '#e07a7a', color0: '#9ad08a',
        borderColor: '#e07a7a', borderColor0: '#9ad08a'
      }
    }
  }

  echarts.registerTheme('dark', theme)
  registered = true
}
