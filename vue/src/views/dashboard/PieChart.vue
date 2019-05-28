<template>
  <div :class="className" :style="{height:height,width:width}"></div>
</template>

<script>
import echarts from 'echarts'
require('echarts/theme/macarons') // echarts theme
import { debounce } from '@/utils'

export default {
  watch: {
    chartData: {
      deep: true,
      handler(val) {
        this.setOptions(val)
      }
    }
  },
  props: {
    className: {
      type: String,
      default: 'chart'
    },
    width: {
      type: String,
      default: '100%'
    },
    height: {
      type: String,
      default: '300px'
    },
    chartData: Object
    // k_num: {
    //   type: Number,
    //   default: 2
    // }
  },
  data() {
    return {
      chart: null
    }
  },
  mounted() {
    this.initChart()
    this.__resizeHandler = debounce(() => {
      if (this.chart) {
        this.chart.resize()
      }
    }, 100)
    window.addEventListener('resize', this.__resizeHandler)
  },
  beforeDestroy() {
    if (!this.chart) {
      return
    }
    window.removeEventListener('resize', this.__resizeHandler)
    this.chart.dispose()
    this.chart = null
  },
  methods: {
    setOptions({ legend_data, series_data } = {}) {
      this.chart.setOption({
        tooltip: {
          trigger: 'item',
          formatter: '{a} <br/>{b} : {c} ({d}%)'
        },
        legend: {
          left: 'center',
          bottom: '10',
          data: legend_data
          // data: ['Cluster1', 'Cluster2', 'Cluster3', 'Cluster4', 'Cluster5']
        },
        calculable: true,
        series: [{
          name: 'Cluster Size',
          type: 'pie',
          roseType: 'radius',
          radius: [15, 95],
          center: ['50%', '38%'],
          data: series_data,
          // data: [
          //   { value: k_num, name: 'Cluster1' },
          //   { value: k_num, name: 'Cluster2' },
          //   { value: k_num, name: 'Cluster3' },
          //   { value: k_num, name: 'Cluster4' },
          //   { value: k_num, name: 'Cluster5' }
          // ],
          animationEasing: 'cubicInOut',
          animationDuration: 2600
        }]
      })
    },
    initChart() {
      this.chart = echarts.init(this.$el, 'macarons')
      this.setOptions(this.chartData)
    }
  }
}
</script>
