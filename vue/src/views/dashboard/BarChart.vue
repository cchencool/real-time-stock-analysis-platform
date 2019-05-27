<template>
  <div :class="className" :style="{height:height,width:width}" />
</template>

<script>
import echarts from 'echarts'
require('echarts/theme/macarons') // echarts theme
import { debounce } from '@/utils'

const animationDuration = 6000

export default {
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
    }
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
    initChart() {
      this.chart = echarts.init(this.$el, 'macarons')

      this.chart.setOption({
        tooltip: {
          trigger: 'axis',
          axisPointer: { // 坐标轴指示器，坐标轴触发有效
            type: 'shadow' // 默认为直线，可选为：'line' | 'shadow'
          }
        },
        grid: {
          top: 10,
          left: '2%',
          right: '2%',
          bottom: '3%',
          containLabel: true
        },
        xAxis: [{
          type: 'category',
          data: ['Node1', 'Node2', 'Node3', 'Node4', 'Node5'],
          axisTick: {
            alignWithLabel: true
          }
        }],
        yAxis: [{
          type: 'value',
          axisTick: {
            show: false
          }
        }],
        // legend: {
        //   left: 'center',
        //   bottom: '10',
        //   data: ['Cluster1', 'Cluster2', 'Cluster3']
        // },
        series: [{
          name: 'Memory',
          type: 'bar',
          stack: 'vistors',
          barWidth: '60%',
          data: [10, 8, 7, 6, 5],
          animationDuration
        }, {
          name: 'CPU',
          type: 'bar',
          stack: 'vistors',
          barWidth: '60%',
          data: [11, 9, 8, 7, 6],
          animationDuration
        }, {
          name: 'GPU',
          type: 'bar',
          stack: 'vistors',
          barWidth: '60%',
          data: [12, 10, 9, 8, 7],
          animationDuration
        }]
      })
    }
  }
}
</script>
