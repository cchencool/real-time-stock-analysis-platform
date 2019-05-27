<template>
  <div class="dashboard-container">

    <github-corner class="github-corner" />

    <div class="grid-container">
      <div class="item7">
        <div>
          <img src="./mountain.png" style="width: 100%;">
        </div>
      </div>

      <div class="item1"><div>Console</div></div>

      <div class="item6">
        <div class="navbar">
          <div class="dropdown">
            <button class="dropbtn">K Clusters &#9662</button>
            <div class="dropdown-content">
              <a id="2" href="#" @click="updateK(2)">2</a>
              <a id="4" href="#" @click="updateK(4)">4</a>
              <a id="5" href="#" @click="updateK(5)">5</a>
              <a id="6" href="#" @click="updateK(6)">6</a>
              <a id="8" href="#" @click="updateK(8)">8</a>
              <a id="10" href="#" @click="updateK(10)">10</a>
            </div>
          </div>

          <div class="dropdown">
            <button class="dropbtn">Clustering Model &#9662</button>
            <div class="dropdown-content">
              <a href="#">K-Means</a>
              <a href="#">DBSCAN</a>
              <a href="#">Gaussian Mixture</a>
              <a href="#">Power Iteration Clustering</a>
              <a href="#">Latent Dirichlet Allocation</a>
            </div>
          </div>

          <div class="dropdown">
            <button class="dropbtn">Regression Model &#9662
            </button>
            <div class="dropdown-content">
              <a href="#">Linear Regression</a>
              <a href="#">Decision Tree</a>
              <a href="#">Random Forests</a>
              <a href="#">Gradient-Boosted Trees</a>
            </div>
          </div>
        </div>
      </div>

      <div class="item3">
        <button class="big-button" @click.prevent="start">
          <svg-icon icon-class="start" class-name="card-panel-icon" />
          Start
        </button>
      </div>

      <div class="item4">
        <button class="big-button" @click.prevent="refresh">
          <svg-icon icon-class="refresh" class-name="card-panel-icon" />
          Refresh
        </button>
      </div>

      <div class="item5">
        <button class="big-button" @click="stop">
          <svg-icon icon-class="stop" class-name="card-panel-icon" />
          Stop
        </button>
      </div>
    </div>

<!--    <panel-group @handleSetLineChartData="handleSetLineChartData" />-->
    <panel-group :k_panel="k_num"/>

    <el-row :gutter="32">
      <el-col :xs="24" :sm="24" :lg="8">
        <div class="chart-wrapper">
          <raddar-chart />
        </div>
      </el-col>
      <el-col :xs="24" :sm="24" :lg="8">
        <div class="chart-wrapper">
          <pie-chart :chartData='kmeans_info'/>
        </div>
      </el-col>
      <el-col :xs="24" :sm="24" :lg="8">
        <div class="chart-wrapper">
          <bar-chart />
        </div>
      </el-col>
    </el-row>

    <div class="line-charts">
        <template v-for="stock in stocksData">
          <el-row style="background:#fff;padding:16px 16px 0;margin-bottom:32px;">
            <h2>{{stock.name}}</h2>
            <line-chart :chart-data=stock.data></line-chart>
          </el-row>
        </template>
    </div>

  </div>
</template>

<script>
import { mapGetters } from 'vuex'
import LineChart from './LineChart'
import GithubCorner from '@/components/GithubCorner'
import PanelGroup from './PanelGroup'
import RaddarChart from './RaddarChart'
import PieChart from './PieChart'
import BarChart from './BarChart'

const lineChartData = {
  newVisitis: {
    expectedData: [100, 120, 161, 134, 105, 160, 165],
    actualData: [120, 82, 91, 154, 162, 140, 145]
  },
  messages: {
    expectedData: [200, 192, 120, 144, 160, 130, 140],
    actualData: [180, 160, 151, 106, 145, 150, 130]
  },
  purchases: {
    expectedData: [80, 100, 121, 104, 105, 90, 100],
    actualData: [120, 90, 100, 138, 142, 130, 130]
  },
  shoppings: {
    expectedData: [130, 140, 141, 142, 145, 150, 160],
    actualData: [120, 82, 91, 154, 162, 140, 130]
  }
}

const pieChartData = {
  legend_data: ['Cluster1', 'Cluster2', 'Cluster3', 'Cluster4', 'Cluster5'],
  series_data: [
    { value: 500, name: 'Cluster1' },
    { value: 400, name: 'Cluster2' },
    { value: 300, name: 'Cluster3' },
    { value: 200, name: 'Cluster4' },
    { value: 100, name: 'Cluster5' }
  ]
}

export default {
  name: 'Dashboard',
  computed: {
    ...mapGetters([
      'name'
    ])
  },
  components: {
    LineChart,
    GithubCorner,
    PanelGroup,
    RaddarChart,
    PieChart,
    BarChart
  },
  // props: {
  //   kmeans_info: Object
  // },
  data() {
    return {
      monitor: null,
      windowSize: 200,
      k_num: 5,
      stocksData: [],
      kmeans_info: pieChartData,
      // kmeans_info: {
      //   0.0: 577,
      //   1.0: 400,
      //   2.0: 300,
      //   3.0: 200,
      //   4.0: 100
      // },
      // stocksData: [
      //   {
      //     name: 'Cluster1',
      //     data: {
      //       pred: [100, 120, 161, 134, 105, 160, 165],
      //       real: [120, 82, 91, 154, 162, 140, 145],
      //       time: [0, 1, 2, 3, 4, 5, 6, 7]
      //     }
      //   },
      //   {
      //     name: 'Cluster2',
      //     data: {
      //       pred: [130, 140, 141, 142, 145, 150, 160],
      //       real: [120, 82, 91, 154, 162, 140, 130],
      //       time: [0, 1, 2, 3, 4, 5, 6, 7]
      //     }
      //   }
      // ],

      lineChartData1: lineChartData.newVisitis,
      lineChartData2: lineChartData.messages,
      lineChartData3: lineChartData.purchases,
      lineChartData4: lineChartData.shoppings,

      clustersDataAll: [
        { name: 'Cluster1', data: lineChartData.newVisitis },
        { name: 'Cluster2', data: lineChartData.messages },
        { name: 'Cluster3', data: lineChartData.purchases },
        { name: 'Cluster4', data: lineChartData.shoppings }
      ],
      clustersData: []
    }
  },
  methods: {
    // handleSetKMeansInfo(info) {
    //   this.$emit('handleSetKMeansInfo', info)
    // },
    // handleSetKMeansInfo: function(info) {
    //   this.$emit('handleSetKMeansInfo', info)
    // },
    updateK: function(clicked_k) {
      this.k_num = clicked_k
      // this.kmeans_info.legend_data = pieChartData.legend_data.slice(0, clicked_k)
      // // var series = {}
      // // for (var i in pieChartData.series_data) {
      // //   this.kmeans_info.series_data
      // // }
      // this.kmeans_info.series_data = pieChartData.series_data.slice(0, clicked_k)
    },
    handleSetLineChartData: function() {
    },
    post: function() {
      this.$http.post('http://jsonplaceholder.typicode.com/posts', {
        title: "Dojo title",
        body: "Dojo body",
        userId: 1
      }).then(function(data) {
        console.log(data)
      })
    },
    get: function() {
      this.$http.get('http://127.0.0.1:5000/get_curr_oltp_result?pname=streamclzreg').then(function(data) {
        console.log(data)
        // this.stocksData = data.body.slice(0, 3);

        if (data.body.data !== undefined) {
          this.stocksData = []
        }

        var cluster_idx = 1
        for (var key in data.body.data) {
          // eslint-disable-next-line eqeqeq
          if (key == 'kmean') {
            var kmeans_info = data.body.data[key]
            // this.handleSetKMeansInfo(this.kmeans_info)

            // legend data
            var k_idx = 0
            var legend_data = []
            var series_data = []
            for (var i in kmeans_info) {
              // legend data
              var cluster_name = 'Cluster' + (k_idx + 1)
              legend_data.push(cluster_name)

              // series data
              var record = {
                value: kmeans_info[i],
                name: cluster_name
              }
              series_data.push(record)

              k_idx += 1
            }

            // update kmeans info
            this.kmeans_info = {
              legend_data: legend_data,
              series_data: series_data
            }
          }

          // eslint-disable-next-line eqeqeq
          if (key != 'kmean') {
            var name = 'Cluster' + cluster_idx

            var preds = []
            var times = []
            var reals = []

            for (var time in data.body.data[key]) {
              times.push(time);
              // preds.push(data.body.data[cluster_idx][time].pred)
              reals.push(data.body.data[key][time].real)

              var pred = data.body.data[key][time].pred
              if (pred) {
                preds.push(pred)
              } else {
                // use the former predicted value to replace current predicted value
                var predsLastIdx = preds.length - 1
                if (predsLastIdx < 0) predsLastIdx = 0
                preds.push(preds[predsLastIdx])
              }
            }

            var sliceStart = preds.length - this.windowSize
            if (sliceStart < 0) {
              sliceStart = 0
            }

            var sliceEnd = preds.length
            var cluster_data = {
              name: name,
              data: {
                pred: preds.slice(sliceStart, sliceEnd),
                real: reals.slice(sliceStart, sliceEnd),
                time: times.slice(sliceStart, sliceEnd)
              }
            }

            cluster_data.data.real.pop()

            cluster_idx++

            this.stocksData.push(cluster_data)
          }
        }
      })
    },
    refresh: function() {
      this.monitor = setInterval(this.get, 5000)
    },
    start: function() {
      this.$http.get('http://127.0.0.1:5000/add_task?pname=streamclzreg&cluster_num=' + this.k_num).then(function(data) {
        console.log(data)
        // m = new Map(Object.entries(data))
      })
    },
    stop: function() {
      this.$http.get('http://127.0.0.1:5000/stop_oltp_processor?pname=streamclzreg').then(function(data) {
        console.log(data)
        clearInterval(this.monitor)
      })
    }
  }
}
</script>

<style lang="scss" scoped>
.dashboard {
  &-container {
    padding: 32px;
    background-color: rgb(240, 242, 245);
    position: relative;

    .github-corner {
      position: absolute;
      top: 0px;
      border: 0;
      right: 0;
  }

    .chart-wrapper {
      background: #fff;
      padding: 16px 16px 0;
      margin-bottom: 32px;
    }
    .my-button {
      background: #1f2d3d;
      color: #f4f4f5;
      border-radius: 6px;
    }
    .big-button {
      background: #40c9c6;
      color: #f4f4f5;
      border-radius: 50px;
      margin: 10px;
      height:80px;
      width:200px;
    }
    /*Container*/
    .item1 {
      grid-area: header;
      background-color: rgba(48,65,86,1);
    }
    .item1 > div {
      text-align: center;
      font-size: 30px;
      font-weight: bold;
      color: #ffffff;
      background-color: rgba(48,65,86,1);
      height: 50px;
      padding-top: 5px;
    }
    .item2 { grid-area: menu; }
    .item3 { grid-area: start; }
    .item4 { grid-area: refresh; }
    .item5 { grid-area: stop; }
    .item6 { grid-area: set; }
    .item7 {
      grid-area: image;
      background-color: red; }
    .item7 > div {
      padding-bottom: 0px;
      background-color: rgba(48,65,86,1);
    }

    .grid-container {
      display: grid;
      grid-template-areas:
        'image image image'
        'header header header'
        'set set set'
        'start  refresh  stop';
      grid-gap: 0px;
      background-color: #ffffff;
      padding: 0px;
      padding-top: 0px;
      padding-left: 0px;
    }

    .grid-container > div {
      /*background-color: rgba(48,65,86,1);*/
      background-color: transparent;
      text-align: center;
      /*padding: 20px 0px;*/
      font-size: 30px;
    }

    /*Drop Down Button*/
    .navbar {
      overflow: hidden;
      background-color: rgba(48,65,86, 0.9);
      padding-top: 0px;
      padding-top: 0px;
    }

    .navbar a {
      float: left;
      font-size: 16px;
      color: white;
      text-align: center;
      padding: 14px 16px;
      text-decoration: none;
    }

    .dropdown {
      float: left;
      overflow: hidden;
    }

    .dropdown .dropbtn {
      width: 300px;
      font-size: 20px;
      font-weight: bold;
      border: none;
      outline: none;
      color: white;
      padding: 14px 16px;
      background-color: inherit;
      font-family: inherit;
      margin: 0;
    }

    .navbar a:hover, .dropdown:hover .dropbtn {
      background-color: red;
    }

    .dropdown-content {
      display: none;
      position: absolute;
      background-color: #f9f9f9;
      min-width: 300px;
      box-shadow: 0px 8px 16px 0px rgba(0,0,0,0.2);
      z-index: 1;
    }

    .dropdown-content a {
      float: none;
      color: black;
      padding: 12px 16px;
      text-decoration: none;
      display: block;
      text-align: left;
      font-size: 20px;
    }

    .dropdown-content a:hover {
      background-color: #ddd;
    }

    .dropdown:hover .dropdown-content {
      display: block;
    }

    .arrow-up {
      width: 0;
      height: 0;
      border-left: 10px solid transparent;
      border-right: 10px solid transparent;
      border-bottom: 10px solid white;
    }

    .arrow-down {
      width: 0;
      height: 0;
      border-left: 10px solid transparent;
      border-right: 10px solid transparent;
      border-top: 10px solid white;
      transform: translate(0px, 10px);
    }
  /*  Console */
    .console {
      text-align: center;
      font-size: 30px;
      font-weight: bold;
      color: #ffffff;
      background-color: rgba(48,65,86,1);
      height: 50px;
      padding-top: 5px;
    }
  }
  &-text {
    font-size: 30px;
    line-height: 46px;
  }
}
</style>
