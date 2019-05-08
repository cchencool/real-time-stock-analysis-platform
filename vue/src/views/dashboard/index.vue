<template>
  <div class="dashboard-container">

    <github-corner class="github-corner" />

    <div class="grid-container">
<!--      <div class="item1"><h3 class="console">Console</h3></div>-->
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
              <a href="#">2</a>
              <a href="#">4</a>
              <a href="#">5</a>
              <a href="#">6</a>
              <a href="#">8</a>
              <a href="#">10</a>
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

<!--    <div style="background:#1f2d3d;color: #ffffff;padding:16px 16px 0;margin-bottom:32px;" class="dashboard-text">-->
<!--      Dashboard-->
<!--    </div>-->

    <panel-group @handleSetLineChartData="handleSetLineChartData" />
<!--    <panel-group><panel-group/>-->

<!--    <div>-->
<!--      <h3 class="console">Console</h3>-->

<!--      <div class="grid-container">-->
<!--        <div class="item2">Cluster</div>-->
<!--        <div class="item3">Main</div>-->
<!--        <div class="item4">Right</div>-->
<!--        <div class="item5">Footer</div>-->
<!--      </div>-->
<!--    </div>-->



<!--    <div style="background:#fff;padding:16px 16px 16px;margin-bottom:32px;">-->
<!--      <p>Clusters Number: <input type="text" v-model="clustersCount" /></p>-->
<!--      <button class="my-button" @click.prevent="confirmFunc">Confirm</button>-->
<!--    </div>-->

<!--    <div style="background:#fff;padding:16px 16px 16px;margin-bottom:32px;">-->
<!--      <button class="start-button" @click.prevent="start">Start</button>-->
<!--      <button class="stop-button" @click.prevent="stop">Stop</button>-->
<!--    </div>-->

    <el-row :gutter="32">
      <el-col :xs="24" :sm="24" :lg="8">
        <div class="chart-wrapper">
          <raddar-chart />
        </div>
      </el-col>
      <el-col :xs="24" :sm="24" :lg="8">
        <div class="chart-wrapper">
          <pie-chart />
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

<!--    <el-row style="background:#fff;padding:16px 16px 0;margin-bottom:32px;">-->
<!--      <h2>Cluster 1</h2>-->
<!--      <button @click="get">Refresh</button>-->
<!--&lt;!&ndash;      <div v-for="stock in stocksData">&ndash;&gt;-->
<!--&lt;!&ndash;        <h3>{{ stock.title}}</h3>&ndash;&gt;-->
<!--&lt;!&ndash;        <article>{{ stock.body }}</article>&ndash;&gt;-->
<!--&lt;!&ndash;      </div>&ndash;&gt;-->
<!--      <line-chart :chart-data="lineChartData1" />-->
<!--    </el-row>-->

<!--    <el-row style="background:#fff;padding:16px 16px 0;margin-bottom:32px;">-->
<!--      <h2>Cluster 2</h2>-->
<!--      <line-chart :chart-data="lineChartData2" />-->
<!--    </el-row>-->

<!--    <el-row style="background:#fff;padding:16px 16px 0;margin-bottom:32px;">-->
<!--      <h2>Cluster 3</h2>-->
<!--      <line-chart :chart-data="lineChartData3" />-->
<!--    </el-row>-->

<!--    <el-row style="background:#fff;padding:16px 16px 0;margin-bottom:32px;">-->
<!--      <h2>Cluster 4</h2>-->
<!--      <line-chart :chart-data="lineChartData4" />-->
<!--    </el-row>-->

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
  data() {
    return {
      monitor: null,
      windowSize: 200,
      stocksData: [],
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

      clustersCount: 2,

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
    confirmFunc: function() {
      this.clustersData = this.clustersDataAll.slice(0, this.clustersCount)
    },
    start: function() {
      this.$http.get('http://127.0.0.1:5000/add_task?pname=streamclzreg').then(function(data) {
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
