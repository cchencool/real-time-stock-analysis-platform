# real-time-stock-analysis-platform

## Project Description

In recent years, increasing numbers of projects in stock market are conducted by big data technology. With the complex features, large volume of data as well as the real-time ﬂuctuating price, and many other inﬂuential factors, in order to handle the task efﬁciently, big data technologies should be applied. Such platform can help organizationsandindividualstohavingabetterunderstandingofthe stock market and making correct decisions in different situations.

We plan to do a wide project which aims to build a real-time stock clustering and prediction platform. In this platform, stock data (with indicators including code, name, changing ratio, trade, open, high, low,volumeandsoon)willbeuploadedeverysecond,sothesystem need to process the data in real time. In terms of clustering, the system will divide the stocks into groups based on their similarities. This will help users have a better understanding of the inner relationship between different stocks.Real-time Stock Clustering and Prediction

## System Requirements

* Linux / MacOS
* Python 3.6
* Hadoop 2.7.7
* Spark 2.4.0

## Techniques Involoved

* Spark Streaming / MLlib
* MongoDB
* Flask
* vue.js + bootstrap (+ deployd)

## File Structure

|  directory | description  |
|:-:|:-:|
| bin | startup scripts. |
| code | project code |
| docs | documentation |
| data | data folder (Please **DON'T** upload large local data to git) |
| config | configuration files for runtime. |
| install | installation scripts & relative configurations |

## System Architechure

![graph](docs/flow_graph.png)

## Reference

* For front-end app fast development: [vue.js](https://vuejs.org)
* For front-end app, web page components: [Bootstrap](https://getbootstrap.com/docs/4.3/examples/)
* For API Building (just for reference): [Deployd](http://deployd.com/)