# real-time-stock-analysis-platform

msbd5003 group project.
#### Attention for submission

* Please **DO NOT** submit large data file to ./data, although you can modify ./data/test_data.csv
* Please **DO NOT** submit files in ./code/website/node_modules
* The two folders mentioned above have been add to .gitignore.
  - If you want to share data files, please use wechat or other method.
  - If you need to run the website app. Please follow the README.md instruction in corresponding module folder (run `npm install` first).

#### Next Step

* Enrich the API & implementation
* Implement build_context & run scripts.

# About Project

In recent years, increasing numbers of projects in stock market are conducted by big data technology. With the complex features, large volume of data as well as the real-time ﬂuctuating price, and many other inﬂuential factors, in order to handle the task efﬁciently, big data technologies should be applied. Such platform can help organizationsandindividualstohavingabetterunderstandingofthe stock market and making correct decisions in different situations.

We plan to do a wide project which aims to build_context a real-time stock clustering and prediction platform. In this platform, stock data (with indicators including code, name, changing ratio, trade, open, high, low,volumeandsoon)willbeuploadedeverysecond,sothesystem need to process the data in real time. In terms of clustering, the system will divide the stocks into groups based on their similarities. This will help users have a better understanding of the inner relationship between different stocks.Real-time Stock Clustering and Prediction

## File Structure

|  directory | description  |
|:-:|:-:|
| bin | startup scripts. |
| code | project code |
| docs | documentation |
| data | data folder for collection module |
| config | configuration files for runtime. |
| install | installation scripts & relative configurations |

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

## System Architechure

![graph](docs/flow_graph.png)

This is the general system structure, here are mainly three layers in our design: Data Producer, Data Process Platform and Visualization & Interaction Layer. The data API layer would be a wrapper of third party APIs to make our system compatible with multiple data sources. The second layer which is the most important part of the system is Data Process Platform. It consists of multiple services with and supports the fundamental functions of the system such as stream and batch data analysis. And on the top is the visualization and interaction layer, it's basically an Dashboard App with some other business functions.

Now, let's look at each part one by one.

## Data Producer

<img src= "docs/producer.png" style="zoom:40%">



Firstly, the data producer should be compatible with different datasource, and output the datastream as an uniformed format. For this project, since our data is coming from tushare API, to prevent being blocked for calling too frequent, we have downloaded some of the data for testing, therefore, we also have an dataFile implementation to mimic the streaming data. Here we set the data interval as 5s. Once a socket connection is setup, the producer will keep throwing current data of all stocks to the receiver until the connection was shutdown.

## Data Process Platform

### Components

<img src="docs/components.png" style="zoom:70%">

For the core part of the system, this picture shows the modules in the data process platform, There are 5 layers from top to the bottom: API, Service, Processor, Resource, Infrastructure. For now, the platform was built based on the MongoDB and Spark. The resource layer will do the management of connections and interactions with lower level. Above, the Processor implement the basic unit of algorithms of the system such as modeling, streaming & batch operations. Higher level is the service layer which manage tasks of the system. One task consists of multiple processors, and it is a single process in Linux. The Task Manager will keep tracking and communicate with them. Finally, on the top level are the APIs provide to outside, which covers Task / Data / Model management.

The modules with dotted line means not implemented yet. And some of the features are still under development. Next I'll introduce the some of the core parts in the platform. Task Manager & Stream Processor.

### Task Manager

<img src="docs/task-manager.png" style="zoom:40%">

This graph illustrate how the APIs interact with Task Manager to create / stop a task and acquire the data generated in time. When user add an Task, the system will initial a task instance running in a single process, such task process will have two thread, one is to listening to the communication commands from the task manager, another thread is the actually job executor to handle the batch or stream jobs. The calculation results will be stored in the local cache of the task process. When a data request coming, the task manager can retrive the newest data from the process cache through internal command.

### Configration Files

<img src="docs/config-file.png" style="zoom:50%">

The configuration file of the data process platform is simple. And Integrate new algorithm is just simply implement the processor interface and then update the pname_dict attribute with API name, classname and process type without shutdown the system.

### Stream Process

#### General Process

![image-20190507152756231](docs/general-stream-process.png)

Now, let's look at the detail of the Streaming Process. In general, our process can be described as following 3 stages: clustering, aggregation and then regression. For each interval (5 seconds), we apply this process in the input stock data. The final output is the prediction on the stock price of each cluster. The reason to do so, is that we believe stocks even belongs to different industry may have strong hidden influential factors on each other, thus doing the regression on stock clusters could be more representative and helpful for other processings.

In this case, to reduce the computational cost and the complexity, we use Streaming KMeans & Streaming LinearRegressionWithSGD to do the clustering and regression respectively.

### Intermediate DStream

<img src="docs/intermediate-dstream.png" style="zoom:45%">

Here shows an intermediate DStream. The red box separate 2 clusters, and each cluster have their own training DStream, prediction DStream as well as the result DStream.

For now, we set the window as 3 times of the batch length, and the sliding interval as 1 batch length, which means, for each output, the models will be incrementally trained with previous 3 data points, and the prediction of next future time point will based on current data and features.

#### Details

![image-20190507152809520](docs/stream-details.png)

Then this flow chart gives the detail about our streaming process procedures.

The reason to using the sliding window is that, we need to using the previous data to build current features, and future data to build current label. As the original string data keep coming, the first transformation will split and cast each value to list structure, then the next transformation can do the feature engineering. Due to the Structured Streaming is still on experimental stage, we decide to not use it. Instead, to take advantage of DataFrame operations, we call rdd.toDF in the transformation to build features. As mentioned before, the feature engineering part include at least 3 shifting operation and several column operations. Since the ML algorithm have separate APIs for training and prediction, we split the stream to 2 branches, one for training the model, another for the inference of current stocks' cluster.

After join the data and cluster label together and aggregate the stocks in the same cluster, from the code structure aspect, the following procedures are similar to above, except that we'll assign each cluster a regression model to do the prediction. As you can see the items in the right bottom corner are mostly stack squares.

Finally, we encapsulate the current price, predicted future price and cluster result to a single structure and store it in the process cache.

### Batch Process

***Unfinished***

<img src="docs/general-batch.png" style="zoom:40%">

About the Batch Processing, due to the time limitation, we haven't finished this part yet. When it was done, we hope it works as the design shows, to using more complex, advanced, offline-trained models to support the real-time inference and evaluation functions.



## Problems & Improvements

<img src="docs/performance-jump-pred.png" style="zoom:40%">

Because this is quite a big system, there are still many problems to solve. For example, you can see the graph in the right. The red line is the real trend while the black is the prediction. You see there are many missing points on the prediction value, and it is quite regular. That’s because we are just testing the system on our own computer, the processing speed cannot catch up the coming data, which will result in the jump on prediction value. To solve this, one is using a real cluster rather than PCs, another is using Kafka to do the data caching. However, the later one may result in to delay on prediction speed.



## Test requests

http://localhost:5000/demo

http://localhost:5000/stop_oltp_processor?pid=

http://localhost:5000/get_curr_oltp_result?pid=



## Reference

* For front-end app fast development: [vue.js](https://vuejs.org)
* For front-end app, web page components: [Bootstrap](https://getbootstrap.com/docs/4.3/examples/)
* For API Building (just for reference): [Deployd](http://deployd.com/)
