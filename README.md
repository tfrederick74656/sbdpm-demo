# sbdpm-demo
Sample Code for Scalable Big Data Programming Models Tutorial at IJCAI 2016

Usage: spark-submit [spark options] sbdpm-demo.jar [exhibit]
- Exhibit 'hw': HelloWorld
- Exhibit 'tweet': Top Hashtags
- Exhibit 'kmeans': KMeans Clustering

Starting Options for KMeans
spark-submit --master yarn-client --driver-memory 2g --executor-memory 2g --num-executors 3 --executor-cores 8 sbdpm-demo.jar kmeans