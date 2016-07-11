// Version 1.02 for IJCAI-16 Tutorial

package sbdpm

import java.util.Arrays
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.JavaConversions
import scala.io.Source

object Demo {

	// Application Specific Variables
	private final val SPARK_MASTER = "yarn-client"
	private final val APPLICATION_NAME = "sbdpm-demo"
	private final val DATASET_PATH_PUBMED = "/tmp/pubmed.csv"
	private final val DATASET_PATH_TWITTER = "/tmp/twitter.csv"

	// HDFS Configuration Files
	private final val CORE_SITE_CONFIG_PATH = new Path("/usr/hdp/current/hadoop-client/conf/core-site.xml")
	private final val HDFS_SITE_CONFIG_PATH = new Path("/usr/hdp/current/hadoop-client/conf/hdfs-site.xml")

	def main(args: Array[String]): Unit = {

		// Configure SparkContext
		val conf = new SparkConf()
			.setMaster(SPARK_MASTER)
			.setAppName(APPLICATION_NAME)
		val sc = new SparkContext(conf)

		// Configure HDFS
		val configuration = new Configuration();
		configuration.addResource(CORE_SITE_CONFIG_PATH);
		configuration.addResource(HDFS_SITE_CONFIG_PATH);

		// Print Usage Information
		System.out.println("\n----------------------------------------------------------------\n")
		System.out.println("Usage: spark-submit [spark options] sbdpm-demo.jar [exhibit]")
		System.out.println(" Exhibit \'hw\': HelloWorld")
		System.out.println(" Exhibit \'tweet\': Top Hashtags")
		System.out.println(" Exhibit \'kmeans\': KMeans Clustering")
		System.out.println("\n----------------------------------------------------------------\n");

		// Exhibit: HelloWorld
		if(args(0) == "hw") {
			System.out.println("Running exhibit: HelloWorld")
			System.out.println("Hello, World!")
		}

		// Exhibit: Top Hashtags
		if(args(0) == "tweet") {
			System.out.println("Running exhibit: Twitter Data")

			val lines = sc.textFile(FileSystem.get(configuration).getUri + DATASET_PATH_TWITTER)
			val texts = lines.map(line => line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")).map(cs => (if (cs.size < 7) ""; else cs(6)))
			val words = texts.flatMap(_.split("[ .,?!:\"]"))
			val hashtags = words.filter(x => x.size > 0 && x(0) == '#')
			val htStats = hashtags.map(h => (h, 1)).reduceByKey(_+_)
			val top100 = htStats.sortBy(_._2, false).take(100)

			// Output Results
			top100.foreach(x => System.out.println(x._1 + "\t\t" + x._2))
		}

		// Exhibit: KMeans Clustering
		if(args(0) == "kmeans") {
			System.out.println("Running exhibit: KMeans Clustering")

			// Do Clustering
			val lines = sc.textFile(FileSystem.get(configuration).getUri + DATASET_PATH_PUBMED)
			val papers = Helper.parseData(lines)
			val featureVectors = FeatureExtraction.constructFeatureVectorsFromPapers(papers).cache()
			val start = System.nanoTime
			val clustersOfPapers = new KMeansClustering(3, 100).clusterPapers(featureVectors)
			val end = System.nanoTime

			// Output Results
			Helper.summarize(papers, clustersOfPapers, 10)
			println((end - start) / 1000000 + "ms")
		}
	}
}