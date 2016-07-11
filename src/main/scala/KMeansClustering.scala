package sbdpm

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

class KMeansClustering(numClusters: Int, numIterations: Int) extends Serializable {
	def clusterPapers(featureVectors: RDD[Vector]): RDD[Int] = {
		val kmModel = KMeans.train(featureVectors, numClusters, numIterations)
		return kmModel.predict(featureVectors)
	}
}