package sbdpm

import org.apache.spark.rdd.RDD

object Helper extends Serializable {
  def parseData(lines: RDD[String]): RDD[Map[String, Array[String]]] = {
    val rows = lines.map(line => line.split(','))
    val papers = rows.map(rowToPaper)
    return papers.filter(m => m.contains("A"))
  }

  // Input:  ["I:1", "N:a1", "N:a2", "A:blah"]
  // Output: {"I": [1], "N": ["a1", "a2"], "A": ["blah"]}
  def rowToPaper(row: Array[String]): Map[String, Array[String]]= {
      return row.groupBy(_(0).toString).map(x => (x._1, x._2.map(_.substring(2)).map(_.toLowerCase)))
  }

  def summarize(papers: RDD[Map[String, Array[String]]], clustersOfPapers: RDD[Int], n: Int): Unit = {
    // Prepare the report

    val numPapersInClusters = clustersOfPapers.groupBy(x=>x).map(x=>(x._1, x._2.size))
    val numPapersInOrderedClusters = numPapersInClusters.sortBy(_._1)

    val keywordsOfPapers = papers.map(getKeywordsOfPaper)

    val clusterKeywordsPairsOfPapers = clustersOfPapers.zip(keywordsOfPapers)
    // clusterKeywordsPairsOfPapers: [(1, ["k1", "k2"]), (0, ["k3", "k4", ...]),... ]

    val keywordsOfClusters = clusterKeywordsPairsOfPapers.groupBy(_._1).mapValues(_.flatMap(_._2))

    val sortedKeywordCountPairsOfClusters = keywordsOfClusters.mapValues(keywords => toSortedKeywordCountPairs(keywords.toArray))
    val sortedKeywordCountPairsOfOrderedClusters = sortedKeywordCountPairsOfClusters.sortBy(_._1)

    val paperCountKeywordCountPairs = numPapersInOrderedClusters.zip(sortedKeywordCountPairsOfOrderedClusters)
    paperCountKeywordCountPairs.collect().foreach(x => {  // it is important to use collect() here, or the print order will be random
      println("cluster " + x._1._1 + ": " + x._1._2 + " papers")
      x._2._2.take(n).foreach(kc=>println("\t" + kc._1 + " (" + kc._2 + ")"))
    })
  }

  def getKeywordsOfPaper(paper: Map[String, Array[String]]): Array[String] = {
    val keywords = paper.getOrElse("K", Array())
    return keywords.flatMap(_.split("[;/_]")).map(_.trim)
  }

  def toSortedKeywordCountPairs(keywords: Array[String]): Array[(String, Int)] = {
    val keywordCountPairs = keywords.groupBy(x=>x).map(x => (x._1, x._2.size))
    return keywordCountPairs.toArray.sortWith(_._2 > _._2)
  }
}
