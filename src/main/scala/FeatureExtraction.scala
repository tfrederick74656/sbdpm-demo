package sbdpm

import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

object FeatureExtraction extends Serializable {
  def constructFeatureVectorsFromPapers(papers: RDD[Map[String, Array[String]]]): RDD[Vector] = {
    val wordsOfPapers = papers.map(getWordsOfPaper)
    return constructFeatureVectors(wordsOfPapers)
  }

  def constructFeatureVectors(wordsOfPapers: RDD[Iterable[String]]): RDD[Vector] = {
    val hashingTF = new HashingTF()
    val tfVectors = hashingTF.transform(wordsOfPapers)
    val idfModel = new IDF().fit(tfVectors)
    val tfidfVectors = idfModel.transform(tfVectors)
    return tfidfVectors
  }

  def getWordsOfPaper(paper: Map[String, Array[String]]): Iterable[String] = {
    val abstracts = paper.getOrElse("A", Array())
    val words = abstracts.flatMap(s => s.split("[ ,.;()]").toIterable)  // Note: parameter for split is regex
    val stopWords = List("", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10",
                         "a", "A", "an", "An", "the", "The", "this", "This", "that", "That", "these", "These", "those", "Those",
                         "some", "Some", "all", "All",
                         "one", "One", "two", "Two", "three", "Three", "four", "Four",
                         "first", "First", "second", "Second", "third", "Third",
                         "be", "is", "are", "was", "were", "being", "been",
                         "do", "does", "did", "doing",
                         "can", "Can", "cannot", "Cannot", "can't", "Can't",
                         "will", "Will", "won't", "Won't", "would", "Would", "wouldn't", "Wouldn't",
                         "have", "Have", "has", "Has", "had", "Had", "having", "Having", "haven't", "Haven't", "hasn't", "Hasn't",
                         "should", "Should",
                         "may", "May", "might", "Might",
                         "I", "me", "my", "mine",
                         "you", "You", "your", "yours",
                         "he", "He", "him", "Him", "his", "His",
                         "she", "She", "her", "Her", "hers", "Hers",
                         "we", "We", "our", "Our", "ours", "Ours",
                         "they", "They", "their", "theirs",
                         "it", "It", "its",
                         "no", "No", "not", "Not",
                         "and", "And", "or", "Or", "but", "But",
                         "to", "To", "from", "From",
                         "of", "Of", "in", "In", "on", "On",
                         "for", "For",
                         "at", "At",
                         "with", "With",
                         "by", "By", "as", "As",
                         "before", "Before", "after", "After", "until", "Until",
                         "if", "If", "then", "Then", "however", "However", "because", "Because", "so", "So", "since", "Since", "such", "Such",
                         "what", "What", "when", "When", "while", "While", "where", "Where", "who", "Who", "whom", "Whom", "how", "How", "which", "Which", "why", "Why",
                         "than", "Than",
                         "too", "Too", "also", "Also", "either", "Either", "neither", "Neither",
                         "there", "There", "here", "Here")
    return words.filter(w => !stopWords.contains(w))
  }
}
