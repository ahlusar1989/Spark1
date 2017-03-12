package wikipedia

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

case class WikipediaArticle(title: String, text: String)

object WikipediaRanking {

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  val conf: SparkConf = new SparkConf(loadDefaults = true)
                              .setMaster("local[2]")
                              .setAppName("Wikipedia")

  val sc: SparkContext = new SparkContext(conf)

  val wikiRdd: RDD[WikipediaArticle] = sc.textFile("wikiepedia.dat").map {
    line =>
      val splitLine = line.split("\t")
      try {
        val title = splitLine(0)
        val text = splitLine(1)
        WikipediaArticle(title, text)
      }
      catch {
        case _ : Throwable => WikipediaArticle("", "")
      }
  }

  /** Returns the number of articles on which the language `lang` occurs.
   */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = {

    val initialCount = 0;
    val addToCounts = (n: Int, v: WikipediaArticle) => n + 1
    val sumPartitionCounts = (p1: Int, p2: Int) => p1 + p2
    val countByKey = rdd.aggregate(initialCount)(addToCounts, sumPartitionCounts)
    return countByKey
  }

  /* (1) Use `occurrencesOfLang` to compute the ranking of the languages
   *     (`val langs`) by determining the number of Wikipedia articles that
   *     mention each language at least once. Sort the
   *     languages by their occurence, in decreasing order
   *
   */
  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    val occurences = List[Int](0)
    for ( lang <- langs) {
      val count = occurrencesOfLang(lang, rdd)
      occurences :+ count
    }
    val mappingTuples = langs.zip(occurences)
    return mappingTuples.sortBy(_._2)
  }



  /* Compute an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   */
  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] = {
       sc.parallelize(langs
                  .zip(rdd
                    .groupBy(article => article.title)
                    .collect()
                    .map(v => v._2.toIterable)))
  }


  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = {
    val ranks = index.flatMap(_._2.map(article => article.text))
                              .distinct
                              .collect
                              .zipWithIndex
                              .toList

  }

  /* (3) Using `reduceByKey` so that the computation of the index and the ranking is combined.
   */
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = ???

  def main(args: Array[String]) {

    /* Languages ranked according to (1) */
    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

    /* Languages ranked according to (2), using the inverted index */
    val langsRanked2: List[(String, Int)] = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))

    /* Languages ranked according to (3) */
    val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))

    /* Output the speed of each ranking */
    println(timing)
    sc.stop()
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}
