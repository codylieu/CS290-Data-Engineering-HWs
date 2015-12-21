/* WikipediaPagesWithNoInlinks.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object WikipediaPagesWithNoInlinks {
  /* need to run spark-shell and spark-submit with --driver-memory 4g to make this work */

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("WikipediaPagesWithNoInlinks")

    val sc = new SparkContext(conf)

    val titlesSorted = sc.textFile("titles-sorted.txt")
    val titlesSortedWithIndexKey = titlesSorted.zipWithIndex.map{ case (k, v) => ((v + 1).toLong, k) }
    val titlesSortedWithoutNames = titlesSortedWithIndexKey.map{ case (k, v) => k }

    val linksSimpleSorted = sc.textFile("links-simple-sorted.txt")
    val pagesWithInlinks = linksSimpleSorted.map(line => line.split(":")).flatMap(line => line(1).trim.split(" ")).map(line => line.toLong).distinct

    val pagesWithNoInlinks = titlesSortedWithoutNames.subtract(pagesWithInlinks)

    val pageNamesWithNoInlinks = pagesWithNoInlinks.map(line => (line, "blah")).join(titlesSortedWithIndexKey).map(line => line._2._2)

    println("NUMBER OF PAGES WITH NO INLINKS: " + pageNamesWithNoInlinks.count)
    // println("First entry: " + pageNamesWithNoInlinks.first) // check
  }
}