/* WikipediaPagesWithNoOutlinks.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WikipediaPagesWithNoOutlinks {
  /* need to run spark-shell and spark-submit with --driver-memory 4g to make this work */

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("WikipediaPagesWithNoOutlinks")

    val sc = new SparkContext(conf)

    val titlesSorted = sc.textFile("titles-sorted.txt")
    val titlesSortedWithIndexKey = titlesSorted.zipWithIndex.map{ case (k, v) => ((v + 1).toLong, k) }
    val titlesSortedWithoutNames = titlesSortedWithIndexKey.map{ case (k, v) => k }

    val linksSimpleSortedWithIndexKey = sc.textFile("links-simple-sorted.txt").map(line => line.split(":")(0).toLong)

    val pagesWithNoOutlinks = titlesSortedWithoutNames.subtract(linksSimpleSortedWithIndexKey)

    val pageNamesWithNoOutlinks = pagesWithNoOutlinks.map(line => (line, "blah")).join(titlesSortedWithIndexKey).map(line => line._2._2)

    println("NUMBER OF PAGES WITH NO OUTLINKS: " + pageNamesWithNoOutlinks.count)
    // println("First entry: " + pageNamesWithNoOutlinks.first) // check
  }
}







