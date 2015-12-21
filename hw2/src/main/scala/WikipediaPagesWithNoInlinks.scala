/* WikipediaPagesWithNoInlinks.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object WikipediaPagesWithNoInlinks {
  /* need to run spark-shell and spark-submit with --driver-memory 4g to make this work */

  val ec2 = "ec2-54-187-152-145.us-west-2.compute.amazonaws.com"
  val SPARK_MASTER = "spark://" + ec2 + ":7077"

  val HDFS = "hdfs://" + ec2 + ":9000"

  val titlesFileName = "titles-sorted.txt"
  val linksFileName = "links-simple-sorted.txt"

  val titlesFilePath = "/home/ec2-user/" + titlesFileName
  val linksFilePath = "/home/ec2-user/" + linksFileName

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster(SPARK_MASTER)
      .setAppName("WikipediaPagesWithNoInlinks")

    val sc = new SparkContext(conf)

    val titlesSorted = sc.textFile(titlesFilePath)
    val titlesSortedWithIndexKey = titlesSorted.zipWithIndex.map{ case (k, v) => ((v + 1).toLong, k) }
    val titlesSortedWithoutNames = titlesSortedWithIndexKey.map{ case (k, v) => k }

    val linksSimpleSorted = sc.textFile(linksFilePath)
    val pagesWithInlinks = linksSimpleSorted.map(line => line.split(":")).flatMap(line => line(1).trim.split(" ")).map(line => line.toLong).distinct

    val pagesWithNoInlinks = titlesSortedWithoutNames.subtract(pagesWithInlinks)

    val pageNamesWithNoInlinks = pagesWithNoInlinks.map(line => (line, "blah")).join(titlesSortedWithIndexKey).map(line => line._2._2)

    println("NUMBER OF PAGES WITH NO OUTLINKS: " + pageNamesWithNoInlinks.count)
    // println("First entry: " + pageNamesWithNoInlinks.first) // check
  }
}