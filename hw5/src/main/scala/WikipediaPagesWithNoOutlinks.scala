/* WikipediaPagesWithNoOutlinks.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext._

object WikipediaPagesWithNoOutlinks {
  /* need to run spark-shell and spark-submit with --driver-memory 4g to make this work */

  val ec2= "ec2-52-25-231-56.us-west-2.compute.amazonaws.com"
  val SPARK_MASTER = "spark://" + ec2 + ":7077"

  val HDFS = "hdfs://" + ec2 + ":9000"

  val titlesFileName = "titles-sorted.txt"
  val linksFileName = "links-simple-sorted.txt"

  val titlesFilePath = "/home/ec2-user/" + titlesFileName
  val linksFilePath = "/home/ec2-user/" + linksFileName

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster(SPARK_MASTER)
      .setAppName("WikipediaPagesWithNoOutlinks")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val titlesWithId = sc.textFile(titlesFilePath).zipWithIndex.map{ case (k, v) => Row((v + 1).toString, k) }
    val titlesSchemaString = "id title"
    val titlesSchema = StructType(titlesSchemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val titles = sqlContext.createDataFrame(titlesWithId, titlesSchema)

    val linksSimpleSortedWithId = sc.textFile(linksFilePath).map(line => Row(line.split(":")(0).toString))
    val linksSchemaString = "id"
    val linksSchema = StructType(linksSchemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val pageIdsWithOutlinks = sqlContext.createDataFrame(linksSimpleSortedWithId, linksSchema)

    val pageIdsWithNoOutlinks = titles.select("id").except(pageIdsWithOutlinks)

    val pageNamesWithNoOutlinks = pageIdsWithNoOutlinks.join(titles, "id")
    pageIdsWithNoOutlinks.explain(true)

    println("NUMBER OF PAGES WITH NO OUTLINKS: " + pageNamesWithNoOutlinks.count())
  }
}
