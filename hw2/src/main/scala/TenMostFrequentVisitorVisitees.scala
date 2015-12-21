/* TenMostFrequentVisitorVisitees.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object TenMostFrequentVisitorVisitees {

  val ec2 = "ec2-54-187-152-145.us-west-2.compute.amazonaws.com"
  val SPARK_MASTER = "spark://" + ec2 + ":7077"

  val HDFS = "hdfs://" + ec2 + ":9000"

  val fileName = "White_House_Visitor_Records_Requests.csv"
  val filePath = "/home/ec2-user/" + fileName

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster(SPARK_MASTER)
      .setAppName("TenMostFrequentVisitorVisitees")

    val sc = new SparkContext(conf)
    val textFile = sc.textFile(filePath)
    val visitorVisitees = textFile.map(line => line.split(","))
      .map(line => ((line(0), line(1), line(2)), (line(19), line(20))))
    val reducedVisitiorVisitees = visitorVisitees.map(line => (line, 1)).reduceByKey((a, b) => a + b)
    val tenMostFrequentVisitorVisitees = reducedVisitiorVisitees.takeOrdered(10)(Ordering[Int].reverse.on(x => x._2))
    tenMostFrequentVisitorVisitees.foreach(println)
  }
}