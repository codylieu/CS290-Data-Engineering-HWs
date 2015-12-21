/* TenMostFrequentVisitors.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object TenMostFrequentVisitors {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("TenMostFrequentVisitors")

    val sc = new SparkContext(conf)
    val textFile = sc.textFile("White_House_Visitor_Records_Requests.csv")
    val visitors = textFile.map(line => line.split(",")).map(line => (line(0), line(1), line(2)))
    val reducedVisitors = visitors.map(line => (line, 1)).reduceByKey((a, b) => a + b)
    val tenMostFrequentVisitors = reducedVisitors.takeOrdered(10)(Ordering[Int].reverse.on(x => x._2))
    tenMostFrequentVisitors.foreach(println)
  }
}