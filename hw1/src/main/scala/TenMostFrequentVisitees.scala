/* TenMostFrequentVisitees.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object TenMostFrequentVisitees {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("TenMostFrequentVisitees")

    val sc = new SparkContext(conf)
    val textFile = sc.textFile("White_House_Visitor_Records_Requests.csv")
    val visitees = textFile.map(line => line.split(",")).map(line => (line(19), line(20)))
    val reducedVisitees = visitees.map(line => (line, 1)).reduceByKey((a, b) => a + b)
    val tenMostFrequentVisitees = reducedVisitees.takeOrdered(10)(Ordering[Int].reverse.on(x => x._2))
    tenMostFrequentVisitees.foreach(println)
  }
}