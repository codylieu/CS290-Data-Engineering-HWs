/* TenMostFrequentVisitorVisitees.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object TenMostFrequentVisitorVisitees {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("TenMostFrequentVisitorVisitees")

    val sc = new SparkContext(conf)
    val textFile = sc.textFile("White_House_Visitor_Records_Requests.csv")
    val visitorVisitees = textFile.map(line => line.split(","))
      .map(line => ((line(0), line(1), line(2)), (line(19), line(20))))
    val reducedVisitiorVisitees = visitorVisitees.map(line => (line, 1)).reduceByKey((a, b) => a + b)
    val tenMostFrequentVisitorVisitees = reducedVisitiorVisitees.takeOrdered(10)(Ordering[Int].reverse.on(x => x._2))
    tenMostFrequentVisitorVisitees.foreach(println)
  }
}