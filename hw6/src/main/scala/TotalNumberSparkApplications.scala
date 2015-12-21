import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext._
import org.apache.spark.streaming._
import _root_.kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka._

object TotalNumberSparkApplications {
  def main(args: Array[String]) {
    
    val conf = new SparkConf()
      .setAppName("TotalNumberSparkApplications")

    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.checkpoint("./checkpoints/")

    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
    val topics = Set("events-test")
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    stream.print()

    val filtered = stream.map(_._2).filter(_ contains "SparkListenerApplicationStart").map(_ => ("numApps", 1))

    val stateDStream = filtered.updateStateByKey(updateFunc)
    stateDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def updateFunc (newValues: Seq[Int], state: Option[Int]): Option[Int] = {
    val currentCount = newValues.foldLeft(0)(_ + _)
    val previousCount = state.getOrElse(0)
    Some(previousCount + currentCount)
  }
}