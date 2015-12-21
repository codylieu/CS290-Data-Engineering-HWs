import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext._
import org.apache.spark.streaming._
import _root_.kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka._

object TotalNumberDistinctEvents {
  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("TotalNumberDistinctEvents")

    val ssc = new StreamingContext(conf, Seconds(10))
    val sqlContext = new SQLContext(ssc.sparkContext)

    val eventTypes = Array("SparkListenerLogStart", "SparkListenerBlockManagerAdded", "SparkListenerEnvironmentUpdate", "SparkListenerApplicationStart", "SparkListenerJobStart", "SparkListenerStageSubmitted", "SparkListenerTaskStart", "SparkListenerTaskEnd", "SparkListenerStageCompleted", "SparkListenerJobEnd", "SparkListenerApplicationEnd")
    var eventCounts = ssc.sparkContext.parallelize(eventTypes).map(name => (name, 0))

    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
    val topics = Set("events-test")
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    val filtered = stream.map(_._2)

    filtered.foreachRDD { rdd =>
        if(!rdd.isEmpty) {
            val eventsLog = sqlContext.read.json(rdd);
            val curEventsCount = eventsLog.map(r => r.getAs[String]("Event")).map(e => (e , 1))
            eventCounts = eventCounts.union(curEventsCount).reduceByKey(_ + _)
        }
        else {
            println("NO NEW DATA RECEIVED")
        }
        eventCounts.collect.foreach(println)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}