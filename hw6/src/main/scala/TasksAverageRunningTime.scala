import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext._
import org.apache.spark.streaming._
import _root_.kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka._

object TasksAverageRunningTime {
  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("TasksAverageRunningTime")

    val ssc = new StreamingContext(conf, Seconds(10))
    val sqlContext = new SQLContext(ssc.sparkContext)

    var tasks = ssc.sparkContext.parallelize(Array(""))
    var averageRunningTime = 0

    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
    val topics = Set("events-test")
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    val filtered = stream.map(_._2).filter(_ contains "SparkListenerTaskEnd")

    filtered.foreachRDD { rdd =>
        if(!rdd.isEmpty) {
            tasks = tasks.union(rdd)

            val tasksDF = sqlContext.read.json(tasks)

            tasksDF.registerTempTable("tasks")

            val launchAndFinishTimes = sqlContext.sql("SELECT `Task Info`.`Launch Time` as launchTime, `Task Info`.`Finish Time` as finishTime FROM tasks")

            val runningTimes = launchAndFinishTimes.map(t => t.getAs[Long]("finishTime") - t.getAs[Long]("launchTime"))
            averageRunningTime = (runningTimes.sum / (runningTimes.count - 1)).toInt
        }
        else {
            println("NO NEW DATA RECEIVED")
        }
        println("AVERAGE RUNNING TIME OF ALL TASKS: " + averageRunningTime)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}