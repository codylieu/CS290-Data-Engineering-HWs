import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext._
import org.apache.spark.streaming._
import _root_.kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka._
import org.apache.spark.rdd.RDD

// Not really any good unique keys to output this by, so my answer will follow the format of Stage ID, Task ID, Launch Time, since this can help uniquely identify a task
object TasksLongerThanAverage {
  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("TasksLongerThanAverage")

    val ssc = new StreamingContext(conf, Seconds(10))
    val sqlContext = new SQLContext(ssc.sparkContext)

    var tasks = ssc.sparkContext.parallelize(Array("")).filter(t => false)
    var averageRunningTime = 0
    var tasksLongerThanAverage = ssc.sparkContext.parallelize(Array("")).map(t => ("", "", 0L, 0L)).filter(t => false)

    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
    val topics = Set("events-test")
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    val filtered = stream.map(_._2).filter(_ contains "SparkListenerTaskEnd")

    filtered.foreachRDD { rdd =>
        if(!rdd.isEmpty) {
            tasks = tasks.union(rdd)

            val tasksDF = sqlContext.read.json(tasks)

            tasksDF.registerTempTable("tasks")

            val tasksClean = sqlContext.sql("SELECT `Stage ID` as stageID, `Task Info`.`Task ID` as taskID, `Task Info`.`Launch Time` as launchTime, `Task Info`.`Finish Time` as finishTime FROM tasks")

            val runningTimes = tasksClean.map(t => t.getAs[Long]("finishTime") - t.getAs[Long]("launchTime"))
            averageRunningTime = (runningTimes.sum / (runningTimes.count)).toInt

            tasksLongerThanAverage = tasksClean.map(t => (t.getAs[String]("stageID"), t.getAs[String]("taskID"), t.getAs[Long]("launchTime"), t.getAs[Long]("finishTime"))).filter(t => t._4 - t._3 > averageRunningTime)

        }
        else {
            println("NO NEW DATA RECEIVED")
        }
        println("AVERAGE RUNNING TIME OF ALL TASKS: " + averageRunningTime)
        tasksLongerThanAverage.collect().foreach(t => println("Task with Stage ID: " + t._1 + ", Task ID: " + t._2 + " Launched at " + t._3 + " and ran for " + (t._4 - t._3)))
    }

    ssc.start()
    ssc.awaitTermination()
  }
}