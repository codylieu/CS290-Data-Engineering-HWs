/* TenMostFrequentVisitees.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext._

object TenMostFrequentVisitees {

  val ec2= "ec2-52-25-231-56.us-west-2.compute.amazonaws.com"
  val SPARK_MASTER = "spark://" + ec2 + ":7077"

  val HDFS = "hdfs://" + ec2 + ":9000"

  val fileName = "White_House_Visitor_Records_Requests.csv"
  val filePath = "/home/ec2-user/" + fileName

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster(SPARK_MASTER)
      .setAppName("TenMostFrequentVisitees")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val textFile = sc.textFile(filePath)

    val schemaString = "last_name first_name"
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    val visitees = textFile.map(_.split(",")).map(line => Row(line(19), line(20)))

    val dfVisitees = sqlContext.createDataFrame(visitees, schema)
    dfVisitees.registerTempTable("visitees")

    val tenMostFrequentVisitees = sqlContext.sql("SELECT last_name, first_name, COUNT(*) AS cnt FROM visitees GROUP BY last_name, first_name ORDER BY cnt DESC LIMIT 10")
    tenMostFrequentVisitees.collect().foreach(println)
    tenMostFrequentVisitees.explain(true)
  }
}