/* TenMostFrequentVisitorVisitees.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext._

object TenMostFrequentVisitorVisitees {

  val ec2= "ec2-52-25-231-56.us-west-2.compute.amazonaws.com"
  val SPARK_MASTER = "spark://" + ec2 + ":7077"

  val HDFS = "hdfs://" + ec2 + ":9000"

  val fileName = "White_House_Visitor_Records_Requests.csv"
  val filePath = "/home/ec2-user/" + fileName

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster(SPARK_MASTER)
      .setAppName("TenMostFrequentVisitorVisitees")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val textFile = sc.textFile(filePath)

    val schemaString = "visitor_last_name visitor_first_name visitor_middle_name visitee_last_name visitee_first_name"
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    val visitorVisitees = textFile.map(_.split(","))
      .map(line => Row(line(0), line(1), line(2), line(19), line(20)))

    val dfVisitorVisitees = sqlContext.createDataFrame(visitorVisitees, schema)
    dfVisitorVisitees.registerTempTable("visitorVisitees")

    val tenMostFrequentVisitorVisitees = sqlContext.sql("SELECT visitor_last_name, visitor_first_name, visitor_middle_name, visitee_last_name, visitee_first_name, COUNT(*) AS cnt FROM visitorVisitees GROUP BY visitor_last_name, visitor_first_name, visitor_middle_name, visitee_last_name, visitee_first_name ORDER BY cnt DESC LIMIT 10")
    tenMostFrequentVisitorVisitees.collect().foreach(println)
    tenMostFrequentVisitorVisitees.explain(true)
  }
}