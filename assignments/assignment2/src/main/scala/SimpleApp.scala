import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}
import java.io.File

object SimpleApp {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val logFile = "stackoverflow-grading.csv"

    val spark = SparkSession.builder
      .appName("Spark Learn")
      .master("local[*]")
      .getOrCreate()

    val logData = spark.read.textFile(logFile).cache()

    val df = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(logFile)

    println(s"\nCount of records: ${df.count()}")
    df.printSchema()
    df.show(5)

    spark.stop()
  }
}
