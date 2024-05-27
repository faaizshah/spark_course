package polytech.umontpellier.fr

import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.SparkSession

object Parquet {
    def main(args: Array[String]): Unit = {

      val programStartTime = System.nanoTime()
      Logger.getLogger("org").setLevel(Level.ERROR)

      val spark = SparkSession.builder
        .appName("Parquet Application")
        .master("local[*]")
        .getOrCreate()

      spark.sparkContext.setLogLevel("ERROR")

      import spark.implicits._


      val flights = spark.read
        .parquet("data/flight-data/parquet/2010-summary.parquet/")

      flights.printSchema()
      flights.show(5)


      val programElapsedTime = (System.nanoTime() - programStartTime) / 1e9
      println(s"\nProgram execution time: $programElapsedTime seconds")
      spark.stop()

    }
}