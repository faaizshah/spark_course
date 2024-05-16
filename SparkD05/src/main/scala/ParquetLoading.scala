package polytech.umontpellier.fr

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ParquetLoading {
  def main(args: Array[String]): Unit = {

    val programStartTime = System.nanoTime()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder
      .appName("ParquetLoading Application")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    //      case class Flight(DEST_COUNTRY_NAME: String,
    //                        ORIGIN_COUNTRY_NAME: String,
    //                        count: BigInt)
    //

    val flightsDF = spark.read
      .parquet("data/flight-data/parquet/2010-summary.parquet/part-r-00000-1a9822ba-b8fb-4d8e-844a-ea30d0801b9e.gz.parquet")

    flightsDF.printSchema()
    flightsDF.show(5)


    val programElapsedTime = (System.nanoTime() - programStartTime) / 1e9
    println(s"\nProgram execution time: $programElapsedTime seconds")
    spark.stop()

  }
}