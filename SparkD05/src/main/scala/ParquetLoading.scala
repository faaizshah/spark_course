package polytech.umontpellier.fr

import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.SparkSession

object ParquetLoading {
  def main(args: Array[String]): Unit = {
    val startTime = System.nanoTime()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder
      .appName("ParquetLoading Application")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val flightsDF = spark.read.parquet("data/parquet/")

    flightsDF.printSchema()
    flightsDF.show(5)

    println(s"\nProgram execution time: ${(System.nanoTime() - startTime) / 1e9} seconds")
    spark.stop()

  }
}