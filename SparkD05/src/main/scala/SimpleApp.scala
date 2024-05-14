package polytech.umontpellier.fr

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SimpleApp {
  def main(args: Array[String]): Unit = {

    val programStartTime = System.nanoTime()

    Logger.getLogger("org").setLevel(Level.ERROR)

    val logFile = "data/SPARK_README.txt"

    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val logData = spark.read.textFile(logFile).cache()

    println ("\nLines count:" +  logData.count())

    val numAs = logData
      .filter(line => line.contains("data"))
      .count()

    val numBs = logData
      .filter(line => line.contains("spark"))
      .count()

    println(s"\nLines with word data: $numAs, Lines with word spark: $numBs")

    val programElapsedTime = (System.nanoTime() - programStartTime) / 1e9
    println(s"\nProgram execution time: $programElapsedTime seconds")
    spark.stop()
  }
}

