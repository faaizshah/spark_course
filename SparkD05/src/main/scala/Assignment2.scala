package polytech.umontpellier.fr

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Assignment2 {
  def main(args: Array[String]): Unit = {

    val programStartTime = System.nanoTime()

    Logger.getLogger("org").setLevel(Level.ERROR)

    val logFile = "data/wikipedia.dat"

    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val logData = spark.read.textFile(logFile).cache()

    println ("\nLines count:" +  logData.count())

    val numFrance = logData
      .filter(line => line.contains("France"))
      .count()

    val numParis = logData
      .filter(line => line.contains("Paris"))
      .count()

    println(s"\nLines with word France: $numFrance, Lines with word 'Paris': $numParis")

    val programElapsedTime = (System.nanoTime() - programStartTime) / 1e9
    println(s"\nProgram execution time: $programElapsedTime seconds")
    spark.stop()
  }
}

