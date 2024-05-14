package polytech.umontpellier.fr

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Assignment2 {
  def main(args: Array[String]): Unit = {

    val programStartTime = System.nanoTime()

    Logger.getLogger("org").setLevel(Level.ERROR)

    val csvDataFile = "data/wikipedia.dat"

    val spark = SparkSession.builder
      .appName("Wikipedia Application")
      .config("spark.driver.memory", "8G")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read.textFile("data/wikipedia.dat")

    println(s"\nCount of records in wikipedia file: ${df.count()}")

    val strA = "France"
    val strB = "Paris"

    val numAs = df
      .filter(line => line.contains(strA))
      .count()

    val numBs = df
      .filter(line => line.contains(strB))
      .count()

    println(s"\nLines with word $strA: $numAs, Lines with word $strB: $numBs")

    val programElapsedTime = (System.nanoTime() - programStartTime) / 1e9
    println(s"\nProgram execution time: $programElapsedTime seconds")
    spark.stop()
  }
}
