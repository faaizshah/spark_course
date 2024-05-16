package polytech.umontpellier.fr

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Assignement2_Arsene {
  def main(args: Array[String]): Unit = {

    val programStartTime = System.nanoTime()

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder
      .appName("Wikipedia App")
      .config("spark.driver.memory", "8G")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read.textFile("data/wikipedia.dat")

    println(s"\nCount of records in wikipedia file: ${df.count()}")

    val wordA = "China"
    val wordB = "England"

    val numAs = df
      .filter(line => line.contains(wordA))
      .count()

    val numBs = df
      .filter(line => line.contains(wordB))
      .count()

    println(s"\nLines with word $wordA: $numAs, Lines with word $wordB: $numBs")

    val programElapsedTime = (System.nanoTime() - programStartTime) / 1e9
    println(s"\nProgram execution time: $programElapsedTime seconds")
    spark.stop()
  }

}
