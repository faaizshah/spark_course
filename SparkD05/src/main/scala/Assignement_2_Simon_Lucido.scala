package polytech.umontpellier.fr

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Wikipedia {
  def main(args: Array[String]): Unit = {

    val programStartTime = System.nanoTime()

    Logger.getLogger("org").setLevel(Level.ERROR)

    val dataFile = "data/wikipedia.dat"

    val spark = SparkSession.builder
      .appName("Stackoverflow Application")
      .config("spark.driver.memory", "8G")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      .text(dataFile)

    println(s"\nCount of records in file: ${df.count()}")
    df.printSchema()
    df.show(5)

    val numFrance = df
      .filter(line => line.getString(0).contains("France"))
      .count()

    println(s"\nLines with word France: $numFrance")

    val numParis = df
      .filter(line => line.getString(0).contains("Paris"))
      .count()

    println(s"\nLines with word Paris: $numParis")

    val programElapsedTime = (System.nanoTime() - programStartTime) / 1e9
    println(s"\nProgram execution time: $programElapsedTime seconds")
    spark.stop()
  }
}
