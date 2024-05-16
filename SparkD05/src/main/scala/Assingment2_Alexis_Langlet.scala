package polytech.umontpellier.fr

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Wikipedia {
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

    val df = spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      .csv(csvDataFile)

    println(s"\nCount of records in CSV file: ${df.count()}")
    df.printSchema()
    df.show(5)


    val programElapsedTime = (System.nanoTime() - programStartTime) / 1e9
    println(s"\nProgram execution time: $programElapsedTime seconds")
    spark.stop()
  }
}

