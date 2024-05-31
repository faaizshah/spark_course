package polytech.umontpellier.fr

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Assignment2JulienDubois {
  def main(args: Array[String]): Unit = {
    val startTime = System.nanoTime()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val wikipediaFilePath = "data/wikipedia.dat"

    val spark = SparkSession.builder
      .appName("Wikipedia Application")
      .config("spark.driver.memory", "8G")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      .csv(wikipediaFilePath)

    println(s"\nCount of records in CSV file: ${df.count()}")
    df.printSchema()
    df.show(5)

    // find the most popular article
    val mostPopularArticle = df
      .na.drop()
      .select("_c1")
      .groupBy("_c1")
      .count()
      .orderBy("count")
      .limit(1)
      .collect()(0)

    println("\nMost popular article:")
    println(s"Title: ${mostPopularArticle(0)}")
    println(s"Count: ${mostPopularArticle(1)}")

    println(
      s"\nProgram execution time: ${(System.nanoTime() - startTime) / 1e9} seconds"
    )
    spark.stop()
  }
}
