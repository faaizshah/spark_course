package polytech.umontpellier.fr

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PG_Yann_Pomie {
  def main(args: Array[String]): Unit = {

    val programStartTime = System.nanoTime()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val jdbcUrl = "jdbc:postgresql://postgresql:5432/dvdrental"

    // Creating Spark Session
    val spark = SparkSession.builder
      .appName("SparkPostgreSQLApp")
      //.config("spark.jars.packages", "org.postgresql:postgresql:42.5.4")
      .getOrCreate()

    // Set log level to Error
    spark.sparkContext.setLogLevel("ERROR")
    val df = spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", jdbcUrl)
      .option("dbtable", "film")
      .option("user", "postgres")
      .option("password", "password")
      .load()

    df.printSchema()
    println("The first 5 films")
    df.show(5) // Print the first 10 rows

    println(s"Total rows in film table: ${df.count()}")

    val ratedMovies = df
      .filter(col("rating").isin("R", "NC-17"))
      .select("title", "rating", "release_year")

    println("All movies rated R or NC-17")
    ratedMovies.show()

    val programElapsedTime = (System.nanoTime() - programStartTime) / 1e9
    println(s"\nProgram execution time: $programElapsedTime seconds")
    spark.stop()
  }
}

