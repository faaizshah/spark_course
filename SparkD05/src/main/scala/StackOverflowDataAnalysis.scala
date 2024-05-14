package polytech.umontpellier.fr

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.col
import org.apache.hadoop.shaded.org.checkerframework.checker.units.qual.h

object StackOverflowDataAnalysis {
  def main(args: Array[String]): Unit = {

    val programStartTime = System.nanoTime()

    Logger.getLogger("org").setLevel(Level.ERROR)

    val csvDataFile = "data/stackoverflow.csv"

    val spark = SparkSession.builder
      .appName("Stackoverflow data analysis Application")
      .config("spark.driver.memory", "8G")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val schema = new StructType()
      .add("postTypeId", IntegerType, nullable = true)
      .add("id", IntegerType, nullable = true)
      .add("acceptedAnswer", StringType, nullable = true)
      .add("parentId", IntegerType, nullable = true)
      .add("score", IntegerType, nullable = true)
      .add("tag", StringType, nullable = true)


    val df = spark.read
      .option("header", "false")
      .schema(schema)
      .csv(csvDataFile)
      .drop("acceptedAnswer")

    println(s"\nCount of records in CSV file: ${df.count()}")
    df.printSchema()
    df.show(5)

    import spark.implicits._
    
    // println("Count accepted answers null: " + df.filter(col("acceptedAnswer").isNull()).count()) // one way to filter, does not work for me
    // println("Count tag null: " + df.filter($"tag".isNull()).count()) // another way to filter, require import spark.implicits._, does not work for me
    // println("Count parentid null: " + df.filter("parentId is null").count()) // and another way to filter again, does work for me

    println("Count tag null: " + df.filter("tag is null").count())
    println("Count parentid null: " + df.filter("parentId is null").count())

    val highScorePosts = df.filter("score > 20").sort(col("score").desc)

    highScorePosts.show(5)

    // Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("stackoverflow")

    // Query 1: Top 5 highest scores
    val top5Scores = spark.sql("SELECT id, score FROM stackoverflow ORDER BY score DESC LIMIT 5")
    top5Scores.show()

    // Query 2: The number of posts for each tag
    spark.sql("SELECT tag, COUNT(*) AS postCount FROM stackoverflow GROUP BY tag ORDER BY postCount DESC").show()

    // Query 3: The number of posts for each tag with a score greater than 5
    spark.sql("SELECT tag, COUNT(*) AS postCount FROM stackoverflow WHERE score > 5 GROUP BY tag ORDER BY postCount DESC").show()

    val programElapsedTime = (System.nanoTime() - programStartTime) / 1e9
    println(s"\nProgram execution time: $programElapsedTime seconds")
    spark.stop()
  }
}

