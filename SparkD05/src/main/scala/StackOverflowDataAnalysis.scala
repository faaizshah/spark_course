package polytech.umontpellier.fr

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, IntegerType, StringType}
import org.apache.spark.sql.functions.col

object StackOverflowDataAnalysis {
  def main(args: Array[String]): Unit = {

    val startTime = System.nanoTime()

    Logger.getLogger("org").setLevel(Level.ERROR)

    val dataFilePath = "data/stackoverflow.csv"

    // Initialize Spark session
    val sparkSession = SparkSession.builder
      .appName("StackOverflow Data Analysis")
      .config("spark.driver.memory", "8G")
      .master("local[*]")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    // Define schema for the CSV data
    val dataSchema = new StructType()
      .add("postTypeId", IntegerType, nullable = true)
      .add("postId", IntegerType, nullable = true)
      .add("acceptedAnswer", StringType, nullable = true)
      .add("parentPostId", IntegerType, nullable = true)
      .add("postScore", IntegerType, nullable = true)
      .add("postTag", StringType, nullable = true)

    // Load CSV data into DataFrame
    val dataFrame = sparkSession.read
      .option("header", "false")
      .schema(dataSchema)
      .csv(dataFilePath)
      .drop("acceptedAnswer")

    // Display record count and schema
    println(s"\nCount of records in CSV file: ${dataFrame.count()}")
    dataFrame.printSchema()
    dataFrame.show(5)

    // Count null values in specific columns
    println("Count postTag null: " + dataFrame.filter("postTag is null").count())
    println("Count parentPostId null: " + dataFrame.filter("parentPostId is null").count())

    // Filter and sort posts with high scores
    val topPosts = dataFrame.filter("postScore > 20").sort(col("postScore").desc)
    topPosts.show(5)

    // Register the DataFrame as a SQL temporary view
    dataFrame.createOrReplaceTempView("stackoverflow_data")

    // Query 1: Top 5 highest scores
    val highestScores = sparkSession.sql("SELECT postId, postScore FROM stackoverflow_data ORDER BY postScore DESC LIMIT 5")
    highestScores.show()

    // Query 2: Number of posts for each tag
    sparkSession.sql("SELECT postTag, COUNT(*) AS postCount FROM stackoverflow_data GROUP BY postTag ORDER BY postCount DESC").show()

    // Query 3: Number of posts for each tag with a score greater than 5
    sparkSession.sql("SELECT postTag, COUNT(*) AS postCount FROM stackoverflow_data WHERE postScore > 5 GROUP BY postTag ORDER BY postCount DESC").show()

    // Calculate and display program execution time
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println(s"\nProgram execution time: $elapsedTime seconds")
    
    // Stop the Spark session
    sparkSession.stop()
  }
}
