package polytech.umontpellier.fr

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object StackOverflowDataAnalysis {
  def main(args: Array[String]): Unit = {
    val programStartTime = System.nanoTime()

    Logger.getLogger("org").setLevel(Level.ERROR)

    val csvDataFile = "data/stackoverflow.csv"

    val spark = SparkSession.builder
      .appName("Stackoverflow Application")
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

    println(s"\nCount of records in CSV file: ${df.count()}")
    df.printSchema()
    df.show(5)

    import spark.implicits._

    println("Count acceptedAnswer null: " + df.filter($"acceptedAnswer".isNull).count())
    println("Count tag null: " + df.filter($"tag".isNull).count())
    println("Count parentId null: " + df.filter($"parentId".isNull).count())

    val highScorePosts = df.filter($"score" > 20)
    highScorePosts.sort($"score".desc).show(5)

    df.createOrReplaceTempView("stackoverflow")

    val top5Scores = spark.sql("SELECT id, score FROM stackoverflow ORDER BY score DESC LIMIT 5")
    top5Scores.show()

    val popularTags = spark.sql("SELECT tag, COUNT(*) AS frequency FROM stackoverflow WHERE tag IS NOT NULL GROUP BY tag ORDER BY frequency DESC LIMIT 10")
    popularTags.show()

    val programElapsedTime = (System.nanoTime() - programStartTime) / 1e9
    println(s"Program execution time: $programElapsedTime seconds")
    spark.stop()
  }
}
