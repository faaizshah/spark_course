package polytech.umontpellier.fr

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType

object StackOverflow {
  def main(args: Array[String]): Unit = {

    val programStartTime = System.nanoTime()

    Logger.getLogger("org").setLevel(Level.ERROR)

    val csvDataFile = "data/stackoverflow.csv"

    val spark = SparkSession.builder
      .appName("Stackoverflow Application")
      .config("spark.driver.memory", "32G")
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

    import spark.implicits._

    val df = spark.read
      .option("header", "false")
      .schema(schema)
      .csv(csvDataFile)
      .drop("acceptedAnswer")

    println(s"\nCount of records in CSV file: ${df.count()}")
    df.printSchema()
    df.show(5)

    val highScorePosts = df
      .filter($"score" > 20)

    highScorePosts.show(5)

    df.createOrReplaceTempView("stackoverflow")

    val top5Scores = spark.sql(
      "SELECT id,score,tag FROM stackoverflow WHERE tag is not null ORDER BY score DESC LIMIT 5"
    )

    top5Scores.show()  
    
    val popularTags = spark.sql("""
      SELECT tag, COUNT(*) as tag_count
      FROM stackoverflow
      WHERE tag IS NOT NULL
      GROUP BY tag
      ORDER BY tag_count DESC
      LIMIT 10
    """)
    popularTags.show()

    val programElapsedTime = (System.nanoTime() - programStartTime) / 1e9
    println(s"\nProgram execution time: $programElapsedTime seconds")
    spark.stop()
  }
}
