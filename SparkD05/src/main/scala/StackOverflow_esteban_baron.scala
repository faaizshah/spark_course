package polytech.umontpellier.fr

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object StackOverflow_esteban_baron {
  def main(args: Array[String]): Unit = {

    val programStartTime = System.nanoTime()

    Logger.getLogger("org").setLevel(Level.ERROR)

    val csvDataFile = "data/stackoverflow.csv"

    val spark = SparkSession.builder
      .appName("Stackoverflow Application")
      .config("spark.driver.memory", "8G")
      .master("local[*]")
      .getOrCreate()

    val schema = new StructType()
      .add("postTypeId", IntegerType, nullable = true)
      .add("id", IntegerType, nullable = true)
      .add("acceptedAnswer", StringType, nullable = true)
      .add("parentId", IntegerType, nullable = true)
      .add("score", IntegerType, nullable = true)
      .add("tag", StringType, nullable = true)

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read
      .option("header", "false")
      .schema(schema)
      .csv(csvDataFile)
      .drop("acceptedAnswer")

    println(s"\nCount of records in CSV file: ${df.count()}")
    df.printSchema()
    df.show(5)

    // import spark.implicits._
    // println("Count acceptedAnswer null: "+ df.filter(col("acceptedAnswer").isNull).count()
    //  + "\nCount tag null: "+ df.filter(col("tag").isNull).count()
    //  + "\nCount parentId null: "+ df.filter(col("parentId").isNull).count() )

    // Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("stackoverflow")

    // Query 1: Top 5 highest scores
    val top5Scores = spark.sql("SELECT id, score FROM stackoverflow ORDER BY score DESC LIMIT 5")
    top5Scores.show()

    // Query 2: Total number of records
    val totalRecords = spark.sql("SELECT COUNT(*) AS total_records FROM stackoverflow")
    totalRecords.show()

    // Query 3: Average score
    val averageScore = spark.sql("SELECT AVG(score) AS average_score FROM stackoverflow")
    averageScore.show()

    // Query 4: Minimum score
    val minScore = spark.sql("SELECT MIN(score) AS min_score FROM stackoverflow")
    minScore.show()
    
    // Filter posts with a score greater than 20
    //    val highScorePosts = df
    //      .filter(col("score") > 20)
    //    highScorePosts.show(5)

    val programElapsedTime = (System.nanoTime() - programStartTime) / 1e9
    println(s"\nProgram execution time: $programElapsedTime seconds")
    spark.stop()
  }
}

