import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object StackOverflowDataAnalysisOnKube {
  def main(args: Array[String]): Unit = {

    val programStartTime = System.nanoTime()
    Logger.getLogger("org").setLevel(Level.ERROR)


    val minioIP = sys.env("MINIO_SERVICE_HOST")
    val ACCESS_KEY = "admin"
    val SECRET_KEY = "do5password"
    val MINIO_ENDPOINT = s"http://$minioIP:9000"
    val connectionTimeOut = "600000"

    val spark = SparkSession.builder
      .appName("Stackoverflow Application")
      .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
      .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
      .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
      .config("spark.hadoop.fs.s3a.path.style.access", value = true)
      .config("fs.s3a.connection.ssl.enabled", value = true)
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("fs.s3a.connection.timeout", connectionTimeOut)
      .config("spark.sql.codegen.wholeStage", value = false)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


    //val csvDataFile = "data/stackoverflow.csv"

    val sourceBucket = "sparkdo5"
    val csvDataFileMinIO = s"s3a://$sourceBucket/stackoverflow.csv"

    val df = spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      .csv(csvDataFileMinIO)

    println(s"\nCount of records in CSV file: ${df.count()}")
    df.printSchema()
    df.show(5)

    val schema = new StructType()
      .add("postTypeId", IntegerType, nullable = true)
      .add("id", IntegerType, nullable = true)
      .add("acceptedAnswer", StringType, nullable = true)
      .add("parentId", IntegerType, nullable = true)
      .add("score", IntegerType, nullable = true)
      .add("tag", StringType, nullable = true)

    import spark.implicits._

    // Read the CSV file
    val df1 = spark.read
      .option("header", "false")
      .schema(schema)
      .csv(csvDataFileMinIO)
      .drop($"acceptedAnswer")


    println(s"Count of records: ${df1.count()}")


    //
    //    println("Count acceptedAnswer null: "+ df.filter($"acceptedAnswer".isNull).count()
    //      + "\nCount tag null: "+ df.filter($"tag".isNull).count()
    //      + "\nCount parentId null: "+ df.filter($"parentId".isNull).count() )

    // Filter posts with a score greater than 10
    val highScorePosts = df1.filter(col("score") > 10)
    highScorePosts.show(5)

    // Register the DataFrame as a SQL temporary view
    df1.createOrReplaceTempView("stackoverflow")

    // Query 1: Top 5 highest scores
    val top5Scores = spark.sql("SELECT id, score FROM stackoverflow ORDER BY score DESC LIMIT 5")
    top5Scores.show()




    val top5ScoresWithTag = spark.sql("""
        SELECT id, score, tag
        FROM stackoverflow
        WHERE tag IS NOT NULL AND TRIM(tag) <> ''
        ORDER BY score DESC
        LIMIT 5
      """)
    top5ScoresWithTag.show()

    // Query 3: Top 5 highest scores with accepted answer
    // Query: Top 5 highest scoring answers
    val top5ScoringAnswers = spark.sql("""
        SELECT id, score, parentId
        FROM stackoverflow
        WHERE parentId IS NOT NULL
        ORDER BY score DESC
        LIMIT 5
      """)
    top5ScoringAnswers.show()


    // Query: Most frequently used tags
    val popularTags = spark.sql("""
      SELECT tag, COUNT(*) as frequency
      FROM stackoverflow
      WHERE tag IS NOT NULL
      GROUP BY tag
      ORDER BY frequency DESC
      LIMIT 10
    """)
    popularTags.show()



    val programElapsedTime = (System.nanoTime() - programStartTime) / 1e9
    println(s"\nProgram execution time: $programElapsedTime seconds")
    spark.stop()
  }
}

