import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object PostgresSpark {
  def main(args: Array[String]): Unit = {

    val programStartTime = System.nanoTime()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val jdbcHostname = "my-postgresql"
    val jdbcPort = 5432
    val jdbcDatabase = "postgres"
    val jdbcUsername = "postgres"
    val jdbcPassword = "postgres"

    val jdbcUrl =
      s"jdbc:postgresql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}"

    val connectionProperties = new java.util.Properties()
    connectionProperties.put("user", jdbcUsername)
    connectionProperties.put("password", jdbcPassword)
    connectionProperties.put("driver", "org.postgresql.Driver")

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
      .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
      )
      .config("fs.s3a.connection.timeout", connectionTimeOut)
      .config("spark.sql.codegen.wholeStage", value = false)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val writeTableName = "stackoverflow"

    // val csvDataFile = "data/stackoverflow.csv"

    val sourceBucket = "sparkdo5"
    val csvDataFileMinIO = s"s3a://$sourceBucket/stackoverflow.csv"

    val df = spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      .csv(csvDataFileMinIO)

    println(s"\nCount of records in CSV file: ${df.count()}")
    df.printSchema()
    df.show(5)

    df.write
      .mode("append")
      .jdbc(jdbcUrl, writeTableName, connectionProperties)

    val newdf = spark.read
      .jdbc(jdbcUrl, writeTableName, connectionProperties)

    println(newdf.count())
    newdf.show(5)

    val programElapsedTime = (System.nanoTime() - programStartTime) / 1e9
    println(s"\nProgram execution time: $programElapsedTime seconds")
    spark.stop()
  }
}
