package polytech.umontpellier.fr

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SimpleApp {
  def main(args: Array[String]): Unit = {

    val programStartTime = System.nanoTime()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val minioIP = sys.env("MINIO_SERVICE_HOST")
    val ACCESS_KEY = "admin"
    val SECRET_KEY = "do5password"
    val MINIO_ENDPOINT = s"http://$minioIP:9000"
    val connectionTimeOut = "600000"

    val spark = SparkSession.builder
      .appName("Simple Application")
    //  .master("local[*]")
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

    val sourceBucket = "sparkdo5"
    val logFileMinIO = s"s3a://$sourceBucket/SPARK_README.txt"
    val logData = spark.read.textFile(logFileMinIO).cache()
    println ("\nLines count:" +  logData.count())

    val numAs = logData
      .filter(line => line.contains("data"))
      .count()

    val numBs = logData
      .filter(line => line.contains("spark"))
      .count()

    println(s"\nLines with word data: $numAs, Lines with word spark: $numBs")

    val programElapsedTime = (System.nanoTime() - programStartTime) / 1e9
    println(s"\nProgram execution time: $programElapsedTime seconds")
    spark.stop()
  }
}

