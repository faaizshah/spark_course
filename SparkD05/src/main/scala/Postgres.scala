package polytech.umontpellier.fr

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Postgres {
  def main(args: Array[String]): Unit = {

    val programStartTime = System.nanoTime()

    Logger.getLogger("org").setLevel(Level.ERROR)
    val minioIP = sys.env("MINIO_SERVICE_HOST")
    val ACCESS_KEY = "admin"
    val SECRET_KEY = "do5password"
    val MINIO_ENDPOINT = s"http://$minioIP:9000"
    val connectionTimeOut = "600000"

    val spark = SparkSession.builder
      .appName("Postgres Application")
      .config("spark.driver.memory", "8G")
      .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
      .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
      .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
      .config("spark.hadoop.fs.s3a.path.style.access", value = true)
      .config("fs.s3a.connection.ssl.enabled", value = true)
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("fs.s3a.connection.timeout", connectionTimeOut)
      .config("spark.sql.codegen.wholeStage", value = false)
      // .master("local[*]")
      .getOrCreate()

      val jdbcUrl = "jdbc:postgresql://192.168.194.92:5432/lego"
      val connectionProperties = new java.util.Properties()
      connectionProperties.setProperty("user", "user")
      connectionProperties.setProperty("password", "password")
      connectionProperties.setProperty("driver", "org.postgresql.Driver")

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read.jdbc(jdbcUrl, "lego_sets", connectionProperties)
    df.show(5)

    println(s"\nNumber of rows in the table: ${df.count()}")

    val oldestSet = df.orderBy("year").first()
    println(s"\nOldest set: ${oldestSet}")

    val programElapsedTime = (System.nanoTime() - programStartTime) / 1e9
    println(s"\nProgram execution time: $programElapsedTime seconds")
    spark.stop()
  }
}

