import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkLearn {
  def main(args: Array[String]): Unit = {
    val programStartTime = System.nanoTime()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val jdbcHostname = "my-postgresql"
    val jdbcPort = 5432


    val jdbcUrl =
      s"jdbc:postgresql://postgres-service:5432/postgres"

    val connectionProperties = new java.util.Properties()
    connectionProperties.put("user", "postgres")
    connectionProperties.put("password", "postgres")
    connectionProperties.put("driver", "org.postgresql.Driver")

    val minioIP = sys.env("MINIO_SERVICE_HOST")
    val ACCESS_KEY = "admin"
    val SECRET_KEY = "do5password"
    val MINIO_ENDPOINT = s"http://$minioIP:9000"
    val connectionTimeOut = "600000"

    val spark = SparkSession.builder
      .appName("Spark PostgreSQL Integration")
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
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Lire les données depuis la table actor
    val actorDataFrame = spark.read
      .jdbc(jdbcUrl, "actor", connectionProperties)

    // Afficher les 5 premières lignes de la table actor
    println("First 5 rows of actor table:")
    actorDataFrame.show(5)

    // Compter les lignes dans la table actor
    val rowCount = actorDataFrame.count()
    println(s"Number of rows in actor table: $rowCount")

    val programElapsedTime = (System.nanoTime() - programStartTime) / 1e9
    println(s"\nProgram execution time: $programElapsedTime seconds")

    spark.stop()
  }
}
