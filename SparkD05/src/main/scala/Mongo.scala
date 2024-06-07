package polytech.umontpellier.fr

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object MongoDBConnection {
  def main(args: Array[String]): Unit = {

    val programStartTime = System.nanoTime()
    Logger.getLogger("org").setLevel(Level.ERROR)

    // val MONGODB_URI = "mongodb://root:do5password@localhost:27017"
    val MONGODB_URI =
      "mongodb://root:do5password@mongodb-0.mongodb-headless.spark-do5.svc.cluster.local:27017"
    // Creating Spark Session
    val spark = SparkSession.builder
      .appName("Mongodb Connection Test")
      .config("spark.mongodb.read.connection.uri", MONGODB_URI)
      .config("spark.mongodb.write.connection.uri", MONGODB_URI)
      //    .master("local[*]")
      .getOrCreate()

    // Set log level to Error
    spark.sparkContext.setLogLevel("ERROR")
    val df = spark.read
      .format("mongodb")
      .option("database", "test")
      .option("collection", "X")
      .load()

    println("\n" + df.count)
    df.printSchema()
    df.select("id", "text").show(5)

    val df1 = df.where(col("text").contains("threat"))
    println(df1.count())
    df1.select("text").show(5, truncate = false)

    val programElapsedTime = (System.nanoTime() - programStartTime) / 1e9
    println(s"\nProgram execution time: $programElapsedTime seconds")
    spark.stop()
  }
}
