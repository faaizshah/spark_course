package polytech.umontpellier.fr

import org.apache.spark.sql.SparkSession

object Mongo {
  def main(args: Array[String]): Unit = {
    val MONGODB_URI = "mongodb://root:my_secret_password@localhost:27017"

    // Creating Spark Session
    val spark = SparkSession.builder
      .appName("Mongodb Connection Test")
      .config("spark.mongodb.read.connection.uri", MONGODB_URI)
      .config("spark.mongodb.write.connection.uri", MONGODB_URI)
      .master("local[*]")
      .getOrCreate()

    // Set log level to Error
    spark.sparkContext.setLogLevel("ERROR")
    val df = spark.read
      .format("mongodb")
      .option("database", "do5")
      .option("collection", "X")
      .load()

    println("\n" + df.count)
    df.printSchema()
    df.select("id", "text").show(5)

  }
}
