package polytech.umontpellier.fr

import org.apache.spark.sql.SparkSession

object MongoDbCollection {

  ²def main(args: Array[String]) = {
    val MONGODB_URI = "mongodb://root:my_secret_password@localhost:2707"
    val spark = SparkSession.builder().appName("Connection test").config("spark.mongodb.read.connection.uri", MONGODB_URI).config("spark.mongodb.write.connection.uri", MONGODB_URI).getOrCreate()

    spark.sparkContext.setLogLevel("DEBUG")
    val df = spark.read.format("mongodb").option("database", "Spark").option("collection", "tweets").load()

    println(df.count())

    val found = df.filter(df("text").contains("threat"))

    found.write
      .format("mongodb")
      .mode("overwrite")
      .option("database", "Spark")
      .option("collection", "tweets")
      .save()

    println(s"${found.count()} Tweets contains threat word")
  }

}
