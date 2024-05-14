package polytech.umontpellier.fr

import org.apache.spark.sql.SparkSession

object MongoDB_esteban_baron {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.read.connection.uri", "mongodb://root:my_secret_password@127.0.0.1")
      .config("spark.mongodb.write.connection.uri", "mongodb://root:my_secret_password@127.0.0.1")
      .master("local[*]")
      .getOrCreate()

    // Lire un élément de la collection "twitter" de la base de données "do5"
    val df = spark.read.format("mongodb").option("database", "do5").option("collection", "twitter").load()

    // tests
    df.show(1)

    spark.stop()
  }
}
