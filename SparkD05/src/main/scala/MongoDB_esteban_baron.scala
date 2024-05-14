package polytech.umontpellier.fr

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

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

    // Afficher un élément de la dataframe
    df.show(1)

    // Mettre tous les champs tweet_count égaux à 1
    val dfUpdated = df.withColumn("tweet_count", lit(1))

    // Écrire les données mises à jour dans MongoDB
    dfUpdated.write.format("mongodb").option("database", "do5").option("collection", "twitter_updated").mode("append").save()

    spark.stop()
  }
}
