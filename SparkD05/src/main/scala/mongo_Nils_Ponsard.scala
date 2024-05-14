object Mongo {
  def main(args: Array[String]): Unit = {
    /* Create the SparkSession.
     * If config arguments are passed from the command line using --conf,
     * parse args for the values to set.
     */
    import org.apache.spark.sql.SparkSession

    val url = "mongodb://root:password@localhost:27017"

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.read.connection.uri", url)
      .config("spark.mongodb.write.connection.uri", url)
      .getOrCreate()

    val df = spark.read
      .format("mongodb")
      .option("uri", url)
      .option("database", "do5")
      .option("collection", "twitter")
      .load()

//    val limited = df.drop("_id").limit(500)

    val found = df.filter(df("text").contains("threat"))
    found.write
      .format("mongodb")
      .mode("overwrite")
      .option("database", "do5")
      .option("collection", "twitter-threats")
      .save()
    println(s"Number of tweets containing the word 'threat': ${found.count()}")
  }
}
