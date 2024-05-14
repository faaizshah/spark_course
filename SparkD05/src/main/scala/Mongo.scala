package polytech.umontpellier.fr

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.col
import org.apache.hadoop.shaded.org.checkerframework.checker.units.qual.h

object Mongo {
  def main(args: Array[String]): Unit = {

    val programStartTime = System.nanoTime()

    Logger.getLogger("org").setLevel(Level.ERROR)

    import org.apache.spark.sql.SparkSession
    val spark = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.read.connection.uri", "mongodb://root:my_secret_password@localhost:27017")
      .config("spark.mongodb.write.connection.uri", "mongodb://root:my_secret_password@localhost:27017")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read
      .format("mongodb")
      .option("database", "spark")
      .option("collection", "spark")
      .load()

    println(s"Count of records in MongoDB collection: ${df.count()}")
    df.printSchema()
    df.show(5)

    println(s"Count of records where the number of followers >1000: ${df.filter(col("followers") > 1000).count()}")
    println(s"Count of records where the number of followers <1000: ${df.filter(col("followers") < 1000).count()}")

    val programElapsedTime = (System.nanoTime() - programStartTime) / 1e9
    println(s"\nProgram execution time: $programElapsedTime seconds")
    spark.stop()
  }
}

