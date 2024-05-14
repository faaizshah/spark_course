package polytech.umontpellier.fr

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.udf

object MongoDB_Yann_Pomie {
  def main(args: Array[String]): Unit = {

    val programStartTime = System.nanoTime()

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder
      .appName("MongoDB connector")
      .master("local[*]")
      .config("spark.mongodb.read.connection.uri", "mongodb://root:my_secret_password@127.0.0.1:27017")
      .config("spark.mongodb.write.connection.uri", "mongodb://root:my_secret_password@127.0.0.1:27017")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read
      .format("mongodb")
      .option("database", "do5")
      .option("collection", "X")
      .load()

    df.printSchema()
    df.select("id", "text").show(5)

    val threats = df.select("id", "username", "text").filter("text like \"%threat%\"")

    threats.show


    threats.write.format("mongodb").mode("overwrite")
      .option("database", "do5")
      .option("collection", "threats")
      .save()

    val programElapsedTime = (System.nanoTime() - programStartTime) / 1e9
    println(s"\nProgram execution time: $programElapsedTime seconds")
    spark.stop()
  }
}

