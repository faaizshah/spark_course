package polytech.umontpellier.fr

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object StackOverflow_Yann_Pomie {
  def main(args: Array[String]): Unit = {

    val programStartTime = System.nanoTime()

    Logger.getLogger("org").setLevel(Level.ERROR)

    val csvDataFile = "data/stackoverflow.csv"

    val spark = SparkSession.builder
      .appName("Stackoverflow Application")
      .config("spark.driver.memory", "8G")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val schema = new StructType()
      .add("postTypeId", IntegerType, nullable = true)
      .add("id", IntegerType, nullable = true)
      .add("acceptedAnswer", StringType, nullable = true)
      .add("parentId", IntegerType, nullable = true)
      .add("score", IntegerType, nullable = true)
      .add("tag", StringType, nullable = true)

    val df = spark.read
      .option("header", "false")
      .schema(schema)
      //.option("inferSchema", "true")
      .csv(csvDataFile)
      .drop("acceptedAnswer")


    println(s"\nCount of records in CSV file: ${df.count()}")
    df.printSchema()
    df.show(5)

    import spark.implicits._

    println("Count acceptedAnswer null: "+ df.filter($"acceptedAnswer".isNull).count()
      + "\nCount tag null: "+ df.filter($"tag".isNull).count()
      + "\nCount parentId null: "+ df.filter($"parentId".isNull).count() )


    val highScorePosts = df
      .filter($"score" > 20)

    highScorePosts.sort($"score".desc).show(20)


    val programElapsedTime = (System.nanoTime() - programStartTime) / 1e9
    println(s"\nProgram execution time: $programElapsedTime seconds")
    spark.stop()
  }
}

