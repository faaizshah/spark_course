package polytech.umontpellier.fr

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.col

object MySQL_esteban_baron {
  def main(args: Array[String]): Unit = {

    val programStartTime = System.nanoTime()
    Logger.getLogger("org").setLevel(Level.ERROR)

    // MySQL connection details
    val MYSQL_URL = "jdbc:mysql://localhost:3306/test"
    val MYSQL_USER = "root"
    val MYSQL_PASSWORD = "my_secret_password"

    // Creating Spark Session
    val spark = SparkSession.builder
      .appName("MySQL Connection Test")
      .master("local[*]")
      .getOrCreate()

    // Set log level to Error
    spark.sparkContext.setLogLevel("ERROR")

    // Reading data from MySQL
    val df = spark.read
      .format("jdbc")
      .option("url", MYSQL_URL)
      .option("dbtable", "X")
      .option("user", MYSQL_USER)
      .option("password", MYSQL_PASSWORD)
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
