package polytech.umontpellier.fr

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.postgresql.Driver

import java.util.Properties

object PostgreSQL {
  def main(args: Array[String]): Unit = {

    val programStartTime = System.nanoTime()
    Logger.getLogger("org").setLevel(Level.ERROR)

    // val MONGODB_URI = "mongodb://root:do5password@localhost:27017"
    val POSTGRESQL_URI =
      "jdbc:postgresql://postgresql.spark-do5.svc.cluster.local:5432/postgres"
    // Creating Spark Session
    val spark = SparkSession.builder
      .appName("PostgreSQL Connection Test")
      .config("spark.postgresql.read.connection.uri", POSTGRESQL_URI)
      .config("spark.postgresql.write.connection.uri", POSTGRESQL_URI)
//      .master("local[*]")
      .getOrCreate()

    val properties = new Properties();
    properties.setProperty("Driver", "org.postgresql.Driver");
    properties.setProperty("user", "postgres");
    properties.setProperty("password", "postgres");

    // Set log level to Error
    spark.sparkContext.setLogLevel("ERROR")
    val df = spark.read
      .jdbc(POSTGRESQL_URI, "public.suppliers", properties);

    println("\n" + df.count)
    df.printSchema()
    df.select("supplier_id", "company_name").show(5)

    val df1 = df.where(col("country").contains("France"))
    println("Suppliers in France: ")
    println(df1.count())

    val programElapsedTime = (System.nanoTime() - programStartTime) / 1e9
    println(s"\nProgram execution time: $programElapsedTime seconds")
    spark.stop()
  }
}
