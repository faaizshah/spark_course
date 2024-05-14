package polytech.umontpellier.fr
import org.apache.spark.sql.SparkSession

object Assignement_2 {
  def main(args: Array[String]): Unit = {
    val file = "./data/wikipedia.dat"

    // Create a Spark session
    val spark = SparkSession
      .builder()
      .appName("LineCounter")
      .master(
        "local[*]" // Execute locally with as many threads as logical cores
      )
      .getOrCreate()

    // Read the file
    val lines = spark.read.textFile(file).rdd

    // Count the number of lines
    val totalLines = lines.count()
    println(s"Total number of lines: $totalLines")

    // Count the number of lines containing "France"
    val franceLines = lines.filter(_.contains("France")).count()
    println(s"Number of lines containing 'France': $franceLines")

    // Count the number of lines containing "Paris"
    val parisLines = lines.filter(_.contains("Paris")).count()
    println(s"Number of lines containing 'Paris': $parisLines")

    // Stop the Spark session
    spark.stop()
  }
}
