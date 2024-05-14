package polytech.umontpellier.fr

import org.apache.spark.sql.SparkSession

object assignement_02_esteban_baron {
  def main(args: Array[String]): Unit = {
    val programStartTime = System.nanoTime()

    val logFile = "data/wikipedia.dat"

    val spark = SparkSession.builder
      .appName("assignement_02_esteban_baron")
      .master("local[*]")
      .getOrCreate()

    val logData = spark.read.textFile(logFile).cache()

    // Total number of lines in the source file
    val totalLines = logData.count()
    println(s"Total number of lines in the source file: $totalLines")

    // Number of lines containing the word "France"
    val linesWithFrance = logData.filter(line => line.contains("France")).count()
    println(s"Number of lines containing the word 'France': $linesWithFrance")

    // Number of lines containing the word "Paris"
    val linesWithParis = logData.filter(line => line.contains("Paris")).count()
    println(s"Number of lines containing the word 'Paris': $linesWithParis")

    val programElapsedTime = (System.nanoTime() - programStartTime) / 1e9
    println(s"\nProgram execution time: $programElapsedTime seconds")
    spark.stop()
  }
}

