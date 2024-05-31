import org.apache.spark.sql.SparkSession

object SparkFileAnalysis {
  def main(args: Array[String]): Unit = {
    val filePath = System.getProperty("user.dir") + "/data/wikipedia.dat" // path to the data file

    val spark = SparkSession.builder
      .appName("Spark File Analysis")
      .master("local[*]")
      .getOrCreate()

    val fileData = spark.read.textFile(filePath).cache()

    val totalLines = fileData.count()
    val linesWithFrance = fileData.filter(line => line.contains("France")).count()
    val linesWithParis = fileData.filter(line => line.contains("Paris")).count()

    println(s"Total number of lines: $totalLines")
    println(s"Number of lines containing 'France': $linesWithFrance")
    println(s"Number of lines containing 'Paris': $linesWithParis")

    spark.stop()
  }
}
