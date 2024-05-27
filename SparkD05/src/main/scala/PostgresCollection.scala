import org.apache.spark.sql.SparkSession

object PostgresCollection {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Spark PostgreSQL Integration")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .master("local[*]")
      .getOrCreate()

    // Configuration of JDBC connection
    val jdbcUrl = "jdbc:postgresql://localhost:5432/postgres"
    val connectionProperties = new java.util.Properties()
    connectionProperties.put("user", "postgres")
    connectionProperties.put("password", "password")
    connectionProperties.put("driver", "org.postgresql.Driver")


    // Read data from PostgreSQL
    // The data used here is from the 'periodic_table' table in the PostgreSQL database.
    // The SQL script to create and populate this table can be found at:
    // https://github.com/neondatabase/postgres-sample-dbs/blob/main/periodic_table.sql
    val periodicTableDF = spark.read
      .jdbc(jdbcUrl, "public.periodic_table", connectionProperties)

    // Row count in the DataFrame
    val rowCount = periodicTableDF.count()

    println(s"Number of rows in periodic table: $rowCount")

    spark.stop()
  }

}