package polytech.umontpellier.fr

import org.apache.spark.ml.linalg.Vectors
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object MachineLearning {
    def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("ML Application")
      .config("spark.driver.memory", "8G")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val denseVec = Vectors.dense(1.0, 2.0, 3.0)
    val size = 3
    val idx = Array(1,2) // locations of non-zero elements in vector
    val values = Array(2.0,3.0)
    val sparseVec = Vectors.sparse(size, idx, values)
    sparseVec.toDense
    denseVec.toSparse
  
    var df = spark.read.json("data/simple-ml")
    df.orderBy("value2").show()

    import org.apache.spark.ml.feature.RFormula
    val supervised = new RFormula()
    .setFormula("lab ~ . + color:value1 + color:value2")

    val fittedRF = supervised.fit(df)
    val preparedDF = fittedRF.transform(df)
    preparedDF.show()
  }
}
