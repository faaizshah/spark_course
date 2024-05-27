package polytech.umontpellier.fr

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object MachineLearning {
  def main(args: Array[String]): Unit = {

    val programStartTime = System.nanoTime()

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read.json("data/simple-ml")
    println(df.count())
    df.orderBy("value2").show(5)

    // Specifying RF: We’re using the RFormula transformer because it makes performing several transformations extremely
    //easy to do.
    import org.apache.spark.ml.feature.RFormula
    val supervised = new RFormula()
      .setFormula("lab ~ . + color:value1 + color:value2")

    /* *
    The next step is to fit the RFormula transformer to the data to let it
    discover the possible values of each column. Not all transformers have this requirement but
    because RFormula will automatically handle categorical variables for us, it needs to determine
    which columns are categorical and which are not, as well as what the distinct values of the
    categorical columns are. For this reason, we have to call the fit method. Once we call fit, it
    returns a “trained” version of our transformer we can then use to actually transform our data.
    * */
    val fittedRF = supervised.fit(df)
    val preparedDF = fittedRF.transform(df)
    preparedDF.show(5)

    // Train the model
    val Array(train, test) = preparedDF.randomSplit(Array(0.7, 0.3))

    import org.apache.spark.ml.classification.LogisticRegression
    val lr = new LogisticRegression().setLabelCol("label").setFeaturesCol("features")

    //println(lr.explainParams())

    val fittedLR = lr.fit(train)

    fittedLR.transform(train).select("label", "prediction").show(5)


    val programElapsedTime = (System.nanoTime() - programStartTime) / 1e9
    println(s"\nProgram execution time: $programElapsedTime seconds")
    spark.stop()
  }
}
