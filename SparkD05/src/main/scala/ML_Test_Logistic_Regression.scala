import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.LogisticRegression
object ML_Test_Logistic_Regression {
  def main(args: Array[String]): Unit = {

    val programStartTime = System.nanoTime()

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read.json("data/simple-ml.json")
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
//    val fittedRF = supervised.fit(df)
//    val preparedDF = fittedRF.transform(df)
//    preparedDF.show(5)
//
//    // Train the model
    val Array(train, test) = df.randomSplit(Array(0.7, 0.3))


    // Set stages
    val rForm = new RFormula()
    val lr = new LogisticRegression().setLabelCol("label").setFeaturesCol("features")

    // Create pipeline

    import org.apache.spark.ml.Pipeline
    val stages = Array(rForm, lr)
    val pipeline = new Pipeline().setStages(stages)

    // Building parameters
    import org.apache.spark.ml.tuning.ParamGridBuilder
    val params = new ParamGridBuilder()
      .addGrid(rForm.formula, Array(
        "lab ~ . + color:value1",
        "lab ~ . + color:value1 + color:value2"))
      .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
      .addGrid(lr.regParam, Array(0.1, 2.0))
      .build()

    // create evaluator Area Under Curve

    import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
    val evaluator = new BinaryClassificationEvaluator()
      .setMetricName("areaUnderROC")
      .setRawPredictionCol("prediction")
      .setLabelCol("label")

    // Cross validation -- Train validation split
    import org.apache.spark.ml.tuning.TrainValidationSplit
    val tvs = new TrainValidationSplit()
      .setTrainRatio(0.75) // also the default.
      .setEstimatorParamMaps(params)
      .setEstimator(pipeline)
      .setEvaluator(evaluator)


    val tvsFitted = tvs.fit(train)

   val result = evaluator.evaluate(tvsFitted.transform(test))

    println("result: " + result)



    import org.apache.spark.ml.PipelineModel
    import org.apache.spark.ml.classification.LogisticRegressionModel
    val trainedPipeline = tvsFitted.bestModel.asInstanceOf[PipelineModel]
    val TrainedLR = trainedPipeline.stages(1).asInstanceOf[LogisticRegressionModel]
    val summaryLR = TrainedLR.summary

    summaryLR.objectiveHistory


    val programElapsedTime = (System.nanoTime() - programStartTime) / 1e9
    println(s"\nProgram execution time: $programElapsedTime seconds")
    spark.stop()
  }
}

