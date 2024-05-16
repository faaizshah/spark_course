import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.tuning.TrainValidationSplit
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.LogisticRegressionModel

object MLTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("local[*]")
      .getOrCreate()

    var df = spark.read.json("data/simple-ml.json")
    val supervised = new RFormula()
      .setFormula("lab ~ . + color:value1 + color:value2")

    val fittedRF = supervised.fit(df)
    val preparedDF = fittedRF.transform(df)
    preparedDF.show()
    val Array(train, test) = preparedDF.randomSplit(Array(0.7, 0.3))
    val rForm = new RFormula()
    val lr =
      new LogisticRegression().setLabelCol("label").setFeaturesCol("features")

    println(lr.explainParams())
    val fittedLR = lr.fit(train)

    fittedLR.transform(train).select("label", "prediction").show()
    val stages = Array(rForm, lr)
    val pipeline = new Pipeline().setStages(stages)

    val params = new ParamGridBuilder()
      .addGrid(
        rForm.formula,
        Array("lab ~ . + color:value1", "lab ~ . + color:value1 + color:value2")
      )
      .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
      .addGrid(lr.regParam, Array(0.1, 2.0))
      .build()

    val evaluator = new BinaryClassificationEvaluator()
      .setMetricName("areaUnderROC")
      .setRawPredictionCol("prediction")
      .setLabelCol("label")

    val tvs = new TrainValidationSplit()
      .setTrainRatio(0.75) // also the default.
      .setEstimatorParamMaps(params)
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
    val tvsFitted = tvs.fit(train)

    println(evaluator.evaluate(tvsFitted.transform(test)))

    val trainedPipeline = tvsFitted.bestModel.asInstanceOf[PipelineModel]
    val TrainedLR =
      trainedPipeline.stages(1).asInstanceOf[LogisticRegressionModel]
    val summaryLR = TrainedLR.summary
    println(summaryLR.objectiveHistory.mkString("Array(", ", ", ")"))

  }
}
