ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.14"

val sparkVersion = "3.2.0"
val mongodbSparkConnectorVersion = "10.2.2"
val mysqlVersion = "8.0.30"
val kafkaVer = "3.1.0"
val awsJavaSdkBundleVersionForSpark= "1.11.271"
val minioSparkSelectVersion = "2.1"

lazy val root = (project in file("."))
  .settings(
    name := "SparkD05",
    idePackagePrefix := Some("polytech.umontpellier.fr"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-graphx" % sparkVersion % "provided",
      "org.apache.spark" % "spark-streaming_2.12" % sparkVersion % "provided",
      "org.apache.spark" % "spark-streaming-kafka-0-10_2.12" % sparkVersion % "provided",
      "org.mongodb.spark" %% "mongo-spark-connector" % mongodbSparkConnectorVersion % "provided",
      "org.apache.kafka" %% "kafka-streams-scala" % kafkaVer % "provided",
      "org.apache.hadoop" % "hadoop-client" % sparkVersion % "provided",
      "org.apache.hadoop" % "hadoop-aws" % sparkVersion % "provided",
      "org.apache.hadoop" % "hadoop-common" %sparkVersion % "provided",
      "mysql" % "mysql-connector-java" % mysqlVersion % "provided",
      "io.minio" % "spark-select_2.11" % minioSparkSelectVersion % "provided",
      "com.amazonaws" % "aws-java-sdk-bundle" % "1.11.271" % "provided",
      "com.databricks" %% "spark-xml" % "0.18.0" % "provided"
    )
  )

// sbt-assembly settings to create jar of project
// Reference: https://github.com/sbt/sbt-assembly

assembly / assemblyMergeStrategy := {
  case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case "application.conf" => MergeStrategy.concat
  case x => MergeStrategy.first
}


