ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.14"

lazy val root = (project in file("."))
  .settings(
    name := "SparkD05"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.1.1",
  "org.apache.spark" %% "spark-mllib" % "3.1.1",
  "org.mongodb.spark" %% "mongo-spark-connector" % "10.3.0",
  "org.apache.spark" %% "spark-core" % "3.1.1",
  "org.postgresql" % "postgresql" % "42.7.3"
)

assembly / assemblyMergeStrategy := {
  case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" =>
    MergeStrategy.concat
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "application.conf"            => MergeStrategy.concat
  case x                             => MergeStrategy.first
}
