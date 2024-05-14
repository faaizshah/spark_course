ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.14"

val sparkVersion = "3.2.0"

lazy val root = (project in file("."))
  .settings(
    name := "SparkD05",
    idePackagePrefix := Some("polytech.umontpellier.fr")
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.mongodb.spark" %% "mongo-spark-connector" % "10.2.2",
)
