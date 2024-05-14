ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
    name := "traitement_distribues_tp"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.5.1",
  "org.mongodb.spark" %% "mongo-spark-connector" % "10.3.0",
  "org.apache.spark" %% "spark-core" % "3.5.1"
)
