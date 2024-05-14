ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
    name := "traitement_distribues_tp"
  )
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.1"