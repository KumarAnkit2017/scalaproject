ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided"


lazy val root = (project in file("."))
  .settings(
    name := "scalaproject",

  )
