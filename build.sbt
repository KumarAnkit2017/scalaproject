ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.4"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.4"

libraryDependencies += "org.apache.spark" %% "spark-avro" % "3.3.4"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.0" % "provided"


lazy val root = (project in file("."))
  .settings(
    name := "scalaproject",

  )
