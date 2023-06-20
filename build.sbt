ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.11"

lazy val root = (project in file("."))
  .settings(
    name := "SparkScalaFinal"
  )


lazy val sparkVersion = "3.3.2"
lazy val circeVersion = "0.14.5"


libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  "org.apache.logging.log4j" % "log4j" % "2.20.0",
)

idePackagePrefix := Some("com.example")


// боремся с распределенными результатами...
libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.10"