ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "SparkScalaFinal"
  )

lazy val sparkVersion = "3.3.2"


libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.logging.log4j" % "log4j" % "2.20.0",
)

idePackagePrefix := Some("com.example")

