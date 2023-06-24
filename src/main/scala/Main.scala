package com.example

import org.apache.log4j.{Level, LogManager, Logger}
import jobs._
import jobs.Reader._
import jobs.Writer._


object FlightAnalyzer  extends SessionWrapper {

  def main(args: Array[String]): Unit = {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val job = Job(
    spark,
    JobConfig(
      ReadConfig(
        filePath = "тест",
        hasHeader = true,
        separator = ','),
      WriteConfig(
        outputFile = "тест",
        format = "Csv"),
      "src/main/resources/file_path.csv",
      //args(0),
      "src/main/resources/metrics_store.csv"
      //args(1)
      )
  )
  job.run()
  }
}
