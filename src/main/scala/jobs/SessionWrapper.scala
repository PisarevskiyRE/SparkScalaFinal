package com.example
package jobs

import org.apache.spark.sql.SparkSession

trait SessionWrapper {
  lazy val spark: SparkSession = SparkSession
    .builder()
    .appName("Spark App")
    .config("spark.master", "local[*]")
    .getOrCreate()
}
