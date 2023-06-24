package com.example
package readers

import org.apache.spark.sql.{DataFrame, SparkSession}

object CsvReader {
  case class Config(file: String, separator: Char = ',', hasHeader: Boolean = true, inferSchema: Boolean = false)

}

case class CsvReader(spark: SparkSession, config: CsvReader.Config) extends DataFrameReader{
  override def read(): DataFrame = {
    spark.read
     .option("inferSchema", config.inferSchema.toString)
      .option("header", config.hasHeader.toString.toLowerCase)
      .option("sep", config.separator.toString)
      .csv(config.file)
  }
}
