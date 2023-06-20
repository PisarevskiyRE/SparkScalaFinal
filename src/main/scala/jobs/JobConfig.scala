package com.example
package jobs

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col


object Reader {
  case class ReadConfig(
                         filePath: String,
                         hasHeader: Boolean = true,
                         separator: Char = ',')
}

object Writer {
  case class WriteConfig(
                          outputFile: String,
                          format: String = "parquet")
}

case class JobConfig  (
                      readerConfig: Reader.ReadConfig,
                      writerConfig: Writer.WriteConfig,
                      configPath: String,
                      storePath: String
                    )
object JobConfig{

  def getPathByName(df: DataFrame, name: String): String = df
    .select("path")
    .filter(col("name") === name)
    .first()
    .getString(0)
}

