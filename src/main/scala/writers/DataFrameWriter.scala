package com.example
package writers

import org.apache.spark.sql.DataFrame

trait DataFrameWriter {

  def write(df: DataFrame, outputPath: String): Unit
}