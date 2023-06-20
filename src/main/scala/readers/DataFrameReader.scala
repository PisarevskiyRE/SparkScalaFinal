package com.example
package readers

import org.apache.spark.sql.DataFrame

trait DataFrameReader {
  def read(): DataFrame
}
