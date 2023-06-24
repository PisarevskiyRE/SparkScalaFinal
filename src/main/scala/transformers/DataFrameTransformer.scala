package com.example
package transformers

import com.example.metrics.MetricStore
import org.apache.spark.sql.DataFrame

trait DataFrameTransformer {
  def getNotCalculateDF(inputDF: DataFrame, store: MetricStore): DataFrame
}
