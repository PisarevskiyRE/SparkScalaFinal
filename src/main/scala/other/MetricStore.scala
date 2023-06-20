package com.example
package other

import com.example.jobs.SessionWrapper
import com.example.readers.CsvReader
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col


import java.sql.{Date, Timestamp}

case class MetricStore(
                        metricName: String,
                        top: Int,
                        order: String,
                        date: Timestamp,
                        dateFrom: Timestamp,
                        dateTo: Timestamp,
                        path: String,
                        pathAll: String
                      )

object MetricStore extends SessionWrapper{
  import spark.implicits._

  def getInitStore(storePath: String): Dataset[MetricStore] = CsvReader(
    spark,
    CsvReader.Config(file = storePath, inferSchema = true)
  ).read().as[MetricStore]


  def getMetricStoreByName(ds: Dataset[MetricStore], name: String): MetricStore = {
    ds.filter(col("metricName") === name).first()
  }

}
