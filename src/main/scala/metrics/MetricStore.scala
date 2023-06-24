package com.example
package metrics

import jobs.SessionWrapper
import readers.CsvReader

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col

import org.apache.spark.sql.{Encoder, Encoders}
import java.sql.Timestamp

case class MetricStore(
                        metricName: String,
                        top: String,
                        order: String,
                        date: String,
                        dateFrom: String,
                        dateTo: String,
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
