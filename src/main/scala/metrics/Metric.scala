package com.example
package metrics

import com.example.jobs.SessionWrapper
import com.example.other.{DfValidator, MetricStore}
import org.apache.spark.sql.DataFrame

trait Metric extends SessionWrapper with DfValidator {
  /*
    0 проверка полей
    1 фитровать по дате
    2 расчет метрики
    3 merge результата (загрузка прошлой+новая)
    4 сохранение результата + обновление MetricStore
   */
  val flightsCols: Seq[String]
  val airportsCols: Seq[String]
  val airlinesCols: Seq[String]

  var newMetrciStore: MetricStore

  // проверка необходимых полей
  def checkCols(): Unit

  // фильтруем DF по дате
  def filterOnDate(): DataFrame

  def getMetric(filteredOnDate: DataFrame): DataFrame

  def mergeMetric(newMetric: DataFrame, metrciStore: MetricStore): Option[DataFrame]

  def requiredMetric(metric: DataFrame): DataFrame

  def calculate(): (DataFrame, DataFrame, MetricStore)

}
