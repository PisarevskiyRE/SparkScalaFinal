package com.example
package metrics

import readers.CsvReader
import transformers.DataFrameOps.getNotCalculateDF

import org.apache.spark.sql.{DataFrame, functions}

import java.nio.file.{Files, Paths}
import java.sql.Timestamp
import java.time.LocalDateTime


class TopAirportsByFlights(flights: DataFrame,
                           airports: DataFrame,
                           airlines: DataFrame,
                           curretnMetrciStore: MetricStore) extends Metric {

  val flightsCols = Seq("ORIGIN_AIRPORT")
  val airportsCols = Seq("IATA_CODE", "AIRPORT")
  val airlinesCols = Seq()

  var newMetrciStore: MetricStore = null

  /*
    0 проверка полей
    1 фитровать по дате
    2 расчет метрики
    3 merge результата (загрузка прошлой+новая)
    4 сохранение результата + обновление MetricStore
   */

  // проверка необходимых полей
  def checkCols(): Unit = {
    validateColumnPresence(flightsCols)(flights)
    validateColumnPresence(airportsCols)(airports)
  }

  // фильтруем DF по дате
  def filterOnDate(): DataFrame ={
    getNotCalculateDF(flights, curretnMetrciStore)
  }

  def getMetric(filteredOnDate: DataFrame): DataFrame = {

    val fromDate = filteredOnDate.select("MinDate").first().getTimestamp(0)
    val toDate = filteredOnDate.select("MaxDate").first().getTimestamp(0)


    val popularAirportsDF = filteredOnDate
      .groupBy("ORIGIN_AIRPORT")
      .count()
      .join(airports, filteredOnDate("ORIGIN_AIRPORT") === airports("IATA_CODE"))
      .select("AIRPORT", "count")

    newMetrciStore = MetricStore(
      metricName = "TopAirportsByFlights",
      top = 10.toString,
      order = curretnMetrciStore.order.toString,
      date = Timestamp.valueOf(LocalDateTime.now()).toString,
      dateFrom = fromDate.toString,
      dateTo = toDate.toString,
      path = curretnMetrciStore.path,
      pathAll = curretnMetrciStore.pathAll)

    popularAirportsDF
  }

  def mergeMetric(newMetric: DataFrame, metrciStore: MetricStore): Option[DataFrame] ={
    //достаем старый результат

    val projectDir = System.getProperty("user.dir")
    val relativePath = curretnMetrciStore.path
    val filePath = Paths.get(projectDir, relativePath).toString


    if (Files.exists(Paths.get(filePath))) {

      val oldMetric = CsvReader(
        spark,
        CsvReader.Config(file = curretnMetrciStore.path)
      ).read()

      // объединяем старый и новый результат
      Some(oldMetric.union(newMetric).groupBy("AIRPORT").agg(functions.sum("count").as("count")))
    }
    else None
  }

  def requiredMetric(metric: DataFrame): DataFrame = {

    metric.createOrReplaceTempView("tempView")

    spark.sql(s" select AIRPORT, count from tempView order by count ${curretnMetrciStore.order}")
            .limit(curretnMetrciStore.top.toInt)
  }


  def calculate(): (DataFrame, DataFrame, MetricStore) ={
      checkCols()
      val filteredOnDate = filterOnDate()

      val metric = getMetric(filteredOnDate)

      val resultAll: Option[DataFrame] = mergeMetric(metric, curretnMetrciStore)

      resultAll match {
        case Some(df) => {
          val resultRequired = requiredMetric(df)
          (df, resultRequired, newMetrciStore)
        }
        case None => {
          val resultRequired = requiredMetric(metric)
          (metric, resultRequired, newMetrciStore)
        }
      }
  }
}

object TopAirportsByFlights {
  def apply(flights: DataFrame,
            airports: DataFrame,
            airlines: DataFrame,
            currentStore: MetricStore) = new TopAirportsByFlights(flights, airports, airlines, currentStore)
}
