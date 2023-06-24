package com.example
package metrics

import readers.CsvReader
import transformers.DataFrameOps.getNotCalculateDF

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, dense_rank, desc}
import org.apache.spark.sql.{DataFrame, functions}

import java.nio.file.{Files, Paths}
import java.sql.Timestamp
import java.time.LocalDateTime


class OnTimeFlights(flights: DataFrame,
                           airports: DataFrame,
                           airlines: DataFrame,
                           curretnMetrciStore: MetricStore) extends Metric {

  val flightsCols = Seq("DEPARTURE_DELAY","ORIGIN_AIRPORT", "AIRLINE", "DESTINATION_AIRPORT")
  val airportsCols = Seq("IATA_CODE", "AIRPORT")
  val airlinesCols = Seq("IATA_CODE", "AIRLINE")

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


    val onTimeFlights = flights
      .filter(flights("DEPARTURE_DELAY") === 0)
      .groupBy("ORIGIN_AIRPORT", "AIRLINE", "DESTINATION_AIRPORT")
      .count()

    val topCarriersByAirport = onTimeFlights
      .withColumn("rank1", dense_rank().over(Window.partitionBy("ORIGIN_AIRPORT").orderBy(desc("count"))))

    val topDestAirportsByAirport = onTimeFlights
      .withColumn("rank2", dense_rank().over(Window.partitionBy("DESTINATION_AIRPORT").orderBy(desc("count"))))


    val topCarriersByAirportResult = topCarriersByAirport.join(airlines, topCarriersByAirport("AIRLINE") === airlines("IATA_CODE"))
    val topDestAirportsByAirportResult = topDestAirportsByAirport.join(airports, topDestAirportsByAirport("DESTINATION_AIRPORT") === airports("IATA_CODE"))

    val result = topCarriersByAirportResult.join(topDestAirportsByAirportResult.as("t1"), Seq("ORIGIN_AIRPORT"), "inner")
      .select("ORIGIN_AIRPORT", "t1.AIRLINE", "t1.DESTINATION_AIRPORT", "rank1", "t1.rank2")

    newMetrciStore = MetricStore(
      metricName = "OnTimeFlights",
      top = 10.toString,
      order = curretnMetrciStore.order,
      date = Timestamp.valueOf(LocalDateTime.now()).toString,
      dateFrom = fromDate.toString,
      dateTo = toDate.toString,
      path = curretnMetrciStore.path,
      pathAll = curretnMetrciStore.pathAll)

    result
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
      Some(oldMetric.union(newMetric).groupBy("ORIGIN_AIRPORT", "AIRLINE", "DESTINATION_AIRPORT")
        .agg(
          functions.sum("rank1").as("rank1"),
          functions.sum("rank2").as("rank2")))
    }
    else None
  }

  def requiredMetric(metric: DataFrame): DataFrame = {

    metric.createOrReplaceTempView("tempView")

    spark.sql(s" select ORIGIN_AIRPORT, AIRLINE, DESTINATION_AIRPORT, rank1, rank2 from tempView order by rank1 ${curretnMetrciStore.order}, rank2 ${curretnMetrciStore.order}")
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

object OnTimeFlights {
  def apply(flights: DataFrame,
            airports: DataFrame,
            airlines: DataFrame,
            currentStore: MetricStore) = new OnTimeFlights(flights, airports, airlines, currentStore)
}
