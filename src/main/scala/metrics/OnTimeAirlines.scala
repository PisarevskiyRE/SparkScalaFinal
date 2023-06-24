package com.example
package metrics

import com.example.readers.CsvReader
import com.example.transformers.DataFrameOps
import org.apache.spark.sql.{DataFrame, functions}

import java.nio.file.{Files, Paths}
import java.sql.Timestamp
import java.time.LocalDateTime

class OnTimeAirlines(flights: DataFrame,
                     airports: DataFrame,
                     airlines: DataFrame,
                     curretnMetrciStore: MetricStore) extends Metric {

  override val flightsCols: Seq[String] = Seq("ARRIVAL_DELAY","AIRLINE")
  override val airportsCols: Seq[String] = Seq()
  override val airlinesCols: Seq[String] = Seq("IATA_CODE","AIRLINE")


  override var newMetrciStore: MetricStore = null

  override def checkCols(): Unit = {
    validateColumnPresence(flightsCols)(flights)
    validateColumnPresence(airlinesCols)(airlines)

  }

  override def filterOnDate(): DataFrame = {
    DataFrameOps.getNotCalculateDF(flights, curretnMetrciStore)
  }

  override def getMetric(filteredOnDate: DataFrame): DataFrame = {

    val fromDate = filteredOnDate.select("MinDate").first().getTimestamp(0)
    val toDate = filteredOnDate.select("MaxDate").first().getTimestamp(0)

    val onTimeAirlines = filteredOnDate
      .filter(filteredOnDate("ARRIVAL_DELAY") === 0)
      .groupBy("AIRLINE")
      .count()
      .join(airlines.as("al"), filteredOnDate("AIRLINE") === airlines("IATA_CODE"))
      .select("al.AIRLINE", "count")



    newMetrciStore = MetricStore(
      metricName = "OnTimeAirlines",
      top = 10,
      order = curretnMetrciStore.order,
      date = Timestamp.valueOf(LocalDateTime.now()),
      dateFrom = fromDate,
      dateTo = toDate,
      path = curretnMetrciStore.path,
      pathAll = curretnMetrciStore.pathAll)

    onTimeAirlines
  }

  override def mergeMetric(newMetric: DataFrame, metrciStore: MetricStore): Option[DataFrame] = {
    val projectDir = System.getProperty("user.dir")
    val relativePath = curretnMetrciStore.path
    val filePath = Paths.get(projectDir, relativePath).toString

    if (Files.exists(Paths.get(filePath))) {
      //достаем старый результат
      val oldMetric = CsvReader(
        spark,
        CsvReader.Config(file = curretnMetrciStore.path)
      ).read()

      // объединяем старый и новый результат
      Some(oldMetric.union(newMetric).groupBy("AIRLINE").agg(functions.sum("count").as("count")))
    }
    else None
  }

  override def requiredMetric(metric: DataFrame): DataFrame = {

    metric.createOrReplaceTempView("tempView")

    spark.sql(s" select AIRLINE, count from tempView order by count ${curretnMetrciStore.order}")
      .limit(curretnMetrciStore.top)
  }

  def calculate(): (DataFrame, DataFrame, MetricStore) = {
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

object OnTimeAirlines {
  def apply(flights: DataFrame,
            airports: DataFrame,
            airlines: DataFrame,
            currentStore: MetricStore) = new OnTimeAirlines(flights, airports, airlines, currentStore)
}
