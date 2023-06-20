package com.example
package metrics

import other.MetricStore
import readers.CsvReader
import transformers.DataFrameOps

import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.{DataFrame, functions}

import java.nio.file.{Files, Paths}
import java.sql.Timestamp
import java.time.LocalDateTime

class FlightsByDayOfWeek(flights: DataFrame,
                     airports: DataFrame,
                     airlines: DataFrame,
                     curretnMetrciStore: MetricStore) extends Metric {

  override val flightsCols: Seq[String] = Seq("DAY_OF_WEEK","ARRIVAL_DELAY")
  override val airportsCols: Seq[String] = Seq()
  override val airlinesCols: Seq[String] = Seq()


  override var newMetrciStore: MetricStore = null

  override def checkCols(): Unit = {
    validateColumnPresence(flightsCols)(flights)
  }

  override def filterOnDate(): DataFrame = {
    DataFrameOps.getNotCalculateDF(flights, curretnMetrciStore)
  }

  override def getMetric(filteredOnDate: DataFrame): DataFrame = {

    val fromDate = filteredOnDate.select("MinDate").first().getTimestamp(0)
    val toDate = filteredOnDate.select("MaxDate").first().getTimestamp(0)

    val FlightsByDayOfWeek = flights
      .groupBy("DAY_OF_WEEK")
      .agg(avg("ARRIVAL_DELAY").alias("average_delay"))
      .select("DAY_OF_WEEK", "average_delay")



    newMetrciStore = MetricStore(
      metricName = "FlightsByDayOfWeek",
      top = 10,
      order = curretnMetrciStore.order,
      date = Timestamp.valueOf(LocalDateTime.now()),
      dateFrom = fromDate,
      dateTo = toDate,
      path = curretnMetrciStore.path,
      pathAll = curretnMetrciStore.pathAll)

    FlightsByDayOfWeek
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
      Some(oldMetric.union(newMetric).groupBy("DAY_OF_WEEK").agg(functions.sum("average_delay").as("average_delay")))
    }
    else None
  }

  override def requiredMetric(metric: DataFrame): DataFrame = {

    metric.createOrReplaceTempView("tempView")

    spark.sql(s" select DAY_OF_WEEK, average_delay from tempView order by average_delay ${curretnMetrciStore.order}")
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

object FlightsByDayOfWeek {
  def apply(flights: DataFrame,
            airports: DataFrame,
            airlines: DataFrame,
            currentStore: MetricStore) = new FlightsByDayOfWeek(flights, airports, airlines, currentStore)
}
