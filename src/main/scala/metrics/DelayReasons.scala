package com.example
package metrics

import readers.CsvReader
import transformers.DataFrameOps

import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions._
import java.nio.file.{Files, Paths}
import java.sql.Timestamp
import java.time.LocalDateTime

class DelayReasons(flights: DataFrame,
                     airports: DataFrame,
                     airlines: DataFrame,
                     curretnMetrciStore: MetricStore) extends Metric {

  override val flightsCols: Seq[String] = Seq("AIR_SYSTEM_DELAY","SECURITY_DELAY", "AIRLINE_DELAY", "LATE_AIRCRAFT_DELAY", "WEATHER_DELAY")
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

    val delayReasons = filteredOnDate
      .select(
        when(col("AIR_SYSTEM_DELAY") > 0, "AIR_SYSTEM_DELAY")
          .when(col("SECURITY_DELAY") > 0, "SECURITY_DELAY")
          .when(col("AIRLINE_DELAY") > 0, "AIRLINE_DELAY")
          .when(col("LATE_AIRCRAFT_DELAY") > 0, "LATE_AIRCRAFT_DELAY")
          .when(col("WEATHER_DELAY") > 0, "WEATHER_DELAY")
          .as("Reason")
      )
      .groupBy("Reason")
      .agg(count("Reason").as("count"))
      .na.drop()



    newMetrciStore = MetricStore(
      metricName = "DelayReasons",
      top = 10,
      order = curretnMetrciStore.order,
      date = Timestamp.valueOf(LocalDateTime.now()),
      dateFrom = fromDate,
      dateTo = toDate,
      path = curretnMetrciStore.path,
      pathAll = curretnMetrciStore.pathAll)

    delayReasons
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
      Some(oldMetric.union(newMetric).groupBy("Reason").agg(functions.sum("count").as("count")))
    }
    else None
  }

  override def requiredMetric(metric: DataFrame): DataFrame = {

    metric.createOrReplaceTempView("tempView")

    spark.sql(s" select Reason, count from tempView order by count ${curretnMetrciStore.order}")
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

object DelayReasons {
  def apply(flights: DataFrame,
            airports: DataFrame,
            airlines: DataFrame,
            currentStore: MetricStore) = new DelayReasons(flights, airports, airlines, currentStore)
}
