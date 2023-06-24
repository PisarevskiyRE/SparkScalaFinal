package com.example
package metrics

import readers.CsvReader
import transformers.DataFrameOps

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, functions}

import java.nio.file.{Files, Paths}
import java.sql.Timestamp
import java.time.LocalDateTime

class DelayPercent(flights: DataFrame,
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

    val totalDelays = filteredOnDate.agg(
      sum(col("AIR_SYSTEM_DELAY")).as("AIR_SYSTEM_DELAY"),
      sum(col("SECURITY_DELAY")).as("SECURITY_DELAY"),
      sum(col("AIRLINE_DELAY")).as("AIRLINE_DELAY"),
      sum(col("LATE_AIRCRAFT_DELAY")).as("LATE_AIRCRAFT_DELAY"),
      sum(col("WEATHER_DELAY")).as("WEATHER_DELAY")
    ).first()

    // Сумма всех причин задержек
    val totalDelayMinutes = totalDelays.toSeq.map(_.asInstanceOf[Long]).sum


    import spark.implicits._

    // Рассчитывание процентов от общего количества задержек рейсов для каждой причины
    val delayReasons = List("AIR_SYSTEM_DELAY", "SECURITY_DELAY", "AIRLINE_DELAY", "LATE_AIRCRAFT_DELAY", "WEATHER_DELAY")
    val resultDF = totalDelays.toSeq.zip(delayReasons).map {
      case (delayMinutes: Long, reason: String) => (reason, (delayMinutes.toDouble / totalDelayMinutes) * 100)
    }.toDF("Reasons", "Percent")



    newMetrciStore = MetricStore(
      metricName = "DelayPercent",
      top = 10.toString,
      order = curretnMetrciStore.order,
      date = Timestamp.valueOf(LocalDateTime.now()).toString,
      dateFrom = fromDate.toString,
      dateTo = toDate.toString,
      path = curretnMetrciStore.path,
      pathAll = curretnMetrciStore.pathAll)

    resultDF
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
      Some(oldMetric.union(newMetric).groupBy("Reason").agg(functions.sum("Percent").as("Percent")))
    }
    else None
  }

  override def requiredMetric(metric: DataFrame): DataFrame = {

    metric.createOrReplaceTempView("tempView")

    spark.sql(s" select Reason, Percent from tempView order by Percent ${curretnMetrciStore.order}")
      .limit(curretnMetrciStore.top.toInt)
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

object DelayPercent {
  def apply(flights: DataFrame,
            airports: DataFrame,
            airlines: DataFrame,
            currentStore: MetricStore) = new DelayReasons(flights, airports, airlines, currentStore)
}
