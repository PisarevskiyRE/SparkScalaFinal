package com.example
package jobs

import jobs.JobConfig.getPathByName
import metrics._
import readers.CsvReader
import other.MetricStore

import com.example.metrics
import com.example.other.MetricStore.getMetricStoreByName
import com.example.transformers.DataFrameOps
import com.example.writers.CsvWriterSingleFile
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}


class Job(spark: SparkSession, config: JobConfig) {

  def run(): Unit = {

    import spark.implicits._


    println(config.configPath)

    // получем конфиги исходных данных
    val configs: DataFrame = CsvReader(
      spark,
      CsvReader.Config(file = config.configPath)
    ).read()

    configs.show()

    println("airlinesFilePath")
    // получаем пути для конкретных файлов
    val airlinesFilePath = getPathByName(configs, "airlines")
    println(airlinesFilePath)
    val airportsFilePath = getPathByName(configs, "airports")
    println(airportsFilePath)
    val flightsFilePath = getPathByName(configs, "flights")
    println(flightsFilePath)


    // загружаем DF
    val airlinesDF = CsvReader(
      spark,
      CsvReader.Config(file = airlinesFilePath)
    ).read()

    val airportsDF = CsvReader(
      spark,
      CsvReader.Config(file = airportsFilePath)
    ).read()

    val flightsDF   = CsvReader(
      spark,
      CsvReader.Config(file = flightsFilePath)
    ).read()


    // загружаем все настройки метрик
    val initMetricStore: Dataset[MetricStore] = MetricStore.getInitStore(config.storePath)



    // 1
    val topAirportsByFlightsMetricStore: MetricStore = getMetricStoreByName(initMetricStore, "TopAirportsByFlights")
    val TopAirportsByFlights = metrics.TopAirportsByFlights(flightsDF, airportsDF, airlinesDF, topAirportsByFlightsMetricStore).calculate()
//    TopAirportsByFlights._1.show()
//    TopAirportsByFlights._2.show()
//    println(TopAirportsByFlights._3)
    CsvWriterSingleFile.write(TopAirportsByFlights._1, TopAirportsByFlights._3.pathAll)
    CsvWriterSingleFile.write(TopAirportsByFlights._2, TopAirportsByFlights._3.path)

    // 2
    val OnTimeAirlinesMetricStore: MetricStore = getMetricStoreByName(initMetricStore, "OnTimeAirlines")
    val OnTimeAirlinesResult = metrics.OnTimeAirlines(flightsDF, airportsDF, airlinesDF, OnTimeAirlinesMetricStore).calculate()
//    OnTimeAirlinesResult._1.show()
//    OnTimeAirlinesResult._2.show()
//    println(OnTimeAirlinesResult._3)
    CsvWriterSingleFile.write(OnTimeAirlinesResult._1, OnTimeAirlinesResult._3.pathAll)
    CsvWriterSingleFile.write(OnTimeAirlinesResult._2, OnTimeAirlinesResult._3.path)

    //3
    val OnTimeFlightsMetricStore: MetricStore = getMetricStoreByName(initMetricStore, "OnTimeFlights")
    val OnTimeFlightsResult = metrics.OnTimeFlights(flightsDF, airportsDF, airlinesDF, OnTimeFlightsMetricStore).calculate()
//    OnTimeFlightsResult._1.show()
//    OnTimeFlightsResult._2.show()
//    println(OnTimeFlightsResult._3)
    CsvWriterSingleFile.write(OnTimeFlightsResult._1, OnTimeFlightsResult._3.pathAll)
    CsvWriterSingleFile.write(OnTimeFlightsResult._2, OnTimeFlightsResult._3.path)



    //4
    val FlightsByDayOfWeekMetricStore: MetricStore = getMetricStoreByName(initMetricStore, "FlightsByDayOfWeek")
    val FlightsByDayOfWeekResult = metrics.FlightsByDayOfWeek(flightsDF, airportsDF, airlinesDF, FlightsByDayOfWeekMetricStore).calculate()
//    FlightsByDayOfWeekResult._1.show()
//    FlightsByDayOfWeekResult._2.show()
//    println(FlightsByDayOfWeekResult._3)
    CsvWriterSingleFile.write(FlightsByDayOfWeekResult._1, FlightsByDayOfWeekResult._3.pathAll)
    CsvWriterSingleFile.write(FlightsByDayOfWeekResult._2, FlightsByDayOfWeekResult._3.path)

    //5
    val DelayReasonsMetricStore: MetricStore = getMetricStoreByName(initMetricStore, "DelayReasons")
    val DelayReasonsResult = metrics.DelayReasons(flightsDF, airportsDF, airlinesDF, DelayReasonsMetricStore).calculate()
    //    DelayReasonsResult._1.show()
    //    DelayReasonsResult._2.show()
    //    println(DelayReasonsResult._3)
    CsvWriterSingleFile.write(DelayReasonsResult._1, DelayReasonsResult._3.pathAll)
    CsvWriterSingleFile.write(DelayReasonsResult._2, DelayReasonsResult._3.path)


    //6
    val DelayPercentMetricStore: MetricStore = getMetricStoreByName(initMetricStore, "DelayPercent")
    val DelayPercentResult = metrics.DelayPercent(flightsDF, airportsDF, airlinesDF, DelayPercentMetricStore).calculate()
    //    DelayPercentResult._1.show()
    //    DelayPercentResult._2.show()
    //    println(DelayPercentResult._3)
    CsvWriterSingleFile.write(DelayPercentResult._1, DelayPercentResult._3.pathAll)
    CsvWriterSingleFile.write(DelayPercentResult._2, DelayPercentResult._3.path)

    // собираем все новые настройки
    val newMetrciStore: DataFrame = List(
      TopAirportsByFlights._3,
      OnTimeAirlinesResult._3,
      OnTimeFlightsResult._3,
      FlightsByDayOfWeekResult._3,
      DelayReasonsResult._3,
      DelayPercentResult._3,
    ).toDF()


    CsvWriterSingleFile.write(newMetrciStore, "/opt/spark-data/output/metrics_store_test.csv")

  }
}

object Job {
  def apply(spark: SparkSession, config: JobConfig) = new Job(spark, config)
}
