package com.example

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Simple extends App{

  val spark = SparkSession
    .builder()
    .appName("TransactionsDF")
    .master("local[*]")
    .getOrCreate()

  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  def read(file: String) = {
    spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(file)
  }

//  val airlinesDF: DataFrame = read("src/main/resources/airlines.csv")
//  val airportsDF: DataFrame = read("src/main/resources/airports.csv")
//  val flightsDF: DataFrame = read("src/main/resources/flights.csv")

  import spark.implicits._


  // Загрузка данных из CSV файлов
  val airlines = read("src/main/resources/airlines.csv")
  val airports = read("src/main/resources/airports.csv")
  val flights = read("src/main/resources/flights.csv")



  // 1 - топ-10 самых популярных аэропортов по количеству совершаемых полетов
  val popularAirports = flights.groupBy("ORIGIN_AIRPORT").count().orderBy(desc("count")).limit(10)
  val popularAirportsResult = popularAirports.join(airports, popularAirports("ORIGIN_AIRPORT") === airports("IATA_CODE"))



    // 2 - топ-10 авиакомпаний, вовремя выполняющих рейсы
  val onTimeAirlines = flights.filter(flights("ARRIVAL_DELAY") === 0)
    .groupBy("AIRLINE").count().orderBy(desc("count")).limit(10)
  val onTimeAirlinesResult = onTimeAirlines.join(airlines, onTimeAirlines("AIRLINE") === airlines("IATA_CODE"))



  // 3 - для каждого аэропорта найдите топ-10 перевозчиков и аэропортов назначения на основании вовремя совершенного вылета
  val onTimeFlights = flights
    .filter(flights("DEPARTURE_DELAY") === 0)
    .groupBy("ORIGIN_AIRPORT", "AIRLINE", "DESTINATION_AIRPORT")
    .count()

  val topCarriersByAirport = onTimeFlights
    .withColumn("rank1", dense_rank().over(Window.partitionBy("ORIGIN_AIRPORT").orderBy(desc("count"))))
    //.filter(col("rank") <= 10)
  val topDestAirportsByAirport = onTimeFlights
    .withColumn("rank2", dense_rank().over(Window.partitionBy("DESTINATION_AIRPORT").orderBy(desc("count"))))
    //.filter(col("rank") <= 10)


  val topCarriersByAirportResult = topCarriersByAirport.join(airlines, topCarriersByAirport("AIRLINE") === airlines("IATA_CODE"))
  val topDestAirportsByAirportResult = topDestAirportsByAirport.join(airports, topDestAirportsByAirport("DESTINATION_AIRPORT") === airports("IATA_CODE"))

  val combinedResult = topCarriersByAirportResult.join(topDestAirportsByAirportResult.as("t1"), Seq("ORIGIN_AIRPORT"), "inner")
    .select("ORIGIN_AIRPORT", "t1.AIRLINE", "t1.DESTINATION_AIRPORT", "rank1", "t1.rank2")



    val flightsByDayOfWeek = flights
      .groupBy("DAY_OF_WEEK")
      .agg(avg("ARRIVAL_DELAY").alias("average_delay"))
      .orderBy("average_delay")



    // 5 - количество рейсов, задержанных по каждой причине
    val delayReasons = flights.select(
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





  // 6 - процент от общего количества минут задержки рейсов по каждой причине
  val totalDelays = flights.agg(
    sum(col("AIR_SYSTEM_DELAY")).as("AIR_SYSTEM_DELAY"),
    sum(col("SECURITY_DELAY")).as("SECURITY_DELAY"),
    sum(col("AIRLINE_DELAY")).as("AIRLINE_DELAY"),
    sum(col("LATE_AIRCRAFT_DELAY")).as("LATE_AIRCRAFT_DELAY"),
    sum(col("WEATHER_DELAY")).as("WEATHER_DELAY")
  ).first()

  val totalDelayMinutes = totalDelays.toSeq.map(_.asInstanceOf[Long]).sum

  val delayReasons2 = List("AIR_SYSTEM_DELAY", "SECURITY_DELAY", "AIRLINE_DELAY", "LATE_AIRCRAFT_DELAY", "WEATHER_DELAY")
  val resultDF = totalDelays.toSeq.zip(delayReasons2).map {
    case (delayMinutes: Long, reason: String) => (reason, (delayMinutes.toDouble / totalDelayMinutes) * 100)
  }.toDF("Причина", "Процент")

  resultDF.show()


  println("1 - Топ-10 самых популярных аэропортов по количеству совершаемых полетов:")
  //popularAirportsResult.show()

  println("2 - Топ-10 авиакомпаний, вовремя выполняющих рейсы:")
  //onTimeAirlinesResult.show()

  println("3 - Топ-10 перевозчиков и аэропортов назначения для каждого аэропорта на основе вовремя совершенного вылета:")
  //combinedResult.show()

  println("4 - Порядок дней недели по своевременности прибытия рейсов:")
  //flightsByDayOfWeek.show()

  println("5 - Количество рейсов, задержанных по причине AIR_SYSTEM_DELAY / SECURITY_DELAY / AIRLINE_DELAY / LATE_AIRCRAFT_DELAY / WEATHER_DELAY:")
  //delayReasons.show()

  println("6 - Процент задержки рейсов по каждой причине:")
 // resultDF.show()

  System.in.read()
}
