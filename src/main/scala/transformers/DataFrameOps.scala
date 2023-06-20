package com.example
package transformers
import other.MetricStore

import com.example.jobs.SessionWrapper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


object DataFrameOps extends DataFrameTransformer with SessionWrapper{

  override def getNotCalculateDF(inputDF: DataFrame, store: MetricStore): DataFrame = {

    import spark.implicits._

    val inputWithDateDF = inputDF
      .withColumn(
        "Date",
        to_timestamp(concat(col("YEAR"), lit("-"), col("MONTH"), lit("-"), col("DAY")))
      )
      .withColumn("MaxDate", max($"Date").over())
      .withColumn("MinDate", min($"Date").over())



    val filteredDF = (store.dateFrom, store.dateTo) match {
      case (null, null) => inputWithDateDF
      case (_,_) => inputWithDateDF
        .filter(col("Date") > store.dateTo)
    }

    filteredDF
  }
}
