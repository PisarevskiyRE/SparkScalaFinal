package com.example
package writers
import org.apache.spark.sql.DataFrame

class CsvWriter extends DataFrameWriter {

  override def write(df: DataFrame, outputPath: String): Unit =  df.write.option("header", "true").csv(outputPath)

}
