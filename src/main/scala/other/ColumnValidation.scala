package com.example
package other

import org.apache.spark.sql.DataFrame

case class MissingColumnsException(msg: String) extends Exception(msg)

private class ColumnChecker(df: DataFrame, requiredCols: Seq[String]) {
  val columns = df.columns.toSeq
  val missingCols = requiredCols.diff(columns)

  def missingColumnsMessage(): String = {
    val missingColNames = missingCols.mkString(", ")
    val dfCols = columns.mkString(", ")
    s"The [${missingColNames}] columns are not in the DF with columns: [${dfCols}]"
  }

  def validateColumnPresence(): Unit = {
    if (missingCols.nonEmpty) throw MissingColumnsException(missingColumnsMessage())
  }
}


trait DfValidator {

  def validateColumnPresence(requiredCols: Seq[String])(df: DataFrame): Unit = {
    val checker = new ColumnChecker(df, requiredCols)
    checker.validateColumnPresence()
  }
}
