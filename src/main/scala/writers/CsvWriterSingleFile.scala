package com.example
package writers
import org.apache.spark.sql.{DataFrame, Row}

import java.io.PrintWriter
import java.io.{File, PrintWriter}
import java.nio.file.Paths



/**
 * пришлось писать кастомный "сохранятор"
 * не нашел нормального и пристого решения обычного сохранения в один csv файл
 */

object CsvWriterSingleFile extends DataFrameWriter{

  override def write(df: DataFrame, outputPath: String): Unit = {

    def dataframeToList(df: DataFrame): List[Map[String, String]] = {

      val rows: Array[Row] = df.collect()
      val columnNames: Array[String] = df.columns

      rows.map { row =>
        columnNames.map(_.toString).zip(row.toSeq.map(_.toString)).toMap
      }.toList
    }


    def saveListToCsv(list: List[Map[String, String]], outputPath: String): Unit = {

      val projectDir = System.getProperty("user.dir")
      val relativePath = outputPath
      val filePath = Paths.get(projectDir, relativePath).toString


      val file = new File(filePath)

      val writer = new PrintWriter(file)

      val header = list.headOption.map(_.keys.mkString(","))
      header.foreach(writer.println)

      list.foreach { row =>
        val line = row.values.mkString(",")
        writer.println(line)
      }
      writer.close()
    }

    val rowList: List[Map[String, String]] = dataframeToList(df)



    saveListToCsv(rowList, outputPath)
  }
}


