package com.company.hadoop.common

import better.files.File

import java.io.{File => JFile}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.collection.immutable.SortedMap


object Common {
  case class PartitionInfo(account: String, db: String, table: String, path: String, bytes: Long)

  /**
   * Hadoop DISK usage for date, usage
   * @param date date in LocalDate format to sort
   * @param bytes bytes for disk usage
   */
  case class DateUsage(date: String, bytes: Long)
  case class AccountDiskUsage(account: String, date: String, bytes: Long)
  case class DBDiskUsage(db: String, date: String, bytes: Long)
  case class TableDiskUsage(table: String, date: String, bytes: Long)

  def getListOfFiles(dir: JFile, ext: String = "csv"): List[JFile] = {
    val files = dir.listFiles
    files.filter(_.isFile).filter(_.getName.endsWith(s".$ext")).toList ++
      files.filter(_.isDirectory).flatMap(f => getListOfFiles(f, ext))
  }

  def getResultFile(normalizeDiskUsageFile: File, postfix: String = "result", ext: String = "csv"): File = {
    val originFileName = getFileNameWithoutExtension(normalizeDiskUsageFile.name) // == normalizeDiskUsageFile's name only
    File(s"${normalizeDiskUsageFile.parent.path}/$originFileName-$postfix.$ext")
  }

  def getFileNameWithoutExtension(fileName: String): String = {
    val dotIndex = fileName.lastIndexOf(".")
    if (dotIndex == -1) {
      fileName // Returns the full file name if there is no extension
    } else {
      fileName.substring(0, dotIndex)
    }
  }

  def measureExecutionTime[T](block: => T): T = {
    val startTime = System.nanoTime()
    val result = block // execute code block
    val endTime = System.nanoTime()
    val elapsed = (endTime - startTime) / 1e6 // Returns execution time in milliseconds
    println(f"elapsed: $elapsed%.2f ms")
    result
  }

  // TODO: make count lines as fast as possible
  def countFileLines(filePath: String): Long = {
    val path = Paths.get(filePath)
    // Use Files.lines to read the file as a stream of lines
    Files.lines(path, StandardCharsets.UTF_8).count()
  }

  def getProgressPercentile(processCount: Long, totalCount: Long): Double = {
    (processCount.toDouble / totalCount.toDouble) * 100
  }

  def writeDiskUsageFile(filePath: String, diskUsageByDateMap: Map[String, SortedMap[String, Long]]): Unit = {
    val fileWriter = File(filePath).newBufferedWriter

    diskUsageByDateMap.foreach { kv =>
      val db = kv._1
      val usageByDates = kv._2
      usageByDates.foreach { dateBytes =>
        println(s"writing: $db,${dateBytes._1},${dateBytes._2}")
        fileWriter.write(s"$db,${dateBytes._1},${dateBytes._2}\n")
      }
    }

    fileWriter.close()
  }
}
