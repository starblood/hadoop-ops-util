package com.company.hadoop

import better.files._
import com.company.hadoop.common.Common._
import com.company.hadoop.common.Progress._
import com.company.hadoop.common.Reducer


object ReduceDBDiskUsageByDate extends Reducer {
  override val REDUCE_KEY: String = "db"

  private def getDBUsageMap(diskUsageByDBFilePath: String): Map[String, List[DateUsage]] = {
    val totalCount = countFileLines(diskUsageByDBFilePath)
    var processCount = 0L

    measureExecutionTime {
      println(s"load $REDUCE_KEY start...")
      val mapResult = File(diskUsageByDBFilePath).lineIterator.foldLeft(Map.empty[String, List[DateUsage]]) { (map, line) =>
        val (db, list) = composeDateUsageMapBy(map, line)
        processCount = processCount + 1
        printProgress(s"group by $REDUCE_KEY is processing...", processCount, totalCount)
        map + (db -> list)
      }
      println(s"load $REDUCE_KEY complete.")
      printMapOperationResult(REDUCE_KEY, mapResult)
      mapResult
    }
  }

  def reduce(diskUsageByDBFilePath: String, resultFilePath: String): Unit = {
    val dbUsageMap = getDBUsageMap(diskUsageByDBFilePath)
    val dbDiskUsageByDate = reduceDiskUsageByDate(dbUsageMap) // db -> SortedMap(sorted by date -> total disk usage)
    measureExecutionTime {
      writeDiskUsageFile(resultFilePath, dbDiskUsageByDate)
    }
  }

  def main(args: Array[String]): Unit = {
    val diskUsageByDBFilePath = args(0)
    val resultFilePath = args(1)

    reduce(diskUsageByDBFilePath, resultFilePath)
  }
}
