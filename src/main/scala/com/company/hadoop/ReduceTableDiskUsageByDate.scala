package com.company.hadoop

import better.files._
import com.company.hadoop.common.Common._
import com.company.hadoop.common.Progress._
import com.company.hadoop.common.Reducer


object ReduceTableDiskUsageByDate extends Reducer {
  override val REDUCE_KEY: String = "table"

  private def getTableUsageMap(diskUsageByTableFilePath: String): Map[String, List[DateUsage]] = {
    val totalCount = countFileLines(diskUsageByTableFilePath)
    var processCount = 0L

    measureExecutionTime {
      println(s"load $REDUCE_KEY start...")
      val mapResult = File(diskUsageByTableFilePath).lineIterator.foldLeft(Map.empty[String, List[DateUsage]]) { (map, line) =>
        val (tableNameWithDB, list) = composeDateUsageMapBy(map, line)
        processCount = processCount + 1
        printProgress(s"group by $REDUCE_KEY is processing...", processCount, totalCount)
        map + (tableNameWithDB -> list)
      }
      println(s"load $REDUCE_KEY complete.")
      printMapOperationResult(REDUCE_KEY, mapResult)
      mapResult
    }
  }

  def reduce(diskUsageByTableFilePath: String, resultFilePath: String): Unit = {
    val tableUsageMap = getTableUsageMap(diskUsageByTableFilePath)
    val tableDiskUsageByDate = reduceDiskUsageByDate(tableUsageMap) // db -> SortedMap(sorted by date -> total disk usage)
    measureExecutionTime {
      writeDiskUsageFile(resultFilePath, tableDiskUsageByDate)
    }
  }

  def main(args: Array[String]): Unit = {
    val diskUsageByTableFilePath = args(0)
    val resultFilePath = args(1)

    reduce(diskUsageByTableFilePath, resultFilePath)
  }
}
