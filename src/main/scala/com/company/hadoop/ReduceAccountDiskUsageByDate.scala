package com.company.hadoop


import better.files._
import com.company.hadoop.common.Common.DateUsage
import com.company.hadoop.common.{Common, Reducer}

import scala.collection.SortedMap


object ReduceAccountDiskUsageByDate extends Reducer {
  override val REDUCE_KEY: String = "account"

  private def getAccountUsageMap(diskUsageByAccountFilePath: String): Map[String, List[DateUsage]] = {
    Common.measureExecutionTime {
      val totalCount = Common.countFileLines(diskUsageByAccountFilePath)
      var processCount = 0L

      val result = File(diskUsageByAccountFilePath).lineIterator.foldLeft(Map.empty[String, List[DateUsage]]) { (map, line) =>
        val (account, list) = composeDateUsageMapBy(map, line)
        processCount = processCount + 1
        val percentile: Double = (processCount.toDouble / totalCount.toDouble) * 100
        println(f"loading accountUsageMap => account: $account, ($processCount/$totalCount): $percentile%.2f%%")
        map + (account -> list)
      }
      println("load AccountUsageMap complete.")
      result.foreach{kv =>
        println(s"account: ${kv._1}, date count: ${kv._2.size}")
      }
      result
    }
  }

  private def reduceAccountDiskUsageByDate(accountUsageMap: Map[String, List[DateUsage]]): Map[String, SortedMap[String, Long]] = {
    val totalCount = accountUsageMap.values.map(_.size).sum

    Common.measureExecutionTime {
      println("reduce Account Disk Usage By Date...")
      var processCount = 0L
      val result: Map[String, SortedMap[String, Long]] = accountUsageMap.map { kv =>
        val account = kv._1
        val list = kv._2
        val dateUsageMap = list.foldLeft(SortedMap.empty[String, Long]) { (map, dateUsage) =>
          val date = dateUsage.date
          val bytes = dateUsage.bytes
          processCount = processCount + 1
          println(f"reducing bytes by date($date) => account: $account, ($processCount/$totalCount): " +
            f"${Common.getProgressPercentile(processCount, totalCount)}%.2f%%")
          if (map.contains(date)) {
            map + (date -> (map.getOrElse(date, 0L) + bytes))
          } else {
            map + (date -> bytes)
          }
        }
        account -> dateUsageMap
      }
      println("reduce Account Disk Usage By Date complete.")
      result
    }
  }

  def reduce(diskUsageByAccountFilePath: String, resultFilePath: String): Unit = {
    val accountUsageMap = getAccountUsageMap(diskUsageByAccountFilePath)
    val accountDiskUsageByDate: Map[String, SortedMap[String, Long]] = reduceAccountDiskUsageByDate(accountUsageMap)
    Common.measureExecutionTime{
      val resultFile = File(resultFilePath).newBufferedWriter
      accountDiskUsageByDate.foreach{kv =>
        val account = kv._1
        val usageByDates = kv._2
        usageByDates.foreach{dateBytes =>
          println(s"writing: $account,${dateBytes._1},${dateBytes._2}")
          resultFile.write(s"$account,${dateBytes._1},${dateBytes._2}\n")
        }
      }
      resultFile.close()
    }
  }

  def main(args: Array[String]): Unit = {
    val diskUsageByAccountFilePath = args(0)
    val resultFilePath = args(1)

    reduce(diskUsageByAccountFilePath, resultFilePath)
  }
}
