package com.company.hadoop.common

import com.company.hadoop.common.Common.{DateUsage, measureExecutionTime}
import com.company.hadoop.common.Progress.printProgress

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.immutable.SortedMap

trait Reducer {
  val REDUCE_KEY: String

  implicit val dateOrdering: Ordering[String] = Ordering.by { key: String =>
    LocalDate.parse(key, DateTimeFormatter.ISO_LOCAL_DATE)
  }

  def composeDateUsageMapBy(map: Map[String, List[DateUsage]], line: String): (String, List[DateUsage]) = {
    val parts = line.split(",")
    val key = parts(0)
    val date = parts(1)
    val bytes = parts(2).toLong
    val list = if (map.contains(key)) {
      map.getOrElse(key, List.empty) :+ DateUsage(date, bytes)
    } else {
      List(DateUsage(date, bytes))
    }
    (key, list)
  }

  def reduceDiskUsageByDate(usageMap: Map[String, List[DateUsage]]): Map[String, SortedMap[String, Long]] = {
    val totalCount = usageMap.values.map(_.size).sum

    measureExecutionTime {
      println("reduce Disk Usage By Date...")
      var processCount = 0L
      val result: Map[String, SortedMap[String, Long]] = usageMap.map { kv =>
        val key = kv._1
        val list = kv._2
        val dateUsageMap = list.foldLeft(SortedMap.empty[String, Long]) { (map, dateUsage) =>
          processCount = processCount + 1
          printProgress(s"reducing disk usage by date for $key", processCount, totalCount)
          reduceDiskUsageByDate(dateUsage, map)
        }
        key -> dateUsageMap
      }
      println("reduce Disk Usage By Date complete.")
      result
    }
  }

  def reduceDiskUsageByDate(dateUsage: DateUsage, map: SortedMap[String, Long]): SortedMap[String, Long] = {
    val date = dateUsage.date
    val bytes = dateUsage.bytes

    val (resultMap, bytesSum) = if (map.contains(date)) {
      val bytesSum = map.getOrElse(date, 0L) + bytes
      (map.updated(date, bytesSum), bytesSum)
    } else {
      (map.updated(date, bytes), bytes)
    }
    println(s"reduceDiskUsageByDate: $date -> $bytesSum")
    resultMap
  }
}
