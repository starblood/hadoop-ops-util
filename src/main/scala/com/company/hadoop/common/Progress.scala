package com.company.hadoop.common

import com.company.hadoop.common.Common.DateUsage

object Progress {
  def printMapOperationResult(key: String, mapOperationResult: Map[String, List[DateUsage]]): Unit = {
    mapOperationResult.foreach { kv =>
      println(s"$key: ${kv._1}, number of date: ${kv._2.size}")
    }
  }

  def printProgress(msg: String, processCount: Long, totalCount: Long): Unit = {
    val percentile: Double = (processCount.toDouble / totalCount.toDouble) * 100
    println(f"$msg => ($processCount/$totalCount): $percentile%.2f%%")
  }
}
