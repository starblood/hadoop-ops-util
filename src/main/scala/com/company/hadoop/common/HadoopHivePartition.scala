package com.company.hadoop.common

import scala.util.matching.Regex
import HadoopDataDate.parseDate
import com.company.hadoop.common.Common.PartitionInfo

object HadoopHivePartition {
  def extractPartitionInfoFromPartitionPath(partitionPath: String, bytes: Long): Option[PartitionInfo] = {
    val pattern = new Regex("/user/([^/]+)/warehouse/([^/]+)\\.db/([^/]+)/([^/]+)$")
    try {
      pattern.findFirstMatchIn(partitionPath) match {
        case Some(m) =>
          val account = m.group(1)
          val dbName = s"${m.group(2)}.db"
          val tableName = m.group(3)
          val partitionName = m.group(4)
          Some(PartitionInfo(account, dbName, tableName, partitionPath, bytes))
        case None => None
      }
    } catch {
      case e: Exception =>
        println(s"ERROR: extractAccountAndDbAndTableAndPartition. partitionPath: $partitionPath, error msg: ${e.getMessage}")
        None
    }
  }

  def extractPartitionInfo(partitionInfoStr: String): Option[PartitionInfo] = {
    val partitionInfoParts = partitionInfoStr.split(",")
    if (partitionInfoStr.contains(",")) {
      if (partitionInfoParts.size == 5) { // latest *.csv format: account,db,table,partition,bytes
        val account = partitionInfoParts(0)
        val db = partitionInfoParts(1)
        val table = partitionInfoParts(2)
        val partition = partitionInfoParts(3)
        val bytes = partitionInfoParts(4).toLong
        Some(PartitionInfo(account, db, table, partition, bytes))
      } else if (partitionInfoParts.size == 2) { // legacy *.csv format: partitionPath,bytes
        val partitionPath = partitionInfoParts(0)
        val bytes = partitionInfoParts(1).toLong
        extractPartitionInfoFromPartitionPath(partitionPath, bytes) match {
          case Some(partitionInfo) => Some(partitionInfo)
          case None => None
        }
      } else {
        None
      }
    } else {
      None
    }

  }

  def getPartitionNameFromPartitionPath(partitionPath: String): Option[String] = {
    val pattern = new Regex("/user/([^/]+)/warehouse/([^/]+)\\.db/([^/]+)/([^/]+)$")
    try {
      pattern.findFirstMatchIn(partitionPath) match {
        case Some(m) => Some(m.group(4)) // partitionName, i.e. dt=20240603
        case None => None
      }
    } catch {
      case e: Exception =>
        println(s"ERROR: getPartitionNameFromPartitionPath. partitionPath: $partitionPath, error msg: ${e.getMessage}")
        None
    }
  }

  def getPartitionDateFromPartitionPath(partitionPath: String): String = {
    val partitionName = getPartitionNameFromPartitionPath(partitionPath) match {
      case Some(str) => str
      case None => ""
    }
    if (partitionName == "") "unknown" else getPartitionDate(partitionName)
  }

  def getPartitionDate(partitionName: String): String = {
    partitionName match {
      case s if s.startsWith("stdt=") => parseDate(s.replace("stdt=", ""))
      case s if s.startsWith("wdt=") => parseDate(s.replace("wdt=", ""))
      case s if s.startsWith("dt=") => parseDate(s.replace("dt=", ""))
      case _ => partitionName
    }
  }
}
