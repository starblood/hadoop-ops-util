package com.company.hadoop

import better.files._
import File._

import scala.collection.immutable.SortedMap

case class DiskUsage(usage: BigDecimal, unit: String)
case class ReplicatedDiskUsage(usage: BigDecimal, unit: String)
case class HdfsAccount(home: String, user: String)
case class HdfsAccountDiskUsage(home: String, user: String, diskUsage: DiskUsage, replicatedDiskUsage: ReplicatedDiskUsage)
case class HdfsAccountDiskUsageInBytes(home: String, user: String, diskUsage: BigDecimal, replicatedDiskUsage: BigDecimal)

object DFSUsagePrinter {
  val BYTE_UNIT_MAP: Map[String, BigDecimal] = Map.newBuilder[String, BigDecimal]
    .addOne("K" -> BigDecimal(1024))
    .addOne("M" -> BigDecimal(1024).pow(2))
    .addOne("G" -> BigDecimal(1024).pow(3))
    .addOne("T" -> BigDecimal(1024).pow(4))
    .addOne("P" -> BigDecimal(1024).pow(5)) // peta byte
    .addOne("E" -> BigDecimal(1024).pow(6)) // exa byte
    .addOne("Z" -> BigDecimal(1024).pow(7)) // zeta byte
    .addOne("Y" -> BigDecimal(1024).pow(8)) // yota byte
    .result()

  def convertToBytes(hdfsAccountDiskUsage: HdfsAccountDiskUsage): HdfsAccountDiskUsageInBytes = {
    val diskUsage: BigDecimal =
      hdfsAccountDiskUsage.diskUsage.usage *
        BYTE_UNIT_MAP.getOrElse[BigDecimal](hdfsAccountDiskUsage.diskUsage.unit.toUpperCase(), 1)

    val replicatedDiskUsage: BigDecimal =
      hdfsAccountDiskUsage.replicatedDiskUsage.usage *
        BYTE_UNIT_MAP.getOrElse[BigDecimal](hdfsAccountDiskUsage.replicatedDiskUsage.unit.toUpperCase(), 1)

    HdfsAccountDiskUsageInBytes(hdfsAccountDiskUsage.home, hdfsAccountDiskUsage.user, diskUsage, replicatedDiskUsage)
  }

  def bytesToHumanReadable(bytes: BigDecimal): String = {
    if (bytes < 0) {
      throw new IllegalArgumentException("bytes must be non-negative")
    }

    val unit = BigDecimal(1024)
    if (bytes < unit) return s"${bytes.toString()} B"

    val units = Array("EB", "PB", "TB", "GB", "MB", "KB", "B")
    val exp = (Math.log(bytes.doubleValue) / Math.log(unit.doubleValue)).toInt
    val index = units.length - 1 - exp

    val readableValue = bytes / unit.pow(exp)
    f"$readableValue%.1f ${units(index)}"
  }

  def main(args: Array[String]): Unit = {
    val dfsResultFilePath = args(0)

    val sortedDiskUsage = File(dfsResultFilePath).lineIterator.foldLeft(SortedMap.empty[BigDecimal, HdfsAccountDiskUsageInBytes]){(map, line) =>
      val tokens = line.split("""\s+""")
      println(s"${tokens(0)} ${tokens(1)}, ${tokens(2)} ${tokens(3)}, ${tokens(4)}")
      val diskUsage = DiskUsage(tokens(0).toDouble, tokens(1))
      val replicatedDiskUsage = ReplicatedDiskUsage(tokens(2).toDouble, tokens(3))
      val hdfsAccount = HdfsAccount(tokens(4), tokens(4).substring(tokens(4).lastIndexOf("/") + 1, tokens(4).length))
      val hdfsAccountDiskUsage = HdfsAccountDiskUsage(hdfsAccount.home, hdfsAccount.user, diskUsage, replicatedDiskUsage)
      val hdfsAccountDiskUsageInBytes = convertToBytes(hdfsAccountDiskUsage)
      map + (hdfsAccountDiskUsageInBytes.diskUsage -> hdfsAccountDiskUsageInBytes)
    }
    val diskUsages = sortedDiskUsage.iterator.foldLeft(List.empty[HdfsAccountDiskUsageInBytes]){(list, kv) =>
      val diskUsage = kv._2
      list :+ diskUsage
    }.reverse
    diskUsages.foreach {diskUsage =>
      println(s"${diskUsage.home} => disk: ${bytesToHumanReadable(diskUsage.diskUsage)}, " +
        s"disk(replicated): ${bytesToHumanReadable(diskUsage.replicatedDiskUsage)}")
    }
    sortedDiskUsage.foreach {kv =>
      println(s"${kv._2}")
    }
  }
}
