package com.company.hadoop

import com.company.hadoop.common.HadoopHivePartition

// redefine java File to JFile to use better.files library
import java.io.{File => JFile}
import scala.io.Source

// scala File
import better.files._
import File._

object NormalizeDiskUsageFile {
  def normalize_disk_usage_file(filePath: String): Unit = {
    val targetFile = File(filePath)
    val resultDir = targetFile.parent.path
    val targetFileNameAndExtension = targetFile.name.split(""".csv""")
    val targetFileName = if (targetFileNameAndExtension.size == 1) targetFileNameAndExtension(0) else targetFile.name
    val normalizeDiskUsageFilePath = s"$resultDir/$targetFileName-normalized.csv"
    val normalizeDiskUsageFile = File(normalizeDiskUsageFilePath)

    println(s"normalizeDiskUsageFilePath: $normalizeDiskUsageFilePath")
    val resultWriter = normalizeDiskUsageFile.newBufferedWriter
    targetFile.lineIterator.foreach{ line =>
      HadoopHivePartition.extractPartitionInfo(line) match {
        case Some(partitionInfo) =>
          val account = partitionInfo.account
          val db = partitionInfo.db
          val table = partitionInfo.table
          val partition = partitionInfo.path
          val bytes = partitionInfo.bytes
          resultWriter.write(s"$account,$db,$table,$partition,$bytes\n")
      }
    }
    resultWriter.close()
  }

  def main(args: Array[String]): Unit = {
    val filePath = args(0)
    normalize_disk_usage_file(filePath)
  }
}
