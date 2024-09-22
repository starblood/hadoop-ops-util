package com.company.hadoop

import com.company.hadoop.common.Common.getResultFile
import com.company.hadoop.common.{HadoopDataDate, HadoopHivePartition}
import common.HadoopHivePartition._

// scala File
import better.files._
import File._


object CreateDiskUsageByAccount {
  private def createDiskUsageByAccount(normalizeDiskUsageFilePath: String): Unit = {
    val normalizeDiskUsageFile = File(normalizeDiskUsageFilePath)
    val resultWriter = getResultFile(normalizeDiskUsageFile, "disk_usage_by_account").newBufferedWriter

    normalizeDiskUsageFile.lineIterator.foreach{ line =>
      HadoopHivePartition.extractPartitionInfo(line) match {
        case Some(partitionInfo) =>
          val account = partitionInfo.account
          val bytes = partitionInfo.bytes
          val date = getPartitionDateFromPartitionPath(partitionInfo.path)
          if (HadoopDataDate.isValidDateFormat(date)) {
            println(s"write: $account,$date,$bytes")
            resultWriter.write(s"$account,$date,$bytes\n")
          }
      }
    }
    resultWriter.close()
  }

  def main(args: Array[String]): Unit = {
    // Path to a file where CSV files with 2 or 5 fields are standardized to 5 fields
    // i.e. <partitionPath>,<bytes> or <account>,<db>,<table>,<partitionPath>,<bytes> => <account>,<db>,<table>,<partitionPath>,<bytes>
    val normalizeDiskUsageFilePath = args(0)
    createDiskUsageByAccount(normalizeDiskUsageFilePath)
  }
}
