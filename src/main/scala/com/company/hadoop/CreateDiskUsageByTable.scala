package com.company.hadoop

import com.company.hadoop.common.Common.getResultFile
import com.company.hadoop.common.HadoopHivePartition._
import com.company.hadoop.common.{HadoopDataDate, HadoopHivePartition}

// scala File
import better.files._


object CreateDiskUsageByTable {
  val RESULT_FILE_POSTFIX = "disk_usage_by_table"
  private def createDiskUsageByAccount(normalizeDiskUsageFilePath: String): Unit = {
    val normalizeDiskUsageFile = File(normalizeDiskUsageFilePath)
    val resultWriter = getResultFile(normalizeDiskUsageFile, RESULT_FILE_POSTFIX).newBufferedWriter

    normalizeDiskUsageFile.lineIterator.foreach{ line =>
      HadoopHivePartition.extractPartitionInfo(line) match {
        case Some(partitionInfo) =>
          val date = getPartitionDateFromPartitionPath(partitionInfo.path)
          val dbPath = s"/user/${partitionInfo.account}/warehouse/${partitionInfo.db}"
          val tablePath = s"$dbPath/${partitionInfo.table}"
          if (HadoopDataDate.isValidDateFormat(date)) {
            println(s"writing: $tablePath,$date,${partitionInfo.bytes}")
            resultWriter.write(s"$tablePath,$date,${partitionInfo.bytes}\n")
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
