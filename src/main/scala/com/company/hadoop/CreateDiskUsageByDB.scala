package com.company.hadoop

import com.company.hadoop.common.Common.getResultFile
import com.company.hadoop.common.{HadoopDataDate, HadoopHivePartition}
import com.company.hadoop.common.HadoopHivePartition._

// scala File
import better.files._


object CreateDiskUsageByDB {
  val RESULT_FILE_POSTFIX = "disk_usage_by_db"
  private def createDiskUsageByAccount(normalizeDiskUsageFilePath: String): Unit = {
    val normalizeDiskUsageFile = File(normalizeDiskUsageFilePath)
    val resultWriter = getResultFile(normalizeDiskUsageFile, RESULT_FILE_POSTFIX).newBufferedWriter

    normalizeDiskUsageFile.lineIterator.foreach{ line =>
      HadoopHivePartition.extractPartitionInfo(line) match {
        case Some(partitionInfo) =>
          val db = partitionInfo.db
          val bytes = partitionInfo.bytes
          val date = getPartitionDateFromPartitionPath(partitionInfo.path)
          val dbPath = s"/user/${partitionInfo.account}/warehouse/$db"
          if (HadoopDataDate.isValidDateFormat(date)) {
            println(s"writing: $dbPath,$date,$bytes")
            resultWriter.write(s"$dbPath,$date,$bytes\n")
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
