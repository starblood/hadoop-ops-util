package com.company.hadoop.common

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object HadoopDataDate {
  val DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  val PARTITION_PATTERNS = List(
    ("^\\d{4}-\\d{2}-\\d{2}$", "yyyy-MM-dd"),  // YYYY-MM-DD
    ("^\\d{6}$", "yyyyMM"),                    // YYYYMM
    ("^\\d{8}$", "yyyyMMdd"),                  // YYYYMMDD
    ("^\\d{4}-\\d{2}-\\d{1}$", "yyyy-MM-dd")   // YYYY-MM-D
  )

  def getDate(dateStr: String): LocalDate = {
    LocalDate.parse(dateStr, DATE_FORMATTER)
  }

  def toDateString(date: LocalDate): String = {
    date.format(DATE_FORMATTER)
  }

  def parseDate(dateStr: String): String = {
    PARTITION_PATTERNS.collectFirst {
      case (pattern, dateFormat) if dateStr.matches(pattern) =>
        try {
          val sdf = new SimpleDateFormat(dateFormat)
          val date = sdf.parse(dateStr)
          new SimpleDateFormat("yyyy-MM-dd").format(date)
        } catch {
          case _: Exception =>
            println(s"Unrecognized date format: $dateStr")
            dateStr
        }
    }.getOrElse(dateStr)
  }

  def isValidDateFormat(dateStr: String): Boolean = {
    PARTITION_PATTERNS.collectFirst {
      case (pattern, _) if dateStr.matches(pattern) => true
      case _ => false
    }
  }.getOrElse(false)
}
