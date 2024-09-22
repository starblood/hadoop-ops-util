package com.company.hadoop

object ReportWeeklyHDFSUsage {
  abstract class HadoopCluster {
    def getAlias(): String
  }
  case object KerberizedHadoopCluster extends HadoopCluster {
    override def getAlias(): String = "Gold Cluster"
  }

  val KERBERIZED_HADOOP_CLUSTER_DISK_SIZE_IN_PB = 10
  val KERBERIZED_HADOOP_CLUSTER_TOTAL_DISK_SIZE_IN_TB: Double = KERBERIZED_HADOOP_CLUSTER_DISK_SIZE_IN_PB * 1024
  val YOUR_CLUSTER_ALIAS = "secure"

  abstract class Severity
  case object SeverityLow extends Severity
  case object SeverityCaution extends Severity
  case object SeverityWarn extends Severity
  case object SeverityFatal extends Severity
  case object SeverityFailure extends Severity


  val SEVERITY_LEVEL_MAP: Map[Severity, String] = Map(
    SeverityLow -> "\uD83D\uDFE2 warning level: low",
    SeverityCaution -> "\uD83D\uDFE1 warning level: cautious",
    SeverityWarn -> "\uD83D\uDFE0 warning level: warn",
    SeverityFatal -> "\uD83D\uDD34 warning level: critical",
    SeverityFailure -> "☠\uFE0F warning level: fatal"
  )

  def getCluster(cluster: String): HadoopCluster = {
    cluster match {
      case YOUR_CLUSTER_ALIAS => KerberizedHadoopCluster
      case _ => throw new IllegalArgumentException(s"$cluster is not supported.")
    }
  }

  def getReportHeader(cluster: HadoopCluster): String = {
    s"${cluster.getAlias()} Disk Usage"
  }

  def getSeverity(diskUsageInPercent: Double): Severity = {
    if (diskUsageInPercent >= 0f && diskUsageInPercent < 60f) SeverityLow
    else if (diskUsageInPercent >= 60f && diskUsageInPercent < 70f) SeverityCaution
    else if (diskUsageInPercent >= 70f && diskUsageInPercent < 80f) SeverityWarn
    else if (diskUsageInPercent >= 80f && diskUsageInPercent < 90f) SeverityFatal
    else SeverityFailure
  }

  def getNextPhaseDiskUsagePercent(currentSeverity: Severity): Double = {
    currentSeverity match {
      case SeverityLow => 60f
      case SeverityCaution => 70f
      case SeverityWarn => 80f
      case SeverityFatal => 90f
      case SeverityFailure => 100f
      case _ => throw new IllegalArgumentException(s"$currentSeverity is not supported Severity Level.")
    }
  }

  def getWarningMessage(cluster: HadoopCluster, diskUsageCurrentInPB: Double, dailyIncreaseInTB: Option[Double]): String = {
    val (diskUsageCurrentInPercent, totalDiskSizeInTB) = cluster match {
      case KerberizedHadoopCluster =>
        ((diskUsageCurrentInPB * 1024) / KERBERIZED_HADOOP_CLUSTER_TOTAL_DISK_SIZE_IN_TB * 100, KERBERIZED_HADOOP_CLUSTER_TOTAL_DISK_SIZE_IN_TB)
      case _ => throw new IllegalArgumentException(s"$cluster is not supported")
    }
    val currentSeverity = getSeverity(diskUsageCurrentInPercent)
    val nextPhaseDiskUsagePercent: Double = getNextPhaseDiskUsagePercent(currentSeverity)
    val nextPhaseDiskUsageInTB: Double = totalDiskSizeInTB * (nextPhaseDiskUsagePercent / 100)
    val daysLeftToNextSeverityPhase: Option[Double] = if (dailyIncreaseInTB.isDefined) {
      Some((nextPhaseDiskUsageInTB - diskUsageCurrentInPB * 1024) / dailyIncreaseInTB.get)
    } else None

    if (daysLeftToNextSeverityPhase.isDefined) {
      f"${daysLeftToNextSeverityPhase.get}%.2f days later ${Math.round(nextPhaseDiskUsagePercent)}%%" +
        f" (${SEVERITY_LEVEL_MAP(getSeverity(nextPhaseDiskUsagePercent))}) reached"
    } else {
      ""
    }
  }

  def reportClusterDiskUsage(cluster: HadoopCluster,
                             day30agoDiskUsageInPB: Double,
                             todayDiskUsageInPB: Double): Unit = {
    val totalDiskSizeInTB = cluster match {
      case KerberizedHadoopCluster => KERBERIZED_HADOOP_CLUSTER_TOTAL_DISK_SIZE_IN_TB
      case _ => throw new IllegalArgumentException(s"$cluster is not supported cluster name")
    }
    val currentUsagePercent: Double = (todayDiskUsageInPB * 1024) / totalDiskSizeInTB * 100
    val dailyIncreaseInTB: Double = (todayDiskUsageInPB - day30agoDiskUsageInPB) * 1024 / 30f

    val diskUsageMessage = f"currently $currentUsagePercent%.2f%% in used ($todayDiskUsageInPB PB / ${totalDiskSizeInTB / 1024} PB)"
    val severityMessage = SEVERITY_LEVEL_MAP(getSeverity(currentUsagePercent))
    val report = if (dailyIncreaseInTB <= 0.0f) {
      s"${getReportHeader(cluster)}\n\t$diskUsageMessage, $severityMessage\n\tDISK Usage has no change"
    } else {
      val dailyIncreaseMessage = f"per day $dailyIncreaseInTB%.2f TB increase(past 30 days usage based)"
      s"${getReportHeader(cluster)}\n\t$diskUsageMessage, $severityMessage\n\t$dailyIncreaseMessage, " +
        s"${getWarningMessage(cluster, todayDiskUsageInPB, Some(dailyIncreaseInTB))}"
    }
    println(report)
  }

  def main(args: Array[String]): Unit = {
    val cluster = getCluster(args(0).trim) // cluster alias
    val day30agoDiskUsageInPB = args(1).toDouble // 30 days before dfs usage
    val todayDiskUsageInPB = args(2).toDouble // today dfs usage

    reportClusterDiskUsage(cluster, day30agoDiskUsageInPB, todayDiskUsageInPB)
  }
}
