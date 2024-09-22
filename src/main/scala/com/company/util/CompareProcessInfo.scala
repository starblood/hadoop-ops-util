package com.company.util


import better.files._
import File._

/**
 * A program that splits process information obtained from the Linux 'ps' command into tokens based on spaces,
 * and finds out which arguments differ between this process and that process.
 * Each process info is recorded in a file, and the program operates by comparing the two files.
 */
object CompareProcessInfo {
  def extractTokens(filePath: String): Set[String] = {
    File(filePath).lineIterator.foldLeft(Set.empty[String]) { case (set, line) =>
      val tokens = line.split("\\s")
      val lineTokenSet = tokens.toList.foldLeft(Set.empty[String]){ case (lineSet, token) =>
        lineSet + token
      }
      set ++ lineTokenSet
    }
  }
  def main(args: Array[String]): Unit = {
    val thisFilePath = args(0)
    val thatFilePath = args(1)

    val thisFileName = File(thisFilePath).name
    val thatFileName = File(thatFilePath).name

    val thisFileTokens = extractTokens(thisFilePath)
    val thatFileTokens = extractTokens(thatFilePath)

    val leftDiff = thisFileTokens -- thatFileTokens
    val rightDiff = thatFileTokens -- thisFileTokens

    println(s"contains only $thisFileName: ${leftDiff.mkString(", ")}")
    println(s"contains only $thatFileName: ${rightDiff.mkString(", ")}")
  }
}
