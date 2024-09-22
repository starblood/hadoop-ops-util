package com.company.server

object ServerNameGenerator {
  def main(args: Array[String]): Unit = {
    val serverName = args(0)
    val inclusiveNumberRange = args(1)
    val serverNamePostfix = args(2)

    val tokens = inclusiveNumberRange.split(",")
    val (startNumber, lastNumber) = (tokens(0).toInt, tokens(1).toInt)

    (startNumber to lastNumber).foreach {number =>
      val numStr = if (number >= 100) number.toString else s"0$number"
      println(s"$serverName$numStr$serverNamePostfix")
    }
  }
}
