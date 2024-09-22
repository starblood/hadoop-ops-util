package com.company.hadoop.common

import scala.sys.process._

object Command {
  def executeCommand(command: String): String = {
    Process(command).!!
  }
}
