package me.iroohom.ClassDemo.Pattern.MatchDemo2

import scala.io.StdIn

/**
 * 守卫
 */
object MatchDemo02 {
  def main(args: Array[String]): Unit = {
    while (true) {
      val readVal = StdIn.readInt()
      readVal match {
        case number if number >= 0 && number <= 3 => println("[0-3]")
        case _ if readVal >= 4 && readVal <= 7 => println("[4-7]")
        case _ => println("其他范围")
      }
    }
  }
}
