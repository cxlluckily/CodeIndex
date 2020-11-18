package me.iroohom.ClassDemo.Loop

//Scala使用break

import scala.util.control.Breaks._

object BreakDemo {
  def main(args: Array[String]): Unit = {
    breakable {
      for (item <- 1 to 10) {
        if (item > 5) break()
        print(item + " ")
      }
    }
  }
}
