package me.iroohom.ClassDemo.Map.mutable

import scala.collection._

object MutableMap {
  def main(args: Array[String]): Unit = {
    val map = mutable.Map[Int, String]()
    val map2 = mutable.Map[Int, String]()

    map += 1 -> "A"
    println(map.getOrElse(1, null))

    map2 += 2 -> "B"
    map ++= map2
    println(map2)

    //遍历
    map.foreach(x => print(x + " "))
    println()
    for (item <- map) print(item + " ")
    println()
    for ((key, value) <- map) println(s"key = ${key}, value = ${value}")

  }

}
