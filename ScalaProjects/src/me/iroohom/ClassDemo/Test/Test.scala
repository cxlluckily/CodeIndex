package me.iroohom.ClassDemo.Test

import scala.collection._

object Test {
  def main(args: Array[String]): Unit = {
    var map = Map(1 -> "大强")

    /**
     * 现在的map已经不是原来的map了
     */
    map += (2 -> "小壮")
    println(map)


    /**
     * map1还是原来的地址
     */
    val map1 = mutable.Map(1 -> "大大")
    map1 += (2 -> "小小")
    println(map1)

  }
}
