package me.iroohom.ClassDemo.Set.mutable

import scala.collection._

/**
 * 在求交集 差集 并集 去重的时候使用Set
 */
object mutableSet {
  def main(args: Array[String]): Unit = {
    val set = mutable.Set(1, 2, 3, 4, 5, 6)
    println(set)
    set += 7
    println(set)
  }

}
