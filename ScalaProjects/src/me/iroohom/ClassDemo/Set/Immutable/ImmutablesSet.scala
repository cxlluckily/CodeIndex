package me.iroohom.ClassDemo.Set.Immutable

import scala.collection._

object ImmutablesSet {
  def main(args: Array[String]): Unit = {
    val set = immutable.Set(1, 2, 3, 4, 5, 6)
    println(set)
    println(set - 1)
    println(set)

  }

}
