package me.iroohom.ClassDemo.Loop

import scala.collection.immutable
import scala.collection.mutable.ArrayBuffer

//if守卫
object IfGuard {
  def main(args: Array[String]): Unit = {
    for (item <- 1 to 10) {
      println(s"item = ${item}")
    }
    println()

    //IF守卫 演示打印奇数
    for (item <- 1 to 10 if (item % 2 == 1)) {
      println(s"item = ${item}")
    }
    println()

    val array = new ArrayBuffer[Int]()
    for (item <- 1 to 10) {
      val square = item * item
      array += square
    }
    println(array)


    //FOR推导式 演示打印数的平方
    val squares: immutable.Seq[Int] = for (item <- 1 to 10) yield {
      item * item
    }
    println(squares)

  }
}
