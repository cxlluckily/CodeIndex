package me.iroohom.HomeWork.day01


import scala.collection.mutable.ArrayBuffer

/**
 * 定义边长数组 并求数组平均值
 */
object ScalaArray {
  def main(args: Array[String]): Unit = {
    val array = ArrayBuffer[Int]()

    for (item <- 1 to 10) array += item
    for (item <- array) print(item + " ")
    println()
    println(s"数组的值求和为: ${array.sum}")
    println(s"数组array的值的平均值是:${(array.sum.toDouble / array.size).formatted("%.2f")}")
  }
}
