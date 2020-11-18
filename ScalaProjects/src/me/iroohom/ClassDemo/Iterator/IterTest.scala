package me.iroohom.ClassDemo.Iterator

import scala.collection.immutable

object IterTest {
  def main(args: Array[String]): Unit = {
    val list: List[Int] = List(1, 2, 3, 4, 5, 6)
    //迭代器
    val iterator: Iterator[Int] = list.iterator

    //使用迭代器遍历list
    while (iterator.hasNext)
      println(iterator.next())

    println()
  }
}
