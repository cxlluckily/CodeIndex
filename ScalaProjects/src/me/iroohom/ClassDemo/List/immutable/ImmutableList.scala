package me.iroohom.ClassDemo.List.immutable

import scala.collection.mutable.ListBuffer

/**
 * 不可变列表示例
 */
object ImmutableList {

  def main(args: Array[String]): Unit = {
    val a = List(1, 2, 3, 4, 5, 6, 7)
    val b = List(3, 4, 5, 6, 7, 8)
    val d = List(a, b)
    //空列表
    val e = Nil

    //判断是否为空
    println(a.isEmpty)
    //拼接
    val c = a ++ b

    println(a ++ b)
    c.foreach(x => print(x + " "))
    //    a.foreach(x => print(x + " "))
    println()
    println(a.take(3))

    //扁平化
    d.flatten.foreach(x => print(x + " "))
    println()
    println(d.flatten)
    println(a.tail.tail.tail.head)
  }
}
