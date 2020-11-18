package me.iroohom.ClassDemo.List.mutable

import scala.collection.mutable.ListBuffer

object MutableList {
  def main(args: Array[String]): Unit = {
    val a = ListBuffer("a", "b", "c")
    val b = ListBuffer("g", "h", "i")
    val d = ListBuffer(1, 2, 3)
    val c = ListBuffer(a, b)
    println(a)

    a ++= ListBuffer("d", "e")
    println(a)

    println(s"a ${a.isEmpty} 为空")

    println(a.reverse) //只是暂时结果 不涉及赋值
    println(a)

    println(a.take(2))
    println(a.takeRight(2))
    println(a.dropRight(1))
    println(a)

    println(c.flatten)

    println(d.zip(b))
    println(d.zip(b).unzip)

    println(a.mkString(", "))

    println(a.union(b))

    println(a.diff(b))
    println(b.diff(a))

  }
}
