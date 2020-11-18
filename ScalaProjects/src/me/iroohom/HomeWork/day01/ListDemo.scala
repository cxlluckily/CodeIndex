package me.iroohom.HomeWork.day01

import scala.collection._
import scala.collection.mutable.ListBuffer

object ListDemo {
  def main(args: Array[String]): Unit = {
    var map = Map("Red" -> 23, "tom" -> 20)
    //1、向Map添加
    map += "jack" -> 30
    println(map)

    //2、获取年龄
    println(map.get("Red"))

    //3、定义可变列表l1 和 不可变列表l2
    val l1 = ListBuffer(2, 4, 6)
    val l2 = List(1, 3, 5)

    //4、将l2追加到l1中
    println(l1)
    l1 ++= l2
    println()
    println(l1)
    println()

    //5、for each 打印l1所有元素
    for (item <- l1) print(item + " ")
    println()
    l1.foreach((item: Int) => print(item + " "))
    println()
    //使用类型推断
    l1.foreach(item => print(item + " "))
    //使用下划线来简化
    l1.foreach(print(_))
    println()

    //6、使用Map返回每个数的平方组成的新列表
    val tmp = l1.map(x => x * x)
    println(tmp.mkString(", "))

    //7、使用filter过滤出l1中的奇数
    println(l1.filter(_ % 2 == 1))

    //8、对l1进行降序排序
    println(l1.sorted.reverse)
    //自定义排序实现降序排序
    println(l1.sortWith((x, y) => if (x < y) false else true))
    //下划线简化
    println(l1.sortWith(_ < _).reverse)

    //9、使用reduce计算所有数的乘积
    println(l1.reduce((x, y) => x * y))
    println(l1.reduce(_ * _))
    println(l1.product)


  }

}
