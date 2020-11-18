package me.iroohom.ClassDemo.Array

import scala.collection.mutable.ArrayBuffer

object ArrayDemo {
  def main(args: Array[String]): Unit = {
    //定义定长数组方式1
    val array = new Array[Int](5)
    array(0) = 1
    array(1) = 2
    array(2) = 3
    array(3) = 4
    array(4) = 5

    //定义数组方式2
    val array2 = Array("a", "b", "c")


    //定义变长数组方式1
    val array3 = ArrayBuffer[Int]()
    array3 += 1
    array3 += 2
    array3 += 3
    array3 += 4
    array3 += 5

    //定义边长数组方式2
    val array4 = ArrayBuffer("a", "b", "c")
    array4 += "d"
    array4 += "e"
    array4 += "f"
    array4 += "g"
    //删除元素
    array4 -= "c"

    //往数组里面插入一个数组
    array4 ++= Array("x", "y", "z")

    for (item <- array4) print(item + " ")


  }

}
