package me.iroohom.ClassDemo.Method

object MethodDemo {
  //定义一个方法 比较两个Int类型的数字的最大值
  def max(one: Int, ano: Int): Int = {
    if (one > ano) one else ano
  }
  def newMax(one:Int,ano:Int): Int = if (one > ano) one else ano

  def main(args: Array[String]): Unit = {
    println(max(4, 5))
    println(newMax(4,5))

  }
}
