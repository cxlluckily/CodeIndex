package me.iroohom.ClassDemo.Method

object DefaultValue {

  def max(x: Int = 0, y: Int = 0): Int = if (x > y) x else y

  def sum(num: Int*): Int = num.sum


  def main(args: Array[String]): Unit = {
    println(max(x = 1, y = 4))
    println(max(1))

    println(sum(1, 2, 3))
  }


}
