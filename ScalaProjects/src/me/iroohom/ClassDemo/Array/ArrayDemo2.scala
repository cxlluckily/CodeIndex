package me.iroohom.ClassDemo.Array

object ArrayDemo2 {

  def main(args: Array[String]): Unit = {
    val array = Array(2, 1, 3, 42, 5, 3, 6, 7, 4, 25)

    //方法 sum、max、min、sorted
    println(s"sum = ${array.sum}")
    println()
    println(s"max = ${array.max}")
    println()
    println(s"min = ${array.min}")
    println()
    println(array.mkString(", "))
    println()
    for (item <- array.sorted) print(item + " ")
    println()
    for (item <- array.sorted.reverse) print(item + " ")

  }

}
