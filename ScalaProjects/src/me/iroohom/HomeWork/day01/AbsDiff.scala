package me.iroohom.HomeWork.day01

/**
 * 求两数之差绝对值
 */
object AbsDiff {
  val absDiff = (x: Int, y: Int) => Math.abs(x - y)

  def main(args: Array[String]): Unit = {
    println(absDiff(3, 4))
    println(absDiff(2, 100))
    println(absDiff(-14, 4))
    println(absDiff(23, -19))
  }

}
