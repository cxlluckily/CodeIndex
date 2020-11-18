package me.iroohom.ClassDemo.Pattern.Unboxing

object MatchDemo04 {
  def main(args: Array[String]): Unit = {
    val array = (1 to 10).toArray

    /**
     * 拆箱操作 TODO:Unboxing
     */
    val Array(_, x, y, z, _*) = array
    println(s"x = $x, y = $y, z = $z")

    val list: List[Int] = (1 to 10).toList
    val List(_, x1, y1, z1, _*) = list
    println(s"x = $x1, y = $y1, z = $z1")
  }
}
