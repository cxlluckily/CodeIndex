package me.iroohom.HomeWork.day01

/**
 * Scala 的可变Map
 */
object ScalaMap {
  def main(args: Array[String]): Unit = {
    var map = Map("Red"->23, "Tom"->20)
    map += "jack" -> 30
    //获取我的年龄
    println(map.get("Red"))
    println(map)





  }

}
