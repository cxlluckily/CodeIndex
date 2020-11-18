package me.iroohom.ClassDemo.Pattern.MatchDemo

import scala.io.StdIn

object MatchDemo {
  def matchFunc(): Unit = {
    while (true) {
      val input = StdIn.readLine

      input match {
        case "hadoop" => println("大数据分布式存储和计算框架")
        case "zookeeper" => println("大数据分布式协调框架")
        case "spark" => println("大数据分布式内存计算框架")
        case _ => println("未匹配")
      }
    }
  }


  /**
   * 类型匹配
   *
   * @param value
   */
  def typeFunc(value: Any): Unit = {
    value match {
      //判断value变量类型，如果是Int类型，将value值赋值给intValue
      case intValue: Int => println(s"Int Type ${intValue}")
      case strValue: String => println(s"Int Type ${strValue}")
      case doubleValue: Double => println(s"Int Type ${doubleValue}")
      case _ => println(s"Int Type ${"其他类型"}")
    }
  }


  def main(args: Array[String]): Unit = {
    typeFunc("字符串")
    typeFunc(2)

  }
}
