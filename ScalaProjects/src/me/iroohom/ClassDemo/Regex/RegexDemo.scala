package me.iroohom.ClassDemo.Regex

import scala.util.matching.Regex


object RegexDemo {
  def main(args: Array[String]): Unit = {
    val regex = """.+@.+\..+""".r

    val email: String = "roohom@qq.com"

    val maybeMatch = regex.findFirstMatchIn(email)
    maybeMatch match {
      case Some(v) => println(v)
      case None => println("匹配不成功")
    }
  }
}
