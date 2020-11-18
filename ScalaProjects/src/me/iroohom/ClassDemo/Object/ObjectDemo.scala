package me.iroohom.ClassDemo.Object

/**
 * @ClassName: ObjectDemo
 * @Author: Roohom
 * @Function:
 * @Date: 2020/11/13 17:21
 * @Software: IntelliJ IDEA
 */
object Dog {
  val LEG_NUM = 4

  def printLegNum(): Unit = {
    println(s"dog's leg nunber is ${LEG_NUM}")
  }

}

object ObjectDemo {
  def main(args: Array[String]): Unit = {
    Dog.printLegNum()
  }

}
