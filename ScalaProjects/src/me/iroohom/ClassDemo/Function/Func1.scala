package me.iroohom.ClassDemo.Function

/**
 * 函数的使用
 */
object Func1 {

  var a = (x: Int, y: Int) => x + y

  val maxFunc = (x: Int, y: Int) => if (x >= y) x else y


  def main(args: Array[String]): Unit = {
    println(a(1, 2))
    println(maxFunc(1, 2))
  }

}
