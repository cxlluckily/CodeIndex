package me.iroohom.ClassDemo.Function

object MaxFuncDemo {
  def max(x:Int,y:Int) = {
    if (x>=y) x else y
  }

  val maxFunc = (x:Int,y:Int) => if (x >= y) x else y


  def main(args: Array[String]): Unit = {
    //调用方法
    println(max(10,111))

    //调用函数
    println(maxFunc(101,11))

    //将方法转换为函数
    val maxFunc2 = max _
    println(maxFunc2(11,222))
  }
}
