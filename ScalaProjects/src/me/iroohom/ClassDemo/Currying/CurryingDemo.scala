package me.iroohom.ClassDemo.Currying

object CurryingDemo {


  /**
   * 两数相加
   *
   * @param x 第一个数
   * @param y 第二个数
   * @return 两数之和
   */
  def addValue(x: Int, y: Int): Int = {
    x + y
  }


  /**
   * 柯里化方式实现两数相加 当一个方法有多个参数时，其中一个参数类型是函数时
   * 建议采用柯里化方式来定义方法
   *
   * @param x
   * @param y
   * @return
   */
  def plusValue(x: Int)(y: Int): Int = {
    x + y
  }


  def operation(x: Int, y: Int, func: (Int, Int) => Int): Int = {
    func(x, y)
  }

  def operationCurrying(x: Int, y: Int)(func: (Int, Int) => Int): Int = {
    func(x, y)
  }

  def main(args: Array[String]): Unit = {
    //普通
    println(addValue(1, 99))
    //柯里化
    println(plusValue(1)(99))

    println(s"add Value = ${operationCurrying(1, 99)((x, y) => x + y)}")
    println(s"sub Value = ${operationCurrying(1, 99)((x, y) => x - y)}")
    println(s"multiply Value = ${operationCurrying(1, 99)((x, y) => x * y)}")

  }
}
