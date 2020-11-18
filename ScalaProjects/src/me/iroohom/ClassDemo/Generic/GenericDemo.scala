package me.iroohom.ClassDemo.Generic


/**
 * 泛型演示
 */
object GenericDemo {

  /**
   * 既有上界，又有下界，应该下界写在前 上界写在后
   *
   * @param array
   * @tparam T
   * @return
   */
  //设置下界 Null
  def getMiddle2[T >: Null](array: Array[T]): T = {
    array(array.length / 2)
  }

  //设置上界 AnyVal
  def getMiddle1[T <: AnyVal](array: Array[T]): T = {
    array(array.length / 2)
  }

  /**
   * 获取数组的中间值
   *
   * @param array 数组
   * @tparam T 泛型 数组内的元素的类型
   * @return 返回数组的中间元素
   */
  def getMiddle[T](array: Array[T]): T = {
    array(array.length / 2)
  }

  def main(args: Array[String]): Unit = {
    println(getMiddle(Array(1, 2, 3, 4, 5)))
    println(getMiddle(Array("a", "b", "c")))

    //报错，因为String 是AnyRef的子类
//    println(getMiddle(Array[String]("1", "2", "3")))

    println("************设置上界************")
    println(getMiddle1(Array[Int](1,2,3,4,5)))
  }
}
