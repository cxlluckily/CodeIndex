package me.iroohom.ClassDemo.Pattern.MatchDemo03

/**
 * 顾客
 *
 * @param name
 * @param age
 */
case class Customer(name: String, age: Int)

/**
 * 订单
 *
 * @param id
 * @param price
 */
case class Order(id: Long, price: Double)

object MatchDemo03 {
  /**
   * 模式匹配 匹配样例类
   *
   * @param obj
   */
  def CaseMatchFunc(obj: Any): Unit = {
    obj match {
      case Customer(name, age) => println(s"name = ${name}, age = $age")
      case Order(id, price) => println(s"id = ${id}, price = $price")

      /**
       * 匹配元组
       */
      case (x, y, _) => println(s"x = $x, y = $y")
      case ("spark", "scala") => println(s" 1 ")
      case _ => println("未匹配")
    }
  }

  def main(args: Array[String]): Unit = {
    val customer: Any = Customer("dada", 27)
    val order: Any = Order(1001L, 99.98)

    /**
     * 使用模式匹配来匹配样例类
     */
    CaseMatchFunc(customer)
    CaseMatchFunc(order)
    CaseMatchFunc((1, "scala", true))
    CaseMatchFunc(("spark", "scala"))

  }
}
