package me.iroohom.ClassDemo.OOP.Constructor

/**
 * customer类 包含成员方法
 */
class Customer {
  var name: String = _
  var age: Int = _

  def printHello(msg: String): Unit = {
      println(msg)
  }

  override def toString: String = s"name = ${name}, age = ${age}"
}


object OopDemo {
  def main(args: Array[String]): Unit = {

    //创建对象
    val customer = new Customer
    customer.name = "dazhuang"
    customer.age = 23
    println(s"name = ${customer.name}, age = ${customer.age}")
    customer.printHello("Hello!")

    println(customer)
  }
}
