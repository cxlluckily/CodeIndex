package me.iroohom.ClassDemo.Exception

object ExceptionDemo {
  def main(args: Array[String]): Unit = {
    var result: Double = 0
    try {
      result = 10 / 0
    } catch {
      case e1: ArithmeticException => println("ArithmeticException")
      case e2: NumberFormatException => println("NumberFormatException")
      case e3: Exception => e3.printStackTrace()
    }
    finally {
      println("finally")
    }

    println(s"result = $result")
  }

}
