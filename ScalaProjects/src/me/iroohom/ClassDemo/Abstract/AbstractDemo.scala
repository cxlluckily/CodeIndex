package me.iroohom.ClassDemo.Abstract

abstract class Shape {
  def area(): Double
}

/**
 * 定义具体矩形类
 *
 * @param edge 边长
 */
class Square(val edge: Double) extends Shape {
  override def area(): Double = {
    edge * edge
  }
}

/**
 * 定义具体类，长方形Rectangle
 *
 * @param width
 * @param depth
 */
class Rectangle(val width: Double, val depth: Double) extends Shape {
  override def area(): Double = {
    width * depth
  }
}

/**
 * 原型Circle
 *
 * @param redius
 */
class Circle(val redius: Double) extends Shape {
  override def area(): Double = {
    Math.PI * redius * redius
  }
}


object AbstractDemo {
  def main(args: Array[String]): Unit = {
    val square = new Square(9.9)
    println(s"square area: ${square.area()}")

    val rectangle = new Rectangle(2, 3)
    println(s"rectangle area: ${rectangle.area()}")

    val circle = new Circle(1)
    println(s"circle area:${circle.area()}")


  }
}
