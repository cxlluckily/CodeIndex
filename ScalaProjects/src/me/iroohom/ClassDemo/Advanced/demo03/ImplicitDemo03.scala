package me.iroohom.ClassDemo.Advanced.demo03

import scala.math.Ordering

object ImplicitDemo03 {
  def main(args: Array[String]): Unit = {
    val list = List(1, 2, 3, 4, 6, -5, 4, 23)

    /**
     * def sorted[B >: A](implicit ord: Ordering[B]): Repr
     */
    println(list.sorted)
    val ints = list.sorted[Int](Ordering.by(item => -item))
    println(ints.toString())

  }

}
