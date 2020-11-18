package me.iroohom.ClassDemo.OOP.OopDemos.Abstract

abstract class Person(val name:String)
{
  def sayHello()
}


object NoNameClass {
  def main(args: Array[String]): Unit = {
    new Person("dada") {
      override def sayHello(): Unit = println(s"$name}}")
    }


  }
}
