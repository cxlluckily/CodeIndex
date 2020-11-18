package me.iroohom.ClassDemo.OOP.OopDemos.OverrideDemo

class Person {
  val name: String = "大壮"

  def getName(): String = this.name

  def sayHello() = {
    println("=====Hello=====")
  }
}

class Student extends Person {
  override val name: String = "大强"

  override def getName(): String = {
    super.sayHello()
    s"subclass , ${this.name}"
  }

  def say(): Unit = {
    //子类的方法中不能调用父类的变量值
  }

}


object OverrideDemo {
  def main(args: Array[String]): Unit = {
    val student = new Student()
    student.sayHello()
  }

}
