package me.iroohom.ClassDemo.OOP.OopDemos

class Person() {
  var name: String = _

  def getName(): String = this.name
}

class Student extends Person {

}


object Demo {
  def main(args: Array[String]): Unit = {
    val stu = new Student()
    stu.name = "dada"
    println(stu.getName())
  }
}
