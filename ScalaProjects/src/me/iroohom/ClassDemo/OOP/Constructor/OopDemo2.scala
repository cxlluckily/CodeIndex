package me.iroohom.ClassDemo.OOP.Constructor

/**
 * @ClassName: OopDemo2
 * @Author: Roohom
 * @Function:
 * @Date: 2020/11/13 16:53
 * @Software: IntelliJ IDEA
 */
class Person1 {
  //定义私有变量
  private var name: String = _
  private var age: Int = _

  def setName(name: String): Unit = {
    this.name = name
  }

  def setAge(age: Int): Unit = {
    this.age = age
  }

  def getName(): String = {
    this.name
  }

  def getAge(): Int = {
    this.age
  }

  private def getNameAndAge(): (String, Int) = {
    (this.name, this.age)
  }

}


object OopDemo2 {
  def main(args: Array[String]): Unit = {
    var person = new Person1
    person.setName("dada")
    println(person.getName())


  }
}
