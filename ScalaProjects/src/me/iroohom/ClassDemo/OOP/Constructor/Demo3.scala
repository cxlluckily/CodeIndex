package me.iroohom.ClassDemo.OOP.Constructor

/**
 * @ClassName: Demo3
 * @Author: Roohom
 * @Function:
 * @Date: 2020/11/13 17:01
 * @Software: IntelliJ IDEA
 */

/**
 * 1、主构造器
 * 紧跟类名称之后，使用一对圆括号括起来，如果主构造器方法中没有变量，省略圆括号、
 * 2、附属构造器
 * 在类中定义的方法 方法名称必须为this
 * 在附属构造方法中第一行必须调用主构造方法或者其他附属构造方法
 */
class Person(val name: String, var age: Int = 0) {

  var gender: String = _

  //定义附属构造器
  def this(name: String, age: Int, gender: String) {
    //第一行代码必须调用主构造方法
    this(name, age)
    this.gender = gender
  }

  def this(name: String, gender: String) {
    //第一行代码必须调用主构造方法
    this(name)
    this.gender = gender
  }

  override def toString: String = s"name = ${name}, age = ${age}"
}


object Demo3 {
  def main(args: Array[String]): Unit = {
    val person = new Person("dada", 23)
    println(person)

    //调用附属构造方法
    var p = new Person("dada", 3, "male")
    println(p)

    var p1 = new Person("xixi", "female")
    println(p1)
  }

}
