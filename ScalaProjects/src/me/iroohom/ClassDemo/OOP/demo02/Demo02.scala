package me.iroohom.ClassDemo.OOP.demo02

class Person {
  var name: String = _
  var age: Int = _
}


object Demo02 {
  def main(args: Array[String]): Unit = {
    var person = new Person
    person.name = "daqiang"
    person.age = 23

    println(person.name)
    println(person.age)
    println(person)
  }

}
