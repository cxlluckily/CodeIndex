package me.iroohom.HomeWork.day02

trait Animal {
  def say()
}

object Dog {
  def apply(name: String): Dog = new Dog(name)
}

class Hen(var name: String) extends Animal {

  override def say(): Unit = {
    println("咯咯咯")
  }
}

class Dog(var name: String) extends Animal {

  override def say(): Unit = {
    println("汪汪汪")
  }
}


object Animals {
  def main(args: Array[String]): Unit = {
    val dog = Dog("大壮")
    dog.say()
    val hen = new Hen("咯咯")
    hen.say()

  }
}
