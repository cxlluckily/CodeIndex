package me.iroohom.HomeWork.day02



class Person(val name:String, var age:Int){
  var sex:String = _

  def say(): Unit ={
    println("我要学好屎噶拉")
  }

}




object Ordinary {
  def main(args: Array[String]): Unit = {
    val person = new Person("阿红", 23)
    person.say()
  }
}
