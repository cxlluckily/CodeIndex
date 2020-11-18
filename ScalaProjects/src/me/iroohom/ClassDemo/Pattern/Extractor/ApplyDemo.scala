package me.iroohom.ClassDemo.Pattern.Extractor

/**
 * 提取器
 *
 * @param name 姓名
 * @param age  年龄
 */

class Student(val name: String, val age: Int)

object Student {
  def apply(name: String, age: Int): Student = new Student(name, age)

  /**
   * unapply 返回一个二元组
   *
   * @param student
   * @return
   */
  def unapply(student: Student): Option[(String, Int)] = {
    if (student != null) {
      Some(student.name -> student.age)
    }
    else {
      None
    }
  }
}


object ApplyDemo {
  def main(args: Array[String]): Unit = {
    val stu = Student("dada", 23)
    stu match {
      case Student(name, age) => println(s"name = $name, age = $age")
    }
  }
}
