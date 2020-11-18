package me.iroohom.ClassDemo.Actor

import scala.actors.Actor

case class Message(msg: String)

case class Reply(msg: String)

/**
 * 老师Actor
 */
class TeacherActor extends Actor {
  /**
   * 接收消息
   */
  override def act(): Unit = {
    loop {
      react {
        case Message(msg) => println(s"${msg}")
          sender ! Reply("要好好学习呀！")
      }
    }
  }
}

/**
 * 学生Actor
 */
class StudentActor(teacherActor: TeacherActor) extends Actor {
  /**
   * 接收消息
   */
  override def act(): Unit = {
    /**
     * 发消息给老师
     */
    teacherActor ! Message("Hello Teacher!")

    react {
      case Reply(msg) => println(s" ${msg}")
    }
  }
}


object RpcActor {
  def main(args: Array[String]): Unit = {
    val teacherActor = new TeacherActor()
    teacherActor.start()

    val studentActor = new StudentActor(teacherActor)
    studentActor.start()


  }
}
