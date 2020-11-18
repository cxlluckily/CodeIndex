package me.iroohom.ClassDemo.Actor

import scala.actors.Actor


class WxActor extends Actor {
  override def act(): Unit = {
    /**
     * 偏函数 receive只会接收一条消息
     */
    //    receive {
    //      case "Hello" => println("嚯")
    //    }

    loop {
      react {
        case "Hello" => println("Hello嚯哗")
        case "Start" => println("Start嚯哗")
        case "Stop" => println("Stop嚯哗")
      }
    }
  }
}


object HelloActor {
  def main(args: Array[String]): Unit = {

    /**
     * 创建一个对象
     */
    val wxActor = new WxActor()

    /**
     * 启动
     */
    wxActor.start()

    /**
     * 自己给自己发送消息
     */
    wxActor ! "Hello"
    wxActor ! "Start"
    wxActor ! "Stop"
  }
}
