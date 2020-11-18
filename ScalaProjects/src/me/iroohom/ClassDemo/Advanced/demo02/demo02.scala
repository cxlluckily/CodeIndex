package ImplicitDemo

/**
 * 签到的笔
 */
class SignPen {
  def signName(): Unit = {
    println("----签到----")
  }
}


object demo02 {
  //默认签到桌上放着一支笔
  implicit val signPen = new SignPen()


  /**
   * 到达会议室门口进行签到
   */
  def signForMeetup(implicit pen: SignPen): Unit = {
    pen.signName()
  }

  def main(args: Array[String]): Unit = {
    //从桌子上拿笔
    signForMeetup

    //自己拿出笔签到
    signForMeetup(new SignPen())
  }
}
