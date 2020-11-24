package me.iroohom.spark

import org.lionsoul.ip2region.{DbConfig, DbSearcher}

object ConvertIpTest {
  def main(args: Array[String]): Unit = {
    val dbSearcher: DbSearcher = new DbSearcher(new DbConfig(), "datas\\dataset\\ip2region.db")


    val dataBlock = dbSearcher.btreeSearch("139.209.239.62")

    //获取解析的省份城市
    val region = dataBlock.getRegion
    println(region)
    val Array(_, _, province, city,_) = region.split("\\|")

    println(s"省份 = ${province},城市 = ${city}")
  }
}
