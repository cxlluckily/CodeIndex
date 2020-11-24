package me.iroohom.spark.utils

import me.iroohom.spark.config.ApplicationConfig
import me.iroohom.spark.etl.Region
import org.lionsoul.ip2region.{DataBlock, DbConfig, DbSearcher}

object IpUtils {
  def convertIpToRegion(ip: String, dbSearcher: DbSearcher): Region = {

    //每查一个ip都需要创建一个DbSearcher 性能不好
    //        val dbSearcher: DbSearcher = new DbSearcher(new DbConfig(), ApplicationConfig.DATAS_PATH)

    val dataBlock: DataBlock = dbSearcher.btreeSearch(ip)

    //获取解析的省份城市
    val region = dataBlock.getRegion

    val Array(_, _, province, city, _) = region.split("\\|")

    Region(ip, province, city)
  }
}
