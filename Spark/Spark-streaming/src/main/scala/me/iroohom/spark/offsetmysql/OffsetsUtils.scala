package me.iroohom.spark.offsetmysql

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import scala.collection.mutable
/**
 * 将消费Kafka Topic偏移量数据存储MySQL数据库，工具类用于读取和保存偏移量数据
 * 表的创建语句：
CREATE TABLE `tb_offset` (
`topic` varchar(255) NOT NULL,
`partition` int NOT NULL,
`groupid` varchar(255) NOT NULL,
`offset` bigint NOT NULL,
PRIMARY KEY (`topic`,`partition`,`groupid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
 */
object OffsetsUtils {
  /**
   * 依据Topic名称和消费组GroupId获取各个分区的偏移量
   *
   * @param topicNames Topics名称
   * @param groupId 消费组ID
   */
  def getOffsetsToMap(topicNames: Array[String], groupId: String): Map[TopicPartition, Long] ={
    // 构建集合
    val map: mutable.Map[TopicPartition, Long] = scala.collection.mutable.Map[TopicPartition, Long]()
    // 声明变量
    var conn: Connection = null
    var pstmt: PreparedStatement = null
    var result: ResultSet = null
    try{
      // a. 加载驱动类
      Class.forName("com.mysql.cj.jdbc.Driver")
      // b. 获取连接
      conn = DriverManager.getConnection(
        "jdbc:mysql://node1.itcast.cn:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true",//
        "root", //
        "123456" //
      )
      // c. 编写SQL，获取PreparedStatement对象
      val topicNamesStr = topicNames.map(topicName => s"\'$topicName\'").mkString(", ")
      val querySQL =
        s"""
           |SELECT
           | `topic`, `partition`, `groupid`, `offset`
           |FROM
           | db_spark.tb_offset
           |WHERE
           | groupid = ? AND topic in ($topicNamesStr)
           |""".stripMargin
      pstmt = conn.prepareStatement(querySQL)
      pstmt.setString(1, groupId)
      // d. 查询数据
      result = pstmt.executeQuery()
      // e. 遍历获取值
      while (result.next()){
        val topicName = result.getString("topic")
        val partitionId = result.getInt("partition")
        val offset = result.getLong("offset")
        // 加入集合中
        map += new TopicPartition(topicName, partitionId) -> offset
      }
    }catch {
      case e: Exception => e.printStackTrace()
    }finally {
      if(null != result) result.close()
      if(null != pstmt) pstmt.close()
      if(null != conn) conn.close()
    }
    // 返回集合，转换为不可变的
    map.toMap
  }
  /**
   * 保存Streaming每次消费Kafka数据后最新偏移量到MySQL表中
   *
   * @param offsetRanges Topic中各个分区消费偏移量范围
北京市昌平区建材城西路金燕龙办公楼一层 电话：400-618-9090
   * @param groupId 消费组ID
   */
  def saveOffsetsToTable(offsetRanges: Array[OffsetRange], groupId: String): Unit = {
    // 声明变量
    var conn: Connection = null
    var pstmt: PreparedStatement = null
    try{
      // a. 加载驱动类
      Class.forName("com.mysql.cj.jdbc.Driver")
      // b. 获取连接
      conn = DriverManager.getConnection(
        "jdbc:mysql://node1.itcast.cn:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true", //
        "root", //
        "123456" //
      )
      // c. 编写SQL，获取PreparedStatement对象
      val insertSQL = "replace into db_spark.tb_offset (`topic`, `partition`, `groupid`, `offset`) values (?, ?, ?, ?)"
      pstmt = conn.prepareStatement(insertSQL)
      // d. 设置参数
      offsetRanges.foreach{offsetRange =>
        pstmt.setString(1, offsetRange.topic)
        pstmt.setInt(2, offsetRange.partition)
        pstmt.setString(3, groupId)
        pstmt.setLong(4, offsetRange.untilOffset)
        // 加入批次
        pstmt.addBatch()
      }
      // e. 批量插入
      pstmt.executeBatch()
    }catch {
      case e: Exception => e.printStackTrace()
    }finally {
      if(null != pstmt) pstmt.close()
      if(null != conn) conn.close()
    }
  }
  def main(args: Array[String]): Unit = {
    /*
    saveOffsetsToTable(
    Array(
    OffsetRange("xx-tp", 0, 11L, 100L), OffsetRange("xx-tp", 1, 11L, 100L),
    OffsetRange("yy-tp", 0, 10L, 500L), OffsetRange("yy-tp", 1, 10L, 500L)
    ),
    "group_id_00001"
    )
    */
    //getOffsetsToMap(Array("xx-tp"), "group_id_00001").foreach(println)
  }
}