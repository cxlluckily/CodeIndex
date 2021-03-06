package me.iroohom.spark.iot

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.json4s.jackson.Json

import scala.util.Random

object MockIotDatas {
    
    def main(args: Array[String]): Unit = {
        
        // 发送Kafka Topic
        val props = new Properties()
        props.put("bootstrap.servers", "node1.roohom.cn:9092")
        props.put("acks", "1")
        props.put("retries", "3")
        props.put("key.serializer", classOf[StringSerializer].getName)
        props.put("value.serializer", classOf[StringSerializer].getName)
        val producer = new KafkaProducer[String, String](props)
        
        val deviceTypes = Array(
            "db", "bigdata", "kafka", "route", "bigdata", "db", "bigdata", "bigdata", "bigdata"
        )
        
        val random: Random = new Random()
        while (true){
            val index: Int = random.nextInt(deviceTypes.length)
            val deviceId: String = s"device_${(index +1) * 10 + random.nextInt(index + 1)}"
            val deviceType: String = deviceTypes(index)
            val deviceSignal: Int = 10 + random.nextInt(90)
            // 模拟构造设备数据
            val deviceData = DeviceData(deviceId, deviceType, deviceSignal, System.currentTimeMillis())
            // 转换为JSON字符串
            val deviceJson: String = new Json(org.json4s.DefaultFormats).write(deviceData)
            println(deviceJson)
            Thread.sleep(100 + random.nextInt(500))
            
            val record = new ProducerRecord[String, String]("iotTopic", deviceJson)
            producer.send(record)
        }
        
        // 关闭连接
        producer.close()
    }
    
}
