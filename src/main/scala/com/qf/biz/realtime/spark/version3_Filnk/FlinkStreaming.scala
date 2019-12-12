package com.qf.biz.realtime.spark.version3_Filnk

import java.text.SimpleDateFormat
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.windowing.time.Time
/**
  * @program: hotbloodgameanylysis
  * @description: FlinkStreaming
  * @author: youzhao
  * @create: 2019-12-12 09:42
  **/
object FlinkStreaming {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties();
    properties.put("bootstrap.servers", "master:9092");
    properties.put("zookeeper.connect", "master:2181");
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("group.id", "test6");

    val consumer: FlinkKafkaConsumer010[String] = new FlinkKafkaConsumer010("myItems_topic5", new SimpleStringSchema(),properties)

    val datasource: DataStream[String] = env.addSource(consumer)

    datasource.map(x => {
      val arr = x.split("\t")
      //事件类型（11） 索引号（3）
      val eventType = arr(3).trim.toInt
      //疗伤药 索引号8
      val item = arr(8).trim
      //玩家名
      val userName = arr(7).trim
      //记录生成的时间
      val time = arr(12).trim

      (userName, eventType, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time).getTime, item)
    }).filter(x => x._2 == "11" && "疗伤药".equals(x._4))
      .map(x => (x._1, x._3))
      .keyBy(0)
      .timeWindow(Time.of(10, TimeUnit.SECONDS),Time.of(5,TimeUnit.SECONDS))
  }
}
