package com.qf.biz.realtime.spark.version2_structured

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession

/**
  * Description：实时统计（根据实时产生的游戏日志信息，追踪出那些开了外挂的游戏玩家）,使用Structured Streaming实现<br/>
  * Copyright (c) ，2019 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年12月06日
  *
  * @author 徐文波
  * @version : 1.0
  */
object HotBloodRealTime {
  def main(args: Array[String]): Unit = {
    //步骤：

    val spark = SparkSession
      .builder
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate


    import spark.implicits._
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "NODE01:9092,NODE02:9092,NODE03:9092")
      .option("subscribe", "realTimeGameLog")
      .option("startingOffsets", "latest")
      .option("includeTimestamp", true) //在kafka中的每条消息后附着一个名为imestamp字段信息，用来记录被structured steraming处理的时间
      .load() //使用window函数要导入

    // 从kafka中读取数据进行处理
    df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
      .as[(String, Timestamp)]
      .map(perEle => {
        val arr = perEle._1.split("\t")

        //事件类型是：11  →索引号是：3
        val eventType = arr(3).trim.toInt

        val userName = arr(7).trim

        //道具的名字是：疗伤药 →索引号是：8
        val item = arr(8).trim

        val time = arr(12).trim

        //返回
        (userName, eventType, item, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time).getTime, perEle._2)
      }).
      filter(perEle => {
        //返回
        perEle._2 == 11 && "疗伤药".equals(perEle._3)
      }).toDF("userName", "eventType", "item", "time", "timestamp") //定制DataFrame字段名
      //.withWatermark("timestamp", "10 seconds")//← 可以注释掉，没有影响
      .createOrReplaceTempView("tb_gamelog")


    spark.sql(
      s"""
         |select
         |   userName ,
         |   avgTime
         |from(
         |   select
         |       userName,
         |       totalCnt,
         |       cast((maxTime-minTime)*1.0/totalCnt as decimal(18,4)) avgTime
         |   from(
         |       select
         |           userName,
         |           count(time) totalCnt,
         |           max(time) maxTime,
         |           min(time) minTime
         |       from tb_gamelog
         |       group by window(timestamp,'10 seconds','5 seconds'),userName
         |   ) t
         |) t2 where t2. totalCnt>=5 and t2.avgTime<1000
            """.stripMargin)

    //方式1：结果输出到控制台
          .writeStream
          .outputMode("complete") //聚合操作的显示必须要使用complete,非聚合要使用append
          //从数据安全性的角度考虑，检查点最好设置，checkpoint的目的地一般设置为：hdfs
         // .option("checkpointLocation", "a_data/output/ck")
          .format("console") //控制台输出
          .option("truncate", "false") //是输出的信息是否截断
          .start()

          .awaitTermination() //等待结束，必须要写。

    // println("\n____________________________________________\n")

    //方式2：结果输出到redis
//          .writeStream
//          .outputMode("complete")
//          .option("checkpointLocation", "a_data/output/ck")
//          .foreach(new RedisSink())
//          .start()
//          .awaitTermination()

  }
}
