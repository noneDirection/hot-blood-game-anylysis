package com.qf.biz.realtime.spark.version1_streaming

import java.text.SimpleDateFormat

import com.qf.utils.JedisUtils
import kafka.serializer.StringDecoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Description：实时统计（根据实时产生的游戏日志信息，追踪出那些开了外挂的游戏玩家）<br/>
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


    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

    //①采用direct方式从kafka中实时读取数据
    //序列化：将内存中的实例（数据）输出到网络上或是指定的存储介质上存储起来的过程。
    //反序列化：从网络上读取数据到内存中，或者从存储介质上读取数据到内存重构实例的过程。
    val dsFromKafka: DStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      Map("metadata.broker.list" -> "NODE01:9092,NODE02:9092,NODE03:9092"),
      Set("realTimeGameLog")
    ).map(_._2)


    //②实时筛选出“吃疗伤药”的玩家信息，并抽离出关注的信息：用户名,时间
    //1	0	0	10	3	324	325	风道	裁决之杖	96609	1	装备回收	2018-02-01 11:08:07
    val filterAndStandardDS: DStream[(String, Long)] = dsFromKafka.filter(perMsg => {
      val arr = perMsg.split("\t")
      //事件类型是：11  →索引号是：3
      val eventType = arr(3).trim.toInt
      //道具的名字是：疗伤药 →索引号是：8
      val item = arr(8).trim

      //返回
      eventType == 11 && "疗伤药".equals(item)
    }).map(perMsg => {
      val arr = perMsg.split("\t")
      // 玩家名
      val userName = arr(7).trim

      // 玩游戏动作触发执行的时间
      val time = arr(12).trim

      //返回 ,时间格式：2018-02-01 11:08:07
      (userName, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time).getTime)
    })


    //③根据业务剥离出开了外挂的玩家：
    //下述DStream中的每个元素形如：(张无忌,[123400,1234023,12340000...])
    val groupAfterDS: DStream[(String, Iterable[Long])] = filterAndStandardDS.groupByKeyAndWindow(Seconds(10), Seconds(5))

    //a)每隔5s，统计过去10s内吃疗伤药的次数>=5  →初次筛选 （疑似开了外挂的玩家）
    val blackLstPlayer: DStream[(String, Double)] = groupAfterDS.filter(_._2.size >= 5)
      //b)平均每次吃疗伤药的时间间隔<1s
      .map(perEle => {
      //将当前玩家吃药的所有时间进行排序
      val sortedLst = perEle._2.toList.sortWith(_ > _)

      //求出最后一次吃药的时间
      val lastTime = sortedLst(0)

      //求出第一次吃药的时间
      val firstTime = sortedLst(sortedLst.size - 1)

      //求平均时间
      val avgTime = (lastTime - firstTime) * 1.0 / sortedLst.size

      //返回
      (perEle._1, avgTime)
    }).filter(_._2 < 1 * 1000)

    //④将开了外挂的玩家的信息保存到redis中  →到这一步就可以了
    //将开了外挂的玩家名，以及每次吃疗伤药的平均时间落地到db中
    //foreachRDD其实此时，DStream中只包含要给RDD,不是多个！！可以与RDD的其他算子之mapPartitions,foreachPartitions进行类比
    blackLstPlayer.foreachRDD(rdd => {
      if (!rdd.isEmpty) {
        rdd.foreachPartition(itr => {
          if (!itr.isEmpty) {

            //获得JedisCluster的实例
            val jedisCluster = JedisUtils.getJedisInstanceFromPool

            //设想：illegalUsers 张三 1.24 李四 2.45
            //redis：读→ 11万次/秒；写：8.1万次/秒
            itr.foreach(perEle => {
              jedisCluster.hset("illegalUsers", perEle._1, perEle._2.toString)
            })

            //释放
            JedisUtils.resourceRelease(jedisCluster)
          }
        })
      }
    })


    //⑤启动
    ssc.start

    //⑥等待结束
    ssc.awaitTermination
  }
}
