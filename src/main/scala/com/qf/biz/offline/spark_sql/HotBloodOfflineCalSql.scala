package com.qf.biz.offline.spark_sql

import java.text.SimpleDateFormat

import com.qf.common.CommonData
import com.qf.utils.CommonUtil
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Description：离线统计（新增用户，活跃用户，次日留存率~七日留存率）→ Spark SQL版<br/>
  * Copyright (c) ，2019 ，Young <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019-12-05
  *
  * @author 李金宜
  */
object HotBloodOfflineCalSql {
 def main(args: Array[String]): Unit = {
    //步骤：
    //①拦截非法的参数
    if (args == null || args.length != 1) {
      println(
        """
          |警告！
          |请录入参数！ <基准日>，如：2018-02-01
          |
        """.stripMargin)
      sys.exit(-1)
    }


    //②获得参数（基准日）
    val Array(baseDate) = args


    //③SparkSession
    val (spark, sc) = getSparkContext

    //④从ES中读取数据，并cache虚拟表
    readDataFromES(spark, sc)

    // ⑤分别计算指标

    //注册自定义函数
    spark.udf.register("getTimeMills", (timeStr: String) => processTime(timeStr))

    //a)新增用户数
    //-- 事件类型=1 && （time>=基准日 && time<基准日下一天）
    spark.sql(
      s"""
         |select
         |  count(distinct userName) `新增用户`
         |from tb_game
         |where  eventType='1' and (
         |                                  getTimeMills(time) >= UNIX_TIMESTAMP('$baseDate','yyyy-MM-dd') * 1000  and
         |                                  getTimeMills(time) < UNIX_TIMESTAMP(DATE_ADD('$baseDate',1),'yyyy-MM-dd')*1000)
          """.stripMargin
    ).show



    //b)活跃用户数
    spark.sql(
      s"""
         |select
         |  count(distinct userName) `活跃用户`
         |from tb_game
         |where  (eventType in ('1','2')) and (
         |                                  getTimeMills(time) >= UNIX_TIMESTAMP('$baseDate','yyyy-MM-dd') * 1000  and
         |                                  getTimeMills(time) < UNIX_TIMESTAMP(DATE_ADD('$baseDate',1),'yyyy-MM-dd')*1000)
          """.stripMargin
    ).show


    //c)次日留存率
    spark.sql(
      s"""
         |select
         |  count(distinct t1.userName) `次日留存数`
         |from tb_game t1,tb_game t2 -- t1:存储的是基准日的用户日志信息； t2:存储的是次日的用户日志信息
         |where
         |  (t1.userName=t2.userName) and -- 去除笛卡尔积
         |  (t1.eventType='1') and -- 基准日的事件类型只是注册
         |  ( t2.eventType='2') and -- 次日的事件类型只是登录
         |  ( getTimeMills(t1.time) >= UNIX_TIMESTAMP('$baseDate','yyyy-MM-dd') * 1000  and getTimeMills(t1.time) < UNIX_TIMESTAMP(DATE_ADD('$baseDate',1),'yyyy-MM-dd')*1000) and -- 基准日的时间
         |  ( getTimeMills(t2.time) >= UNIX_TIMESTAMP(DATE_ADD('$baseDate',1),'yyyy-MM-dd')*1000  and getTimeMills(t2.time) < UNIX_TIMESTAMP(DATE_ADD('$baseDate',2),'yyyy-MM-dd')*1000)
          """.stripMargin
    ).show


    //⑥将计算后的结果落地到RDBMS中固化起来
    //save2DB(baseDate, newUserCnt, activeUserCnt, container)

    //⑦资源释放
    spark.stop
  }


  /**
    * 处理时间，形如：yyyy年MM月dd日,E,HH:mm:ss，如：2018年2月1日,星期一,10:02:05
    *
    * @param time
    */
  private def processTime(time: String) = {
    val sdf = new SimpleDateFormat(CommonUtil.getPropertiesValueByKey(CommonData.TIME_PATTERN2))
    val date = sdf.parse(time)
    val baseDateMillis: Long = date.getTime

    //返回
    baseDateMillis
  }


  /**
    * 从es分布式集群中读取数据，并对数据进行标准化，清洗掉脏数据，将rdd转换成DataFrame，将DataFrame映射为一张虚拟表，然后，将虚拟表缓存起来
    *
    * @param spark
    * @param sc
    * @return
    */
  private def readDataFromES(spark: SparkSession, sc: SparkContext) = {

    //spark sql相关
    import spark.implicits._

    //es相关
    import org.elasticsearch.spark._


    val query =
      """
        |{
        |  "query": {
        |    "match_all": {}
        |  }
        |}
      """.stripMargin

    //RDD→ DataFrame
    val df: DataFrame = sc.esRDD("gamelog", query)
      .map(perEle => {
        val record = perEle._2
        //用户名
        val userName = record.getOrElse("userName", "").asInstanceOf[String]
        //事件类型
        val eventType = record.getOrElse("eventType", "").asInstanceOf[String]
        //玩游戏的时点
        val time = record.getOrElse("time", "").asInstanceOf[String]
        //客户端ip
        val ip = record.getOrElse("ip", "").asInstanceOf[String]
        (userName, eventType, time, ip)
      }) //从es记录中筛选出需要的field
      .filter(perEle => { //将没有ip的记录信息过滤掉
      val ip: String = perEle._4
      val regex = """(\d{1,3}\.){3}\d{1,3}""" //若是正则表达式中包含特殊的符号，使用三双引号，不用转义
      ip.matches(regex)
    }).toDF("userName", "eventType", "time", "ip")

    //DataFrame映射为虚拟表
    df.createOrReplaceTempView("tb_game")

    //缓存表
    spark.sqlContext.cacheTable("tb_game")
  }

  /**
    * 获得SparkContext的实例
    *
    * @return
    */
  private def getSparkContext = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .config("es.nodes", "master,slave1,slave2")
      .config("port", "9200")
      .getOrCreate

    val sc = spark.sparkContext

    (spark, sc)
  }
}
