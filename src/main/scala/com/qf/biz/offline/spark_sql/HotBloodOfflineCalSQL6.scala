package com.qf.biz.offline.spark_sql

import java.text.SimpleDateFormat

import com.qf.common.CommonData
import com.qf.dao.IGameAnaylysisResult
import com.qf.dao.impl.GameAnaylysisResultImpl
import com.qf.entity.GameAnaylysisResultBean
import com.qf.utils.CommonUtil
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Description：离线统计（新增用户，活跃用户，次日留存率~七日留存率）→ Spark SQL优化版, sql合并版(新增用户，活跃用户，次日留存率~7日留存率)，落地到db中<br/>
  * Copyright (c) ，2019 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年12月03日
  *
  * @author 徐文波
  * @version : 1.0
  */
object HotBloodOfflineCalSQL6 {

  def main(args: Array[String]): Unit = {
    //记录开始时间
    val beginTime = System.currentTimeMillis


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
    //b)活跃用户数
    //c)次日留存率
    val row = spark.sql(
      s"""
         |select
         |    max(newUser) `新增用户数`,
         |    max(activeUser) `活跃用户数`,
         |    max(nextDaystayCnt)*100.0/max(newUser) `次日留存率`,
         |    max(twoDaystayCnt)*100.0/max(newUser) `2日留存率`,
         |    max(threeDaystayCnt)*100.0/max(newUser) `3日留存率`,
         |    max(fourDaystayCnt)*100.0/max(newUser) `4日留存率`,
         |    max(fiveDaystayCnt)*100.0/max(newUser) `5日留存率`,
         |    max(sixDaystayCnt)*100.0/max(newUser) `6日留存率`,
         |    max(sevenDaystayCnt)*100.0/max(newUser) `7日留存率`
         | from
         | (
         |   (select
         |     count(distinct userName) `newUser`,
         |     0 `activeUser`,
         |     0 `nextDaystayCnt`,
         |     0 `twoDaystayCnt`,
         |     0 `threeDaystayCnt`,
         |     0 `fourDaystayCnt`,
         |     0 `fiveDaystayCnt`,
         |     0 `sixDaystayCnt`,
         |     0 `sevenDaystayCnt`
         |   from tb_game
         |   where  eventType='1' and (
         |                                     getTimeMills(time) >= UNIX_TIMESTAMP('$baseDate','yyyy-MM-dd') * 1000  and
         |                                     getTimeMills(time) < UNIX_TIMESTAMP(DATE_ADD('$baseDate',1),'yyyy-MM-dd')*1000)
         |   )
         |
         |   union all
         |
         |   (select
         |    0  `newUser`,
         |     count(distinct userName) `activeUser`,
         |     0 `nextDaystayCnt`,
         |     0 `twoDaystayCnt`,
         |     0 `threeDaystayCnt`,
         |     0 `fourDaystayCnt`,
         |     0 `fiveDaystayCnt`,
         |     0 `sixDaystayCnt`,
         |     0 `sevenDaystayCnt`
         |   from tb_game
         |   where  (eventType in ('1','2')) and (
         |                                     getTimeMills(time) >= UNIX_TIMESTAMP('$baseDate','yyyy-MM-dd') * 1000  and
         |                                     getTimeMills(time) < UNIX_TIMESTAMP(DATE_ADD('$baseDate',1),'yyyy-MM-dd')*1000)
         |   )
         |
         |   union all
         |
         |   (select
         |     0 `newUser`,
         |     0 `activeUser`,
         |     count(distinct nextDayUser) `nextDaystayCnt`,
         |     count(distinct twoDayUser)  `twoDaystayCnt`,
         |     count(distinct threeDayUser)  `threeDaystayCnt`,
         |     count(distinct fourDayUser)  `fourDaystayCnt`,
         |     count(distinct fiveDayUser)  `fiveDaystayCnt`,
         |     count(distinct sixDayUser)  `sixDaystayCnt`,
         |     count(distinct sevenDayUser)  `sevenDaystayCnt`
         |   from
         |       (select
         |         case when
         |            (getTimeMills(t2.time) >= UNIX_TIMESTAMP(DATE_ADD('$baseDate',1),'yyyy-MM-dd')*1000  and getTimeMills(t2.time) < UNIX_TIMESTAMP(DATE_ADD('$baseDate',2),'yyyy-MM-dd')*1000)
         |         then t1.userName end `nextDayUser`,
         |
         |         case when
         |            (getTimeMills(t2.time) >= UNIX_TIMESTAMP(DATE_ADD('$baseDate',2),'yyyy-MM-dd')*1000  and getTimeMills(t2.time) < UNIX_TIMESTAMP(DATE_ADD('$baseDate',3),'yyyy-MM-dd')*1000)
         |         then t1.userName end `twoDayUser`,
         |
         |         case when
         |            (getTimeMills(t2.time) >= UNIX_TIMESTAMP(DATE_ADD('$baseDate',3),'yyyy-MM-dd')*1000  and getTimeMills(t2.time) < UNIX_TIMESTAMP(DATE_ADD('$baseDate',4),'yyyy-MM-dd')*1000)
         |         then t1.userName end `threeDayUser`,
         |
         |         case when
         |            (getTimeMills(t2.time) >= UNIX_TIMESTAMP(DATE_ADD('$baseDate',4),'yyyy-MM-dd')*1000  and getTimeMills(t2.time) < UNIX_TIMESTAMP(DATE_ADD('$baseDate',5),'yyyy-MM-dd')*1000)
         |         then t1.userName end `fourDayUser`,
         |
         |         case when
         |            (getTimeMills(t2.time) >= UNIX_TIMESTAMP(DATE_ADD('$baseDate',5),'yyyy-MM-dd')*1000  and getTimeMills(t2.time) < UNIX_TIMESTAMP(DATE_ADD('$baseDate',6),'yyyy-MM-dd')*1000)
         |         then t1.userName end `fiveDayUser`,
         |
         |         case when
         |            (getTimeMills(t2.time) >= UNIX_TIMESTAMP(DATE_ADD('$baseDate',6),'yyyy-MM-dd')*1000  and getTimeMills(t2.time) < UNIX_TIMESTAMP(DATE_ADD('$baseDate',7),'yyyy-MM-dd')*1000)
         |         then t1.userName end `sixDayUser`,
         |
         |         case when
         |            (getTimeMills(t2.time) >= UNIX_TIMESTAMP(DATE_ADD('$baseDate',7),'yyyy-MM-dd')*1000  and getTimeMills(t2.time) < UNIX_TIMESTAMP(DATE_ADD('$baseDate',8),'yyyy-MM-dd')*1000)
         |         then t1.userName end `sevenDayUser`
         |       from
         |          tb_game t1, -- t1:存储的是基准日的用户日志信息
         |          tb_game t2  -- t2:存储的是次日~七日的用户日志信息
         |       where   (t1.userName=t2.userName) and -- 去除笛卡尔积
         |                   (t2.eventType='2') and -- 次日的事件类型只是登录
         |                   (t1.eventType='1') and -- 基准日的事件类型只是注册
         |                   (getTimeMills(t1.time) >= UNIX_TIMESTAMP('$baseDate','yyyy-MM-dd') * 1000  and getTimeMills(t1.time) < UNIX_TIMESTAMP(DATE_ADD('$baseDate',1),'yyyy-MM-dd')*1000) -- 基准日的时间
         |       )
         |    )
         |)
      """.stripMargin)
      .rdd.first


    //取出各个字段值
    val date = baseDate;
    val newAddUserCnt = row.getAs[Long]("新增用户数")
    val activeUserCnt = row.getAs[Long]("活跃用户数")
    val nextDayRate = f"${row.getAs[java.math.BigDecimal]("次日留存率")}%.2f".concat("%")
    val twoDayRate = f"${row.getAs[java.math.BigDecimal]("2日留存率")}%.2f".concat("%")
    val threeDayRate = f"${row.getAs[java.math.BigDecimal]("3日留存率")}%.2f".concat("%")
    val fourDayRate = f"${row.getAs[java.math.BigDecimal]("4日留存率")}%.2f".concat("%")
    val fiveDayRate = f"${row.getAs[java.math.BigDecimal]("5日留存率")}%.2f".concat("%")
    val sixDayRate = f"${row.getAs[java.math.BigDecimal]("6日留存率")}%.2f".concat("%")
    val sevenDayRate = f"${row.getAs[java.math.BigDecimal]("7日留存率")}%.2f".concat("%")


    //⑥将计算后的结果落地到RDBMS中固化起来
    //①Dao实例准备
    val dao: IGameAnaylysisResult = new GameAnaylysisResultImpl

    //②构建实例
    val bean = new GameAnaylysisResultBean(date,
      newAddUserCnt,
      activeUserCnt,
      nextDayRate,
      twoDayRate,
      threeDayRate,
      fourDayRate,
      fiveDayRate,
      sixDayRate,
      sevenDayRate
    )
    //save
    dao.save(bean)

    //⑦资源释放
    spark.stop


    //记录结束时间
    val endTime = System.currentTimeMillis
    println(s"Spark SQL，sql合在一起书写，一共耗时：${endTime - beginTime}毫秒")
    //→ Spark SQL，sql合在一起书写，一共耗时：43782毫秒
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
      .config("es.nodes", "node01,node02,node03")
      .config("port", "9200")
      .getOrCreate

    val sc = spark.sparkContext

    (spark, sc)
  }
}
