package com.qf.biz.offline.spark_core

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.qf.common.{CommonData, EventType}
import com.qf.utils.CommonUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Description：离线统计（新增用户，活跃用户，次日留存率）→ Spark Core版本<br/>
  * Copyright (c) ，2019 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年12月03日
  *
  * @author 徐文波
  * @version : 1.0
  */
object HotBloodOfflineCal {


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


    //print(s"基准日：$baseDate")

    //③SparkSession
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .config("es.nodes", "node01,node02,node03")
      .config("port", "9200")
      .getOrCreate

    val sc = spark.sparkContext

    //将基准日信息封装到广播变量中
    val bcBaseDate = sc.broadcast(baseDate)

    //④从ES中读取数据，并cache
    val rddFromES: RDD[(String, String, String, String)] = readDataFromES(sc)


    // ⑤分别计算指标
    //a)新增用户数
    val newUserRDD: RDD[(String, String)] = calNewAddUser(sc, rddFromES, bcBaseDate).cache
    val newUserCnt = newUserRDD.count()
    println(s"新增用户数是：$newUserCnt")

    //b)活跃用户数
    val activeUserCnt: Long = calActiveUser(sc, rddFromES, bcBaseDate)
    println(s"活跃用户数是：$activeUserCnt")

    //c)次日留存率 = (基准日注册的用户RDD join 次日登录的用户RDD).count / 基准日注册的用户数
    val nextDayStayRate: Double = calNextDayStayRate(sc, newUserCnt, newUserRDD, rddFromES, bcBaseDate)
    println(f"次日留存率是：${nextDayStayRate * 100}%.2f".concat("%"))

    //⑥将计算后的结果落地到RDBMS中固化起来

    //⑦资源释放
    spark.stop
  }


  /**
    * 计算次日留存率
    *
    * @param sc
    * @param newUserCnt
    * @param newUserRDD
    * @param rddFromES
    * @param bcBaseDate
    * @return
    */
  def calNextDayStayRate(sc: SparkContext, newUserCnt: Long, newUserRDD: RDD[(String, String)], rddFromES: RDD[(String, String, String, String)], bcBaseDate: Broadcast[String]): Double = {
    //步骤：
    //①基准日的注册用户数
    //②次日留存数 = ( 基准日注册的用户RDD join 次日登录的用户RDD).count
    val baseDateRDD: RDD[(String, String)] = newUserRDD;
    val nextDayLoginRDD: RDD[(String, String)] = calBaseDayNewAddRDD(sc, rddFromES, bcBaseDate, 1)
    val nextDayStayCnt = baseDateRDD.join(nextDayLoginRDD).map(_._1).count

    //③次日留存率 = 次日留存数/基准日的注册用户数
    val nextDayStayRate = nextDayStayCnt.toDouble / newUserCnt

    //④返回
    nextDayStayRate
  }


  /**
    * 计算活跃用户数
    *
    * @param sc
    * @param rddFromES
    * @param bcBaseDate
    * @return
    */
  def calActiveUser(sc: SparkContext, rddFromES: RDD[(String, String, String, String)], bcBaseDate: Broadcast[String]): Long = {

    val nowDayActiveUserCnt = calBaseDayNewAddRDD(sc, rddFromES, bcBaseDate, 2).count
    //返回
    nowDayActiveUserCnt
  }


  /**
    * 计算新增用户的RDD
    *
    * @param rddFromES
    * @return
    */
  def calNewAddUser(sc: SparkContext, rddFromES: RDD[(String, String, String, String)], bcBaseDate: Broadcast[String]) = {

    //获得基准日的新增用户RDD
    val nowDayNewAddUserRDD = calBaseDayNewAddRDD(sc, rddFromES, bcBaseDate, 0)

    //返回
    nowDayNewAddUserRDD
  }


  /**
    * 获得基准日的新增用户RDD
    *
    * @param rddFromES
    * @param bcBaseDate
    * @param flg 0→ 注册用户；1→次日登录用户；2 → 活跃用户
    * @return
    */
  private def calBaseDayNewAddRDD(sc: SparkContext, rddFromES: RDD[(String, String, String, String)], bcBaseDate: Broadcast[String], flg: Int) = {

    //步骤：
    //①获得基准日信息，如：2018-02-01
    //②对rdd中满足条件的元素进行分析，计算，返回
    //条件：（玩游戏的时间 >= 基准日信息 and  玩游戏的时间 < 基准日的次日） 且 （事件类型 ==注册）
    val (baseDateMillisTmp, nextDayMillisTmp, nextDayStartMillisTmp, nextDayEndMillisTmp) = getBaseDayAndNextDayMillis(bcBaseDate)

    //将上述Driver进程中的变量封装到广播变量中，可以节省Executor进程的内存空间
    val bcTime = sc.broadcast((baseDateMillisTmp, nextDayMillisTmp, nextDayStartMillisTmp, nextDayEndMillisTmp))


    val nowDayNewAddRDD: RDD[(String, String)] = rddFromES.filter(perEle => {
      //从广播变量中获取时间
      val tmpTime = bcTime.value
      val baseDateMillis = tmpTime._1
      val nextDayMillis = tmpTime._2
      val nextDayStartMillis = tmpTime._3
      val nextDayEndMillis = tmpTime._4


      //时间
      val timeStr = perEle._3 //2018年2月1日,星期一,10:02:01
      val sdf = new SimpleDateFormat(CommonUtil.getPropertiesValueByKey(CommonData.TIME_PATTERN2))
      val timeMills: Long = sdf.parse(timeStr).getTime

      //事件类型
      val eventType = perEle._2

      //正式进行过滤
      val condition = flg match {
        case 0 => (timeMills >= baseDateMillis && timeMills < nextDayMillis) && (eventType.equals(EventType.REGISTER.getEventType))
        case 1 => (timeMills >= nextDayStartMillis && timeMills < nextDayEndMillis) && eventType.equals(EventType.LOGIN.getEventType)
        case 2 => (timeMills >= baseDateMillis && timeMills < nextDayMillis) && (eventType.equals(EventType.REGISTER.getEventType) || eventType.equals(EventType.LOGIN.getEventType))
      }

      //判断
      condition
    }).map(_._1)
      .distinct
      .map((_, ""))


    //返回
    nowDayNewAddRDD
  }


  /**
    * 获得基准日以及下一日对应的毫秒值，以及后天对应的毫秒值
    *
    * @param bcBaseDate
    * @return
    */
  def getBaseDayAndNextDayMillis(bcBaseDate: Broadcast[String]) = {
    val baseDateStr = bcBaseDate.value.concat(" 00:00:00")
    val sdf = new SimpleDateFormat(CommonUtil.getPropertiesValueByKey(CommonData.TIME_PATTERN))
    val date = sdf.parse(baseDateStr)
    val baseDateMillis: Long = date.getTime


    val nextDayMillis: Long = getSpecialDayMills(date, 1)


    //次日开始
    val nextDayStartMillis = nextDayMillis

    //次日结束
    val nextDayEndMillis: Long = getSpecialDayMills(date, 2)



    //println(s"开始时间：$date, 结束时间：${calendar.getTime}")//开始时间：Thu Feb 01 00:00:00 CST 2018, 结束时间：Fri Feb 02 00:00:00 CST 2018

    (baseDateMillis, nextDayMillis, nextDayStartMillis, nextDayEndMillis)
  }


  /**
    * 获得指定日的毫秒值
    *
    * @param date
    * @return
    */
  private def getSpecialDayMills(date: Date, distanceDayt: Int) = {
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.add(Calendar.DATE, distanceDayt)

    val nextDayMillis = calendar.getTimeInMillis
    nextDayMillis
  }

  /**
    * 从es分布式集群中读取数据，并对数据进行标准化，清洗掉脏数据，将最终的结果缓存起来
    *
    * @param sc
    */
  private def readDataFromES(sc: _root_.org.apache.spark.SparkContext) = {
    import org.elasticsearch.spark._

    val query =
      """
        |{
        |  "query": {
        |    "match_all": {}
        |  }
        |}
      """.stripMargin

    val rddFromES: RDD[(String, String, String, String)] = sc.esRDD("gamelog", query)
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
    }).cache

    //返回
    rddFromES
  }
}
