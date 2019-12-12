package com.qf.biz.scheduler

import java.util.{Timer, TimerTask}

import com.qf.dao.IBlackListDao
import com.qf.dao.impl.BlackListDaoImpl
import com.qf.entity.BlackList
import com.qf.utils.JedisUtils

/**
  * Description：定时器 （热血传奇游戏运营项目完善→开启定时器，读取redis中的数据，写入到rdbms中存储起来）,使用批处理进行优化<br/>
  * Copyright (c) ，2019 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年12月07日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object GameScheduler2 {
  def main(args: Array[String]): Unit = {

    //步骤：
    //前提：
    // a)准备JedisCluster的实例
    val jedisCluster = JedisUtils.getJedisInstanceFromPool
    //b)dao层
    val dao: IBlackListDao = new BlackListDaoImpl

    //c)准备一个容器
    val container: java.util.List[BlackList] = new java.util.LinkedList[BlackList]


    //①Timer
    val timer = new Timer

    //②开启定时器
    //TimerTask task →需要定时执行的任务
    // long delay → 第一次开始执行时，需要延迟的时间
    // long period →任务每次执行间隔的时间
    timer.schedule(new TimerTask {
      override def run(): Unit = {
        //思路：
        //a)读取redis中存储的开了外挂的游戏玩家的信息（类型是：hash,key→illegalUsers）
        val blackLstUsers: java.util.Map[String, String] = jedisCluster.hgetAll("illegalUsers")

        //b)将结果存入到rdbms中 （业务拓展，如：给每个开了外挂的玩家三次机会，若超过，由游戏后台的运营人员立马注销该玩家的账户）

        import scala.collection.JavaConverters._

        val map = blackLstUsers.asScala
        for ((k, v) <- map) {
          val bean = new BlackList(k.toString, v.toDouble, 1)
          container.add(bean)
        }

        //调用dao，正式操作db server
        dao.batachDealWith(container)

        //清空容器
        container.clear

      }
    }, 0,
      2000)
  }
}
