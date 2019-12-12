package com.qf.biz.realtime.spark.version2_structured

import com.qf.utils.JedisUtils
import org.apache.spark.sql.{ForeachWriter, Row}
import redis.clients.jedis.{Jedis, JedisCluster}

/**
  * Description：Structured Streaming计算后的结果实时落地到redis中<br/>
  * Copyright (c) ，2019 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年12月09日  
  *
  * @author 徐文波
  * @version : 1.0
  */
class RedisSink extends ForeachWriter[Row] {
  var jedisCluster: Jedis =_

  /**
    * 打开
    *
    * @param partitionId
    * @param version
    * @return
    */
  override def open(partitionId: Long, version: Long): Boolean = {
    jedisCluster = JedisUtils.getJedisInstanceFromPool
    true
  }

  /**
    * 处理
    *
    * @param value
    */
  override def process(value: Row): Unit = {
    //写入数据到redis
    val userName = value.getAs[String]("userName").trim
    val avgTime = value.getAs[Double]("avgTime").toString.trim
    jedisCluster.hset("illegalUsers", userName, avgTime)
  }

  /**
    * 资源释放
    *
    * @param errorOrNull
    */
  override def close(errorOrNull: Throwable): Unit = {
    JedisUtils.resourceRelease(jedisCluster)
  }
}
