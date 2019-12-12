package com.qf.offline

import org.apache.spark.sql.SparkSession

/**
  * Description：离线统计（新增用户，活跃用户，次日留存率）<br/>
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

    //④从ES中读取数据，并cache
    import org.elasticsearch.spark._

    val query=
      """
        |{
        |  "query": {
        |    "match_all": {}
        |  }
        |}
      """.stripMargin

    sc.esRDD("gamelog",query)
      .foreach(println)

    // ⑤分别计算指标
    //a)新增用户数
    //b)活跃用户数
    //c)次日留存率

    //⑥将计算后的结果落地到RDBMS中固化起来

    //⑦资源释放
    spark.stop
  }
}
