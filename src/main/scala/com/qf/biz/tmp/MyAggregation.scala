package com.qf.biz.tmp

import org.apache.flink.api.common.functions.AggregateFunction

import scala.collection.mutable.ListBuffer

/**
  * @program: hotbloodgameanylysis
  * @description: 自定义聚合函数（UDAF)
  * @author: youzhao
  * @create: 2019-12-12 10:19
  **/
object MyAggregation extends AggregateFunction[(String,Long),MyAccumulator,MyAccumulator]{
  val container = ListBuffer[Long]()
  override def add(value: (String, Long), accumulator: MyAccumulator) ={
    accumulator.userName = value._1

    accumulator.cnt += 1

    container.append(value._2)

    container.sortWith(_ < _)


    accumulator.maxTime = math.max(container(container.size - 1), container(0))
    accumulator.minTime = math.min(container(container.size - 1), container(0))

    accumulator.avgTime = (accumulator.maxTime - accumulator.minTime) * 1.0 / accumulator.cnt

    // println(s"容器中的元素: 最大→ ${accumulator.maxTime},最小→${accumulator.minTime}")

    accumulator
  }

  override def createAccumulator() = {
    container.clear
    MyAccumulator("", 0, 0, 0, 0.0)
  }

  override def getResult(acc: MyAccumulator) = acc

  override def merge(a: MyAccumulator, b: MyAccumulator) = {
      a.cnt += b.cnt

      if (a.maxTime < b.maxTime)
        a.maxTime = b.maxTime


      if (a.minTime > b.minTime)
        a.minTime = b.minTime

      a.avgTime = (a.maxTime - a.minTime) * 1.0 / a.cnt

      a
  }
}
case class MyAccumulator(var userName:String,var cnt: Long, var maxTime:Long,var minTime:Long,var avgTime: Double)