package com.qf.biz.tmp

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

/**
  * Description：自定义【带标点水印的赋值器】子类<br/>
  * Copyright (c) ，2019 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年12月09日  
  *
  * @author 徐文波
  * @version : 1.0
  */
class CustomWatermarkEmitter extends AssignerWithPunctuatedWatermarks[String] {
  override def checkAndGetNextWatermark(lastElement: String, extractedTimestamp: Long): Watermark = {
    val arr = lastElement.split("\t")
    val time = arr(12).trim
    new Watermark(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time).getTime)
  }

  override def extractTimestamp(element: String, previousElementTimestamp: Long): Long = {
    val arr = element.split("\t")
    val time = arr(12).trim
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time).getTime
  }
}