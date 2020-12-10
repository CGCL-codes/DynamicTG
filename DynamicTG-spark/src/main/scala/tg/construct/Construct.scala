package com.spark.tg.construct

import org.apache.spark._
import org.apache.spark.streaming._

class Construct {
  def process(): Unit = {
    val conf = new SparkConf().setAppName("tg-construct")
    val ssc = new StreamingContext(conf, Seconds(1))

    val events = ssc.receiverStream()
  }
}
