package tg.construct

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import tg.dtg.common.values.NumericValue
import tg.dtg.events.{Event, EventTemplate}
import tg.dtg.query.Predicate

class Construct(val eventTemplate: EventTemplate,
                predicate: Predicate,
                start: NumericValue, end: NumericValue, step: NumericValue) {
  def process(file: String, wl: Long): Unit = {
    val conf = new SparkConf().setAppName("tg-construct")
    val ssc = new StreamingContext(conf, Seconds(1))

    val events = ssc.receiverStream(new FileSource(file, eventTemplate))

    events.window(Milliseconds(wl),Milliseconds(wl))
      .foreachRDD(rdd => processWindow(rdd))
  }

  def processWindow(events: RDD[Event]): Unit = {
    val fcaches = events.map(e => )
  }
}
