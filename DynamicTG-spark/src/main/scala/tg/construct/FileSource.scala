package tg.construct

import java.util.concurrent.TimeUnit

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import tg.dtg.events.{Event, EventTemplate}

import scala.io.Source

class FileSource(file: String, eventTemplate: EventTemplate) extends Receiver[Event](StorageLevel.MEMORY_AND_DISK){
  private var isRun = true

  override def onStart(): Unit = {
    isRun = true
    new Thread(new Runnable {
      override def run(): Unit = {
        val source = Source.fromFile(file)
        val it = source.getLines()
        val start = System.currentTimeMillis()
        var now: Long = start
        while (isRun && it.hasNext) {
          val line = it.next()
          val event = eventTemplate.str2event(line)
          while (isRun && now - start < event.timestamp) {
            now = System.currentTimeMillis()
            TimeUnit.MILLISECONDS.sleep(1)
          }
          if(now - start >= event.timestamp) store(event)
        }
        source.close()
      }
    }).start()
  }

  override def onStop(): Unit = {
    isRun = false
  }
}
