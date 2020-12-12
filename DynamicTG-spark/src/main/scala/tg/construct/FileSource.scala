package tg.construct

import java.util.concurrent.TimeUnit

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import tg.dtg.events.{Event, EventTemplate}

import scala.io.Source

class FileSource(file: String, eventTemplate: EventTemplate) extends Receiver[Event](StorageLevel.MEMORY_AND_DISK){
  private var isRun = true
  private val thread: Thread = new Thread(new Runnable {
    override def run(): Unit = {
      val source = Source.fromFile(file)
      val it = source.getLines()
      var current = System.currentTimeMillis()
      var now: Long = current
      while (isRun && it.hasNext) {
        val line = it.next()
        val event = eventTemplate.str2event(line)
        while (isRun && now - current < event.timestamp) {
          now = System.currentTimeMillis()
          TimeUnit.MILLISECONDS.sleep(1)
        }
        if(now - current < event.timestamp) store(event)
        current = now
      }
    }
  })

  override def onStart(): Unit = {
    isRun = true
    thread.start()
  }

  override def onStop(): Unit = {
    isRun = false
  }
}
