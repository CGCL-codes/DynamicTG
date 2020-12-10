package com.spark.tg.construct

import java.io.File

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scala.io.Source

class FileSource(file: File) extends Receiver[Event](StorageLevel.MEMORY_AND_DISK){
  private var thread: Thread = _
  override def onStart(): Unit = {
    thread = new Thread(new Runnable {
      override def run(): Unit = {
        val source = Source.fromFile(file)
      }
    })
  }

  override def onStop(): Unit = ???
}
