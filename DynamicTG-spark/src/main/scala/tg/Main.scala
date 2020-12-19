package tg

import tg.example.Example

object Main {
  def main(args: Array[String]): Unit = {
    val example = Example.getExample(args)
    example.start()
  }
}
