package tg

import java.io.FileOutputStream

import scala.collection.mutable.ArrayBuffer

/**
 * Created by meihuiyao on 2020/12/19
 */

object Helper {
  private val fos = new FileOutputStream("out")
  def println(obj: Any): Unit = {
    Console.withOut(fos){
      Console.println(obj)
    }
    fos.flush()
  }

  def binarySearch(arr: ArrayBuffer[(Long,Long)], fromIndex: Int, toIndex: Int, key: Long): Int = {
    var low = fromIndex
    var high = toIndex - 1
    while (low <= high) {
      val mid = (low + high) >>> 1
      val midVal = arr(mid)._2
      if (midVal < key) low = mid + 1
      else if (midVal > key) high = mid - 1
      else return mid // key found
    }
    -(low + 1) // key not found.
  }
}
