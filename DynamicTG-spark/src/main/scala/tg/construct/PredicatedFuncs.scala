package tg.construct

import tg.dtg.common.values.Value
import com.google.common.collect
import java.util

sealed trait PredicatedFunctions {
  def mkRange(min: Value, max: Value): collect.Range[Value]
  def mapPartitionId(tree: util.TreeMap[Value,Int], v: Value): Int
  def getAttrId(tree: util.TreeMap[Value,Long], v: Value): Long
}

object PredicatedFuncs {
  def gt(): PredicatedFunctions = new GtFunctions
}

class GtFunctions extends PredicatedFunctions {
  override def mkRange(min: Value, max: Value): collect.Range[Value] = collect.Range.closedOpen[Value](min,max)

  override def mapPartitionId(tree: util.TreeMap[Value, Int], v: Value): Int = {
    tree.higherEntry(v).getValue
  }

  override def getAttrId(tree: util.TreeMap[Value, Long], v: Value): Long = {
    tree.higherEntry(v).getValue
  }
}
