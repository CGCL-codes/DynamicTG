package tg.graph

import com.google.common.collect
import tg.dtg.common.values.Value

trait Attribute extends Serializable

case class RangeAttr(range: collect.Range[Value]) extends Attribute

object RangeAttr {
  def apply(min:Value,max:Value): RangeAttr = {
    RangeAttr(collect.Range.closedOpen(min,max))
  }
}

case class Attr(value: Value) extends Attribute
