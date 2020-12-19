package tg.graph

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import tg.dtg.common.values.{NumericValue, StrVal, Value}
import tg.dtg.events.Event

/**
 *
 * @param events a rdd in the form (event_id, event).
 * @param attrs a rdd in the form (attr_id, range).
 * @param fedges a rdd in the form (attr_id, (event_id, timestamp))
 * @param tedges a rdd in the form (event_id, (attr_id, timestamp))
 * @param rangeEdge use range edges representation or not
 */
class Graph(val events: RDD[(Long,Event)],
            val attrs: RDD[(Long, Attribute)],
            val fedges: RDD[(Long,(Long,Long))],
            val tedges: RDD[(Long,(Long,Long))],
            val rangeEdge: Boolean = true) {

  def write(path: String): Unit = {
    import Graph._

    val dir = new File(path)
    assert(!dir.exists() && dir.mkdir(), "no such directory and cannot mkdir")

    events.map{case (id,e)=> event2str(id,e)}
      .saveAsTextFile(new File(dir,"events").getCanonicalPath)
    attrs.map{case (id,a)=>attr2str(id,a)}
      .saveAsTextFile(new File(dir,"attrs").getCanonicalPath)
    fedges.map(edge2str)
      .saveAsTextFile(new File(dir,"fedges").getCanonicalPath)
    tedges.map(edge2str)
      .saveAsTextFile(new File(dir,"tedges").getCanonicalPath)
  }
}

object Graph {
  def read(path: String)(implicit sc:SparkContext): Graph = {
    val dir = new File(path)
    assert(!dir.exists(), "no such directory and cannot mkdir")

    val efile = new File(dir,"events")
    val afile = new File(dir,"attrs")
    val ffile = new File(dir,"fedges")
    val tfile = new File(dir, "tedges")

    new Graph(
      sc.textFile(efile.getCanonicalPath)
        .map(str2edge),
      sc.textFile(afile.getCanonicalPath)
        .map(str2attr),
      sc.textFile(ffile.getCanonicalPath)
        .map(str2edge),
      sc.textFile(tfile.getCanonicalPath)
        .map(str2edge)
    )
  }

  def event2str(id: Long, event: Event): String = {
    val size = event.size()

    val vs = for(i <- (0 until size)) yield event.get(i)

    val vstr = vs.map(v2s).mkString(",")
    s"$id,${event.timestamp},$vstr"
  }

  def str2event(str: String): (Long,Event) = {
    val sps = str.split(",")
    val id = sps(0).toLong
    val timestamp = sps(1).toLong
    val vs = sps.view(2,sps.length)
      .map(s2v).toArray
    (id,new Event(timestamp,vs))
  }

  def attr2str(id: Long, attr: Attribute): String = {
    val str = attr match {
      case Attr(v) =>
        s"${v2s(v)}"
      case RangeAttr(range) =>
        s"${v2s(range.lowerEndpoint())},${v2s(range.upperEndpoint())}"
    }
    s"$id,$str"
  }

  def str2attr(str: String): (Long,Attribute) = {
    val sps = str.split(",")
    val id = sps(0).toLong
    sps.size match {
      case 2 =>
        (id,Attr(s2v(sps(1))))
      case 3 =>
        (id,RangeAttr.apply(s2v(sps(1)),s2v(sps(2))))
    }
  }

  def edge2str(tuple: (Long,(Long,Long))): String = {
    s"${tuple._1},${tuple._2._1},${tuple._2._2}"
  }

  def str2edge(str: String): (Long,(Long,Long)) = {
    val sps = str.split(",")
    (sps(0).toLong, (sps(1).toLong, sps(2).toLong))
  }

  private def v2s(value: Value): String = {
    value match {
      case v: StrVal =>
        s"s${v.strVal()}"
      case v: NumericValue =>
        s"d${v.numericVal()}"
    }
  }

  private def s2v(s: String): Value = {
    if(s(0)=='s') Value.str(s.substring(1))
    else Value.numeric(s.substring(1).toDouble)
  }
}
