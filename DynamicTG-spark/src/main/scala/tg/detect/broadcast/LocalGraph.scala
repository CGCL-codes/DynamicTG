package tg.detect.broadcast

import org.apache.spark.SparkContext
import tg.dtg.events.Event
import tg.dtg.query.Operator
import tg.graph.{Attribute, Graph}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Created by meihuiyao on 2020/12/30
 */

case class LocalGraph(events: Map[Long,Event],
                      attrs: Map[Long,Attribute],
                      fedges: mutable.Map[Long,ArrayBuffer[(Long,Long)]],
                      tedges: mutable.Map[Long,ArrayBuffer[(Long,Long)]],
                      op: Operator = Operator.gt)

object LocalGraph {
  def fromGraph(graph: Graph): LocalGraph = {
    val levents = graph.events.collect().toMap
    val lattrs = graph.attrs.collect().toMap
    val lfedges = new mutable.HashMap[Long, ArrayBuffer[(Long,Long)]]
    graph.fedges.collect().foreach{case (aid, tp)=>
      lfedges.getOrElseUpdate(aid, ArrayBuffer.empty).append(tp);
    }

    val ltedges = new mutable.HashMap[Long, ArrayBuffer[(Long,Long)]]
    graph.tedges.collect().foreach{case (eid, tp)=>
      ltedges.getOrElseUpdate(eid, ArrayBuffer.empty).append(tp);
    }

    val sortedfedges = lfedges.map{case (aid, arr)=>
      (aid, arr.sortBy(_._2))
    }

    LocalGraph(levents, lattrs,sortedfedges,ltedges, if(graph.rangeEdge) Operator.gt else Operator.eq);
  }
}
