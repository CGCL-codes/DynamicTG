package tg.construct

import org.apache.spark._
import org.apache.spark.rdd.RDD
import tg.Helper._
import tg.dtg.common.values.Value
import tg.dtg.events.Event
import tg.dtg.query.{Operator, Predicate}
import tg.graph.{Attr, Attribute, Graph, RangeAttr}

class Constructor extends Serializable {

  def process(events: RDD[Event], predicate: Predicate, wl: Long,
              start: Value = Value.numeric(0), end: Value = Value.numeric(Double.MaxValue))(callback: Graph => Unit)(implicit sc: SparkContext): Unit = {
      val count = events.count()
      if(count > 0) {
        println(s"start processing ${count} events")
        val startTime = System.currentTimeMillis()

        val graph =
          if (predicate.op == Operator.eq) processEqWindow(events, predicate)
          else processWindow(events, predicate, start, end)

        graph.events.setName("events").cache()
        graph.attrs.setName("attrs").cache()
        graph.fedges.setName("fedges").cache()
        graph.tedges.setName("tedges").cache()

        val eCount = graph.events.count()
        val aCount = graph.attrs.count()
        val fCount = graph.fedges.count()
        val tCount = graph.tedges.count()

        val endTime = System.currentTimeMillis()

        println(s"finish construct a graph, " +
          s"events $eCount, attrs $aCount, from edges $fCount, to edges $tCount, " +
          s"using ${endTime - startTime}")
        callback.apply(graph)
      }

  }

  def processEqWindow(events: RDD[Event], predicate: Predicate): Graph = {
    val keyedEvents = events.zipWithUniqueId().map{case (e,id)=>(id,e)}
    val plid = predicate.leftOperand
    val prid = predicate.rightOperand
    val func = predicate.func
    val fCaches = keyedEvents.map{case (id,e) =>
      (e.get(plid),(id, e.timestamp))
    }
    val tCaches = keyedEvents.map { case (id, e) =>
      (func.apply(e.get(prid)),(id, e.timestamp))
    }

    val attrs = fCaches.map(_._1)
      .union(tCaches.map(_._1))
      .distinct()
      .zipWithUniqueId()

    val fedges = fCaches.join(attrs)
      .map{case (_,((eid, t),aid)) => (aid,(eid,t))}

    val tedges = tCaches.join(attrs)
      .map{case (_,((eid, t),aid)) => (eid,(aid,t)) }

    new Graph(keyedEvents, attrs.map{case (v,id)=>(id,Attr(v))}, fedges, tedges,false)
  }

  def processWindow(events: RDD[Event],
                    predicate: Predicate,
                    start: Value, end: Value): Graph = {

    val funcs = predicate.op match {
      case Operator.gt => PredicatedFuncs.gt()
      case Operator.eq => null
      case _ => throw new UnsupportedOperationException(s"unsupported operator ${predicate.op}")
    }

    val keyedEvents = events.zipWithUniqueId().map{case (e,id)=>(id,e)}
    val fCaches = keyedEvents.map{case (id,e) =>
      (e.get(predicate.leftOperand),id, e.timestamp)
    }
    val tCaches = keyedEvents.map{case (id,e) =>
      (id, (predicate.func.apply(e.get(predicate.rightOperand)),e.timestamp))
    }
    val rvals = tCaches.map(_._2._1).distinct().sortBy(_.numericVal())
    val indexRvals = rvals.zipWithIndex().map{case (v,i)=>(i,v)}
    val shiftRvals = indexRvals.map{case (i,v)=>(i-1,v)}
    val indexedRanges: RDD[(Long, Attribute)] = indexRvals.fullOuterJoin(shiftRvals)
      .map{case (id, (rv, srv)) =>
        if(rv.isDefined && srv.isDefined) {
          (id, RangeAttr(funcs.mkRange(rv.get, srv.get)))
        }else if(rv.isDefined) {
          (id, RangeAttr(funcs.mkRange(rv.get, end)))
        }else if(srv.isDefined) {
          (id, RangeAttr(funcs.mkRange(start, srv.get)))
        }else {
          throw new IllegalArgumentException("data not valid")
        }
      }.map{case (id,range)=>(id+1, range)}

    val rangeIndexes = indexedRanges.mapPartitionsWithIndex{case (id,iter) =>
      Iterator((id,mkTreeMap(iter.map(item => (item._2.asInstanceOf[RangeAttr].range.lowerEndpoint(), item._1)))))
    }
    val globalIndexes = mkTreeMap(rangeIndexes.flatMap {case (id,tree) =>
      if(tree.isEmpty) None
      else Some((tree.firstKey(), id))
    }.collect().iterator)

    fCaches.cache().setName("fCaches")
    indexedRanges.count()
    tCaches.cache().setName("tCaches")
    rangeIndexes.count()

    val fedges = fCaches.map{case (v,eid, t) => (funcs.mapPartitionId(globalIndexes,v),(v,eid, t))}
      .join(rangeIndexes)
      .map{case (_, ((v,eid, t),tree))=>
        (funcs.getAttrId(tree,v),(eid,t))
      }
    val tedges = tCaches.map{case (eid,(v,timestamp))=> (funcs.mapPartitionId(globalIndexes,v),(eid,v,timestamp))}
      .join(rangeIndexes)
      .map{case (_,((eid,v,timestamp),tree))=>
        (eid, (funcs.getAttrId(tree,v),timestamp))
      }
    new Graph(keyedEvents, indexedRanges, fedges, tedges, true)
  }

  private def mkTreeMap[K,V](elems: Iterator[(K,V)]): java.util.TreeMap[K,V] = {
    val tree = new java.util.TreeMap[K,V]()
    elems.foreach{case (k,v) => tree.put(k,v)}
    tree
  }
}
