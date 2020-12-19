package tg.construct

import java.util

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import tg.dtg.common.values.Value
import tg.dtg.events.{Event, EventTemplate}
import tg.dtg.query.{Operator, Predicate}
import tg.graph.{Attr, Attribute, Graph, RangeAttr}

class Constructor(val eventTemplate: EventTemplate,
                  predicate: Predicate,
                  start: Value = null, end: Value = null)(implicit val sc: SparkContext = null) {
  private val funcs = predicate.op match {
    case Operator.gt => PredicatedFuncs.gt()
    case _ => throw new UnsupportedOperationException(s"unsupported operator ${predicate.op}")
  }

  private def createContext(): StreamingContext = {
    if(sc == null) {
      val conf = new SparkConf().setAppName("tg-construct")
      new StreamingContext(conf, Seconds(1))
    }else new StreamingContext(sc, Seconds(1))
  }

  def process(file: String, wl: Long): Unit = {
    val ssc = createContext()

    val events = ssc.receiverStream(new FileSource(file, eventTemplate))

    val processFunc = if(predicate.op == Operator.eq) processEqWindow else processWindow
    val windowed = events.window(Milliseconds(wl),Milliseconds(wl))
    windowed.fo
//    foreachRDD(rdd => processFunc(rdd))
  }

  def processEqWindow(events: RDD[Event]): Graph = {
    val keyedEvents = events.zipWithUniqueId().map{case (e,id)=>(id,e)}
    val fCaches = keyedEvents.map{case (id,e) =>
      (e.get(predicate.leftOperand),(id, e.timestamp))
    }
    val tCaches = keyedEvents.map { case (id, e) =>
      (predicate.func.apply(e.get(predicate.rightOperand)),(id, e.timestamp))
    }

    val attrs = fCaches.map(_._1)
      .union(tCaches.map(_._1))
      .zipWithUniqueId()

    val fedges = fCaches.join(attrs)
      .map{case (_,((eid, t),aid)) => (aid,(eid,t))}

    val tedges = tCaches.join(attrs)
      .map{case (_,((eid, t),aid)) => (eid,(aid,t)) }

    new Graph(keyedEvents, attrs.map{case (v,id)=>(id,Attr(v))}, fedges, tedges)
  }

  def processWindow(events: RDD[Event]): Graph = {
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
    val globalIndexes = mkTreeMap(rangeIndexes.map {case (id,tree) =>
      (tree.firstKey(), id)
    }.collect().iterator)

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
    new Graph(keyedEvents, indexedRanges, fedges, tedges)
  }

  private def mkTreeMap[K,V](elems: Iterator[(K,V)]): java.util.TreeMap[K,V] = {
    val tree = new java.util.TreeMap[K,V]()
    elems.foreach{case (k,v) => tree.put(k,v)}
    tree
  }
}
