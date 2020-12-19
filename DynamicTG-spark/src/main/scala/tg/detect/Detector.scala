package tg.detect

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import tg.dtg.events.Event
import tg.dtg.query.Predicate
import tg.graph.Graph

import scala.collection.mutable.ArrayBuffer

class Detector(val graph: Graph,
               val predicate: Predicate)(implicit val sc: SparkContext = null) {

  private def createContext: SparkContext = if(sc==null) {
    val conf = new SparkConf().setAppName("tg-construct")
    new SparkContext(conf)
  }else sc

  def process(): Unit = {
    val sc = createContext

    // a valid short path is two connected edges that start from an event and end in another event
    // where the timestamp of the later event is greater than that of the previous one
    val validShortPaths = graph.tedges.map{case (eid,(aid,timestamp))=>
      (aid,(eid,(aid,timestamp)))
    }.join(graph.fedges)
      .filter{case (_,((_,(_,timestamp1)), (_, timestamp2)))=> timestamp2 > timestamp1}
      .map{case (_,((eid1, e1), (eid2, e2)))=>
        (eid1, eid2)
      }

    val (starts,ends) = preFilter(validShortPaths)
    val events = graph.events.leftOuterJoin(starts)
      .map{case (eid,(event, optStart)) =>
        (eid, (event, optStart.isDefined))
      }.leftOuterJoin(ends)
      .map{case (eid, ((event, isStart), optEnd)) =>
        (eid, (event, isStart, optEnd.isDefined))
      }

    extract(events,validShortPaths)
  }

  def preFilter(validShortPaths: RDD[(Long,Long)]): (RDD[(Long, Null)],RDD[(Long, Null)]) = {

    val ends = graph.events.leftOuterJoin(validShortPaths)
      .filter{case (_,(_, opId))=>opId.isEmpty}
      .map{case (eid,_)=>(eid,null)}

    val reversedPaths = validShortPaths.map{case (eid1,eid2)=>(eid2, eid1)}
    val starts = graph.events.leftOuterJoin(reversedPaths)
      .filter{case (_,(_, opId))=>opId.isEmpty}
      .map{case (eid,_)=>(eid,null)}

    (starts,ends)
  }

  def extract(events: RDD[(Long,(Event,Boolean,Boolean))],
              validShortPaths: RDD[(Long,Long)]): Unit = {

    var paths: RDD[ArrayBuffer[(Long,(Event,Boolean,Boolean))]] =  events.filter(x => x._2._2).map(x=>ArrayBuffer(x))

    var completed = paths.filter(_.last._2._3)
    var seqs: RDD[Array[Event]] = completed.map(buf2arr)
    var uncomp = paths.filter(!_.last._2._3).map(t => (t.last._1,t))
    var count = uncomp.count()

    while (count > 0) {
      paths = uncomp.join(validShortPaths)
          .map{case (_, (list, destId))=>
            (destId, list)
          }.join(events)
          .map{case (l, (list, t)) =>
            list.append((l,t))
            list
          }
      completed = paths.filter(_.last._2._3)
      seqs = seqs.union(completed.map(buf2arr))
      uncomp = paths.filter(!_.last._2._3).map(t => (t.last._1,t))
      count = uncomp.count()
    }
  }

  private def buf2arr(buf: ArrayBuffer[(Long,(Event,Boolean,Boolean))]): Array[Event] = {
    val arr = new Array[Event](buf.size)
    for(i <- arr.indices) {
      arr(i) = buf(i)._2._1
    }
    arr
  }
}
