package tg.detect.broadcast

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import tg.Helper._
import tg.dtg.events.Event
import tg.dtg.query.Operator
import tg.graph.Graph

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Created by meihuiyao on 2020/12/30
 */

class BroadcastDetector extends Serializable {

  def process(graph: Graph)(callback: RDD[Array[Event]] => Unit)(implicit sc: SparkContext): Unit = {
    println(s"begin extract CETs")
    val s = System.currentTimeMillis();
    val lgraph = LocalGraph.fromGraph(graph)
    val (starts, ends) = localPrefilter(lgraph)

    val count = extract(lgraph, starts, ends)
    val e = System.currentTimeMillis();
    println(s"extract $count seqs, using ${e - s}")
    //    callback.apply(seqs)
  }

  def extract(localGraph: LocalGraph, starts: mutable.HashSet[Long], ends: mutable.HashSet[Long])(implicit sc: SparkContext): Long = {
    val bevents = sc.broadcast(localGraph.events)
    val battrsSize = sc.broadcast(localGraph.attrs.size)
    val bfedges = sc.broadcast(localGraph.fedges)
    val btedges = sc.broadcast(localGraph.tedges)
    val bop = sc.broadcast(localGraph.op)

    var paths: RDD[ArrayBuffer[(Long, Event)]] = sc.parallelize(starts.toSeq)
      .map(l => {
        val event = bevents.value(l)
        ArrayBuffer((l, event))
      })
    var completed = paths.filter(seq => ends.contains(seq.last._1))
    var seqs: RDD[Array[Event]] = completed.map(buf2arr)
    var uncomp = paths.filter(seq => !ends.contains(seq.last._1)).map(seq => (seq.last._1, seq))
    var count = uncomp.count()

    var iterCnt = 0;
    var seqCnt = completed.count();

    paths.cache()

    while (count > 0) {
      println(s"begin $iterCnt iteration, paths $count")
      paths = uncomp.flatMap { case (eid, seq) =>
        btedges.value(eid).flatMap { case (aid, tm) => // traverse by to-edges
          // find from-edges of attribute aid
          val fedgesInAttr = bop.value match {
            case Operator.eq =>
              val op = bfedges.value.get(aid).map(edges=>{
                val f = binarySearch(edges,0,edges.size,tm)
                val tf = if(f >= 0) f+1 else -(f+1)
                edges.view(tf, edges.size)
              })
              if(op.isEmpty) Iterator.empty
              else Iterator(op.get.iterator)
            case Operator.gt =>
              (aid until battrsSize.value).map(i => {
                val op = bfedges.value.get(i).map(edges=>{
                  val f = binarySearch(edges,0,edges.size,tm)
                  val tf = if(f >= 0) f+1 else -(f+1)
                  edges.view(tf, edges.size).iterator
                })
                if(op.isEmpty) Iterator.empty
                else op.get
              }).iterator
            case _ => Iterator.empty
          }
          // traverse by from-edges
          fedgesInAttr.flatMap(fedges => {
            fedges.flatMap { case (eid2, tm2) =>
              val nseq = new ArrayBuffer[(Long, Event)](seq.size + 1)
              nseq.appendAll(seq)
              nseq.append((eid2, bevents.value(eid2)))
              Some(nseq)
            }
          })
        }
      }
      paths.setName("paths").cache()

      completed = paths.filter(seq => ends.contains(seq.last._1))
      seqs = seqs.union(completed.map(buf2arr))
      seqCnt = completed.count() + seqCnt
      uncomp = paths.filter(seq => !ends.contains(seq.last._1)).map(seq => (seq.last._1, seq))
      count = uncomp.count()
      iterCnt += 1
      println(s"finish $iterCnt iteration, current seqs $seqCnt")
    }
    seqCnt
  }

  def localPrefilter(localGraph: LocalGraph): (mutable.HashSet[Long], mutable.HashSet[Long]) = {
    val metas = new mutable.HashMap[Long, (Long, Long)]()
    val starts = new mutable.HashSet[Long]()
    val ends = new mutable.HashSet[Long]()
    val eids = localGraph.events.keys
    starts ++= eids
    ends ++= eids

    def reachAttr(aid: Long, eid: Long, etm: Long): Unit = {
      val meta = metas.getOrElseUpdate(aid, {
        val opMo = localGraph.fedges.get(aid)
          .flatMap(es => {
            if (es.isEmpty) None
            else Some(es.maxBy(_._2)._2)
          })
        val mo = opMo.getOrElse(-100L)
        (mo, mo + 1)
      })
      if (etm < meta._1) {
        ends -= eid
      }
      if (etm < meta._2) {
        metas += aid -> (meta._1, etm)
      }
    }

    val size = localGraph.attrs.size
    localGraph.events.foreach { case (eid, e) =>
      localGraph.tedges.get(eid).foreach(attrs => {
        // 到达属性节点
        if (localGraph.op == Operator.eq) {
          attrs.foreach { case (aid, _) => reachAttr(aid, eid, e.timestamp) }
        } else if (localGraph.op == Operator.gt) {
          attrs.foreach { case (aidStart, _) =>
            (aidStart until size).foreach { aid =>
              reachAttr(aid, eid, e.timestamp)
            }
          }
        }
      })
    }
    metas.foreach { case (aid, (mo, mi)) =>
      if (mi < mo) {
        localGraph.fedges.get(aid)
          .foreach(tedges =>
            tedges.foreach { case (eid, tm) =>
              if (tm > mi) {
                starts -= eid
              }
            })
      }
    }
    (starts, ends)
  }

  private def buf2arr(buf: ArrayBuffer[(Long, Event)]): Array[Event] = {
    val arr = new Array[Event](buf.size)
    for (i <- arr.indices) {
      arr(i) = buf(i)._2
    }
    arr
  }

}
