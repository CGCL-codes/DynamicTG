package tg.example

import java.io.{File, FileOutputStream}
import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import tg.construct.{Constructor, PredicatedFunctions}
import tg.detect.Detector
import tg.dtg.common.values.Value
import tg.dtg.events.{Event, EventTemplate}
import tg.dtg.query.{Expression, Query}
import tg.example.Example.{Window, serializables}
import tg.graph.{Attribute, Graph}
import tg.Helper._
import tg.detect.broadcast.BroadcastDetector

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

abstract class Example(val args: Config) extends Serializable {
  private val numWindow = 1

  def start(): Unit = {
    val parameters = "************************************\n" +
      "name: " + getName + "\n" +
      "input events: " + args.path + "\n" +
      "window: " + args.wl + ", " + args.sl + "\n" +
      s"graphDir: ${args.graphDir}\n" +
      s"resultDir: ${args.resultDir}\n" +
      "************************************"
    println(parameters)

    implicit val sc: SparkContext = initSparContext

    val predicate = getQuery.condition.predicates().asScala.head._2
    val constructor = new Constructor()
    val detector = new BroadcastDetector()

    var graph_index = 1
    val onConstruct: Graph => Graph = graph => {
      if (!args.graphDir.isEmpty) {
        val indexedDir = new File(args.graphDir,"graph-"+graph_index)
        graph.write(indexedDir.getCanonicalPath)
        graph_index += 1
      }
      graph
    }

    var detect_index = 1
    val onDetect: RDD[Array[Event]] => Unit = rdd => {
      if (args.resultDir.nonEmpty) {
        val indexedDir = new File(args.resultDir,"graph-"+detect_index)
        rdd.map(arr => arr.map(_.timestamp.toString).mkString(", "))
          .saveAsTextFile(indexedDir.getCanonicalPath)
        detect_index += 1
      }
    }

    val finalCall: Graph => Unit = _ => {}

    fetchOneWindowX.foreach { events =>
      events.setName("inputs").cache()
      args.action match {
        case "construct" =>
          constructor.process(events, predicate, args.wl)(onConstruct.andThen(finalCall))
        case "detect" =>
          require(args.graphDir.nonEmpty, "no graph specified")
          detector.process(Graph.read(args.graphDir))(onDetect)
        case _ =>
          constructor.process(events, predicate, args.wl)(onConstruct.andThen(graph => {
            detector.process(graph)(onDetect)
          }))
      }
    }
  }

  private def fetchOneWindowX(implicit sc: SparkContext): Iterator[RDD[Event]] = {
    val rdd =sc.textFile(args.path, sc.getConf.getInt("spark.default.parallelism",8))
    Iterator(rdd.map(getTemplate.str2event)
      .filter(e=>e.timestamp < args.wl)
      .repartition(sc.getConf.getInt("spark.default.parallelism",10)*2)
    )
  }

  private def fetchOneWindow(implicit sc: SparkContext): Iterator[RDD[Event]] = {
    val file = args.path
    val source = Source.fromFile(file)
    val it = source.getLines().map(getTemplate.str2event)
    val fetchSize = 1 << 18;
    var i = 0
    var rdd: RDD[Event] = sc.parallelize(List.empty)
    def fetch(size: Int): Unit = {
      val buf = new ArrayBuffer[Event](size)
      while (it.hasNext) {
        buf.append(it.next())
      }
      rdd = rdd.union(sc.parallelize(buf))
    }
    while (i + fetchSize < args.wl) {
      fetch(fetchSize)
      i += fetchSize
    }
    if(i < args.wl) {
      fetch((args.wl - i).toInt)
    }
    Iterator(rdd.repartition(sc.getConf.getInt("spark.default.parallelism",8)))
  }

  private def windowedRDD(implicit sc: SparkContext): Iterator[RDD[Event]] = {
    windowEvents.map(window => sc.parallelize(window.events)).iterator
  }

  private def windowEvents: ArrayBuffer[Window] = {
    val windowedEvents = new ArrayBuffer[Window]
    val file = args.path
    val source = Source.fromFile(file)
    val it = source.getLines().map(getTemplate.str2event)
    val query = getQuery
    var nextW = 0L
    while (it.hasNext) {
      val event = it.next
      while (event.timestamp >= nextW) {
        val window = Window(nextW)
        windowedEvents.append(window)
        nextW = nextW + query.sl
      }
      var i = 0
      while (!(windowedEvents(i).start <= event.timestamp && event.timestamp < windowedEvents(i).start + query.wl)) i += 1
      if (i < numWindow) {
        while (i < windowedEvents.size) {
          if (windowedEvents(i).start <= event.timestamp && event.timestamp < windowedEvents(i).start + query.wl) {
            windowedEvents(i).events.append(event)
          }
          i += 1
        }
      }
    }
    windowedEvents
  }

  def getName: String

  def getQuery: Query

  protected def setPrecision(precisions: Double*): Unit = {
    require(precisions.nonEmpty)
    tg.dtg.util.Config.initValue(precisions.min)
  }

  def getTemplate: EventTemplate

  def initSparContext: SparkContext = {
    var conf = new SparkConf()
      .set("spark.locality.wait","0s")
      .registerKryoClasses(serializables)
      .setAppName("tg")

    if(System.getProperty("os.name").equals("Mac OS X")) {
      conf = conf.setMaster("local[4]")
    }

    val sc = new SparkContext(conf)
    sc
  }
}

object Example {
  def parse(args: Array[String]): Config = {
    import scopt.OParser
    val builder = OParser.builder[Config]
    val parser = {
      import builder._
      OParser.sequence(
        programName("tg-spark"),
        opt[Unit]('c', "construct")
          .action((_, c) => c.copy(action = "construct"))
          .text("construct graph"),
        opt[Unit]('d', "detect")
          .action((_, c) => c.copy(action = "detect"))
          .text("construct graph"),
        opt[String]('p', "path")
          .action((p, c) => c.copy(path = p))
          .text("input file path"),
        opt[Long]("wl")
          .action((l, c) => c.copy(wl = l))
          .text("window length"),
        opt[Long]("sl")
          .action((l, c) => c.copy(sl = l))
          .text("slide length"),
        opt[String]("graph")
          .action((dir, c) => c.copy(graphDir = dir))
          .validate(path => {
            if(validateDirectory(path)) success
            else failure("given path $path is invalid")
          })
          .text("write graph"),
        opt[String]("result")
          .action((dir, c) => c.copy(resultDir = dir))
          .validate(path => {
            if(validateDirectory(path)) success
            else failure("given path $path is invalid")
          })
          .text("write result")
      )
    }
    OParser.parse(parser, args, Config()) match {
      case Some(v) =>
        if (v.sl <= 0) v.copy(sl = v.wl)
        else v
      case None =>
        throw new RuntimeException("error parsing arguments")
    }
  }

  def getExample(args: Array[String]): Example = {
    require(args.length > 0, "must specify example name")
    var config: Config = null
    val nargs = util.Arrays.copyOfRange(args, 1, args.length)
    if (args(0).startsWith("stock")) {
      val ratios = args(0).substring(5)
      var ratio = 1.0
      if (ratios.length > 0) ratio = ratios.toDouble
      config = parse(nargs)
      new StockExample(config,ratio)
      //      new Stock(config.asInstanceOf[Stock.Argument], ratio)
    } else if ("kite".equalsIgnoreCase(args(0))) {
      config = parse(nargs)
      new KiteExample(config)
    } else {
      throw new UnsupportedOperationException("not supported example")
    }
  }

  private[example] val serializables: Array[Class[_]] = Array(
    classOf[Event],
    classOf[EventTemplate],
    classOf[Value],
    classOf[Attribute],
    classOf[Query],
    classOf[Expression],
    classOf[PredicatedFunctions],
    classOf[Constructor],
    classOf[Detector],
  )

  private[example] def validateDirectory(dir: String): Boolean = {
    val file = new File(dir)
    file.exists() || file.mkdir()
  }

  private[example] case class Window(start: Long, events: ArrayBuffer[Event]=ArrayBuffer.empty)
}