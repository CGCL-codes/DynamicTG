package tg.example

import java.util

import org.apache.spark.{SparkConf, SparkContext}
import tg.construct.Constructor
import tg.dtg.events.EventTemplate
import tg.dtg.query.Query

import scala.collection.JavaConverters._

abstract class Example(val args: Config) {

  def start(): Unit = {
    val localParameters = getParameters
    val parameters = "************************************\n" +
      "name: " + getName + "\n" +
      "input events: " + args.path + "\n" +
      "window: " + args.wl + ", " + args.sl + "\n" +
//      "parallism: " + args.parallism + "\n" +
      "isWrite: " + args.isWrite + "\n" +
      (if (localParameters.isEmpty) "" else localParameters + "\n") +
      "************************************"
    println(parameters)

    val conf = new SparkConf().setAppName("tg")
    implicit val sc: SparkContext = new SparkContext(conf)

    val constructor = new Constructor(getTemplate, getQuery.condition.predicates().asScala.head._2)

  }

  protected def getParameters: String = ""

  def getName: String

  def getQuery: Query

  protected def setPrecision(precisions: Double*): Unit = {
    require(precisions.nonEmpty)
    tg.dtg.util.Config.initValue(precisions.min)
  }

  def getTemplate: EventTemplate
}

object Example {
  def parse(args: Array[String]): Config = {
    import scopt.OParser
    val builder = OParser.builder[Config]
    val parser = {
      import builder._
      OParser.sequence(
        programName("tg-spark"),
        opt[String]('p',"path")
          .action((p,c)=>c.copy(path = p))
          .text("input file path")
          .required(),
        opt[Long]("wl")
          .action((l,c)=>c.copy(wl=l))
          .text("window length"),
        opt[Long]("sl")
          .action((l,c)=>c.copy(sl=l))
          .text("slide length"),
        opt[Unit]("write")
          .action((_,c)=>c.copy(isWrite = true))
          .text("if write graph"),
        opt[String]("graph")
          .action((p,c)=>c.copy(graphDir = p))
          .text("graph dir")
      )
    }
    OParser.parse(parser,args, Config()) match {
      case Some(v) =>
        if(v.sl <= 0) v.copy(sl=v.wl)
        else v
      case None =>
        throw new RuntimeException("error parsing arguments")
    }
  }
  def getExample(args: Array[String]): Example = {
    require(args.length > 0, "must specify example name")
    var config:Config = null
    val nargs = util.Arrays.copyOfRange(args, 1, args.length)
    if (args(0).startsWith("stock")) {
      val ratios = args(0).substring(5)
      var ratio = 1.0
      if (ratios.length > 0) ratio = ratios.toDouble
      config = parse(nargs)
      null
//      new Stock(config.asInstanceOf[Stock.Argument], ratio)
    }
    else if ("kite".equalsIgnoreCase(args(0))) {
      config = parse(nargs)
      new KiteExample(config)
    }
    else {
      throw new UnsupportedOperationException("not supported example")
    }
  }
}