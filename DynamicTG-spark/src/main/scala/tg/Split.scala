package tg

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by meihuiyao on 2021/1/4
 */

object Split {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf()
      .set("spark.locality.wait","0s")
      .setAppName("tg")

    if(System.getProperty("os.name").equals("Mac OS X")) {
      conf = conf.setMaster("local[4]")
    }

    val sc = new SparkContext(conf)
    val data = sc.textFile(args(0))
    val p = if(args.length > 1) args(1).toInt else 160
    data.repartition(p)
      .saveAsTextFile(args(0)+"-splits")
  }
}
