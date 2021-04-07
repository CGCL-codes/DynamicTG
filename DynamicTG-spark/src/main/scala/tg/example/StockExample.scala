package tg.example
import tg.dtg.common.values.Value
import tg.dtg.events.EventTemplate
import tg.dtg.query.{Operator, Predicate, Query}

/**
 * Created by meihuiyao on 2021/1/1
 */

class StockExample(override val args: Config,
                   val ratio: Double) extends Example(args){
  private val template = new EventTemplate.Builder()
    .addStr("id")
    .addNumeric("price")
    .build()

  private val pricePredicate = new Predicate(Operator.gt,
    template.indexOf("price"),
    template.indexOf("price"),
    new tg.dtg.query.Function[Value,Value]() {
      override def apply(v1: Value): Value = Value.numeric(v1.numericVal()*ratio)
    })

  override def getName: String = s"stock$ratio"

  override def getQuery: Query = new Query("S",pricePredicate,args.wl,args.sl)

  override def getTemplate: EventTemplate = template
}
