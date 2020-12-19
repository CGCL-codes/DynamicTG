package tg.example
import tg.dtg.events.EventTemplate
import tg.dtg.query.{Operator, Predicate, Query}

class KiteExample(override val args: Config) extends Example(args) {
  private val template = new EventTemplate.Builder().addStr("src").addStr("dest").build
  private val kitePredicate = new Predicate(Operator.eq, template.indexOf("src"), template.indexOf("dest"))

  override def getName: String = "kite"

  override def getQuery: Query = new Query("C", kitePredicate, args.wl, args.sl);

  override def getTemplate: EventTemplate = template
}
