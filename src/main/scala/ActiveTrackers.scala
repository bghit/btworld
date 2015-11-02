import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._



class ActiveTrackers(context: SQLContext) extends Query {

  override var outputDF: DataFrame = _

  override def execute(inputDF: DataFrame) = {
    import context.implicits._

    outputDF = inputDF.select('tracker, 'tg).groupBy('tg).agg('tg, count("*").as("trackers"))

  }

}
