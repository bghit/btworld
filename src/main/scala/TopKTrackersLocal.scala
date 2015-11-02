import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * Created by bogdan on 11/2/15.
 */

class TopKTrackersLocal(context: SQLContext) extends Query {
  override var outputDF: DataFrame = _


  override def execute(inputDF: DataFrame) = {
    import context.implicits._

    val groupedDF = inputDF.select('tracker, 'tg, 'sessions)

    val result = groupedDF.groupBy($"tg").agg($"tracker", $"tg", $"sessions")

    outputDF = result.orderBy('sessions).limit(Utils.TOP_K_RECORDS)
  }
}
