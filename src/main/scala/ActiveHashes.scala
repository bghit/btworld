import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._

/**
 * Created by bogdan on 10/26/15.
 */
class ActiveHashes(context: SQLContext, inputDF: DataFrame) extends Query(context: SQLContext, inputDF) {

  override def name: String = "TrackerOverTime"


  override def execute(): DataFrame = {
    import context.implicits._

    val activeHashes = inputDF.select('hash, Utils.timegroup('ts).as('tg)).distinct.groupBy('tg).agg('tg, count('hash))

    return activeHashes
  }

}
