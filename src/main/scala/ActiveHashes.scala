import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._

/**
 * Created by bogdan on 10/26/15.
 */

class ActiveHashes(context: SQLContext) extends Query {

  override var outputDF: DataFrame = _

  override def execute(inputDF: DataFrame) = {
    import context.implicits._
    context.sql("SET spark.sql.shuffle.partitions=50")
    outputDF = inputDF.select('hash, Utils.timegroup('ts).as('tg)).distinct.groupBy('tg).agg('tg, count('hash))
  }

  override def save(path: String) = {
    outputDF.rdd.saveAsTextFile(path+"/"+this.getClass.getName)
  }

  override def cache() = {
    outputDF.cache()
  }

  override def execute(inputDF: DataFrame, fullInputDF: DataFrame): Unit = {

  }
}
