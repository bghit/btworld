import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._

/**
 * Created by bogdan on 11/19/15.
 */
class JoinTopKTrackers (context: SQLContext) extends Query with Serializable {
  override var outputDF: DataFrame = _

  override def cache(): Unit = {
    outputDF.cache()
  }

  override def execute(inputDF: DataFrame): Unit = {

  }

  override def execute(inputDF: DataFrame, fullInputDF: DataFrame): Unit = {
    //context.sql("SET spark.sql.shuffle.partitions=4")
    outputDF = fullInputDF.join(inputDF, "tracker")
              .select("hash", "tracker", "ts")
              .groupBy("hash", "tracker")
              .agg(col("hash"), col("tracker"), min(col("ts")).as("birth"), max(col("ts")).as("death"))
  }

  override def save(path: String): Unit = {
    outputDF.rdd.saveAsTextFile(path+"/"+this.getClass.getName)
  }
}
