import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._

/**
 * Created by bogdan on 11/27/15.
 */
class NewbornSwarms(context: SQLContext) extends Query {
  override var outputDF: DataFrame = _

  override def cache(): Unit = {
    outputDF.cache()
  }

  override def execute(inputDF: DataFrame): Unit = {
    outputDF = inputDF.select("tracker", "hash", "birth", "death")
                      .groupBy("tracker", "birth")
                      .agg(col("tracker"), col("birth"), count(col("hash")).as("hashcount"))
  }

  override def execute(inputDF: DataFrame, fullInputDF: DataFrame): Unit = {

  }

  override def save(path: String): Unit = {
    outputDF.rdd.saveAsTextFile(path+"/"+this.getClass.getName)
  }
}
