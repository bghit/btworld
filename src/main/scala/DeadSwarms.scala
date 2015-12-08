import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * Created by bogdan on 11/27/15.
 */
class DeadSwarms(context: SQLContext) extends Query {
  override var outputDF: DataFrame = _

  override def cache(): Unit = {
    outputDF.cache()
  }

  override def execute(inputDF: DataFrame): Unit = {
    outputDF = inputDF.select("tracker", "hash", "birth", "death")
                      .groupBy("tracker", "death")
                      .agg(col("tracker"), col("death"), count(col("hash")).as("hashcount"))
  }

  override def execute(inputDF: DataFrame, fullInputDF: DataFrame): Unit = {

  }

  override def save(path: String): Unit = {
    outputDF.rdd.saveAsTextFile(path+"/"+this.getClass.getName)
  }
}
