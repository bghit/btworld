import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._



class ActiveTrackers(context: SQLContext) extends Query {

  override var outputDF: DataFrame = _

  override def execute(inputDF: DataFrame) = {
    import context.implicits._
    //context.sql("SET spark.sql.shuffle.partitions=1")

    outputDF = inputDF.select('tracker, 'tg).groupBy('tg).agg('tg, count("*").as("trackers"))

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
