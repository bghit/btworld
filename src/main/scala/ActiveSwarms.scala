import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * Created by bogdan on 11/2/15.
 */

class ActiveSwarms (context: SQLContext) extends Query {

  override var outputDF: DataFrame = _

  override def execute(inputDF: DataFrame) = {
    import context.implicits._

    outputDF = inputDF.select('tracker, 'tg, 'hashcount).groupBy('tg).agg('tg, sum('hashcount).as("swarmcount"))
  }


  override def save(path: String) = {
    outputDF.rdd.saveAsTextFile(path+"/"+this.getClass.getName)
  }

  override def cache() = {
    outputDF.cache()
  }
}
