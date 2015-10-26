import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * Created by bogdan on 10/26/15.
 */

abstract class Query (context: SQLContext, inputRDD: DataFrame) {

  def name: String

  def execute() : DataFrame
}
