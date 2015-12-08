import org.apache.spark.sql.DataFrame

/**
 * Created by Bogdan Ghit on 10/26/15.
 */

trait Query {

  var outputDF: DataFrame

  def execute(inputDF: DataFrame)

  def execute(inputDF: DataFrame, fullInputDF: DataFrame)

  def save(path: String)

  def cache()
}
