import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by bogdan on 10/26/15.
 */

case class BTScrape(hash: String, tracker: String, ts: String, seeders: Long, leechers: Long, downloads: Long)

object Workflow {

  val sparkConf = new SparkConf().setAppName("Workflow")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  def main(args: Array[String]) {

    val inputScrapes = sc.textFile(args(0)).map(_.split("\\s+"))
                         .map(p => BTScrape(p(0), p(1), p(2), p(3).toLong, p(4).toLong, p(5).toLong))
                         .toDF()


    val trackerOverTime = new TrackerOverTime(sqlContext, inputScrapes)
    trackerOverTime.execute().rdd.saveAsTextFile(args(1) + "/TrackerOverTime")
    val activeHashes = new ActiveHashes(sqlContext, inputScrapes)
    activeHashes.execute().rdd.saveAsTextFile(args(1) + "/ActiveHashes")
  }

}
