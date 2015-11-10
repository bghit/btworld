import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Bogdan Ghit on 10/26/15.
 */

case class BTScrape(hash: String, tracker: String, ts: String, seeders: Long, leechers: Long, downloads: Long)

object Workflow {

  val sparkConf = new SparkConf().setAppName("Workflow")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)
  import Workflow.sqlContext.implicits._

  def main(args: Array[String]) {

    //sqlContext.sql("SET spark.sql.shuffle.partitions=4")

    val inputScrapes = sc.textFile(args(0)).map(_.split("\\s+"))
                         .map(p => BTScrape(p(0), p(1), p(2), p(3).toLong, p(4).toLong, p(5).toLong))
                         .toDF()


    val trackerOverTime = new TrackerOverTime(sqlContext)
    trackerOverTime.execute(inputScrapes)
    trackerOverTime.cache()

    val topKTrackersLocal = new TopKTrackersLocal(sqlContext)
    topKTrackersLocal.execute(trackerOverTime.outputDF)
    topKTrackersLocal.save(args(1))

    val topKTrackersGlobal = new TopKTrackersGlobal(sqlContext)
    topKTrackersGlobal.execute(topKTrackersLocal.outputDF)
    topKTrackersGlobal.save(args(1))
    topKTrackersGlobal.outputDF.show(10)

    /*val activeTrackers = new ActiveTrackers(sqlContext)
    activeTrackers.execute(trackerOverTime.outputDF)
    activeTrackers.save(args(1))

    activeTrackers.outputDF
    val activeSwarms = new ActiveSwarms(sqlContext)
    activeSwarms.execute(trackerOverTime.outputDF)
    activeSwarms.save(args(1))*/


    /*val activeHashes = new ActiveHashes(sqlContext)
    activeHashes.execute(inputScrapes)
    activeHashes.save(args(1))*/


  }

}
