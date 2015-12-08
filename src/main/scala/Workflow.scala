import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Bogdan Ghit on 10/26/15.
 */

case class BTScrape(hash: String, tracker: String, ts: String, seeders: Long, leechers: Long, downloads: Long)

case class ConcatScrape(hash: String, tracker: String, ts: String, seeders: Long, leechers: Long, downloads: Long) {
  override def toString() = {
    hash + " " + tracker + " " + ts + " " + seeders + " " + leechers + " " + downloads
  }
}

object Workflow {


  val sparkConf = new SparkConf().setAppName("Workflow")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)

  import Workflow.sqlContext.implicits._


  def workflow(input: String, output: String) {
    val textScrapes = sc.textFile(input)
    val inputScrapes = textScrapes.map(_.split("\\s+")).map(p => BTScrape(p(0), p(1), p(2), Utils.guard(p(3)), Utils.guard(p(4)), Utils.guard(p(5)))).toDF()
    //inputScrapes.cache()

    val trackerOverTime = new TrackerOverTime(sqlContext)
    trackerOverTime.execute(inputScrapes)
    trackerOverTime.cache()

    val topKTrackersLocal = new TopKTrackersLocal(sqlContext)
    topKTrackersLocal.execute(trackerOverTime.outputDF)

    val topKTrackersGlobal = new TopKTrackersGlobal(sqlContext)
    topKTrackersGlobal.execute(topKTrackersLocal.outputDF)

    val joinTopKTrackers = new JoinTopKTrackers(sqlContext)
    joinTopKTrackers.execute(topKTrackersGlobal.outputDF, inputScrapes)
    joinTopKTrackers.cache()

    val newbornSwarms = new NewbornSwarms(sqlContext)
    newbornSwarms.execute(joinTopKTrackers.outputDF)
    newbornSwarms.save(output)

    val deadSwarms = new DeadSwarms(sqlContext)
    deadSwarms.execute(joinTopKTrackers.outputDF)
    deadSwarms.save(output)

    val activeTrackers = new ActiveTrackers(sqlContext)
    activeTrackers.execute(trackerOverTime.outputDF)
    activeTrackers.save(output)

    val activeSwarms = new ActiveSwarms(sqlContext)
    activeSwarms.execute(trackerOverTime.outputDF)
    activeSwarms.save(output)

  }

  def singleQuery(input: String, output: String) {
    val textScrapes = sc.textFile(input)
    val inputScrapes = textScrapes.map(_.split("\\s+")).map(p => BTScrape(p(0), p(1), p(2), p(3).toLong, p(4).toLong, p(5).toLong)).toDF()

    val activeHashes = new ActiveHashes(sqlContext)
    activeHashes.execute(inputScrapes)
    activeHashes.save(output)
  }

  def sortJob(input: String, output: String) {
    val textScrapes = sc.textFile(input)
    //val inputScrapes = textScrapes.map(_.split("\\s+")).map(p => (p(0), p(1))).reduceByKey((a,b) => b).sortByKey()
    val inputScrapes = textScrapes.map(_.split("\\s+")).map(p => BTScrape(p(0), p(1), p(2), Utils.guard(p(3)), Utils.guard(p(4)), Utils.guard(p(5)))).toDF()
    inputScrapes.select('tracker, 'hash, 'ts, 'seeders, 'leechers, 'downloads, 'seeders+'leechers, 'seeders+'downloads, 'leechers+'downloads)
    inputScrapes.rdd.saveAsTextFile(output+"/sort")
  }

  def main(args: Array[String]) {
    //singleQuery(args(0), args(1))

    if (args(0) == "BTWorld")
      workflow(args(1), args(2))
    if (args(0) == "Sort")
      sortJob(args(1), args(2))

  }

}
