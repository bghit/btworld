import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._

/**
 * Created by Bogdan Ghit on 11/9/15.
 */
class TopKTrackersGlobal(context: SQLContext) extends Query with Serializable {
  override var outputDF: DataFrame = _
  var output: RDD[Scrape] = _

  override def cache(): Unit = {
    outputDF.cache()
  }

  override def execute(inputDF: DataFrame): Unit = {
    import context.implicits._

    val auxDF = inputDF.select('tracker, 'sessions).groupBy('tracker).agg(max('sessions).as('sessions))


   outputDF = auxDF.rdd.map(p => (p(0), Scrape(p.getString(0), "", p.getLong(1))))
      .combineByKey[TopKRank](
        createCombiner = (s: Scrape) => {
          var top = new TopKRank
          top.insert(s)
          top
        },

        mergeValue = (top: TopKRank, s: Scrape) => {
          top.insert(s)
          top
        },

        mergeCombiners = (topa: TopKRank, topb: TopKRank) => {
          var top = new TopKRank
          top.merge(topa, topb)
        }).map(_._2.queue.toArray).flatMap(x => x).toDF().select('tracker, 'sessions)

  }

  override def save(path: String): Unit = {
    outputDF.rdd.saveAsTextFile(path+"/"+this.getClass.getName)
  }
}
