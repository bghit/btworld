import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * Created by Bogdan Ghit on 11/2/15.
 */

class TopKTrackersLocal(context: SQLContext) extends Query with Serializable {
  override var outputDF: DataFrame = _

  var output: RDD[Scrape] = _

  override def execute(inputDF: DataFrame) = {
    import context.implicits._

    outputDF = inputDF.select('tracker, 'tg, 'sessions)

    output = outputDF.rdd.map(p => (p(1), Scrape(p.getString(0), p.getString(1), p.getDouble(2))))
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
      }).map(_._2.queue.toArray).flatMap(x => x)
  }


  override def save(path: String) = {
    outputDF.rdd.saveAsTextFile(path+"/"+this.getClass.getName)
  }

  override def cache() = {
    output.cache()
  }
}


