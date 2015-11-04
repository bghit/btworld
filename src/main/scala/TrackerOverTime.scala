import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * Created by Bogdan Ghit on 10/26/15.
 */


class TrackerOverTime(context: SQLContext) extends Query {

  override var outputDF: DataFrame = _

  override def execute(inputDF : DataFrame) = {
    import context.implicits._

    val totScrapes = inputDF.select('hash, 'tracker, Utils.timegroup('ts).as('tg),
                              ('seeders+'leechers).as('sessions),
                              Utils.ratio('seeders, 'leechers).as('slratio),
                              Utils.noSeed('seeders, 'leechers).as('noseed),
                              Utils.noLeech('seeders, 'leechers).as('noleech))

    val swarmStats = totScrapes.groupBy('hash, 'tracker, 'tg)
                            .agg('tracker, 'tg, avg('sessions).as('sessions),
                             sum('noseed).as('noseed),
                             sum('noleech).as('noleech),
                             count("*").as('sampleCount))

    outputDF = swarmStats.groupBy('tracker, 'tg)
              .agg('tracker, swarmStats("tg"), count("*").as('hashcount),
                sum(swarmStats("sessions")).as('sessions),
                sum(swarmStats("noseed")).as('noseed),

                sum(swarmStats("noleech")).as('noleech),
                sum(swarmStats("sampleCount")).as('sampleCount))
  }

  override def save(path: String) = {
    outputDF.rdd.saveAsTextFile(path+"/"+this.getClass.getName)
  }

  override def cache() = {
    outputDF.cache()
  }

}
