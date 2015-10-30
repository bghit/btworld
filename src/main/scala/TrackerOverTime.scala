import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._
/**
 * Created by bogdan on 10/26/15.
 */


class TrackerOverTime(context: SQLContext, inputDF: DataFrame) extends Query(context: SQLContext, inputDF) {

  override def name: String = "TrackerOverTime"


  override def execute(): DataFrame = {
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

    val trackerOverTime = swarmStats.groupBy('tracker, 'tg)
              .agg('tracker, 'tg, count("*").as('hashcount),
                sum(swarmStats("sessions")).as('sessions),
                sum(swarmStats("noseed")).as('noseed),
                sum(swarmStats("noleech")).as('noleech),
                sum(swarmStats("sampleCount")).as('sampleCount))

    return trackerOverTime
 }
}
