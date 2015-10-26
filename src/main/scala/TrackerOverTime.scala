import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._
/**
 * Created by bogdan on 10/26/15.
 */


class TrackerOverTime(context: SQLContext, inputDF: DataFrame) extends Query(context: SQLContext, inputDF) {

  override def name: String = "TrackerOverTime"


  override def execute() {
    import context.implicits._

    val fullScrapes = inputDF.select('hash, 'tracker, Utils.TIMEGROUP('ts).as('tg), 'seeders, 'leechers, 'downloads)

    val swarmStats = inputDF.groupBy('hash, 'tracker, Utils.TIMEGROUP('ts))
                            .agg('tracker, Utils.TIMEGROUP('ts).as('tg), avg('seeders+'leechers).as('sessions),
                              count('seeders === 0).as('noseed),
                              count('leechers === 0).as('noleech),
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
