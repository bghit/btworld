import java.text.{FieldPosition, SimpleDateFormat}
import java.util.Calendar

import org.apache.spark.sql.functions._

object Utils {
  def convert(ts: String): String = {
    if (ts == null)
      return null

    val sdf = new SimpleDateFormat("yyyyMMdd'T'HHmmss")
    val parsedTS = sdf.parse(ts)

    val cal = Calendar.getInstance()
    cal.setTime(parsedTS)
    cal.set(Calendar.HOUR_OF_DAY, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.SECOND, 0)

    return sdf.format(cal.getTime(), new StringBuffer(), new FieldPosition(0)).toString
  }

  val timegroup = udf((ts: String) => Utils.convert(ts))

  val ratio = udf((A: Float, B: Float) => if (B == 0) A else A / B)

  val noSeed = udf((seeders: Long, leechers: Long) => if (seeders == 0 && leechers > 0) 1 else 0)

  val noLeech = udf((seeders: Long, leechers: Long) => if (seeders > 0 && leechers == 0) 1 else 0)

}
