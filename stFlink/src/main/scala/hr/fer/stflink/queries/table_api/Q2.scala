package hr.fer.stflink.queries.table_api

import hr.fer.stflink.core.common.{sttuple, SlidingWindow, ST_EndTime, ST_LengthAtTime}
import hr.fer.stflink.core.data_types.{TemporalPoint, stFlink}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Q2 {

  /** Q2
    *
    * Continuously each minute, report location of mobile objects which have travelled
    * more than 3 km in past 10 minutes.
    *
    */
  def main(args: Array[String]) {

    if (args.length != 2) {
      System.err.println("USAGE:\n<JAR name> <hostname> <port>")
      return
    }
    val hostName = args(0)
    val port = args(1).toInt

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val rawstream = env.socketTextStream(hostName, port)
    val ststream: DataStream[sttuple] = rawstream.map{ tuple => sttuple(tuple) }
      .assignAscendingTimestamps(tuple => tuple.timestamp.getTime)

    val q2 = stFlink
      .tPoint(ststream, SlidingWindow(Time.minutes(10), Time.minutes(1)))
      .toTable(tEnv, 'id, 'tempPoint)
      .select('id, 'tempPoint)
      .where(ST_LengthAtTime('tempPoint, ST_EndTime('tempPoint)) > 3000 )

    q2.toDataStream[(Int, TemporalPoint)]
      .map (element => element._2.atFinal().geom)
      .print

    env.execute("stFlink queries - Table API: Q2")
  }
}
