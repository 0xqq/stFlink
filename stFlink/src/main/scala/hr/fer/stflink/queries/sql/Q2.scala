package hr.fer.stflink.queries.sql

import java.sql.Timestamp

import hr.fer.stflink.core.common.{sttuple, SlidingWindow, endTime, lengthAtTime}
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

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val rawstream = env.socketTextStream("localhost", 9999)
    val ststream: DataStream[sttuple] = rawstream.map{ tuple => sttuple(tuple) }
      .assignAscendingTimestamps( tuple => tuple.timestamp.getTime )

    val temporalstream = stFlink
      .tPoint(ststream, SlidingWindow(Time.minutes(10), Time.minutes(1)))

    tEnv.registerDataStream("TemporalPoints", temporalstream, 'id, 'tempPoint)
    tEnv.registerFunction("endTime", endTime)
    tEnv.registerFunction("lengthAtTime", lengthAtTime)

    val q2 = tEnv.sql(
      "SELECT id, tempPoint " +
      "FROM TemporalPoints " +
      "WHERE lengthAtTime(tempPoint, endTime(tempPoint)) > 3000 " +
      "GROUP BY id"
    )

    q2.toDataStream[(Int, TemporalPoint, Timestamp)]
      .map (element => element._2.atFinal().geom)
      .print

    env.execute("stFlink queries - Table API SQL: Q2")
  }
}
