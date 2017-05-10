package hr.fer.stflink.queries.sql

import hr.fer.stflink.core.common.{sttuple, TumblingWindow, endTime, lengthAtTime}
import hr.fer.stflink.core.data_types.{TemporalPoint, stFlink}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Q4 {

  /** Q4
    *
    * Find all mobile objects (id, position and distance traveled) that have travelled
    * more than 10 km during last hour.
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
      .tPoint(ststream, TumblingWindow(Time.minutes(60)))

    tEnv.registerDataStream("TemporalPoints", temporalstream, 'id, 'tempPoint)
    tEnv.registerFunction("lengthAtTime", lengthAtTime)
    tEnv.registerFunction("endTime", endTime)

    val q4 = tEnv.sql(
      "SELECT id, tempPoint, lengthAtTime(tempPoint, endTime(tempPoint)) as distanceTraveled " +
      "FROM TemporalPoints" +
      "WHERE distanceTraveled > 10000"
    )

    q4.toDataStream[(Int, TemporalPoint, Double)]
      .map (element => (element._1, element._2.atFinal().geom, element._3))
      .print

    env.execute("stFlink queries - Table API SQL: Q4")
  }
}
