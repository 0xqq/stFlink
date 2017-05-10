package hr.fer.stflink.queries.table_api

import hr.fer.stflink.core.common.{sttuple, TumblingWindow, minDistance, pointOfInterest}
import hr.fer.stflink.core.data_types.stFlink
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Q3 {

  /** Q3
    *
    * For each mobile object, find its minimal distance from the point of interest
    * during the last half an hour.
    *
    */
  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val rawstream = env.socketTextStream("localhost", 9999)
    val ststream: DataStream[sttuple] = rawstream.map{ tuple => sttuple(tuple) }
      .assignAscendingTimestamps(tuple => tuple.timestamp.getTime)

    val q3 = stFlink
      .tPoint(ststream, TumblingWindow(Time.minutes(30)))
      .toTable(tEnv, 'id, 'tempPoint)
      .select('id, minDistance('tempPoint, pointOfInterest()) as 'minimalDistance)

    q3.toDataStream[(Int, Double)]
      .print

    env.execute("stFlink queries - Table API: Q3")
  }
}