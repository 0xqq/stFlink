package hr.fer.stflink.queries.sql

import hr.fer.stflink.core.common.{sttuple, areaOfInterest, within}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.locationtech.jts.geom.Point

object Q1 {

  /** Q1
    *
    * Continuously report mobile objects (id and position) within the area of interest.
    *
    */
  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val rawstream = env.socketTextStream("localhost", 9999)
    val ststream: DataStream[sttuple] = rawstream.map{ tuple => sttuple(tuple) }
      .assignAscendingTimestamps( tuple => tuple.timestamp.getTime )

    tEnv.registerDataStream("Points", ststream, 'id, 'position, 'timestamp)
    tEnv.registerFunction("within", within)
    tEnv.registerFunction("areaOfInterest", areaOfInterest)

    val q1 = tEnv.sql(
      "SELECT id, position " +
      "FROM Points " +
      "WHERE within(position, areaOfInterest())"
    )

    q1.toDataStream[(Int, Point)]
      .print

    env.execute("stFlink queries - Table API SQL: Q1")
  }
}
