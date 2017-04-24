package hr.fer.stflink.queries.sql

import hr.fer.stflink.core.common.{GeoLifeTuple, TumblingWindow, minDistance, pointOfInterest}
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

    val stream = env.socketTextStream("localhost", 9999)
    val geoLifeStream: DataStream[GeoLifeTuple] = stream.map{ tuple => GeoLifeTuple(tuple) }
      .assignAscendingTimestamps( geoLifeTuple => geoLifeTuple.timestamp.getTime )

    val ststream = stFlink
      .tPoint(geoLifeStream, TumblingWindow(Time.minutes(30)))

    tEnv.registerDataStream("TemporalPoints", ststream, 'id, 'tempPoint)
    tEnv.registerFunction("minDistance", minDistance)
    tEnv.registerFunction("pointOfInterest", pointOfInterest)

    val q3 = tEnv.sql(
      "SELECT id, minDistance(tempPoint, pointOfInterest()) as minimalDistance " +
      "FROM TemporalPoints" +
      "GROUP BY id"
    )

    q3.toDataStream[(Int, Double)]
      .print

    env.execute("stFlink queries - Table API SQL: Q3")
  }
}