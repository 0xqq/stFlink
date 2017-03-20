package hr.fer.stflink.operations.table_api

import java.sql.Timestamp
import hr.fer.stflink.operations.common._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.locationtech.jts.geom._
import org.apache.flink.table.api.scala._

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

    val stream = env.socketTextStream("localhost", 9999)
    val geoLifeStream: DataStream[GeoLifeTuple] = stream.map{ tuple => GeoLifeTuple(tuple) }
      .assignAscendingTimestamps(tuple => tuple.timestamp.getTime)

    val q1 = geoLifeStream
      .toTable(tEnv, 'id, 'point, 'timestamp)
      .select('id, 'point)
      .where(within('point, areaOfInterest()))

    q1.toDataStream[(Int, Point, Timestamp)]
      .print()

    env.execute("stFlink operations - Table API: Q1")
  }
}