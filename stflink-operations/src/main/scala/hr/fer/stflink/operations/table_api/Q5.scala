package hr.fer.stflink.operations.table_api

import hr.fer.stflink.operations.common._
import hr.fer.stflink.operations.data_model.{TemporalPoint, stFlink}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.locationtech.jts.geom.LineString

object Q5 {

  /** Q5
    *
    * Find trajectories of the mobile objects that have been less than 500 meters
    * from a point of interest within last 15 minutes.
    *
    */
  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.socketTextStream("localhost", 9999)
    val geoLifeStream: DataStream[GeoLifeTuple] = stream.map{ tuple => GeoLifeTuple(tuple) }
      .assignAscendingTimestamps(tuple => tuple.timestamp.getTime)

    val q5 = stFlink
      .tPoint(geoLifeStream, TumblingWindow(Time.minutes(15)))
      .toTable(tEnv, 'id, 'tempPoint)
      .select('id, 'tempPoint, subTrajectory('tempPoint, startTime('tempPoint), endTime('tempPoint)) as 'subTrajectory)
      .where(distance('tempPoint, pointOfInterest(), endTime('tempPoint)) < 500 )

    q5.toDataStream[(Int, TemporalPoint, LineString)]
      .map (element => (element._1, element._3))
      .print()

    env.execute("stFlink operations - Table API: Q5")
  }
}