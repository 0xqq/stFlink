package hr.fer.stflink.queries.table_api

import hr.fer.stflink.core.common._
import hr.fer.stflink.core.data_types.{TemporalPoint, stFlink}
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
    * from a point of interest within last 5 minutes.
    *
    */
  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val rawstream = env.socketTextStream("localhost", 9999)
    val ststream: DataStream[sttuple] = rawstream.map{ tuple => sttuple(tuple) }
      .assignAscendingTimestamps(tuple => tuple.timestamp.getTime)

    val q5 = stFlink
      .tPoint(ststream, TumblingWindow(Time.minutes(5)))
      .toTable(tEnv, 'id, 'tempPoint)
      .select('id, 'tempPoint, subTrajectory('tempPoint, startTime('tempPoint), endTime('tempPoint)) as 'subtrajectory)
      .where(distance('tempPoint, pointOfInterest(), endTime('tempPoint)) < 500 )

    q5.toDataStream[(Int, TemporalPoint, LineString)]
      .map (element => (element._1, element._3))
      .print

    env.execute("stFlink queries - Table API: Q5")
  }
}