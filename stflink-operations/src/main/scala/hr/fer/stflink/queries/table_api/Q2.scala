package hr.fer.stflink.queries.table_api

import hr.fer.stflink.core.common.{GeoLifeTuple, SlidingWindow, endTime, lengthAtTime}
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

    val stream = env.socketTextStream("localhost", 9999)
    val geoLifeStream: DataStream[GeoLifeTuple] = stream.map{ tuple => GeoLifeTuple(tuple) }
      .assignAscendingTimestamps(geoLifeTuple => geoLifeTuple.timestamp.getTime)

    val q2 = stFlink
      .tPoint(geoLifeStream, SlidingWindow(Time.minutes(10), Time.minutes(1)))
      .toTable(tEnv, 'id, 'tempPoint)
      .select('id, 'tempPoint)
      .where(lengthAtTime('tempPoint, endTime('tempPoint)) > 3000 )

    q2.toDataStream[(Int, TemporalPoint)]
      .map (element => element._2.atFinal().geom)
      .print

    env.execute("stFlink queries - Table API: Q2")
  }
}
