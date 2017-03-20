package hr.fer.stflink.operations.table_api

import hr.fer.stflink.operations.common._
import hr.fer.stflink.operations.data_model.{TemporalPoint, stFlink}
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
      .assignAscendingTimestamps(tuple => tuple.timestamp.getTime)

    val q2 = stFlink
      .tPoint(geoLifeStream, SlidingWindow(Time.minutes(10), Time.minutes(1)))
      .toTable(tEnv, 'driverId, 'tempPoint)
      .select('driverId, 'tempPoint)
      .where(lengthAtTime('tempPoint, endTime('tempPoint)) > 3000 )

    val resultStream = q2.toDataStream[(Int, TemporalPoint)]
      .map (element => element._2.atFinal().geom)
    resultStream.print()

    env.execute("stFlink operations - Table API: Q2")
  }
}
