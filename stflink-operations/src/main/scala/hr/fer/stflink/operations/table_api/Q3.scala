package hr.fer.stflink.operations.table_api

import hr.fer.stflink.operations.common._
import hr.fer.stflink.operations.data_model.stFlink
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
      .assignAscendingTimestamps(tuple => tuple.timestamp.getTime)

    val q3 = stFlink
      .tPoint(geoLifeStream, TumblingWindow(Time.minutes(30)))
      .toTable(tEnv, 'id, 'tempPoint)
      .select('id, minDistance('tempPoint, pointOfInterest()) as 'minDistance)

    q3.toDataStream[(Int, Double)]
      .print()

    env.execute("stFlink operations - Table API: Q3")
  }
}