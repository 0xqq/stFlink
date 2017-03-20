package hr.fer.stflink.operations.table_api_sql

import hr.fer.stflink.operations.common._
import hr.fer.stflink.operations.data_model.{stFlink}
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
      .assignAscendingTimestamps( tuple => tuple.timestamp.getTime )

    val ststream = stFlink
      .tPoint(geoLifeStream, TumblingWindow(Time.minutes(30)))

    tEnv.registerDataStream("TemporalPoints", ststream, 'id, 'tempPoint)
    tEnv.registerFunction("minDistance", minDistance)
    tEnv.registerFunction("pointOfInterest", pointOfInterest)

    val q3 = tEnv.sql(
      "SELECT id, minDistance(tempPoint, pointOfInterest()) as minDistance " +
      "FROM TemporalPoints" +
      "GROUP BY id"
    )

    q3.toDataStream[(Int, Double)].print()

    env.execute("stFlink operations - Table API SQL: Q3")
  }
}
