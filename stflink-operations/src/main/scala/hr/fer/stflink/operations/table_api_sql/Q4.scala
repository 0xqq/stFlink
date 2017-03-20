package hr.fer.stflink.operations.table_api_sql

import hr.fer.stflink.operations.common._
import hr.fer.stflink.operations.data_model.{TemporalPoint, stFlink}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Q4 {

  /** Q4
    *
    * Find all mobile objects (id, position and distance traveled) that have travelled
    * more than 10 km during last hour.
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
      .tPoint(geoLifeStream, TumblingWindow(Time.minutes(60)))

    tEnv.registerDataStream("TemporalPoints", ststream, 'id, 'tempPoint)
    tEnv.registerFunction("lengthAtTime", lengthAtTime)
    tEnv.registerFunction("endTime", endTime)

    val q4 = tEnv.sql(
      "SELECT id, tempPoint, lengthAtTime(tempPoint, endTime(tempPoint)) as length " +
      "FROM TemporalPoints" +
      "WHERE length > 10000"
    )

    q4.toDataStream[(Int, TemporalPoint, Double)]
      .map (element => (element._1, element._2.atFinal().geom, element._3))
      .print()

    env.execute("stFlink operations - Table API SQL: Q4")
  }
}