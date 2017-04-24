package hr.fer.stflink.queries.streaming_api

import java.util.concurrent.TimeUnit

import hr.fer.stflink.core.common.GeoLifeTuple
import hr.fer.stflink.core.data_types.temporal
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object Q4 {

  /** Q4
    *
    * Find all mobile objects (id, position and distance traveled) that have travelled
    * more than 10 km during last hour.
    *
    */
  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.socketTextStream("localhost", 9999)
    val geoLifeStream: DataStream[GeoLifeTuple] = stream.map{ tuple => GeoLifeTuple(tuple) }

    val q4 = geoLifeStream
      .assignAscendingTimestamps(geoLifeTuple => geoLifeTuple.timestamp.getTime )
      .keyBy(0)
      .timeWindow(Time.of(60, TimeUnit.MINUTES))
      .apply { temporal.temporalPoint _ }
      .filter( mo => mo.location.lengthAtTime(mo.location.endTime) > 10000)
      .map( mo => (mo.id, mo.location.atFinal.geom, mo.location.lengthAtTime(mo.location.endTime)) )

    q4.print

    env.execute("stFlink queries - Streaming API: Q4")
  }
}
