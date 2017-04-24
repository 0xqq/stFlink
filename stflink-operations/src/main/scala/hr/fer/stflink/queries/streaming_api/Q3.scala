package hr.fer.stflink.queries.streaming_api

import java.util.concurrent.TimeUnit

import hr.fer.stflink.core.common.{GeoLifeTuple, Helpers}
import hr.fer.stflink.core.data_types.temporal
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object Q3 {

  /** Q3
    *
    * For each mobile object, find its minimal distance from the point of interest
    * during the last half an hour.
    *
    */
  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.socketTextStream("localhost", 9999)
    val geoLifeStream: DataStream[GeoLifeTuple] = stream.map{ tuple => GeoLifeTuple(tuple) }

    val pointOfInterest = Helpers.createPointOfInterest

    val q3 = geoLifeStream
      .assignAscendingTimestamps( geoLifeTuple => geoLifeTuple.timestamp.getTime )
      .keyBy(0)
      .timeWindow(Time.of(30, TimeUnit.MINUTES))
      .apply { temporal.temporalPoint _ }
      .map( mo => (mo.id, mo.location.minDistance(pointOfInterest)) )

    q3.print

    env.execute("stFlink queries - Streaming API: Q3")
  }
}
