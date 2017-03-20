package hr.fer.stflink.operations.streaming_api

import java.util.concurrent.TimeUnit

import hr.fer.stflink.operations.common._
import hr.fer.stflink.operations.data_model._
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
      .assignAscendingTimestamps( tuple => tuple.timestamp.getTime )
      .keyBy(0)
      .timeWindow(Time.of(30, TimeUnit.MINUTES))
      .apply { stFlinkDataModel.temporalPoint _ }
      .map( mo => (mo.id, mo.location.minDistance(pointOfInterest)) )

    q3.print

    env.execute("stFlink operations - Streaming API: Q3")
  }
}
