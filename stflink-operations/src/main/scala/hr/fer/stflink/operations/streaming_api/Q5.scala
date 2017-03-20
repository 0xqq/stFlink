package hr.fer.stflink.operations.streaming_api

import java.util.concurrent.TimeUnit

import hr.fer.stflink.operations.common._
import hr.fer.stflink.operations.data_model._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object Q5 {

  /** Q5
    *
    * Find trajectories of the mobile objects that have been less than 500 meters
    * from a point of interest within last 15 minutes.
    *
   */
  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.socketTextStream("localhost", 9999)
    val geoLifeStream: DataStream[GeoLifeTuple] = stream.map{ tuple => GeoLifeTuple(tuple) }

    val pointOfInterest = Helpers.createPointOfInterest

    val q5 = geoLifeStream
      .assignAscendingTimestamps( tuple => tuple.timestamp.getTime )
      .keyBy(0)
      .timeWindow(Time.of(15, TimeUnit.MINUTES))
      .apply { stFlinkDataModel.temporalPoint _ }
      .filter( mo => mo.location.distance(pointOfInterest, mo.location.endTime).asInstanceOf[Double] < 500)
      .map( mo => (mo.id, mo.location.subTrajectory(mo.location.startTime, mo.location.endTime)) )

    q5.print

    env.execute("stFlink operations - Streaming API: Q5")
  }
}
