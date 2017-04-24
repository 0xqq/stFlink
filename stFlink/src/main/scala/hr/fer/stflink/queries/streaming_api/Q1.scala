package hr.fer.stflink.queries.streaming_api

import hr.fer.stflink.core.common.{GeoLifeTuple, Helpers}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic

object Q1 {

  /** Q1
    *
	  * Continuously report mobile objects (id and position) within the area of interest.
	  *
	  */
  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.socketTextStream("localhost", 9999)
    val geoLifeStream: DataStream[GeoLifeTuple] = stream.map{ tuple => GeoLifeTuple(tuple) }

    var areaOfInterest = Helpers.createAreaOfInterest

    val q1 = geoLifeStream
      .assignAscendingTimestamps( geoLifeTuple => geoLifeTuple.timestamp.getTime )
      .filter( geoLifeTuple => geoLifeTuple.position.within(areaOfInterest))
      .map( geoLifeTuple => (geoLifeTuple.id, geoLifeTuple.position))

    q1.print

    env.execute("stFlink queries - Streaming API: Q1")

  }
}