package hr.fer.stflink.operations.streaming_api

import hr.fer.stflink.operations.common._
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
      .assignAscendingTimestamps( tuple => tuple.timestamp.getTime )
      .filter( geolife => geolife.position.within(areaOfInterest))
      .map( geolife => (geolife.id, geolife.position))

    q1.print

    env.execute("stFlink operations - Streaming API: Q1")

  }
}