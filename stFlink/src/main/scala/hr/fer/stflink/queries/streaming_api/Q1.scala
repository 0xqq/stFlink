package hr.fer.stflink.queries.streaming_api

import hr.fer.stflink.core.common.{sttuple, Helpers}
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

    val rawstream = env.socketTextStream("localhost", 9999)
    val ststream: DataStream[sttuple] = rawstream.map{ tuple => sttuple(tuple) }

    var areaOfInterest = Helpers.createAreaOfInterest

    val q1 = ststream
      .assignAscendingTimestamps( tuple => tuple.timestamp.getTime )
      .filter( tuple => tuple.position.within(areaOfInterest))
      .map( tuple => (tuple.id, tuple.position))

    q1.print

    env.execute("stFlink queries - Streaming API: Q1")

  }
}