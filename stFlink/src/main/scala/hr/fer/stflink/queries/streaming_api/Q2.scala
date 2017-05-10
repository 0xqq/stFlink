package hr.fer.stflink.queries.streaming_api

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.TimeCharacteristic
import java.util.concurrent.TimeUnit

import hr.fer.stflink.core.common.sttuple
import hr.fer.stflink.core.data_types.temporal

object Q2 {

  /** Q2
    *
    * Continuously each minute, report location of mobile objects which have travelled
    * more than 3 km in past 10 minutes.
    *
	  */
  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val rawstream = env.socketTextStream("localhost", 9999)

    val ststream: DataStream[sttuple] = rawstream.map{ tuple => sttuple(tuple) }

    val q2 = ststream
      .assignAscendingTimestamps( tuple => tuple.timestamp.getTime )
      .keyBy(0)
      .timeWindow(Time.of(10, TimeUnit.MINUTES), Time.of(1, TimeUnit.MINUTES))
      .apply { temporal.temporalPoint _ }
      .filter( mo => mo.location.lengthAtTime(mo.location.endTime) > 3000 )
      .map( mo => mo.location.atFinal.geom )

    q2.print

    env.execute("stFlink queries - Streaming API: Q2")
  }
}
