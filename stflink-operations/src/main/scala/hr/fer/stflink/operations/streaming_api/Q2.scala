package hr.fer.stflink.operations.streaming_api

import hr.fer.stflink.operations.common._
import hr.fer.stflink.operations.data_model._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.TimeCharacteristic
import java.util.concurrent.TimeUnit

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
    val stream = env.socketTextStream("localhost", 9999)

    val geoLifeStream: DataStream[GeoLifeTuple] = stream.map{ tuple => GeoLifeTuple(tuple) }

    val q2 = geoLifeStream
      .assignAscendingTimestamps( geolife => geolife.timestamp.getTime )
      .keyBy(0)
      .timeWindow(Time.of(10, TimeUnit.MINUTES), Time.of(1, TimeUnit.MINUTES))
      .apply { stFlinkDataModel.temporalPoint _ }
      .filter( mo => mo.location.lengthAtTime(mo.location.endTime) > 3000 )
      .map( mo => mo.location.atFinal.geom )

    q2.print

    env.execute("stFlink operations - Streaming API: Q2")
  }
}
