package hr.fer.stflink.core.data_types

import hr.fer.stflink.core.common.{GeoLifeTuple, SlidingWindow, TumblingWindow}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream

object stFlink {

  def tPoint(dataStream: DataStream[GeoLifeTuple], win: SlidingWindow): DataStream[sttuple] = {
    dataStream
      .keyBy(0)
      .timeWindow(win.size, win.slide)
      .apply { temporal.temporalPoint _ }
  }

  def tPoint(dataStream: DataStream[GeoLifeTuple], win: TumblingWindow): DataStream[sttuple] = {
    dataStream
      .keyBy(0)
      .timeWindow(win.size)
      .apply { temporal.temporalPoint _ }
  }

}