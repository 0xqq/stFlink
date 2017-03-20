package hr.fer.stflink.operations.data_model

import org.apache.flink.api.scala._
import hr.fer.stflink.operations.common.{GeoLifeTuple, SlidingWindow, TumblingWindow}
import org.apache.flink.streaming.api.scala.DataStream

object stFlink {

  def tPoint(dataStream: DataStream[GeoLifeTuple], win: SlidingWindow): DataStream[sttuple] = {
    dataStream
      .keyBy(0)
      .timeWindow(win.size, win.slide)
      .apply { stFlinkDataModel.temporalPoint _ }
  }

  def tPoint(dataStream: DataStream[GeoLifeTuple], win: TumblingWindow): DataStream[sttuple] = {
    dataStream
      .keyBy(0)
      .timeWindow(win.size)
      .apply { stFlinkDataModel.temporalPoint _ }
  }

}