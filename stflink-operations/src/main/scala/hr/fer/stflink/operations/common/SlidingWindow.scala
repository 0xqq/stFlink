package hr.fer.stflink.operations.common

import org.apache.flink.streaming.api.windowing.time.Time

case class SlidingWindow (size: Time, slide: Time, i:Int)

object SlidingWindow {
  def apply(size: Time, slide: Time ) : SlidingWindow = {
    new SlidingWindow(size, slide, 0)
  }
}