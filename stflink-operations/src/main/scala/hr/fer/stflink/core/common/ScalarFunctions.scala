package hr.fer.stflink.core.common

import java.sql.Timestamp

import hr.fer.stflink.core.data_types.TemporalPoint
import org.apache.flink.table.functions.ScalarFunction
import org.locationtech.jts.geom._

object startTime extends ScalarFunction {
  def eval(tempPoint: TemporalPoint): Timestamp = {
    tempPoint.startTime
  }
}

object endTime extends ScalarFunction {
  def eval(tempPoint: TemporalPoint): Timestamp = {
    tempPoint.endTime
  }
}

object lengthAtTime extends ScalarFunction {
  def eval(tempPoint: TemporalPoint, tinstant: Timestamp): Double = {
    tempPoint.lengthAtTime(tinstant)
  }
}

object within extends ScalarFunction {
  def eval(sourceGeom: Point, destGeom: Geometry) : Boolean = {
    sourceGeom.within(destGeom)
  }
}

object minDistance extends ScalarFunction {
  def eval(temporalPoint: TemporalPoint, geom : Geometry) : Double = {
    temporalPoint.minDistance(geom)
  }
}

object distance extends ScalarFunction {
  def eval(sourceGeom: TemporalPoint, destGeom: Geometry, tinstance: Timestamp) : Double = {
    sourceGeom.distance(destGeom, tinstance).asInstanceOf[Double]
  }
}

object subTrajectory extends ScalarFunction {
  def eval(temporalPoint: TemporalPoint, begin : Timestamp, end: Timestamp) : LineString = {
    temporalPoint.subTrajectory(begin, end)
  }
}

object areaOfInterest extends ScalarFunction {
  def eval(): Geometry = {
    var aoi = Helpers.createAreaOfInterest
    aoi.asInstanceOf[Geometry]
  }
}

object pointOfInterest extends ScalarFunction {
  def eval(): Geometry = {
    var poi = Helpers.createPointOfInterest
    poi.asInstanceOf[Geometry]
  }
}






