package hr.fer.stflink.operations.common

import java.sql.Timestamp

import hr.fer.stflink.operations.data_model.stFlinkDataModel
import org.locationtech.jts.geom.{GeometryFactory, Point}

case class GeoLifeTuple(id: Int, position: Point, timestamp: Timestamp) {
  def this(line: String) {
    this(line.split(",")(0).toInt, new GeometryFactory().createPoint(stFlinkDataModel.latlonToUTM(line.split(",")(1).toDouble, line.split(",")(2).toDouble)), Timestamp.valueOf(line.split(",")(6) + " " + line.split(",")(7)))
  }
}

object GeoLifeTuple {
	def apply(line: String): GeoLifeTuple = {
		var id = line.split(",")(0).toInt
		var point = new GeometryFactory().createPoint(stFlinkDataModel.latlonToUTM(line.split(",")(1).toDouble, line.split(",")(2).toDouble))
		var time = Timestamp.valueOf(line.split(",")(6) + " " + line.split(",")(7))
		var geoLife = new GeoLifeTuple(id, point, time)
		geoLife
	}
}