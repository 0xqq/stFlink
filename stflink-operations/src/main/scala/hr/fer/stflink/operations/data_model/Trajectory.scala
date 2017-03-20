package hr.fer.stflink.operations.data_model

import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.geom.LineString

class Trajectory(var tDomain: TimePeriod, var lineS: LineString) {
    var _temporalDomain: TimePeriod = tDomain
    var _linestring: LineString = lineS
    
    def temporalDomain = {
      _temporalDomain
    }
    
    def temporalDomain_= (value: TimePeriod): Unit = {
		_temporalDomain = value
    }
    
    def linestring = {
      _linestring
    }
    
    def linestring_= (value: LineString): Unit = {
		_linestring = value
    }
    
    def this() {
      this(new TimePeriod(), new GeometryFactory().createLineString(Array(new Coordinate(1,1))))
    }

}
