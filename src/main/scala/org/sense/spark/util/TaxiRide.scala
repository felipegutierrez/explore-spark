package org.sense.spark.util

import org.joda.time.DateTime

case class TaxiRide(rideId: Long, isStart: Boolean, startTime: DateTime, endTime: DateTime,
                    startLon: Float, startLat: Float, endLon: Float, endLat: Float,
                    passengerCnt: Short, taxiId: Long, driverId: Long)

object TaxiRide {
  def distance(lat1: Float, lon1: Float, lat2: Float, lon2: Float): Float = {
    distance(lat1, lon1, lat2, lon2, "K")
  }

  def distance(lat1: Float, lon1: Float, lat2: Float, lon2: Float, unit: String): Float = {
    if ((lat1.equals(lat2)) && (lon1.equals(lon2))) {
      0.0F
    } else {
      val theta = lon1 - lon2
      var dist = Math.sin(Math.toRadians(lat1)) * Math.sin(Math.toRadians(lat2)) + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.cos(Math.toRadians(theta))
      dist = Math.acos(dist)
      dist = Math.toDegrees(dist)
      dist = dist * 60 * 1.1515
      if (unit.equals("K")) dist = dist * 1.609344
      else if (unit.equals("N")) dist = dist * 0.8684
      dist.toFloat
    }
  }
}
