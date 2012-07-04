package spark.tutorial.geo

import spark._
import org.joda.time.DateTime

/**
 * A coordinate of a point on the earth surface, represented by a pair of a latitude and longitude.
 */
case class Coordinate(val lat:Double, val lon:Double) {
  /**
   * Gives the equivalent representation in the Well-Known-Text format.
   */
  def toWKT:String = "POINT (%f %f)" format (lat, lon)
}

/**
 * The piece of data we will be working with.
 * 
 * Each taxi observation consists in:
 * - a taxi driver identifier
 * - a indicator wether the taxi got hired or not
 * - the date of the observation
 * - the location of the objservation
 */
case class Datum(val id:String, val hired:Boolean, val date:DateTime, val location:Coordinate)

object GeoTutorial {

  def exec(sc:SparkContext):Unit = {
    
  }
}