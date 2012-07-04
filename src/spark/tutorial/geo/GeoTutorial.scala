package spark.tutorial.geo

import spark.SparkContext

/**
 * A coordinate of a point on the earth surface, represented by a pair of a latitude and longitude.
 */
case class Coordinate(val lat:Double, val lon:Double)

case class Datum(val id:String, val hired:Boolean, val )

object GeoTutorial {

  def exec(sc:SparkContext):Unit = {
    
  }
}