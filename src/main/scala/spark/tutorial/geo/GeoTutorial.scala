package spark.tutorial.geo

import spark._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.ISODateTimeFormat
import org.scala_tools.time.Imports._

/**
 * A coordinate of a point on the earth surface, represented by a pair of a latitude and longitude.
 */
case class Coordinate(val lat: Double, val lon: Double) {
  /**
   * Gives the equivalent representation in the Well-Known-Text format.
   */
  def toWKT: String = "POINT (%f %f)" format (lon, lat)
}

/**
 * The piece of data we will be working with.
 *
 * Each taxi observation consists in:
 * - a taxi driver identifier
 * - a indicator whether the taxi got hired or not
 * - the date of the observation
 * - the location of the observation
 */
case class Datum(
  val id: String,
  val hired: Boolean,
  val date: DateTime,
  val location: Coordinate)

/**
 * Some data representing a taxi course.
 */
case class TaxiCourse(val id: String, val start: Coordinate, val end: Coordinate)
/**
 * A few utility functions that are not particularly interesting for the understanding
 * of the problem.
 */
object GeoTutorialUtils {
  /**
   * Converts a collection of coordinate to a collection of points in the WKT string format.
   * The resulting string can be pasted onto the display.
   */
  def coordinateToWKTPoints(locations: Seq[Coordinate]): String = {
    val s = locations.map(_.toWKT).mkString(",")
    "GEOMETRYCOLLECTION(%s)" format s
  }

  private[this] val formatter = ISODateTimeFormat.dateHourMinuteSecond()
  /**
   * Reads a line of the file into a Datum object.
   */
  def stringToDatum(s: String): Datum = {
    val splits = s.split(" ")
    val id = splits(0)
    val hired = if (splits(1) == "1") true else false
    val lat = splits(2).toDouble
    val lon = splits(3).toDouble
    // TODO(tjh) remove hack
    val date = formatter.parseDateTime(splits(4).replace(",", "T"))
    new Datum(id, hired, date, new Coordinate(lat, lon))
  }
  
  private def splitIntoTaxiCoursesReq(obs:Seq[Datum]):List[TaxiCourse] = {
    val good_obs = obs.dropWhile(_.hired==false)
    if (good_obs.isEmpty) {
      Nil
    } else {
      // Remove all the non hired elements
      val hired_obs = good_obs.takeWhile(_.hired == true)
      val other_courses = splitIntoTaxiCoursesReq(good_obs.dropWhile(_.hired == true))
      val course = new TaxiCourse(hired_obs.head.id, hired_obs.head.location, hired_obs.last.location)
      course :: other_courses
    }
  }
  def splitIntoTaxiCourses(obs:Seq[Datum]):Seq[TaxiCourse] = {
    splitIntoTaxiCoursesReq(obs.sortBy(_.date)).toArray
  }
}

object GeoTutorial {

  import GeoTutorialUtils._

  def exec(sc: SparkContext): Unit = {

    val fname = "/tmp/cabspotting.txt"
    val numSplits = 1
    val raw_data = sc.textFile(fname, numSplits)

    println(raw_data.count)

    val locations = raw_data.map(s => stringToDatum(s))
    println(locations.first())

    val sample_locations = locations.sample(false, 0.1, 1).collect()
    println(coordinateToWKTPoints(sample_locations.map(_.location)))

    val by_date_drivers = locations.groupBy(datum => (datum.date, datum.id))
    println(by_date_drivers.first())
  }

}

object GeoTutorialExec extends App {
  val sc = new SparkContext("local[1]", "geo")
  GeoTutorial.exec(sc)
}