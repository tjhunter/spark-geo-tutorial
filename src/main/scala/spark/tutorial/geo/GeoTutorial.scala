package spark.tutorial.geo

import spark._
import spark.SparkContext._
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

  /**
   * The distance between two coordinates, in meters.
   */
  def distanceTo(other: Coordinate): Double = {
    val lat1 = math.Pi / 180.0 * lat
    val lon1 = math.Pi / 180.0 * lon
    val lat2 = math.Pi / 180.0 * other.lat
    val lon2 = math.Pi / 180.0 * other.lon
    // Uses the haversine formula:
    val dlon = lon2 - lon1
    val dlat = lat2 - lat1
    val a = math.pow(math.sin(dlat / 2), 2) + math.cos(lat1) *
      math.cos(lat2) * math.pow(math.sin(dlon / 2), 2)
    val c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    val meters = 6367 * c * 1000
    meters
  }
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
case class Observation(
  val id: String,
  val hired: Boolean,
  val date: DateTime,
  val location: Coordinate)

/**
 * Some data representing a taxi trip.
 *
 * @param the id of the driver
 * @param start: the start coordinate of the trip
 * @param end the end coordinate of the trip
 */
case class TaxiTrip(
  val id: String,
  val start: Coordinate,
  val end: Coordinate) {
  def toWKT: String = "LINESTRING (%f %f, %f %f)" format (start.lon, start.lat, end.lon, end.lat)
}

/**
 * A bounding box for coordinates.
 */
case class BoundingBox(val bottomLeft: Coordinate, val topRight: Coordinate) {
  /**
   * Returns the union of two bounding boxes.
   */
  def union(other: BoundingBox): BoundingBox = {
    val bottomLeftLat = math.min(bottomLeft.lat, other.bottomLeft.lat)
    val bottomLeftLon = math.min(bottomLeft.lon, other.bottomLeft.lon)
    val topRightLat = math.max(topRight.lat, other.topRight.lat)
    val topRightLon = math.max(topRight.lon, other.topRight.lon)
    new BoundingBox(
      new Coordinate(bottomLeftLat, bottomLeftLon),
      new Coordinate(topRightLat, topRightLon))
  }

  /**
   * Returns the union of this bounding box with a coordinate.
   */
  def union(other: Coordinate): BoundingBox = union(BoundingBox(other, other))

  /**
   * String representation that can be plotted on a map.
   */
  def toWKT: String = "POLYGON (%f %f, %f %f, %f %f, %f %f)".format(
    bottomLeft.lon, bottomLeft.lat,
    bottomLeft.lon, topRight.lat,
    topRight.lon, topRight.lat,
    topRight.lon, bottomLeft.lat)
}

/**
 * A few utility functions that are not particularly interesting for the understanding
 * of the problem.
 */
object GeoTutorialUtils {
  /**
   * Converts a collection of coordinate to a collection of points in the WKT string format.
   * The resulting string can be pasted onto the display.
   */
  def locationsToWKTString(locations: Seq[Coordinate]): String = {
    val s = locations.map(_.toWKT).mkString(",")
    "GEOMETRYCOLLECTION(%s)" format s
  }

  def wkt2(trips: Seq[TaxiTrip]): String = {
    val s = trips.map(_.toWKT).mkString(",")
    "GEOMETRYCOLLECTION(%s)" format s
  }

  private[this] val formatter = ISODateTimeFormat.dateHourMinuteSecond()
  /**
   * Reads a line of the file into a Datum object.
   */
  def stringToObservation(s: String): Observation = {
    val splits = s.split(" ")
    val id = splits(0)
    val hired = if (splits(1) == "1") true else false
    val lat = splits(2).toDouble
    val lon = splits(3).toDouble
    val date = formatter.parseDateTime(splits(4))
    new Observation(id, hired, date, new Coordinate(lat, lon))
  }

  private def splitIntoTaxiTripsReq(obs: Seq[Observation]): List[TaxiTrip] = {
    val good_obs = obs.dropWhile(_.hired == false)
    if (good_obs.isEmpty) {
      Nil
    } else {
      // Remove all the non hired elements
      val hired_obs = good_obs.takeWhile(_.hired == true)
      val other_trips = splitIntoTaxiTripsReq(good_obs.dropWhile(_.hired == true))
      val trip = new TaxiTrip(hired_obs.head.id, hired_obs.head.location, hired_obs.last.location)
      trip :: other_trips
    }
  }

  private[this] implicit def dateTimeOrdering: Ordering[DateTime] = Ordering.fromLessThan(_ isBefore _)

  /**
   * Takes a sequence of observations for a driver and splits it into a sequence of taxi trips.
   */
  def splitIntoTaxiTrips(obs: Seq[Observation]): Seq[TaxiTrip] = {
    splitIntoTaxiTripsReq(obs.sortBy(_.date)).toArray.toSeq
  }

  /**
   * Creates an empty bounding box.
   */
  def emptyBoundingBox = new BoundingBox(new Coordinate(1000, 1000), new Coordinate(-1000, -1000))
}

object GeoTutorial {

  def exec(sc: SparkContext): Unit = {

    import spark.tutorial.geo.GeoTutorialUtils._
    import spark.tutorial.geo._
    import spark._
    val fname = "/tmp/cabspotting.txt"
    val numSplits = 1
    val raw_data = sc.textFile(fname, numSplits)

    println("Number of raw data points: " + raw_data.count)

    val observations = raw_data.map(s => stringToObservation(s))
    println("Example of locations " + observations.first())

    val sampleObservations = observations.sample(false, 1e-3, numSplits)
    println("num sample locations: " + sampleObservations.count)
    
    val localSampleObservations = sampleObservations.collect
    println(locationsToWKTString(localSampleObservations.map(_.location)))

    val by_date_drivers = observations.groupBy(datum => (datum.date.toYearMonthDay(), datum.id))
    println("num blocks:" + by_date_drivers.count())

    val taxiTrips = by_date_drivers.flatMap({ case (key, seq) => splitIntoTaxiTrips(seq) })

    val cachedTaxiTrips = taxiTrips.cache()

    println("num taxi trips:" + cachedTaxiTrips.count())
    
    val trip = cachedTaxiTrips.first()
    println(trip.toWKT)

    // K-means
    val numClusters = 100
    val numIterations = 10
    var clusterCenters = (for (idx <- 0 until numClusters) yield {
      val datum = localSampleObservations(idx % localSampleObservations.size)
      ((datum.location, datum.location), 0)
    }).toArray

    var clustered_trips: RDD[(Int, TaxiTrip)] = null
    var clustered_trips_centers: Array[((Coordinate, Coordinate), Int)] = null

    for (iter <- 0 until numIterations) {
      val clusterCenters_bc = sc.broadcast(clusterCenters)
      clustered_trips_centers = clusterCenters
      // Aggregate the taxi trips by closeness to each centroid
      clustered_trips = cachedTaxiTrips.map(trip => {
        val distances = clusterCenters_bc.value.map(center => {
          val ((from, to), _) = center
          val d1 = trip.start.distanceTo(from)
          val d2 = trip.end.distanceTo(to)
          math.sqrt(d1 * d1 + d2 * d2)
        })
        // Find the argmin
        val minDistance = distances.min
        val minIdx = distances.findIndexOf(_ == minDistance)
        (minIdx, trip)
      })
      // Recompute the center of each centroid
      val tripsByCentroid = clustered_trips.groupBy(_._1).mapValues(_.map(_._2))
      val newCenters = tripsByCentroid.mapValues(trips => {
        var count = 0
        var start_lat = 0.0
        var start_lon = 0.0
        var end_lat = 0.0
        var end_lon = 0.0
        for (trip <- trips) {
          count += 1
          start_lat += trip.start.lat
          start_lon += trip.start.lon
          end_lat += trip.end.lat
          end_lon += trip.end.lon
        }
        ((new Coordinate(start_lat / count, start_lon / count), new Coordinate(end_lat / count, end_lon / count)), count)
      })
      val centers = newCenters.collect().map(_._2).sortBy(-_._2)
      println("New centers:\n")
      for (((start, from), size) <- centers) {
        println("%d :  %s".format(size, TaxiTrip("", start, from).toWKT))
      }
      clusterCenters = centers
      println(wkt2(clusterCenters.take(20).map(z => new TaxiTrip("", z._1._1, z._1._2))))
    }

    // More work to look at a cluster
    val cached_clustered_trips = clustered_trips.cache()

    def union(b: BoundingBox, c: Coordinate) = b.union(c)
    def combo(b1: BoundingBox, b2: BoundingBox) = b1.union(b2)
    for (cluster_id <- Array(0, 1, 2, 10, 20)) {
      val cluster_trips = cached_clustered_trips.filter(_._1 == cluster_id).map(_._2)
      val fromBBox = cluster_trips.map(_.start).aggregate(emptyBoundingBox)(union _, combo _)
      val toBBox = cluster_trips.map(_.end).aggregate(emptyBoundingBox)(union _, combo _)
      println("Detailled analysis of cluster #" + cluster_id)
      val (start, from) = clustered_trips_centers(cluster_id)._1
      println("GEOMETRYCOLLECTION(%s,\n%s,\n%s)" format (fromBBox.toWKT, TaxiTrip("", start, from).toWKT, toBBox.toWKT))
    }
  }
}

object GeoTutorialExec extends App {
  val sc = new SparkContext("local[1]", "geo")
  GeoTutorial.exec(sc)
}