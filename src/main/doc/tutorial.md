% Spark tutorial 
% Timothy Hunter
% July 2012

Spark geo tutorial
===================

This is a small tutorial intended to show how to use [spark](http://spark-project.org) 
for some geocoded vizualization. At this end 
of this tutorial, you will be able to infer the common trip patterns of [taxicabs in San Francisco].

In this tutorial, we will load a dataset with spark, make some preliminary analysis and plot some features of the
data in the web browser. Then we will run a simple clustering algorithm that gives more insight on the data.

[taxicabs in San Francisco]: http://www.youtube.com/watch?v=OxCPL4KsDfI&feature=plcp

Getting started
----------------

We will run spark on [EC2] using data already stored in [S3]

[EC2]: xxx
[S3]: xxx


*Setting up the spark and creating an ec2 account*

Launch a small cluster:

```bash
cd spark/ec2
./spark-ec2 -k radlab_mm_tjhunter -i ~/.ssh/radlab_mm_tjhunter.pem -t m1.medium -m m1.medium launch geo-tutorial
```

Wait for it to start...

Make sure we are running with the latest version of spark. In the remote master:

```bash
cd spark/
git pull origin
./sbt/sbt update compile publish-local
```

Let us use a spark shell for now:

```bash
MASTER=localhost:5050 ./spark-shell
```

This should run fine. Now go out by hitting `Ctrl+D`, we are going to download the files for this tutorial.

```bash
cd ~
git clone git://github.com/tjhunter/spark-geo-tutorial.git
cd spark-geo-tutorial
./sbt/sbt update compile publish-local
```

We implemented a host of useful code to save you from some typing during this tutorial.
These commands will make sure this code is available to spark during our interactive session.


```bash
export SPARK_CLASSPATH=`./sbt/sbt get-jars | grep .ivy2`
# HOW TO SEND IT TO THE SLAVES?
```


Access to the data
------------------

```bash
export AWS_ACCESS_KEY=...
export AWS_SECRET_ACCESS_KEY=...
```

Running spark
--------------

In order to start spark, you also need to provide some code dependencies that we will be using to manipulate time objects. 
Also, you will not need to type some useful code to manipulate the data structures.


```bash
cd ~/spark
MASTER=localhost:5050 ./spark-shell
```

Now that you are in the spark shell, let s get the data in. First we will import some useful utilities:

```scala
import spark.tutorial.geo.GeoTutorialUtils._
import spark.tutorial.geo._
```

The taxi dataset is stored in a file (replace with hadoop):

In the cluster:

```scala
# FIXME: HOW TO IMPORT EVERYTHING IN THE BUCKET?
val fname = "s3n://$AWS_ACCESS_KEY:$AWS_SECRET_ACCESS_KEY@cabspotting-data/2009-3-22.txt"
val numSplits = 10
val raw_data = sc.textFile(fname)
println("Number of raw data points: " + raw_data.count)
```

Nothing has happened yet. Now spark will load the file and return the size of the dataset:

```scala
println("Number of raw data points: " + raw_data.count)
```

This can take a while depending on the number of machines and the network bandwidth. Have a look at the data:
  
```scala
raw_data.first
```

> String = aslagni 0 37.7656517029 -122.407623291 2011-01-14T00:00:00

Each line is an observation for a taxi. It has an ID for the driver, a status (is the taxi hired or not hired),
the latitude and longitude and a timestamp. We will represent it in the code with the following class:

```scala
  case class Observation(
    val id: String,
    val hired: Boolean,
    val date: DateTime,
    val location: Coordinate)

  case class Coordinate(val lat: Double, val lon: Double)
```

To save you some typing, we have implemented a function (`spark.tutorial.geo.GeoTutorialUtils.stringToObservation`)
that reads text lines into an `Observation` object. You can map the raw data (a bunch of strings) into a set of 
`Observation` objects:

```scala
  val observations = raw_data.map(s => stringToObservation(s))
```

You can now manipulate the observations as a regular collections. For example, the first observations we have is:

```scala
  observations.first
```

  > res2: spark.tutorial.geo.Observation = Observation(aslagni,false,2011-01-14T00:00:00.000-08:00,Coordinate(37.7656517029,-122.407623291))

Just to see them on a map, we are going to sample a few observations ans display them, using the `sample` method:

```scala
  val sampleObservations = observations.sample(false, 1e-3, numSplits)
```

How many did we get?

```scala
  sampleObservations.count
```

This is enough to get a rough estimate. We are now going to vizualize this data in the browser. For your convenience,
we have added a few methods that converts all the geo objects into the WKT (well known text) format. You can
plot the data in your browser using the web page here: ...

```scala
  println(locationsToWKTString(localSampleObservations.map(_.location)))
```

You can copy the resulting string in the browser. You should get a display like this:

Now we are going to extract the taxi trips from this data. A taxi trip is a sequence of hired points, followed and 
starting with non-hired points. First, we will partition all the observations by day and by driver, and then work
on each of the subsequences:

```scala
  val by_date_drivers = observations.groupBy(datum => (datum.date.toYearMonthDay(), datum.id))
```

We have already implemented a function to extract the sequences of points for you in `spark.tutorial.geo.GeoTutorialUtils.splitIntoTaxiTrips`)
Now we can get all the taxi trips:

```scala
  val taxiTrips = by_date_drivers.flatMap({ case (key, seq) => splitIntoTaxiTrips(seq) })
```

Since we are going to make some repeated calls to this dataset, we will ask spark to cache it in memory:

```scala
  val cachedTaxiTrips = taxiTrips.cache()
```

Now, we are going to cluster the trips by origin and destination, using the K-means algorithm.