name := "spark-kmeans"

version := "0.0.1"

scalaVersion := "2.9.1"

libraryDependencies += "org.spark-project" %% "spark-core" % "0.5.1-SNAPSHOT"

// Dependencies to represent and manipulate time

libraryDependencies += "joda-time" % "joda-time" % "2.1"

libraryDependencies += "org.joda" % "joda-convert" % "1.2"

libraryDependencies += "org.scala-tools.time" %% "time" % "0.5"
