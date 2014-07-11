import sbt.Keys._

name := "blog-spark-kmeans"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.0.0"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.0.0"

libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "3.0.3"

//libraryDependencies += "rky" % "rky-voronoi" % "1.0.0"

libraryDependencies += "be.humphreys" % "simplevoronoi" % "0.2-SNAPSHOT"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

// for voronoi
resolvers += Resolver.mavenLocal