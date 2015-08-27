name := "druid-spark-batch"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"
libraryDependencies += "io.druid" % "druid-processing" % "[0.8,)"
libraryDependencies += "io.druid" % "druid-server" % "[0.8,)"
libraryDependencies += "io.druid" % "druid-indexing-service" % "[0.8,)"
libraryDependencies += "io.druid" % "druid-indexing-hadoop" % "[0.8,)"
libraryDependencies += "com.sun.jersey" % "jersey-servlet" % "1.17.1"
libraryDependencies += "org.xerial.snappy" % "snappy-java" % "1.1.2-M1"

resolvers += Resolver.mavenLocal
