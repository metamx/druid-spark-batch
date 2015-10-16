organization := "io.druid.extensions"
name := "druid-spark-batch"

isSnapshot := true

version := "0.45-SNAPSHOT"

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.1" exclude ("log4j", "log4j") exclude ("org.apache.hadoop", "hadoop-client")
libraryDependencies += "org.spark-project.akka" %% "akka-actor" % "2.3.4-spark"
// For Path
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.4.0" exclude("javax.servlet", "servlet-api")
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.4.0" exclude("javax.servlet", "servlet-api")
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"
libraryDependencies += "io.druid" % "druid-processing" % "0.8.2-SNAPSHOT"
libraryDependencies += "io.druid" % "druid-server" % "0.8.2-SNAPSHOT"
libraryDependencies += "io.druid" % "druid-indexing-service" % "0.8.2-SNAPSHOT"
libraryDependencies += "io.druid" % "druid-indexing-hadoop" % "0.8.2-SNAPSHOT"
libraryDependencies += "com.sun.jersey" % "jersey-servlet" % "1.17.1"
libraryDependencies += "org.xerial.snappy" % "snappy-java" % "1.1.2-M1"

resolvers += Resolver.mavenLocal
