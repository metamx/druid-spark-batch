organization := "io.druid.extensions"
name := "druid-spark-batch"

net.virtualvoid.sbt.graph.Plugin.graphSettings

licenses := Seq("Apache License, Version 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0"))
homepage := Some(url("https://github.com/metamx/druid-spark-batch"))

scalaVersion := "2.11.7"
crossScalaVersions := Seq("2.10.5", "2.11.7")

val druid_version = "f5737d6" // 0.8.2-rc2
val hadoop_version = "2.4.0-mmx6"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.1-mmx2" exclude ("log4j", "log4j") exclude ("org.apache.hadoop", "hadoop-client")
libraryDependencies += "org.spark-project.akka" %% "akka-actor" % "2.3.4-spark"
// For Path
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % hadoop_version exclude("javax.servlet", "servlet-api")
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % hadoop_version exclude("javax.servlet", "servlet-api")

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"
libraryDependencies += "io.druid" % "druid-processing" % druid_version
libraryDependencies += "io.druid" % "druid-server" % druid_version
libraryDependencies += "io.druid" % "druid-indexing-service" % druid_version
libraryDependencies += "io.druid" % "druid-indexing-hadoop" % druid_version
libraryDependencies += "com.sun.jersey" % "jersey-servlet" % "1.17.1"
libraryDependencies += "org.xerial.snappy" % "snappy-java" % "1.1.2-M1"


resolvers += Resolver.mavenLocal

publishMavenStyle := true

publishTo := Some("central-local" at "https://metamx.artifactoryonline.com/metamx/libs-releases-local")
pomIncludeRepository := { _ => false }

pomExtra := (
  <scm>
    <url>https://github.com/metamx/druid-spark-batch.git</url>
    <connection>scm:git:git@github.com:metamx/druid-spark-batch.git</connection>
  </scm>
  <developers>
    <developer>
      <name>Charles Allen</name>
      <organization>Metamarkets Group Inc.</organization>
      <organizationUrl>https://www.metamarkets.com</organizationUrl>
    </developer>
  </developers>
)

testOptions += Tests.Argument(TestFrameworks.JUnit, "-Duser.timezone=UTC")
