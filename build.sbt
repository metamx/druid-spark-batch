/*
 *  Licensed to Metamarkets Group Inc. (Metamarkets) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  Metamarkets licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
organization := "org.apache.druid.extensions"
name := "druid-spark-batch"

licenses := Seq("Apache License, Version 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0"))
homepage := Some(url("https://github.com/metamx/druid-spark-batch"))
crossScalaVersions := Seq("2.11.12", "2.12.8")
scalaVersion := "2.11.12"
releaseIgnoreUntrackedFiles := true

val druidVersion = "0.15.1-incubating"
val hadoopVersion = "2.6.0"
val sparkVersion = "2.4.3"
val guavaVersion = "15.0" //Newer than 15.0 cause issues with Spark 2.4
val mesosVersion = "1.8.1"
val codahaleMetricsVersion = "4.1.0"
val parquetVersion = "1.10.1"
//DO NOT pull in version newer than 2.6.7 as it causes issues with JSON parsing for Granularity
//Make sure this version always matches with Druid
val jacksonVersion = "2.6.7"

val sparkDeps = Seq(
  ("org.apache.spark" %% "spark-sql" % sparkVersion
    exclude("org.roaringbitmap", "RoaringBitmap")
    exclude("log4j", "log4j")
    exclude("org.slf4j", "slf4j-log4j12")
    exclude("com.google.guava", "guava")
    exclude("org.apache.hadoop", "hadoop-client")
    exclude("org.apache.hadoop", "hadoop-yarn-api")
    exclude("org.apache.hadoop", "hadoop-yarn-common")
    exclude("com.sun.jersey", "jersey-server")
    exclude("com.sun.jersey", "jersey-core")
    exclude("com.sun.jersey", "jersey-core")
    exclude("com.sun.jersey.contribs", "jersey-guice")
    exclude("org.eclipse.jetty", "jetty-server")
    exclude("org.eclipse.jetty", "jetty-plus")
    exclude("org.eclipse.jetty", "jetty-util")
    exclude("org.eclipse.jetty", "jetty-http")
    exclude("org.eclipse.jetty", "jetty-servlet")
    exclude("io.netty", "netty")
    exclude("org.apache.mesos", "mesos")
    exclude("io.dropwizard.metrics", "metrics-core")
    exclude("io.dropwizard.metrics", "metrics-jvm")
    exclude("io.dropwizard.metrics", "metrics-json")
    exclude("io.dropwizard.metrics", "metrics-ganglia")
    exclude("io.dropwizard.metrics", "metrics-graphite")
  ) % "provided",
  "org.apache.spark" %% "spark-avro" % sparkVersion
)
libraryDependencies ++= sparkDeps

val hadoopDep = ("org.apache.hadoop" % "hadoop-client" % hadoopVersion
  exclude("asm", "asm")
  exclude("org.ow2.asm", "asm")
  exclude("org.jboss.netty", "netty")
  exclude("commons-logging", "commons-logging")
  exclude("com.google.guava", "guava")
  exclude("org.mortbay.jetty", "servlet-api-2.5")
  exclude("javax.servlet", "servlet-api")
  exclude("junit", "junit")
  exclude("org.slf4j", "slf4j-log4j12")
  exclude("log4j", "log4j")
  exclude("commons-beanutils", "commons-beanutils")
  exclude("org.apache.hadoop", "hadoop-yarn-api")
  exclude("com.sun.jersey", "jersey-server")
  exclude("com.sun.jersey", "jersey-core")
  exclude("com.sun.jersey", "jersey-core")
  exclude("com.sun.jersey.contribs", "jersey-guice")
  exclude("org.eclipse.jetty", "jetty-server")
  exclude("org.eclipse.jetty", "jetty-plus")
  exclude("org.eclipse.jetty", "jetty-util")
  exclude("org.eclipse.jetty", "jetty-http")
  exclude("org.eclipse.jetty", "jetty-servlet")
  exclude("commons-beanutils", "commons-beanutils-core")
  exclude("com.fasterxml.jackson.core", "jackson-core")
  exclude("com.fasterxml.jackson.core", "jackson-annotations")
  exclude("com.fasterxml.jackson.dataformat", "jackson-dataformat-smile")
  exclude("com.fasterxml.jackson.datatype", "jackson-datatype-joda")
  exclude("com.fasterxml.jackson.core", "jackson-databind")
  exclude("io.netty", "netty")
) % "provided"
// For Path
libraryDependencies += hadoopDep

//io.dropwizard.metrics deps are needed to prevent
//java.lang.IncompatibleClassChangeError: Found interface com.codahale.metrics.Timer$Context, but class was expected
libraryDependencies ++= Seq(
  "org.apache.druid" % "druid-indexing-service" % druidVersion % "provided",
  "org.apache.druid.extensions" % "druid-avro-extensions" % druidVersion % "provided",
  "org.apache.druid.extensions" % "druid-parquet-extensions" % druidVersion % "provided",
  //"io.confluent" % "kafka-schema-registry-client" % "3.3.1",
  "org.apache.parquet" % "parquet-common" % parquetVersion,
  "org.apache.parquet" % "parquet-encoding" % parquetVersion,
  "org.apache.parquet" % "parquet-column" % parquetVersion,
  "org.apache.parquet" % "parquet-hadoop" % parquetVersion,
  "org.apache.parquet" % "parquet-avro" % parquetVersion,
  "org.apache.mesos" % "mesos"  % mesosVersion % "provided" classifier "shaded-protobuf",
  "io.dropwizard.metrics" % "metrics-core" % codahaleMetricsVersion % "provided",
  "io.dropwizard.metrics" % "metrics-jvm" % codahaleMetricsVersion % "provided",
  "io.dropwizard.metrics" % "metrics-json" % codahaleMetricsVersion % "provided",
  "io.dropwizard.metrics" % "metrics-graphite" % codahaleMetricsVersion % "provided",
  "io.dropwizard.metrics" % "metrics-ganglia" % "3.2.6" % "provided",
  "org.joda" % "joda-convert" % "2.2.1" % "provided",
  "com.sun.jersey" % "jersey-servlet" % "1.19.4" % "provided",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
  "org.apache.logging.log4j" % "log4j-web" % "2.12.1",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)

val druidOverrides = Set(
  "com.google.guava" % "guava" % guavaVersion, //Hadoop pulls in Guava 20 which breaks Spark 2.4
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-smile" % jacksonVersion,
  "com.fasterxml.jackson.datatype" % "jackson-datatype-joda" % jacksonVersion,
  "com.fasterxml.jackson.datatype" % "jackson-datatype-joda" % jacksonVersion,
  "com.fasterxml.jackson.jaxrs" % "jackson-jaxrs-json-provider" % jacksonVersion,
  "com.fasterxml.jackson.jaxrs" % "jackson-jaxrs-smile-provider" % jacksonVersion
)

dependencyOverrides ++= druidOverrides

releaseCrossBuild := true

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".html" => MergeStrategy.first
  case PathList("org", "apache", "commons", "logging", xs@_*) => MergeStrategy.first
  case PathList("javax", "annotation", xs@_*) => MergeStrategy.last //favor jsr305
  case PathList("mime.types") => MergeStrategy.filterDistinctLines
  case PathList("com", "google", "common", "base", xs@_*) => MergeStrategy.last // spark-network-common pulls these in
  case PathList("org", "apache", "spark", "unused", xs@_*) => MergeStrategy.first
  case PathList("META-INF", xs@_*) =>
    xs map {
      _.toLowerCase
    } match {
      case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
        MergeStrategy.discard
      case ps@(x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
        MergeStrategy.discard
      case "services" :: xs =>
        MergeStrategy.filterDistinctLines
      case "jersey-module-version" :: xs => MergeStrategy.first
      case "sisu" :: xs => MergeStrategy.discard
      case "maven" :: xs => MergeStrategy.discard
      case "plexus" :: xs => MergeStrategy.discard
      case _ => MergeStrategy.discard
    }
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

resolvers ++= Seq(
  Resolver.mavenLocal,
  "JitPack.IO" at "https://jitpack.io",
  "Clojars.org" at "https://clojars.org/repo/",
  "Confluent" at "http://packages.confluent.io/maven/"
)

publishMavenStyle := true

scalacOptions in ThisBuild ++= Seq("-unchecked", "-deprecation")

fork in Test := true
