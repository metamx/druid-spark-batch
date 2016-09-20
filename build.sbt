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
organization := "io.druid.extensions"
name := "druid-spark-batch"

licenses := Seq("Apache License, Version 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0"))
homepage := Some(url("https://github.com/metamx/druid-spark-batch"))
crossScalaVersions := Seq("2.11.7", "2.10.6")

val druid_version = "0.9.2-SNAPSHOT"
// This is just used here for Path, so anything that doesn't break spark should be fine
val hadoop_version = "2.4.0"
val spark_version = "2.0.0"
val guava_version = "16.0.1"
val mesos_version = "0.25.0"

val sparkDep = ("org.apache.spark" %% "spark-core" % spark_version
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
  exclude("com.esotericsoftware.minlog", "minlog")
  /*
  exclude("com.fasterxml.jackson.core", "jackson-core")
  exclude("com.fasterxml.jackson.core", "jackson-annotations")
  exclude("com.fasterxml.jackson.dataformat", "jackson-dataformat-smile")
  exclude("com.fasterxml.jackson.datatype", "jackson-datatype-joda")
  exclude("com.fasterxml.jackson.core", "jackson-databind")
  */
  exclude("io.netty", "netty")
  exclude("org.apache.mesos", "mesos")
  ) % "provided"
libraryDependencies += sparkDep

val hadoopDep = ("org.apache.hadoop" % "hadoop-client" % hadoop_version
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

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"
libraryDependencies += "io.druid" % "druid-processing" % druid_version % "provided"
libraryDependencies += "io.druid" % "druid-server" % druid_version % "provided"
libraryDependencies += "io.druid" % "druid-indexing-service" % druid_version % "provided"
libraryDependencies += "io.druid" % "druid-indexing-hadoop" % druid_version % "provided"
libraryDependencies +=
  "org.joda" % "joda-convert" % "1.8.1" % "provided" // Prevents intellij silliness and sbt warnings
libraryDependencies += "com.google.guava" % "guava" % guava_version % "provided"// Prevents serde problems for guice exceptions
libraryDependencies += "com.sun.jersey" % "jersey-servlet" % "1.17.1" % "provided"

libraryDependencies += "org.apache.mesos" % "mesos"  % mesos_version % "provided"  classifier "shaded-protobuf"

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

resolvers += Resolver.mavenLocal
resolvers += "JitPack.IO" at "https://jitpack.io"

publishMavenStyle := true

//TODO: remove this before moving to druid.io
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
// WTF SBT?
javaOptions in Test += "-Duser.timezone=UTC"
fork in Test := true
