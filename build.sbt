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

net.virtualvoid.sbt.graph.Plugin.graphSettings

licenses := Seq("Apache License, Version 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0"))
homepage := Some(url("https://github.com/metamx/druid-spark-batch"))

scalaVersion := "2.10.5"
crossScalaVersions := Seq("2.10.5", "2.11.7")

// Requires 0.8.2 or later and https://github.com/druid-io/druid/pull/1940
val druid_version = "e0c8883"
// This is just used here for Path, so anything that doesn't break spark should be fine
val hadoop_version = "2.4.0"
// Requires a patch for https://issues.apache.org/jira/browse/SPARK-11016
val spark_version = "1.5.1-mmx2"

// Use Spark's jetty version instead of Druid's to prevent class collisions
val sparkEclipseJettyVersion = "8.1.14.v20131031"
val explicitDependencies = Seq(
  "org.eclipse.jetty" % "jetty-server" % sparkEclipseJettyVersion ,
  "org.eclipse.jetty" % "jetty-plus" % sparkEclipseJettyVersion ,
  "org.eclipse.jetty" % "jetty-util" % sparkEclipseJettyVersion ,
  "org.eclipse.jetty" % "jetty-http" % sparkEclipseJettyVersion ,
  "org.eclipse.jetty" % "jetty-servlet" % sparkEclipseJettyVersion
)

libraryDependencies += ("org.apache.spark" %% "spark-core" % spark_version
  exclude ("log4j", "log4j") exclude ("org.apache.hadoop", "hadoop-client")
  )

libraryDependencies += "org.spark-project.akka" %% "akka-actor" % "2.3.4-spark"
// For Path
libraryDependencies += ("org.apache.hadoop" % "hadoop-client" % hadoop_version
  exclude("asm", "asm") exclude("org.ow2.asm", "asm") exclude("org.jboss.netty", "netty")
  exclude("commons-logging", "commons-logging") exclude("com.google.guava", "guava")
  exclude("org.mortbay.jetty", "servlet-api-2.5") exclude("javax.servlet", "servlet-api")
  exclude("junit", "junit")
  )

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"
libraryDependencies += "io.druid" % "druid-processing" % druid_version % "compile"
libraryDependencies += ("io.druid" % "druid-server" % druid_version % "compile"
  excludeAll ExclusionRule(organization = "org.eclipse.jetty")
  )
libraryDependencies += "io.druid" % "druid-indexing-service" % druid_version % "compile"
libraryDependencies += "io.druid" % "druid-indexing-hadoop" % druid_version % "compile"
libraryDependencies += "com.sun.jersey" % "jersey-servlet" % "1.17.1"
libraryDependencies += "org.xerial.snappy" % "snappy-java" % "1.1.2-M1"
libraryDependencies += "org.joda" % "joda-convert" % "1.8.1" % "compile"
libraryDependencies ++= explicitDependencies


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
