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
val guava_version = "16.0.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % spark_version % "provided"

// For Path
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % hadoop_version % "provided"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"
libraryDependencies += "io.druid" % "druid-processing" % druid_version % "compile"
libraryDependencies += ("io.druid" % "druid-server" % druid_version % "compile"
  //excludeAll ExclusionRule(organization = "org.eclipse.jetty")
  )
libraryDependencies += "io.druid" % "druid-indexing-service" % druid_version % "compile"
libraryDependencies += "io.druid" % "druid-indexing-hadoop" % druid_version % "compile"
libraryDependencies += "org.joda" % "joda-convert" % "1.8.1" % "compile" // Prevents intellij silliness and sbt warnings
libraryDependencies += "com.google.guava" % "guava" % guava_version // Prevents serde problems for guice exceptions
libraryDependencies += "com.sun.jersey" % "jersey-servlet" % "1.17.1"
libraryDependencies += "org.xerial.snappy" % "snappy-java" % "1.1.2-M1"
libraryDependencies += "com.metamx" %% "scala-util" % "1.11.7"


assemblyMergeStrategy in Compile := {
  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case PathList("org", "apache", "commons", "logging", xs @ _* )  => MergeStrategy.first
  case PathList("javax", "annotation", xs @ _*) => MergeStrategy.last //favor jsr305
  case PathList("mime.types") => MergeStrategy.filterDistinctLines
  case PathList("META-INF", xs @ _*) => {
    xs map {
      _.toLowerCase
    } match {
      case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
        MergeStrategy.discard
      case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
        MergeStrategy.discard
      case "services" :: xs =>
        MergeStrategy.filterDistinctLines
      case "jersey-module-version" :: xs => MergeStrategy.first
      case "sisu" :: xs=> MergeStrategy.discard
      case "maven" :: xs => MergeStrategy.discard
      case "plexus" :: xs => MergeStrategy.discard
      case _ => MergeStrategy.discard
    }
  }
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case PathList("org", "apache", "commons", "logging", xs @ _* )  => MergeStrategy.first
  case PathList("javax", "annotation", xs @ _*) => MergeStrategy.last //favor jsr305
  case PathList("mime.types") => MergeStrategy.filterDistinctLines
  case PathList("META-INF", xs @ _*) => {
    xs map {
      _.toLowerCase
    } match {
      case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
        MergeStrategy.discard
      case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
        MergeStrategy.discard
      case "services" :: xs =>
        MergeStrategy.filterDistinctLines
      case "jersey-module-version" :: xs => MergeStrategy.first
      case "sisu" :: xs=> MergeStrategy.discard
      case "maven" :: xs => MergeStrategy.discard
      case "plexus" :: xs => MergeStrategy.discard
      case _ => MergeStrategy.discard
    }
  }
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.copy(`classifier` = Some("assembly"))
}

addArtifact(artifact in (Compile, assembly), assembly)

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
