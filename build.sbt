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
val spark_version = "1.5.2-mmx0"
val guava_version = "16.0.1"

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
  exclude("com.fasterxml.jackson.core", "jackson-core")
  exclude("com.fasterxml.jackson.core", "jackson-annotations")
  exclude("com.fasterxml.jackson.dataformat", "jackson-dataformat-smile")
  exclude("com.fasterxml.jackson.datatype", "jackson-datatype-joda")
  exclude("com.fasterxml.jackson.core", "jackson-databind")
  exclude("io.netty", "netty")
  exclude("org.apache.mesos", "mesos")
  )
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
  )
// For Path
libraryDependencies += hadoopDep

// Pom will list them as compile dependencies even though they are in a fat jar.
// As such we add the provided scope to their artifacts
pomPostProcess := {
  import xml.transform._
  new RuleTransformer(new RewriteRule {
    override def transform(node: xml.Node) = node match {
      case n if (n \ "artifactId").text.equals("hadoop-client") =>
        xml.Elem(n.prefix, n.label, n.attributes, n.scope, n.child.filter(!_.label.equals("exclusions")) ++ <scope>provided</scope> : _*)
      case n if (n \ "artifactId").text.startsWith("spark-core") =>
        xml.Elem(n.prefix, n.label, n.attributes, n.scope, true,n.child.filter(!_.label.equals("exclusions")) ++ <scope>provided</scope> : _*)
      case n if (n \ "artifactId").text.equals("mesos") =>
        xml.Elem(n.prefix, n.label, n.attributes, n.scope, true, n.child.filter(!_.label.equals("exclusions")) ++ <scope>provided</scope> : _*)
      case _ => node
    }
  })
}
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"
libraryDependencies += "io.druid" % "druid-processing" % druid_version % "provided"
libraryDependencies += "io.druid" % "druid-server" % druid_version % "provided"
libraryDependencies += "io.druid" % "druid-indexing-service" % druid_version % "provided"
libraryDependencies += "io.druid" % "druid-indexing-hadoop" % druid_version % "provided"
libraryDependencies +=
  "org.joda" % "joda-convert" % "1.8.1" % "provided" // Prevents intellij silliness and sbt warnings
libraryDependencies += "com.google.guava" % "guava" % guava_version % "provided"// Prevents serde problems for guice exceptions
libraryDependencies += "com.sun.jersey" % "jersey-servlet" % "1.17.1" % "provided"
// TODO: make this not part of the package
libraryDependencies += "org.apache.mesos" % "mesos"  % "0.25.0" classifier "shaded-protobuf"

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".html" => MergeStrategy.first
  case PathList("org", "apache", "commons", "logging", xs@_*) => MergeStrategy.first
  case PathList("javax", "annotation", xs@_*) => MergeStrategy.last //favor jsr305
  case PathList("mime.types") => MergeStrategy.filterDistinctLines
  case PathList("com", "google", "common", "base", xs@_*) => MergeStrategy.last // spark-network-common pulls these in
  case PathList("org", "apache", "spark", "unused", xs@_*) => MergeStrategy.first
  case PathList("META-INF", xs@_*) => {
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
  }
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

// Scala lib is not assembled in fat jar due to pom annoying-ness
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
artifact in(Compile, assembly) := {
  val art = (artifact in(Compile, assembly)).value
  art.copy(`classifier` = Some("assembly"))
}

addArtifact(artifact in(Compile, assembly), assembly)

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
