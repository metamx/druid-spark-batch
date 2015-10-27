logLevel := Level.Warn

resolvers ++= Seq(
  "Central" at "https://oss.sonatype.org/content/repositories/releases/"
)

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.7.5")
addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.0")
addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.0.0")
