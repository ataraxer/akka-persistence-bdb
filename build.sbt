organization := "com.github.bseibel"

name := "akka-persistence-bdb"

version := "1.0.1"

scalaVersion := "2.11.6"

crossScalaVersions := Seq("2.10.5")

parallelExecution in Test := false

fork := true

resolvers += "Oracle" at "http://download.oracle.com/maven"

scalacOptions ++= Seq(
  "-encoding",
  "UTF-8",
  "-deprecation",
  "-unchecked",
  "-feature",
  "-optimise",
  "-language:implicitConversions",
  "-language:reflectiveCalls",
  "-language:postfixOps",
  "-Yinline-warnings")

publishMavenStyle := true

pomIncludeRepository := { _ => false }

publishTo <<= version {
  (v: String) =>
    Some("bintray" at "https://api.bintray.com/maven/bseibel/release/akka-persistence-bdb")
}

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials-bintree")

resolvers += "krasserm at bintray" at "http://dl.bintray.com/krasserm/maven"

libraryDependencies ++= Seq(
  "com.github.krasserm" %% "akka-persistence-testkit" % "0.3.3" % "test",
  "com.sleepycat" % "je" % "6.0.11",
  "com.typesafe.akka" %% "akka-persistence-experimental" % "2.3.4" % "compile",
  "com.typesafe.akka" %% "akka-testkit" % "2.3.4" % "test",
  "org.scalatest" %% "scalatest" % "2.1.4" % "test",
  "commons-io" % "commons-io" % "2.4" % "test",
  "commons-codec" % "commons-codec" % "1.9" % "compile")

