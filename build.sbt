import AssemblyKeys._

assemblySettings

name := "Nimrod"

version := "0.14.1"

scalaVersion := "2.10.3"

scalacOptions += "-deprecation"


libraryDependencies ++= Seq(
   "org.scalatest" %% "scalatest" % "2.0" % "test",
   "com.twitter" %% "util-eval" % "6.3.6",
   "org.apache.commons" % "commons-compress" % "1.5",
   "com.typesafe.akka" %% "akka-actor" % "2.2.3",
   "com.typesafe.akka" %% "akka-remote" % "2.2.3",
   "io.netty" % "netty-all" % "4.0.14.Final"
)
