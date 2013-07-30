import AssemblyKeys._

assemblySettings

name := "Nimrod"

version := "0.13.7"

scalaVersion := "2.9.2"

scalacOptions += "-deprecation"


libraryDependencies ++= Seq(
   "org.scalatest" %% "scalatest" % "1.8" % "test",
   "com.twitter" %% "util-eval" % "6.3.6"
)
