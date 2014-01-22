name := "Nimrod"

version in ThisBuild := "0.14.1"

scalaVersion in ThisBuild := "2.10.3"

scalacOptions in ThisBuild += "-deprecation"

libraryDependencies in ThisBuild ++= Seq(
   "org.scalatest" %% "scalatest" % "2.0" % "test"
)
