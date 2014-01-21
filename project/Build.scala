import sbt._
import Keys._

object Build extends Build {
  lazy val root = Project("root", file(".")).aggregate(executor, mt)

  lazy val executor = Project("executor",file("executor"))

  lazy val mt = Project("mt",file("mt")).dependsOn("executor")
}
