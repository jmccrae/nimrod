import AssemblyKeys._

assemblySettings

name := "nimrod-executor"

libraryDependencies ++= Seq(
   "com.twitter" %% "util-eval" % "6.3.6",
   "org.apache.commons" % "commons-compress" % "1.5",
   "io.netty" % "netty-all" % "4.0.14.Final",
   "it.unimi.dsi" % "fastutil" % "6.5.12"
)
