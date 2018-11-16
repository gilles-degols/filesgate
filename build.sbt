name := "filesgate"
organization := "net.degols"
version := "0.0.1"

scalacOptions ++= Seq("-deprecation", "-feature", "-language:postfixOps")

scalaVersion := "2.12.1"
lazy val playVersion = "2.6.1"
lazy val akkaVersion = "2.5.2"

libraryDependencies += "com.google.inject" % "guice" % "3.0"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion exclude("log4j", "log4j") exclude("org.slf4j","slf4j-log4j12")
)

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream-kafka" % "0.19"
)

libraryDependencies += "joda-time" % "joda-time" % "2.10"

libraryDependencies += "commons-io" % "commons-io" % "2.4"

// Cluster library
val clusterLibraryVersion = "0.0.1"
val clusterPath = "../cluster"
lazy val clusterLibrary: RootProject = RootProject(file(clusterPath))
val useLocalClusterLibrary = true
val localClusterAvailable = scala.reflect.io.File(scala.reflect.io.Path(clusterPath)).exists
lazy val filesgate = if(localClusterAvailable && useLocalClusterLibrary) {
  (project in file(".")).dependsOn(clusterLibrary)
} else {
  (project in file("."))
}

lazy val clusterDependency = if(localClusterAvailable && useLocalClusterLibrary) {
  Seq()
} else {
  Seq("net.degols.libs" %% "cluster" % clusterLibraryVersion exclude("log4j", "log4j") exclude("org.slf4j","slf4j-log4j12"))
}
libraryDependencies ++= clusterDependency


// Akka Remoting
libraryDependencies += "com.typesafe.akka" %% "akka-remote" % akkaVersion
