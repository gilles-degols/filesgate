
name := "fgate" // Intellij Idea does not like having a project with a sub-project having the same name
organization := "net.degols.libs"
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

libraryDependencies += "com.typesafe.play" %% "play-ws" % playVersion


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


// Test
libraryDependencies += "org.mockito" % "mockito-core" % "2.18.0" % Test
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.0-SNAP10" % Test
libraryDependencies += "org.scalatestplus.play" %% "scalatestplus-play" % "3.1.2" % Test
libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.13.5" % Test
libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % "2.5.19" % Test

// play json
libraryDependencies += "com.typesafe.play" %% "play-json" % playVersion

// Akka Remoting
libraryDependencies += "com.typesafe.akka" %% "akka-remote" % akkaVersion

// Akka streaming
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % akkaVersion