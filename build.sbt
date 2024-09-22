ThisBuild / organization := "com.company"
ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "hadoop-ops-util",
    libraryDependencies ++= Seq(
      "com.softwaremill.sttp.client3" %% "core" % "3.9.2",
      "org.json4s" %% "json4s-native" % "4.0.7",
      "org.json4s" %% "json4s-native-core" % "4.0.7",
      "org.apache.commons" % "commons-lang3" % "3.14.0",
      "com.github.pathikrit" %% "better-files" % "3.9.2"
    )
  )

resolvers ++= Seq(
  "Central Repository" at "https://repo.maven.apache.org/maven2"
)
