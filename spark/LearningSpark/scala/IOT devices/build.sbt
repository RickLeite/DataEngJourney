ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "IOT devices analysis",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.4.0",
      "org.apache.spark" %% "spark-sql" % "3.4.0"
    )
  )
