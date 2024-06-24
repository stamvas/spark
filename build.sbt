ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "ergasia4"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.3"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.3"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "3.2.3"