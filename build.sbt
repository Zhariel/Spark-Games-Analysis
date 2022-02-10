ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "SparkProject"
  )

version := "0.1"

scalaVersion := "2.12.15"
libraryDependencies += "org.apache.spark" %% "spark-core"% "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-sql"% "3.1.2"