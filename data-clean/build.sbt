name := "data-clean"

version := "1.0.0"

scalaVersion := "2.11.8"



libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.0",
  "org.apache.spark" %% "spark-sql" % "1.5.1",
  "com.databricks" %% "spark-csv" % "1.4.0",
  "org.apache.spark" %% "spark-mllib" % "1.5.1",
  "org.scalatest" %% "scalatest" % "3.0.0-M15" % Test
)

scalacOptions ++= Seq(
  "-unchecked",
  "-deprecation",
  "-Xlint",
  "-Ywarn-dead-code",
  "-language:_",
  "-encoding", "UTF-8"
)

testOptions += Tests.Argument(TestFrameworks.JUnit, "-v")
