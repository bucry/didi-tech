name := "train"

version := "1.0.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
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

incOptions := incOptions.value.withNameHashing(true).withRecompileOnMacroDef(false)