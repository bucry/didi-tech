import sbt._
import Keys._


object DiDiBuild extends Build {
  lazy val root = Project(id = "didi", base = file(".")) aggregate(`data-clean`, report, flatter)
  lazy val `data-clean` = Project(id = "data-clean", base = file("data-clean"))
  lazy val report = Project(id = "report", base = file("report"))
  lazy val flatter = Project(id = "flatter", base = file("flatter"))
}