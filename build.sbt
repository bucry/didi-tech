scalaVersion in ThisBuild := "2.11.8"
name := "didi"
version := "1.0.0"
scalaVersion := "2.11.8"

val didiVersion = sys.env.get("VERSION") match {
  case Some(number) => number.toString
  case _ => "1.0"
}

val buildVersion = sys.env.get("GO_PIPELINE_COUNTER") match {
  case Some(number) => s"$didiVersion.$number"
  case _ => s"$didiVersion-SNAPSHOT"
}

organization in ThisBuild := "com.didi"

version in ThisBuild := buildVersion

publishTo in ThisBuild := Some(Resolver.file("file", new File(Path.userHome.getAbsolutePath + "/.m2/repository")))

lazy val root = Project(id = "didi", base = file(".")) aggregate(`data-clean`, report, train)
lazy val `data-clean` = project in file("data-clean")
lazy val report = project in file("report")
lazy val train = project in file("train")

lazy val makeVersionTxt = taskKey[File]("Make version.txt")

makeVersionTxt := {
  val file = target.value / "version.txt"
  IO.write(file, version.value)
  file
}
