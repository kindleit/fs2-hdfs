import sbt._

object Dependencies {
  val fs2Version        = "0.10.5"
  val catsVersion       = "1.1.0"

  lazy val fs2            = Seq("fs2-core", "fs2-io").map("co.fs2" %% _ % fs2Version)
  lazy val catsTestkit    = "org.typelevel"     %% "cats-testkit"          % catsVersion
  lazy val specs2         = "org.specs2"        %% "specs2-scalacheck"     % "4.2.0"
  lazy val hdfsCli        = "org.apache.hadoop" % "hadoop-client"          % "3.1.0"
  lazy val hdfsSrv        = Seq(
    "org.apache.hadoop" % "hadoop-hdfs"         % "3.1.0" classifier "tests",
    "org.apache.hadoop" % "hadoop-common"       % "3.1.0" classifier "tests",
    "org.apache.hadoop" % "hadoop-minicluster"  % "3.1.0").map(_.exclude("org.slf4j", "slf4j-log4j12"))
  lazy val logging        =("io.chrisdavenport" %% "log4cats-slf4j"        % "0.0.6" ) +:
                           ("org.slf4j"         %  "slf4j-simple"          % "1.7.25") +: Seq.empty

}
