import Dependencies._

scalacOptions in ThisBuild ++= Seq(
  "-deprecation",
  "-encoding", "utf8",
  "-explaintypes",
  "-feature",
  "-language:existentials",
  "-language:postfixOps",
  "-language:implicitConversions",
  "-language:higherKinds",
  "-language:experimental.macros",
  "-unchecked",
  "-Xcheckinit",
  "-Xfatal-warnings",
  "-Xfuture",
  "-Xlint:adapted-args",
  "-Xlint:by-name-right-associative",
  "-Xlint:constant",
  "-Xlint:delayedinit-select",
  "-Xlint:doc-detached",
  "-Xlint:inaccessible",
  "-Xlint:infer-any",
  "-Xlint:missing-interpolator",
  "-Xlint:nullary-override",
  "-Xlint:nullary-unit",
  "-Xlint:option-implicit",
  "-Xlint:package-object-classes",
  "-Xlint:poly-implicit-overload",
  "-Xlint:private-shadow",
  "-Xlint:stars-align",
  "-Xlint:type-parameter-shadow",
  "-Xlint:unsound-match",
  "-Ybackend-parallelism", "4",
  "-Yno-adapted-args",
  "-Ypartial-unification",
  "-Ywarn-dead-code",
  "-Ywarn-extra-implicit",
  "-Ywarn-inaccessible",
  "-Ywarn-infer-any",
  "-Ywarn-nullary-override",
  "-Ywarn-nullary-unit",
  "-Ywarn-numeric-widen",
  "-Ywarn-unused:implicits",
  "-Ywarn-unused:imports",
  "-Ywarn-unused:locals",
  "-Ywarn-unused:params",
  "-Ywarn-unused:patvars",
  "-Ywarn-unused:privates",
  "-Ywarn-value-discard")

def getInteractive(q: Seq[String]) =
  q.filterNot(Set("-Ywarn-unused:imports", "-Xfatal-warnings")) ++
    Seq("-Yrangepos", "-Ydelambdafy:inline")

scalacOptions in (Compile, console) ~= getInteractive
scalacOptions in (Test, console) ~= getInteractive

addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.7")

lazy val root = (project in file("."))
  .settings(inThisBuild(Seq(
                          organization := "com.rodolfohansen",
                          scalaVersion := "2.12.6",
                          version      := "0.1.0-SNAPSHOT")),
            name := "fs2-hdfs",
            libraryDependencies ++= fs2,
            libraryDependencies  += hdfsCli % "provided",
            libraryDependencies ++= logging,
            libraryDependencies ++= hdfsSrv.map(_ % "test"),
            libraryDependencies ++= Seq(specs2 % "test", catsTestkit % "test"),
            )
