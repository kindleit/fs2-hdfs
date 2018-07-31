import com.typesafe.sbt.pgp.PgpKeys.publishSigned
import Dependencies._

lazy val contributors = Seq(
  "kryptt" -> "Rodolfo Hansen"
)

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
  "-Yno-adapted-args",
  "-Ypartial-unification",
  "-Ywarn-dead-code",
  "-Ywarn-inaccessible",
  "-Ywarn-infer-any",
  "-Ywarn-nullary-override",
  "-Ywarn-nullary-unit",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard") ++ (
  if (scalaVersion.value.startsWith("2.12")) Seq(
    "-Xlint:constant",
    "-Ybackend-parallelism", "4",
    "-Ywarn-extra-implicit",
    "-Ywarn-unused:implicits",
    "-Ywarn-unused:imports",
    "-Ywarn-unused:locals",
    "-Ywarn-unused:params",
    "-Ywarn-unused:patvars",
    "-Ywarn-unused:privates",
    ) else Seq())

def getInteractive(q: Seq[String]) =
  q.filterNot(Set("-Ywarn-unused:imports", "-Xfatal-warnings")) ++
    Seq("-Yrangepos", "-Ydelambdafy:inline")

scalacOptions in (Compile, console) ~= getInteractive
scalacOptions in (Test, console) ~= getInteractive

addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.7")

lazy val scaladocSettings = Seq(
   scalacOptions in (Compile, doc) ++= Seq(
    "-doc-source-url", scmInfo.value.get.browseUrl + "/tree/master€{FILE_PATH}.scala",
    "-sourcepath", baseDirectory.in(LocalRootProject).value.getAbsolutePath,
    "-implicits",
    "-implicits-show-all"
  ),
   scalacOptions in (Compile, doc) ~= { _ filterNot { _ == "-Xfatal-warnings" } },
   autoAPIMappings := true
)

lazy val publishingSettings = Seq(
  publishTo := {
   val nexus = "https://oss.sonatype.org/"
   if (version.value.trim.endsWith("SNAPSHOT"))
     Some("snapshots" at nexus + "content/repositories/snapshots")
   else
     Some("releases" at nexus + "service/local/staging/deploy/maven2")
  },
  credentials ++= (for {
   username <- Option(System.getenv().get("SONATYPE_USERNAME"))
   password <- Option(System.getenv().get("SONATYPE_PASSWORD"))
  } yield Credentials("Sonatype Nexus Repository Manager", "oss.sonatype.org", username, password)).toSeq,
  publishMavenStyle := true,
  pomIncludeRepository := { _ => false },
  pomExtra := {
    <url>https://github.com/krpytt/fs2-hdfs</url>
    <developers>
      {for ((username, name) <- contributors) yield
      <developer>
        <id>{username}</id>
        <name>{name}</name>
        <url>http://github.com/{username}</url>
      </developer>
      }
    </developers>
  },
  pomPostProcess := { node =>
   import scala.xml._
   import scala.xml.transform._
   def stripIf(f: Node => Boolean) = new RewriteRule {
     override def transform(n: Node) =
       if (f(n)) NodeSeq.Empty else n
   }
   val stripTestScope = stripIf { n => n.label == "dependency" && (n \ "scope").text == "test" }
   new RuleTransformer(stripTestScope).transform(node)(0)
  }
)

lazy val releaseSettings = Seq(
  releaseCrossBuild := true,
  releasePublishArtifactsAction := PgpKeys.publishSigned.value
)

lazy val root = (project in file("."))
  .settings(scaladocSettings)
  .settings(publishingSettings)
  .settings(releaseSettings)
  .settings(organization := "net.kindleit",
            name := "fs2-hdfs",
            scalaVersion := "2.12.6",
            crossScalaVersions := Seq("2.11.12", "2.12.6"),
            scmInfo := Some(ScmInfo(url("https://github.com/kindleit/fs2-hdfs"), "git@github.com:kindleit/fs2-hdfs.git")),
            homepage := None,
            licenses += ("MIT", url("http://opensource.org/licenses/MIT")),
            libraryDependencies ++= fs2,
            libraryDependencies  += hdfsCli % "provided",
            libraryDependencies ++= logging,
            libraryDependencies ++= hdfsSrv.map(_ % "test"),
            libraryDependencies += specs2 % "test",
            )
