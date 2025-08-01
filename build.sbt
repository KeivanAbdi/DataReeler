val scala3Version    = "3.6.3"
val pekkoVersion     = "1.1.3"
val pekkoHttpVersion = "1.1.0"
val circeVersion     = "0.14.10"
val scalatagsVersion = "0.13.1"
val jodaTimeVersion  = "2.13.0"
val logbackVersion   = "1.5.18"
val scalaTestVersion = "3.2.18"

lazy val root = project
  .in(file("."))
  .settings(
    name         := "datareeler",
    version      := "0.1.0",
    organization := "com.keivanabdi",
    scalaVersion := scala3Version,
    libraryDependencies ++= Seq(
      "com.lihaoyi"      %% "scalatags"                 % scalatagsVersion,
      "org.apache.pekko" %% "pekko-actor"               % pekkoVersion,
      "org.apache.pekko" %% "pekko-stream"              % pekkoVersion,
      "org.apache.pekko" %% "pekko-http"                % pekkoHttpVersion,
      "io.circe"         %% "circe-core"                % circeVersion,
      "io.circe"         %% "circe-generic"             % circeVersion,
      "io.circe"         %% "circe-parser"              % circeVersion,
      "joda-time"         % "joda-time"                 % jodaTimeVersion,
      "ch.qos.logback"    % "logback-classic"           % logbackVersion,
      "org.apache.pekko" %% "pekko-stream-testkit"      % pekkoVersion     % Test,
      "org.apache.pekko" %% "pekko-actor-testkit-typed" % pekkoVersion     % Test,
      "org.scalatest"    %% "scalatest"                 % scalaTestVersion % Test
    ),
    testFrameworks += new TestFramework("org.scalatest.tools.Framework"),
    licenses += ("MIT", url("https://opensource.org/licenses/MIT")),
    developers := List(
      Developer(
        "KeivanAbdi",
        "Keivan A Khorsand",
        "keivan.a.khorsand@gmail.com",
        url("https://github.com/KeivanAbdi")
      )
    ),
    scmInfo := Some(
      ScmInfo(
        url("https://github.com/KeivanAbdi/DataReeler"),
        "scm:git@github.com:KeivanAbdi/DataReeler.git"
      )
    ),
    pomIncludeRepository := { _ => false },
    publishMavenStyle    := true,
    publishTo := {
      val centralSnapshots =
        "https://central.sonatype.com/repository/maven-snapshots/"
      if (isSnapshot.value) Some("central-snapshots" at centralSnapshots)
      else localStaging.value
    },
    homepage := Some(url("https://github.com/keivanabdi/datareeler"))
  )
