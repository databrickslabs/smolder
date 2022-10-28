import scala.sys.process._

import complete.DefaultParsers._
import org.apache.commons.lang3.StringUtils
import sbt.Tests._
import sbt.Keys._
import sbt.librarymanagement.ModuleID
import sbt.nio.Keys._

lazy val scala212 = "2.12.8"
lazy val scala211 = "2.11.12"

lazy val sparkVersion = sys.env.getOrElse("SPARK_VERSION", "3.2.1")

def majorMinorVersion(version: String): String = {
  StringUtils.ordinalIndexOf(version, ".", 2) match {
    case StringUtils.INDEX_NOT_FOUND => version
    case i => version.take(i)
  }
}

ThisBuild / scalaVersion := sys.env.getOrElse("SCALA_VERSION", scala212)
ThisBuild / organization := "com.databricks.labs"
//ThisBuild / scalastyleConfig := baseDirectory.value / "scalastyle-config.xml"
ThisBuild / publish / skip := true

ThisBuild / organizationName := "Databricks, Inc."
ThisBuild / startYear := Some(2020)
ThisBuild / licenses += ("Apache-2.0", new URL("https://www.apache.org/licenses/LICENSE-2.0.txt"))

// Compile Java sources before Scala sources, so Scala code can depend on Java
// but not vice versa
Compile / compileOrder := CompileOrder.JavaThenScala

// Test concurrency settings
// Tests are run serially in one or more forked JVMs. This setup is necessary because the shared
// Spark session used by many tasks cannot be used concurrently.
val testConcurrency = 1
Test / fork := true
concurrentRestrictions in Global := Seq(
  Tags.limit(Tags.ForkedTestGroup, testConcurrency)
)

def groupByHash(tests: Seq[TestDefinition]): Seq[Tests.Group] = {
  tests
    .groupBy(_.name.hashCode % testConcurrency)
    .map {
      case (i, groupTests) =>
        val options = ForkOptions()
          .withRunJVMOptions(Vector("-Dspark.ui.enabled=false", "-Xmx1024m"))

        Group(i.toString, groupTests, SubProcess(options))
    }
    .toSeq
}

//lazy val mainScalastyle = taskKey[Unit]("mainScalastyle")
//lazy val testScalastyle = taskKey[Unit]("testScalastyle")
// testGrouping cannot be set globally using the `Test /` syntax since it's not a pure value
lazy val commonSettings = Seq(
  //mainScalastyle := scalastyle.in(Compile).toTask("").value,
  //testScalastyle := scalastyle.in(Test).toTask("").value,
  testGrouping in Test := groupByHash((definedTests in Test).value),
  //test in Test := ((test in Test) dependsOn mainScalastyle).value,
  //test in Test := ((test in Test) dependsOn testScalastyle).value,
  //test in Test := ((test in Test) dependsOn scalafmtCheckAll).value,
  //test in Test := ((test in Test) dependsOn (headerCheck in Compile)).value,
  //test in Test := ((test in Test) dependsOn (headerCheck in Test)).value,
  test in assembly := {},
  assemblyMergeStrategy in assembly := {
    // Assembly jar is not executable
    case p if p.toLowerCase.contains("manifest.mf") =>
      MergeStrategy.discard
    case _ =>
      // Be permissive for other files
      MergeStrategy.first
  },
  scalacOptions += "-target:jvm-1.8",
  resolvers += "Apache Snapshots" at "https://repository.apache.org/snapshots/"
)

lazy val functionsYml = settingKey[File]("functionsYml")
ThisBuild / functionsYml := (ThisBuild / baseDirectory).value / "functions.yml"

lazy val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-catalyst" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

lazy val providedSparkDependencies = sparkDependencies.map(_ % "provided")
lazy val testSparkDependencies = sparkDependencies.map(_ % "test")

lazy val testCoreDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.0.3" % "test",
  "org.mockito" % "mockito-all" % "1.9.5" % "test",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % "test" classifier "tests",
  "org.apache.spark" %% "spark-core" % sparkVersion % "test" classifier "tests",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "test" classifier "tests",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "test" classifier "tests",
  "org.xerial" % "sqlite-jdbc" % "3.20.1" % "test"
)

lazy val coreDependencies = (providedSparkDependencies ++ testCoreDependencies ++ Seq(
  "log4j" % "log4j" % "1.2.17",
  "org.slf4j" % "slf4j-api" % "1.7.25",
  "org.slf4j" % "slf4j-log4j12" % "1.7.25",
  "org.jdbi" % "jdbi" % "2.63.1",
  // Fix versions of libraries that are depended on multiple times
  "org.apache.hadoop" % "hadoop-client" % "2.7.3",
  "io.netty" % "netty" % "3.9.9.Final",
  "io.netty" % "netty-all" % "4.1.17.Final",
  "org.yaml" % "snakeyaml" % "1.16"
)).map(_.exclude("com.google.code.findbugs", "jsr305"))

lazy val scalaLoggingDependency = settingKey[ModuleID]("scalaLoggingDependency")
ThisBuild / scalaLoggingDependency := {
  (ThisBuild / scalaVersion).value match {
    case `scala211` => "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2"
    case `scala212` => "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2"
    case _ =>
      throw new IllegalArgumentException(
        "Only supported Scala versions are: " + Seq(scala211, scala212))
  }
}

lazy val core = (project in file("."))
  .settings(
    commonSettings,
    name := "smolder",
    publish / skip := false,
    // Adds the Git hash to the MANIFEST file. We set it here instead of relying on sbt-release to
    // do so.
    packageOptions in (Compile, packageBin) +=
    Package.ManifestAttributes("Git-Release-Hash" -> currentGitHash(baseDirectory.value)),
    bintrayRepository := "smolder",
    libraryDependencies ++= coreDependencies :+ scalaLoggingDependency.value,
    Compile / unmanagedSourceDirectories +=
    baseDirectory.value / "src" / "main" / "shim" / majorMinorVersion(sparkVersion),
    Test / unmanagedSourceDirectories +=
    baseDirectory.value / "src" / "test" / "shim" / majorMinorVersion(sparkVersion),
  )

/**
 * @param dir The base directory of the Git project
 * @return The commit of HEAD
 */
def currentGitHash(dir: File): String = {
  Process(
    Seq("git", "rev-parse", "HEAD"),
    // Set the working directory for Git to the passed in directory
    Some(dir)
  ).!!.trim
}

lazy val sparkClasspath = taskKey[String]("sparkClasspath")
lazy val sparkHome = taskKey[String]("sparkHome")

// Publish to Bintray
ThisBuild / description := "An open-source toolkit for large-scale EHR processing"
ThisBuild / homepage := Some(url("https://databricks.com/solutions/industries/healthcare"))
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/databrickslabs/smolder"),
    "scm:git@github.com:databrickslabs/smolder.git"
  )
)
ThisBuild / developers := List(
  Developer(
    "ryandecosmo",
    "Ryan DeCosmo",
    "ryan.decosmo@databricks.com",
    url("https://github.com/ryandecosmo"))
)
ThisBuild / pomIncludeRepository := { _ =>
  false
}
ThisBuild / publishMavenStyle := true

ThisBuild / bintrayOrganization := Some("com.databricks.labs")
ThisBuild / bintrayRepository := "smolder"

lazy val stableVersion = settingKey[String]("Stable version")
ThisBuild / stableVersion := IO
  .read((ThisBuild / baseDirectory).value / "stable-version.txt")
  .trim()

lazy val stagedRelease = (project in file("src/test"))
  .settings(
    commonSettings,
    resourceDirectory in Test := baseDirectory.value / "resources",
    scalaSource in Test := baseDirectory.value / "scala",
    unmanagedSourceDirectories in Test += baseDirectory.value / "shim" / majorMinorVersion(
      sparkVersion),
    libraryDependencies ++= testSparkDependencies ++ testCoreDependencies :+
    "com.databricks.labs" %% "smolder" % stableVersion.value % "test",
    resolvers := Seq("bintray-staging" at "https://dl.bintray.com/com.databricks.labs/smolder"),
    org
      .jetbrains
      .sbt
      .extractors
      .SettingKeys
      .sbtIdeaIgnoreModule := true // Do not import this SBT project into IDEA
  )

import ReleaseTransformations._

// Don't use sbt-release's cross facility	
releaseCrossBuild := false

def crossReleaseStep(step: ReleaseStep): Seq[ReleaseStep] = {
  Seq(
    releaseStepCommandAndRemaining(s"""set ThisBuild / scalaVersion := "$scala211""""),
    step,
    releaseStepCommandAndRemaining(s"""set ThisBuild / scalaVersion := "$scala212""""),
    step
  )
}

releaseProcess := Seq[ReleaseStep](
    checkSnapshotDependencies,
    inquireVersions,
    runClean
  ) ++
  crossReleaseStep(runTest) ++
  Seq(
    setReleaseVersion,
    updateStableVersion,
    commitReleaseVersion,
    commitStableVersion,
    tagRelease
  ) ++
  crossReleaseStep(publishArtifacts) ++
  crossReleaseStep(releaseStepCommandAndRemaining("stagedRelease/test")) ++
  Seq(
    setNextVersion,
    commitNextVersion
  )
