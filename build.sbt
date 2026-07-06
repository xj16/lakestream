import Dependencies._

ThisBuild / scalaVersion     := "2.12.18"
ThisBuild / organization     := "dev.xj16"
ThisBuild / version          := "0.1.0"
ThisBuild / scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xlint",
  "-encoding", "utf8"
)

// Spark is compiled against Scala 2.12; we align to that toolchain.
lazy val root = (project in file("."))
  .settings(
    name := "lakestream",
    Compile / mainClass := Some("dev.xj16.lakestream.LakeStreamApp"),

    libraryDependencies ++= Seq(
      sparkSql       % Provided,
      sparkStreaming % Provided,
      sparkSqlKafka,
      kafkaClients,
      deltaCore,
      deltaStorage,
      hadoopAws,
      awsSdkBundle,
      pureconfig,
      scalaLogging,
      logback,

      // test
      scalaTest      % Test,
      sparkSql       % Test,
      sparkStreaming % Test
    ),

    // Spark + Java module access needs these on JDK 11/17 test runs.
    Test / fork := true,
    Test / javaOptions ++= Seq(
      "-Xmx2g",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
    ),

    // Deterministic, single-threaded tests: SparkSession is a shared resource.
    Test / parallelExecution := false,

    // Assembly: build a fat JAR for spark-submit / the Docker image.
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
      case PathList("META-INF", xs @ _*)             => MergeStrategy.discard
      case "reference.conf"                          => MergeStrategy.concat
      case "application.conf"                         => MergeStrategy.concat
      case x =>
        val old = (assembly / assemblyMergeStrategy).value
        old(x)
    },
    assembly / assemblyJarName := s"${name.value}-assembly-${version.value}.jar",
    // Spark deps are Provided at runtime on the cluster; keep them out of the fat JAR.
    assembly / assemblyPackageScala / assembleArtifact := true
  )
