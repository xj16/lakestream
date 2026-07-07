// Fat-JAR assembly for spark-submit and the runtime Docker image.
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.2.0")

// Uniform code formatting.
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.2")

// Statement/branch coverage (sbt coverage test / coverageReport).
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.12")
