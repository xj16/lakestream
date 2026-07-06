# ---------- build stage ----------
# Build the fat JAR with sbt-assembly on a JDK 17 + sbt image. The sbt launcher
# reads project/build.properties and bootstraps the pinned sbt version.
FROM sbtscala/scala-sbt:eclipse-temurin-17_1.x AS build

WORKDIR /app

# Warm the dependency cache first (better layer caching).
COPY project/build.properties project/plugins.sbt project/Dependencies.scala ./project/
COPY build.sbt ./
RUN sbt update

# Now the sources.
COPY src ./src
RUN sbt assembly

# ---------- runtime stage ----------
# Official Spark image already ships Spark 3.5.x + Hadoop 3 + a JRE (Java 17).
FROM apache/spark:3.5.1-scala2.12-java17-ubuntu

USER root
WORKDIR /opt/lakestream

# Copy the assembled application JAR.
COPY --from=build /app/target/scala-2.12/lakestream-assembly-0.1.0.jar app.jar

# Packages the Kafka + Delta + S3A jars are pulled in via --packages at submit
# time (see entrypoint) so the image stays slim and versions stay pinned.
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

COPY docker/entrypoint.sh /opt/lakestream/entrypoint.sh
RUN chmod +x /opt/lakestream/entrypoint.sh

USER spark
ENTRYPOINT ["/opt/lakestream/entrypoint.sh"]
