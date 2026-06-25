val scala213 = "2.13.16"
val sparkVersion = "4.1.2"
val jacksonVersion = "2.15.2"
val scalaTestVersion = "3.2.19"

lazy val commonSettings = Seq(
  organization := "com.github.pyal",
  scalaVersion := scala213,
  crossScalaVersions := Seq(scala213),
  scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-Xfatal-warnings"),
  Test / parallelExecution := false,
  Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat,
  Test / fork := true,
  Test / javaOptions ++= Seq(
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  ),
  libraryDependencies ++= Seq(
    "com.github.pyal"                  %% "polyconf"             % "0.1.0-SNAPSHOT",
    "org.apache.spark"                 %% "spark-core"           % sparkVersion % Provided,
    "org.apache.spark"                 %% "spark-sql"            % sparkVersion % Provided,
    ("com.google.cloud.spark"           % "spark-bigquery-with-dependencies_2.13" % "0.39.1")
      .exclude("com.google.guava", "guava"),
    ("org.elasticsearch"               %% "elasticsearch-spark-30" % "8.15.0")
      .exclude("org.apache.spark", "spark-core_2.13")
      .exclude("org.apache.spark", "spark-sql_2.13"),
    ("com.google.cloud"                 % "google-cloud-pubsub"  % "1.128.1")
      .exclude("com.google.guava", "guava"),
    ("io.delta"                        %% "delta-spark"          % "3.2.0")
      .exclude("org.apache.spark", "spark-core_2.13")
      .exclude("org.apache.spark", "spark-sql_2.13"),
    "com.fasterxml.jackson.core"       % "jackson-core"        % jacksonVersion,
    "com.fasterxml.jackson.core"       % "jackson-databind"    % jacksonVersion,
    "com.fasterxml.jackson.module"    %% "jackson-module-scala" % jacksonVersion,
    "org.apache.logging.log4j"         % "log4j-core"          % "2.20.0",
    "org.apache.logging.log4j"         % "log4j-1.2-api"       % "2.20.0",
    "org.rogach"                      %% "scallop"             % "5.1.0",
    "org.scalatest"                   %% "scalatest"           % scalaTestVersion % Test,
  ),
  excludeDependencies ++= Seq(
    "org.slf4j" % "slf4j-log4j12",
    "org.slf4j" % "slf4j-reload4j",
    "log4j"     % "log4j",
    "org.apache.parquet" % "parquet-avro",
  ),
)

lazy val publishSettings = Seq(
  publishTo := Some(
    "GitHub Packages" at s"https://maven.pkg.github.com/pyal/${name.value}"
  ),
  credentials += Credentials(Path.userHome / ".sbt" / ".github-credentials"),
  publishMavenStyle := true,
)

lazy val `polyconf-spark` = (project in file("."))
  .settings(commonSettings, publishSettings, name := "polyconf-spark")
  .settings(
    assembly / assemblyMergeStrategy := {
      case "module-info.class"                             => MergeStrategy.discard
      case "META-INF/MANIFEST.MF"                          => MergeStrategy.discard
      case "META-INF/DEPENDENCIES"                         => MergeStrategy.discard
      case "META-INF/io.netty.versions.properties"         => MergeStrategy.last
      case "META-INF/services/org.apache.logging.log4j.spi.Provider" => MergeStrategy.concat
      case "META-INF/services/com.fasterxml.jackson.databind.Module" => MergeStrategy.concat
      case x if x.startsWith("META-INF/services/")         => MergeStrategy.concat
      case x if x.endsWith(".conf")                        => MergeStrategy.concat
      case x if x.endsWith(".properties")                  => MergeStrategy.last
      case x if x.startsWith("META-INF/")                  => MergeStrategy.discard
      case _                                               => MergeStrategy.first
    }
  )
