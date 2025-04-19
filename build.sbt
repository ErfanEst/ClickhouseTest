ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.13"

lazy val root = (project in file("."))
  .settings(
    name := "LoadToClickhouse"
  )

libraryDependencies ++= Seq(
  "com.clickhouse" % "clickhouse-jdbc" % "0.4.6" excludeAll
    ExclusionRule("org.apache.logging.log4j", "log4j-slf4j2-impl")
//    ExclusionRule("org.apache.logging.log4j", "log4j-core"),
//    ExclusionRule("org.apache.logging.log4j", "log4j-api")
  ,

  // Spark core and sql
  "org.apache.spark" %% "spark-core" % "3.5.1" excludeAll
    ExclusionRule("org.apache.logging.log4j", "log4j-slf4j2-impl"),
//    ExclusionRule("org.apache.logging.log4j", "log4j-core"),
//    ExclusionRule("org.apache.logging.log4j", "log4j-api")

  "org.apache.spark" %% "spark-sql" % "3.5.1" excludeAll(
    ExclusionRule("org.apache.logging.log4j", "log4j-slf4j2-impl"),
    ExclusionRule("org.apache.logging.log4j", "log4j-core"),
    ExclusionRule("org.apache.logging.log4j", "log4j-api")
  ),

  "org.slf4j" % "slf4j-simple" % "2.0.13",

  "org.yaml" % "snakeyaml" % "1.29",

  "com.typesafe" % "config" % "1.4.2",
  "com.github.mfathi91" % "persian-date-time" % "4.2.1"
)
