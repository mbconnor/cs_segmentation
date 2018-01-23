name := "cs-ranker"

version := "1.0"

scalaVersion := "2.10.6"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql"  % "2.1.0",
  "org.apache.spark" %% "spark-hive" % "2.1.0",
  "org.apache.spark" %% "spark-mllib" % "2.1.0"
)