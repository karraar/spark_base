name := "SparkBase"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.5"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.scalatest" %% "scalatest" % "3.1.1" % "test"
)
