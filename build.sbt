name := "SparkBase"

version := "0.1"

scalaVersion := "2.11.12"

val scalatestVersion = "3.1.1"
val liftwebVersion = "3.4.1"

val sparkVersion = "2.4.5"
libraryDependencies ++= Seq(
  "net.liftweb" %% "lift-json" % liftwebVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.scalatest" %% "scalatest" % scalatestVersion % "test"
)
