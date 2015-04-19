import sbtprotobuf.{ProtobufPlugin=>PB}

Seq(PB.protobufSettings: _*)

name := """sparksql-protobuf"""

version := "1.0"

scalaVersion := "2.10.5"

resolvers ++= Seq(
  "Twitter Maven" at "http://maven.twttr.com"
)

// Change this to another test framework if you prefer
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.3.0",
  "org.apache.spark" %% "spark-sql" % "1.3.0",
  "com.twitter" % "parquet-protobuf" % "1.5.0",
  "junit" % "junit" % "4.12" % "test",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)
