import sbtprotobuf.{ProtobufPlugin=>PB}

name := """sparksql-protobuf"""

version := "1.0"

scalaVersion := "2.10.5"

resolvers ++= Seq(
  "Twitter Maven" at "http://maven.twttr.com"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.3.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.3.0" % "provided",
  "org.apache.parquet" % "parquet-protobuf" % "1.8.1" % "provided",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)

Seq(PB.protobufSettings: _*)
sourceDirectory in PB.protobufConfig := new File("src/test/protobuf")
javaSource in PB.protobufConfig <<= (sourceDirectory in Test)(_ / "generated")
unmanagedSourceDirectories in Test += baseDirectory.value / "generated"

parallelExecution in Test := false

coverageExcludedPackages := ".*ProtoLIST.*"
