import sbtprotobuf.{ProtobufPlugin=>PB}

name := "sparksql-protobuf"
organization := "com.github.saurfang"

scalaVersion := "2.10.5"
crossScalaVersions := Seq("2.10.5", "2.11.6")

scalacOptions ++= Seq("-target:jvm-1.7" )
javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

version := "0.1.0"

resolvers ++= Seq(
  "Twitter Maven" at "http://maven.twttr.com"
)

libraryDependencies ++= Seq(
  "org.apache.parquet" % "parquet-protobuf" % "1.8.1" % "provided",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)

Seq(PB.protobufSettings: _*)
sourceDirectory in PB.protobufConfig := new File("src/test/protobuf")
javaSource in PB.protobufConfig <<= (sourceDirectory in Test)(_ / "generated")
unmanagedSourceDirectories in Test += baseDirectory.value / "generated"

parallelExecution in Test := false

ScoverageSbtPlugin.ScoverageKeys.coverageExcludedPackages := ".*ProtoLIST.*"

spName := "saurfang/sparksql-protobuf"
sparkVersion := "1.3.0"
sparkComponents += "sql"
credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")
licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")
publishArtifact in (Compile, packageDoc) := false
