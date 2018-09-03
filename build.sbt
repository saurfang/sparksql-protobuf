import sbtprotobuf.{ProtobufPlugin=>PB}

name := "sparksql-protobuf"
organization := "com.github.saurfang"

scalaVersion := "2.11.6"
crossScalaVersions := Seq("2.11.6")

scalacOptions ++= Seq("-target:jvm-1.8" )
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

enablePlugins(GitVersioning)
git.baseVersion := "0.1.3"

resolvers ++= Seq(
  "Twitter Maven" at "https://maven.twttr.com"
)

libraryDependencies ++= Seq(
  "org.apache.parquet" % "parquet-protobuf" % "1.8.3" % "provided",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)

Seq(PB.protobufSettings: _*)
sourceDirectory in PB.protobufConfig := new File("src/test/protobuf")
javaSource in PB.protobufConfig <<= (sourceDirectory in Test)(_ / "generated")
unmanagedSourceDirectories in Test += baseDirectory.value / "generated"

parallelExecution in Test := false

coverageExcludedPackages := ".*ProtoLIST.*"

spName := "saurfang/sparksql-protobuf"
sparkVersion := "2.3.1"
sparkComponents += "sql"
credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")
licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")
publishArtifact in (Compile, packageDoc) := false
spAppendScalaVersion := true
