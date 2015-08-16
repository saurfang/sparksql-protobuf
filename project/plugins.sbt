addSbtPlugin("com.github.gseitz" % "sbt-protobuf" % "0.4.0")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.6.0")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.0.4")

addSbtPlugin("org.scoverage" % "sbt-coveralls" % "1.0.0")

resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"
addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.2")

addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "0.8.4")

addSbtPlugin("me.lessis" % "bintray-sbt" % "0.3.0")
