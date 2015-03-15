// To create a fat jar
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.13.0")

// To generate eclipse project
addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "3.0.0")

// To check code style
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.6.0") 

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

