lazy val root = (project in file(".")).
  settings(
    organization := "com.irvingc.spark",
    version := "0.2.0-SNAPSHOT",
    name := "dbscan-on-spark",
    homepage := Some(url("http://www.irvingc.com/dbscan-on-spark")),
    description := "An implementation of DBSCAN runing on top of Apache Spark",
    licenses += ("Apache-2.0" -> url( "http://www.apache.org/licenses/LICENSE-2.0.html")),
    scalaVersion := "2.10.4",

    resolvers += bintray.Opts.resolver.mavenRepo("archery"),

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-mllib" % "1.2.0" % "provided",
      "com.meetup" %% "archery" % "0.3.0",
      "com.github.davidmoten" % "rtree" % "0.5.8",
      "org.scalatest" %% "scalatest" % "2.0" % "test"
    ),

    publishMavenStyle := true,
    pomExtra :=
      <scm>
        <url>git@github.com:irvingc/dbscan-on-spark.git</url>
        <connection>scm:git:git@github.com:irvingc/dbscan-on-spark.git</connection>
      </scm>
      <developers>
        <developer>
          <id>irvingc</id>
          <name>Irving Cordova</name>
          <email>irving@irvingc.com</email>
          <url>http://www.irvingc.com/</url>
        </developer>
      </developers>
)

// Create a default Scala style task to run with tests
lazy val checkStyle = taskKey[Unit]("checkStyle")

checkStyle := org.scalastyle.sbt.ScalastylePlugin.scalastyle.in(Compile).toTask("").value

(test in Test) <<= (test in Test) dependsOn checkStyle 

// Prevent scala from being included in the fat jar
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
 
// configure bintray plugin
seq(bintraySettings:_*)
