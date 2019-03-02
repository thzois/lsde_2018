import sbt._
import Keys._
import scala._


lazy val root = (project in file("."))
.settings(
    name := "bitcoin_trans_graph_computations",
    version := "0.1"
)
 .configs( IntegrationTest )
  .settings( Defaults.itSettings : _*)
.enablePlugins(JacocoItPlugin)
 

scalacOptions += "-target:jvm-1.7"

crossScalaVersions := Seq("2.10.5", "2.11.7")

resolvers += Resolver.mavenLocal

fork  := true

assemblyJarName in assembly := "bitcoin_trans_graph_computations.jar"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.1" % "provided" 
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.1.1" % "provided"
