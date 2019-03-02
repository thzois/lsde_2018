import sbt._
import Keys._
import scala._


lazy val root = (project in file("."))
.settings(
    name := "bitcoin_transactions_to_graph",
    version := "0.1"
)
 .configs( IntegrationTest )
  .settings( Defaults.itSettings : _*)
.enablePlugins(JacocoItPlugin)
 

scalacOptions += "-target:jvm-1.7"

crossScalaVersions := Seq("2.10.5", "2.11.7")

resolvers += Resolver.mavenLocal

fork  := true

assemblyJarName in assembly := "bitcoin_transactions_to_graph.jar"

libraryDependencies += "com.github.zuinnote" % "hadoopcryptoledger-fileformat" % "1.2.0" % "compile"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.1" % "provided"
 
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.1.1"  % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.1" % "provided"

libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.0.1" % "it"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.1" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.1" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.1" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-minicluster" % "2.7.1" % "it"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test,it"
