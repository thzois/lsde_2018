import sbt._
import Keys._
import scala._


lazy val root = (project in file("."))
.settings(
    name := "btc_blockchain_schema",
    version := "0.1"
)
.configs( IntegrationTest )
.settings( Defaults.itSettings : _*)
.enablePlugins(JacocoItPlugin)
 

crossScalaVersions := Seq("2.10.5", "2.11.8")

scalacOptions += "-target:jvm-1.7"

resolvers += Resolver.mavenLocal

fork  := true

assemblyJarName in assembly := "btc_blockchain_schema.jar"

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}

libraryDependencies += "com.github.zuinnote" %% "spark-hadoopcryptoledger-ds" % "1.1.2" % "compile"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.1" % "provided"

