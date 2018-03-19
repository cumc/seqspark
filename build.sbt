name := "seqspark"

version := "1.0"

organization := "org.dizhang"

scalaVersion := "2.11.12"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Ypartial-unification")

libraryDependencies ++= Seq(
	"org.slf4j" % "slf4j-api" % "1.7.5" % "provided",
	"org.slf4j" % "slf4j-log4j12" % "1.7.5" % "provided",
	"org.apache.spark" % "spark-core_2.11" % "2.1.2" % "provided",
	"org.apache.spark" % "spark-sql_2.11" % "2.1.2" % "provided",
	"org.apache.spark" % "spark-mllib_2.11" % "2.1.2" % "provided",
	"com.typesafe" % "config" % "1.2.1",
	"org.scalanlp" %% "breeze" % "0.12",
	"org.scalanlp" %% "breeze-natives" % "0.12",
	"org.scalatest" % "scalatest_2.11" % "2.2.4" % "test",
  "org.typelevel" %% "spire" % "0.14.1",
	"org.typelevel" %% "cats-free" % "1.0.1",
	"org.typelevel" %% "cats-core" % "1.0.1"
)

//resolvers += "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"
resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
resolvers ++= Seq(
	Resolver.sonatypeRepo("releases"),
	Resolver.sonatypeRepo("snapshots")
)

addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.6")

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)


assemblyMergeStrategy in assembly := {
	case "reference.conf"                            => MergeStrategy.concat
	case "pom.xml"                                => MergeStrategy.discard
	case "pom.properties"                                => MergeStrategy.discard
	case x =>
		val oldStrategy = (assemblyMergeStrategy in assembly).value
		oldStrategy(x)
}

fork in Test := true
parallelExecution in Test := false
//testOptions in Test := Seq(Tests.Filter(s => ! s.endsWith("SingleStudySpec")))

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
assemblyJarName in assembly := "SeqSpark.jar"
test in assembly := {}
mainClass in (Compile, run) := Some("org.dizhang.seqspark.SingleStudy")
mainClass in (Compile, packageBin) := Some("org.dizhang.seqspark.SingleStudy")
