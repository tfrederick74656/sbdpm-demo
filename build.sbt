lazy val root = (project in file(".")).
	settings(
	name := "sbdpm-demo",
	version := "1.0",
	scalaVersion := "2.10.4"
)
libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "1.5.2",
	"org.apache.spark" %% "spark-mllib" % "1.5.2"
)