name:= "CSV reader"
version := "1.0"
scalaVersion:= "2.12.11"
libraryDependencies++= Seq(
	"org.apache.spark" % "spark-core_2.12" % "3.2.1",
	"org.apache.spark" % "spark-sql_2.12" % "3.2.1",
	"org.apache.spark" % "spark-streaming_2.12" % "3.2.1",
)
