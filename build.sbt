import sbt._
import Keys._
import complete.DefaultParsers._

name := "AdjoeIncrementalJson2ParquetAthenaImporter"

version := "0.1"

scalaVersion := "2.11.6"


lazy val core = Project(id = "core", base = file("core")).settings(commonSettings: _*)
lazy val jobs = Project(id = "jobs", base = file("jobs")).settings(commonSettings: _*).dependsOn(core)


lazy val root = Project(id = "root", base = file(".")).settings(rootSettings: _*).aggregate(
  core,
  jobs
).dependsOn(
  core,
  jobs
)

lazy val rootSettings = commonSettings


lazy val commonSettings = Seq(
  organization := "io.adjoe",
  version := "0.1.0",
  scalaVersion := "2.11.6",
  assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false, includeDependency = true),
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case PathList("org", "apache", "spark", "unused", xs @ _*) => MergeStrategy.discard
    case PathList("org.slf4j", "impl", xs @ _*) => MergeStrategy.first
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  },



  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-mllib" % "2.3.1" % "provided",
    "org.apache.spark" %% "spark-core"  % "2.3.1" % "provided",
    "org.apache.spark" %% "spark-hive"  % "2.3.1" % "provided",
    "org.apache.spark" %% "spark-sql"   % "2.3.1" % "provided",
    "com.typesafe" % "config" % "1.3.0" exclude("org.apache.commons", "commons-lang3"),
    "com.amazonaws" % "aws-java-sdk" % "1.11.377" % "provided",
    "log4j" % "log4j" % "1.2.17"
  ) ,

  resolvers ++= Seq(
    "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven",
    "jitpack" at "https://jitpack.io"
  )


)

/*************************************************
  *                     JOB CONFIG
  ************************************************/

val generalStepConfigClientMode = "--deploy-mode,client,--master,yarn,--conf,yarn.scheduler.capacity.maximum-am-resource-percent=0.95,--conf,spark.default.parallelism=100"
val memoryParameters = "--conf,spark.driver.maxResultSize=1G,--conf,spark.executor.extraJavaOptions=-Xss1024m,--driver-java-options,-Xss1024m,--conf,spark.executor.memory=40g,--conf,spark.executor.memoryOverhead=15g,--conf,spark.driver.memory=5g,--conf,spark.driver.memoryOverhead=15g"
val dataHandlingParameters = "--conf,spark.sql.caseSensitive=true,--conf,spark.sql.parquet.mergeSchema=false,--conf,spark.hadoop.fs.s3a.experimental.input.fadvise=normal,--conf,spark.hadoop.fs.s3a.fast.upload=true,--conf,spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem,--conf,spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2,--conf,spark.sql.parquet.filterPushdown=true,--conf,spark.speculation=false,--conf,spark.sql.parquet.fs.optimized.committer.optimization-enabled=true"


/*************************************************
  *                     JOB DEFINITION
  ************************************************/

val convert2parquet = inputKey[Unit]("")
convert2parquet := {
  val args: Seq[String] = spaceDelimited("<arg>").parsed
  val s3JarLocation = args(0)
  val s3Bucket = args(1)
  val databaseFolder = args(2)
  val databaseJsonSubpath = args(3)
  val databaseParquetSubpath = args(4)
  val tmpFolder = args(5)
  val jobName = args(6)
  val region = args(7)
  val emrClusterId = args(8)
  val maxNestingDepth = args(9)

  Seq("aws", "emr", "add-steps",
    "--cluster-id", emrClusterId,
    "--steps",
    s"""Type='Spark',Name='Json2Parquet4Athena',ActionOnFailure='CONTINUE',Args=[$generalStepConfigClientMode,$memoryParameters,$dataHandlingParameters,--packages,org.apache.hadoop:hadoop-aws:2.8.1,--class,io.adjoe.spark.ParquetConversion,$s3JarLocation,$s3Bucket,$databaseFolder,$databaseJsonSubpath,$databaseParquetSubpath,$tmpFolder,$jobName,$region,$maxNestingDepth]""").!
}



run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))



