package io.adjoe.spark

import com.amazonaws.regions.Regions
import org.apache.spark.sql._
import io.adjoe.util.AwsHelper

object ParquetConversion {

  def main(args: Array[String]): Unit = {
    implicit val ss = SparkSession.builder().getOrCreate()
    implicit val sqlc = ss.sqlContext

    import sqlc.implicits._

    val s3Bucket = args(0)
    var dbType = args(1)
    var formatTypeSource = args(2)
    var formatTypeTarget = args(3)
    val tmpFolder = args(4)
    val jobName = args(5)
    var region = Regions.valueOf(args(6))
    var maxNestingDepth = args(7).toInt

    println("start import")

    val job = new DataLakeConversionJob(jobName, tmpFolder, maxNestingDepth)
    job.initWorkLogTable(s3Bucket, jobName)

    AwsHelper.listSubdirsInDirectory(s3Bucket,
                                     s"$dbType/$formatTypeSource",
                                     region) foreach { tablePath =>
      val tableName = tablePath match {
        case r""".*/(.*)/$k.*""" => k
        case _                   => "nope"
      }

      job.transformData(tableName,
                        dbType,
                        formatTypeSource,
                        formatTypeTarget,
                        s3Bucket,
                        region)

    }

    job.deleteTemporaryData(s3Bucket)
  }

  implicit class Regexp(sc: StringContext) {
    def r =
      new scala.util.matching.Regex(sc.parts.mkString,
                                    sc.parts.tail.map(_ => "x"): _*)
  }

}
