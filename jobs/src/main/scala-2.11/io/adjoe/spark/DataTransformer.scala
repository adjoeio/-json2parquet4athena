package io.adjoe.spark

import java.text.SimpleDateFormat
import java.util.Calendar

import com.amazonaws.regions.Regions
import io.adjoe.util.AwsHelper
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

case class CompletedTables(date: String,
                           table: String,
                           partition: String,
                           maxPartition: String)

class DataTransformer( jobName: String = "work_done",
                       workingFolder: String = "tmp")
    extends DataTransformJobProgressPersister {
  val logger = Logger.getLogger(classOf[Nothing])

  val now = Calendar.getInstance().getTime()
  val dayFormat = new SimpleDateFormat("yyyy-MM-dd")
  var today = dayFormat.format(now) //dayFormat.format(now)

  val nowFormat = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss")
  val currentTS = nowFormat.format(now)

  val TEMPORARY_WORKING_FOLDER = s"$workingFolder/$currentTS/"

  def deleteTemporaryData(s3Bucket: String) = {
    logger.info(
      s"deleting temporary data: s3://$s3Bucket/$TEMPORARY_WORKING_FOLDER")
    AwsHelper.deleteS3FolderRecursively(s"$TEMPORARY_WORKING_FOLDER", s3Bucket)
  }

  def deleteErroneousPartitions(
      tableName: String,
      dbType: String,
      formatTypeTarget: String,
      s3Bucket: String,
      region: Regions)(implicit spark: SparkSession) = {

    import spark.sqlContext.implicits._

    logger.info(
      "Check the given table for partitions which can not be loaded and delete them. Most probably will trigger after a failed execution of this job.")

    AwsHelper
      .listSubdirsInDirectory(s3Bucket,
                              s"$dbType/$formatTypeTarget/$tableName",
                              region)
      .foreach { part =>
        val partition = part match {
          case r""".*/([^/].*)$k/""" => k
          case _                     => "nope"
        }
        try {

          val partitionCount = spark.read
            .parquet(
              s"s3a://$s3Bucket/$dbType/$formatTypeTarget/$tableName/$partition")
            .count

          logger.info(
            s"Converted partition $part of table $tableName in bucket $s3Bucket with format $formatTypeTarget has $partitionCount tuples.")
          partitionCount == 0 match {
            case true =>
              logger.info("The partition is empty. Delete it and recreate it.")
              AwsHelper.deleteS3FolderRecursively(
                s"$dbType/$formatTypeTarget/$tableName/$partition",
                s3Bucket,
                region)
              removeTablePartitionFromDoneWork(tableName,
                                               partition,
                                               s3Bucket,
                                               jobName)

            case false =>
            //partition seems fine. mark it as finished

          }

        } catch {
          case e: Exception =>
            logger.warn(
              s"The partition is erroneous and could not be loaded. Delete it and recreate it:" +
                s"${e.getMessage}. => STACKTRACE" +
                s"${e.getStackTrace}")
            AwsHelper.deleteS3FolderRecursively(
              s"$dbType/$formatTypeTarget/$tableName/$partition",
              s3Bucket,
              region)

            removeTablePartitionFromDoneWork(tableName,
                                             partition,
                                             s3Bucket,
                                             jobName)

        }
      }
  }

  def getPartitionsToRecalcIncrementally(tableName: String,
                                         dbType: String,
                                         formatTypeSource: String,
                                         formatTypeTarget: String,
                                         s3Bucket: String,
                                         region: Regions) = {
    var convertedPartitions = AwsHelper.listSubdirsInDirectory(
      s3Bucket,
      s"$dbType/$formatTypeTarget/$tableName",
      region)

    convertedPartitions = convertedPartitions.size > 0 match {
      case true =>
        convertedPartitions.sortWith((x, y) => x > y).map { x =>
          val partition = x match {
            case r""".*/([^/].*)$k/""" => k
            case _                     => "nope"
          }
          partition
        }
      case _ => convertedPartitions
    }

    logger.debug(convertedPartitions.mkString(","))
    val latestUnconvertedPartitions = AwsHelper
      .listSubdirsInDirectory(s3Bucket,
                              s"$dbType/$formatTypeSource/$tableName",
                              region)
      .sortWith((x, y) => x > y)
      .map { x =>
        val partition = x match {
          case r""".*/([^/].*)$k/""" => k
          case _                     => "nope"
        }
        partition
      } //recalc the last partition always
      .filter(x =>
        convertedPartitions.size > 0 match {
          case true  => !convertedPartitions.sortWith(_ > _).tail.contains(x)
          case false => true
      })
      .sortWith(_ < _)

    logger.debug(latestUnconvertedPartitions.mkString(","))
    latestUnconvertedPartitions
  }

}
