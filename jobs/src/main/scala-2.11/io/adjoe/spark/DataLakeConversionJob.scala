package io.adjoe.spark

import java.util.Calendar

import com.amazonaws.regions.Regions
import io.adjoe.util.AwsHelper
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

class DataLakeConversionJob(jobName: String = "work_done",
                            workingFolder: String = "tmp",
                            maxNestingDepth: Int  = 1)
    extends DataFrameSchemaNormalizer(jobName, workingFolder, maxNestingDepth) {

  def doCompleteImport(tableName: String,
                       dbType: String,
                       formatTypeSource: String,
                       formatTypeTarget: String,
                       s3Bucket: String,
                       region: Regions)(implicit spark: SparkSession) = {

    import spark.sqlContext.implicits._

    AwsHelper.deleteS3FolderRecursively(s"$dbType/$formatTypeTarget/$tableName",
                                        s3Bucket)

    removeTableFromDoneWork(tableName, s3Bucket, jobName)

    val maxUnconvertedPartition =
      getMaxPartition(tableName, dbType, formatTypeSource, s3Bucket, region)

    val sourceData =
      spark.read.json(s"s3a://$s3Bucket/$dbType/$formatTypeSource/$tableName")

    sourceData.count() > 0 match {
      case false => //nothing to do
      case _ =>
        println(
          s"Creating ALL 'dt' partitions for JSON data of table '$tableName' and transform it to PARQUET.")

        val schemaPreservingTuple =
          getSchemaPreservingTuple(sourceData, s3Bucket)
            .drop("dt")
            .withColumn("drop", lit(true))
            .withColumn("tmp_dt", lit("dt=00000000"))
            .persist()

        schemaPreservingTuple.write
          .mode("overwrite")
          .json(
            s"s3a://$s3Bucket/$TEMPORARY_WORKING_FOLDER$tableName/dt=00000000")
        schemaPreservingTuple.printSchema

        getPartitionsToRecalcIncrementally(tableName,
                                           dbType,
                                           formatTypeSource,
                                           formatTypeTarget,
                                           s3Bucket,
                                           region)
          .foreach { part =>
            println(part)

            val unconvertedNew = normalizeSchema(
              spark.read.json(
                s"s3a://$s3Bucket/$dbType/$formatTypeSource/$tableName/$part"),
              s3Bucket
            ).withColumn("drop", lit(false))
              .withColumn("tmp_dt", lit(part))
              .drop("dt")

            schemaPreservingTuple.write
              .mode("overwrite")
              .json(
                s"s3a://$s3Bucket/$TEMPORARY_WORKING_FOLDER$tableName/$part")
            unconvertedNew.write
              .mode("append")
              .json(
                s"s3a://$s3Bucket/$TEMPORARY_WORKING_FOLDER$tableName/$part")
            unconvertedNew.printSchema

            Thread.sleep(2000)

            spark.read
              .json(
                s"s3a://$s3Bucket/$TEMPORARY_WORKING_FOLDER$tableName/$part")
              .filter(!$"drop")
              .drop("drop")
              .filter($"tmp_dt" === part)
              .drop($"tmp_dt")
              .drop("dt")
              .write
              .mode("overwrite")
              .parquet(
                s"s3a://$s3Bucket/$dbType/$formatTypeTarget/$tableName/$part")

            addPartitionToDoneWork(tableName,
                                   part,
                                   maxUnconvertedPartition,
                                   s3Bucket,
                                   jobName,
                                   today)
          }

        schemaPreservingTuple.unpersist()
        sourceData.unpersist()
    }

  }

  def transformData(tableName: String,
                    dbType: String,
                    formatTypeSource: String,
                    formatTypeTarget: String,
                    s3Bucket: String,
                    region: Regions)(implicit spark: SparkSession): Any = {
    import spark.sqlContext.implicits._

    println("PROCESSING: " + tableName)

    println("FIND PREVIOUSLY IMPORTED PARTITIONS: ")
    var previouslyCreatedPartitions = AwsHelper.listSubdirsInDirectory(
      s3Bucket,
      s"$dbType/$formatTypeTarget/$tableName",
      region)

    hasNewPartitions(tableName, dbType, s3Bucket, jobName, region) match {
      case false =>
        println("No new partitions. Nothing to do!")
        return None
      case _ =>
    }

    previouslyCreatedPartitions.size > 0 match {
      case true =>
        println("HAS MORE THAN 0 PARTITIONS!")
        val local_now = Calendar.getInstance().getTime()
        val local_currentMinuteAsString = dayFormat.format(local_now)
        println(
          "################# last import partition: " + previouslyCreatedPartitions
            .sortWith((x, y) => x > y)
            .head)
        previouslyCreatedPartitions
          .sortWith((x, y) => x > y)
          .head
          .contains("dt=" + local_currentMinuteAsString) match {

          case true =>
            println("nothing to do - already imported")

          case false =>
            previouslyCreatedPartitions = AwsHelper.listSubdirsInDirectory(
              s3Bucket,
              s"$dbType/$formatTypeTarget/$tableName",
              region)

            val (doFullImport, alreadyComputed) =
              previouslyCreatedPartitions.nonEmpty match {
                case true =>
                  val maxUnconvertedPartition = getMaxPartition(
                    tableName,
                    dbType,
                    formatTypeSource,
                    s3Bucket,
                    region)

                  wasJobInterupted(tableName, dbType, s3Bucket, jobName) match {
                    case true =>
                      deleteErroneousPartitions(tableName,
                                                dbType,
                                                formatTypeTarget,
                                                s3Bucket,
                                                region)
                    case _ =>
                  }

                  val latestConvertedPartition =
                    previouslyCreatedPartitions.sortWith((x, y) => x > y).head
                  println(
                    s"Latest partition of converted table '$tableName': " + latestConvertedPartition)

                  val convertedOld = getSchemaPreservingTuple(
                    spark.read
                      .parquet(
                        s"s3a://$s3Bucket/$dbType/$formatTypeTarget/$tableName" /*latestConvertedPartition*/ )
                      .withColumn("drop", lit(true)),
                    s3Bucket
                  ).withColumn("tmp_dt", lit(latestConvertedPartition))
                  convertedOld.write
                    .mode("overwrite")
                    .json(
                      s"s3a://$s3Bucket/$TEMPORARY_WORKING_FOLDER$tableName/dt=00000000")
                  convertedOld.printSchema

                  getPartitionsToRecalcIncrementally(tableName,
                                                     dbType,
                                                     formatTypeSource,
                                                     formatTypeTarget,
                                                     s3Bucket,
                                                     region)
                    .foreach { part =>
                      println(part)
                      val loc =
                        s"s3a://$s3Bucket/$dbType/$formatTypeSource/$tableName/$part"
                      val unconvertedNew =
                        normalizeSchema(spark.read.json(loc), s3Bucket)
                          .withColumn("drop", lit(false))
                          .withColumn("tmp_dt", lit(part))

                      convertedOld.write
                        .mode("overwrite")
                        .json(
                          s"s3a://$s3Bucket/$TEMPORARY_WORKING_FOLDER$tableName/$part")
                      unconvertedNew.write
                        .mode("append")
                        .json(
                          s"s3a://$s3Bucket/$TEMPORARY_WORKING_FOLDER$tableName/$part")
                      unconvertedNew.printSchema

                      Thread.sleep(2000)

                      spark.read
                        .json(
                          s"s3a://$s3Bucket/$TEMPORARY_WORKING_FOLDER$tableName/$part")
                        .filter(!$"drop")
                        .drop("drop")
                        .filter($"tmp_dt" === part)
                        .drop($"tmp_dt")
                        .drop("dt")
                        .write
                        .mode("overwrite")
                        .parquet(
                          s"s3a://$s3Bucket/$dbType/$formatTypeTarget/$tableName/$part")

                      addPartitionToDoneWork(tableName,
                                             part,
                                             maxUnconvertedPartition,
                                             s3Bucket,
                                             jobName,
                                             today)

                    }

                  val merged = spark.read.json(
                    s"s3a://$s3Bucket/$TEMPORARY_WORKING_FOLDER$tableName")

                  val recalc = needsRecalculation(
                    convertedOld.columns.contains("dt") match {
                      case true => convertedOld.drop("dt")
                      case _    => convertedOld
                    },
                    merged.columns.contains("dt") match {
                      case true => merged.drop("dt")
                      case _    => merged
                    }
                  )

                  println("recalc complete data: " + recalc)
                  (recalc, true)

                case false =>
                  println("HAS MORE 0 PARTITIONS!")
                  (true, false)

              }

            doFullImport match {
              case true =>
                alreadyComputed match {
                  case true =>
                    doCompleteImport(tableName,
                                     dbType,
                                     formatTypeSource,
                                     formatTypeTarget,
                                     s3Bucket,
                                     region)

                  case false =>
                    doCompleteImport(tableName,
                                     dbType,
                                     formatTypeSource,
                                     formatTypeTarget,
                                     s3Bucket,
                                     region)
                }

              case false =>
            }
        }

      case false =>
        doCompleteImport(tableName,
                         dbType,
                         formatTypeSource,
                         formatTypeTarget,
                         s3Bucket,
                         region)

    }

  }

}
