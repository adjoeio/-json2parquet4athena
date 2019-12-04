package io.adjoe.spark

import com.amazonaws.regions.Regions
import io.adjoe.util.{AwsHelper, DataFrameHelperFunctions}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, max}

import scala.util.{Success, Try}

trait DataTransformJobProgressPersister extends DataFrameHelperFunctions{
  /*****************
    * FUNCTIONS TO REMEMBER WHAT HAS BEEN DONE
    */

  def initWorkLogTable(s3Bucket: String, jobName: String)(implicit spark:SparkSession) ={
    import spark.sqlContext.implicits._
    try {
      spark.read.parquet(s"s3a://$s3Bucket/$jobName").count > 0 match {
        case true =>
        case _ =>
          Seq(CompletedTables("0001-01-01","example","00010101","00010101")).toDF.write.mode("overwrite").parquet(s"s3a://$s3Bucket/$jobName")
      }
    } catch {
      case _ =>
        Seq(CompletedTables("0001-01-01","example","00010101","00010101")).toDF.write.mode("overwrite").parquet(s"s3a://$s3Bucket/$jobName")
    }

  }

  def hasNewPartitions(tableName: String, dbType: String, s3Bucket:String, jobName: String, region: Regions)(implicit spark:SparkSession) = {
    val lastPartition = getMaxPartition(tableName, dbType, "json", s3Bucket, region)

    val hasNoNewPartitions = Try{
      spark.read.parquet(s"s3a://$s3Bucket/$jobName")
        .filter(col("table") === lit(tableName))
        .agg(max(col("partition")) as "maxPartition")
        .select(col("maxPartition") === lit(lastPartition))
        .collect()
        .map(_.getBoolean(0))
        .head
    }

    hasNoNewPartitions match {
      case Success(newPartitions) =>
        println(s"Table $tableName has latest partition $lastPartition. New partitions: ${!newPartitions}")

        !newPartitions

      case _ => true

    }

  }

  def wasJobInterupted(tableName: String, dbType: String, s3Bucket:String, jobName: String)(implicit spark:SparkSession) = {
    val finished_partitions_raw = spark.read.parquet(s"s3a://$s3Bucket/$jobName")
      .filter(col("table") === lit(tableName))

    val last_time_worked_on = finished_partitions_raw.agg(max(col("date")) as "date")
    val wasUnInterrupted = Try {
      finished_partitions_raw
        .join(last_time_worked_on, Seq("date"))
        .groupBy(col("maxPartition") as "maxJsonPartition")
        .agg(max(col("partition")) as "lastFinishedParquetPartition")
        .select(col("maxJsonPartition") === col("lastFinishedParquetPartition"))
        .collect()
        .map(_.getBoolean(0))
        .head
    }

    wasUnInterrupted match {
      case Success(uninterrupted) => !uninterrupted
      case _ => true
    }

  }

  def removeTableFromDoneWork(tableName: String, s3Bucket: String, jobName: String)(implicit spark:SparkSession) = {
    import spark.sqlContext.implicits._

    val current_table = spark.read
      .parquet(s"s3a://$s3Bucket/$jobName")
      .filter($"table" !== lit(tableName))

    current_table.write
      .mode("overwrite")
      .parquet(s"s3a://$s3Bucket/${jobName}_tmp")

    val current_table_loaded =
      spark.read.parquet(s"s3a://$s3Bucket/${jobName}_tmp")

    current_table_loaded.write
      .mode("overwrite")
      .parquet(s"s3a://$s3Bucket/$jobName")

    AwsHelper.deleteS3FolderRecursively(s"${jobName}_tmp", s3Bucket)
  }

  def removeTablePartitionFromDoneWork(tableName: String, partition:String, s3Bucket: String, jobName: String)(implicit spark:SparkSession) = {
    import spark.sqlContext.implicits._

    val current_table = spark.read
      .parquet(s"s3a://$s3Bucket/$jobName")
      .filter(
        ($"table" !== lit(tableName)) or ($"partition" !== lit(partition))
      )

    current_table.write
      .mode("overwrite")
      .parquet(s"s3a://$s3Bucket/${jobName}_tmp")

    val current_table_loaded =
      spark.read.parquet(s"s3a://$s3Bucket/${jobName}_tmp")

    current_table_loaded.write
      .mode("overwrite")
      .parquet(s"s3a://$s3Bucket/$jobName")

    AwsHelper.deleteS3FolderRecursively(s"${jobName}_tmp", s3Bucket)
  }


  def addPartitionToDoneWork(tableName: String, part: String, maxUnconvertedPartition: String, s3Bucket: String, jobName: String, today: String)(implicit spark:SparkSession) = {
    import spark.sqlContext.implicits._
    Seq(
      CompletedTables(today,
        tableName,
        part,
        maxUnconvertedPartition)).toDF.write
      .mode("append")
      .parquet(s"s3a://$s3Bucket/$jobName")
  }

}
