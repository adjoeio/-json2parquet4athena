package io.adjoe.util

import com.amazonaws.regions.Regions
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

trait DataFrameHelperFunctions {

  implicit class Regexp(sc: StringContext) {
    def r =
      new scala.util.matching.Regex(sc.parts.mkString,
                                    sc.parts.tail.map(_ => "x"): _*)
  }

  def getMaxPartition(tableName: String,
                      dbType: String,
                      format: String,
                      s3Bucket: String,
                      region: Regions) = {
    val latestUnconvertedPartition = AwsHelper
      .listSubdirsInDirectory(s3Bucket, s"$dbType/$format/$tableName")
      .sortWith((x, y) => x > y)
      .map { x =>
        val partition = x match {
          case r""".*/([^/].*)$k/""" => k
          case _                     => "nope"
        }
        partition
      }
      .sortWith(_ > _)
      .head

    latestUnconvertedPartition

  }

  def deleteComments(df: DataFrame)(implicit spark: SparkSession) = {
    df.columns.foldLeft(df)((acc, col) => deleteComment(acc, col))
  }

  def deleteComment(df: DataFrame, colname: String)(
      implicit spark: SparkSession) = {
    var innerSchema = df.schema
    val oldCol = innerSchema(colname)
    val newCol = StructField(oldCol.name, oldCol.dataType, oldCol.nullable)

    val colsComingBefore =
      innerSchema.fields.splitAt(innerSchema.fieldIndex(colname))._1
    val colsComingAfter =
      innerSchema.fields.splitAt(innerSchema.fieldIndex(colname))._2.tail
    innerSchema = StructType((colsComingBefore :+ newCol) ++ colsComingAfter)

    val result = spark.sqlContext.createDataFrame(df.rdd, innerSchema)
    result
  }

  def setComment(
      df: DataFrame,
      colname: String,
      comment: String,
      append: Boolean = false)(implicit spark: SparkSession): DataFrame = {
    var innerSchema = df.schema

    val oldComment: Seq[String] = innerSchema(colname).getComment() match {
      case None => Seq.empty[String]
      case Some(comment) =>
        append match {
          case true => comment.split(",")
          case _    => Seq.empty[String]
        }

    }

    val newCol = innerSchema(colname)
      .withComment((comment.split(",") ++ oldComment).toSet.toSeq.mkString(","))

    val colsComingBefore =
      innerSchema.fields.splitAt(innerSchema.fieldIndex(colname))._1
    val colsComingAfter =
      innerSchema.fields.splitAt(innerSchema.fieldIndex(colname))._2.tail
    innerSchema = StructType((colsComingBefore :+ newCol) ++ colsComingAfter)

    val result = spark.sqlContext.createDataFrame(df.rdd, innerSchema)
    result
  }
}
