package io.adjoe.util

import java.io.File
import java.text.SimpleDateFormat

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.{
  GetObjectRequest,
  ListObjectsRequest,
  S3ObjectSummary
}
import org.apache.spark.sql.SQLContext
import scala.collection.JavaConversions._
import collection.JavaConverters._

object AwsHelper {
  def upload2S3(
      localFilePath: String,
      s3Path: String,
      bucketName: String,
      region: Regions)(implicit sqlc: SQLContext) = {
    //file to upload
    val fixedS3Path = s3Path.contains(s"s3a://$bucketName") match {
      case true =>
        s3Path.replace(s"s3a://$bucketName/", "")
      case false =>
        s3Path
    }
    val fileToUpload = new File(localFilePath)
    val fileName = localFilePath.split("/").last
    val uploadPath = (fixedS3Path + "/" + fileName).replace("//", "/")

    val amazonS3Client =
      AmazonS3ClientBuilder.standard().withRegion(region).build()

    val res = amazonS3Client.putObject(bucketName, uploadPath, fileToUpload)
  }

  def downloadFromS3(s3Path: String,
                     localFilePath: String,
                     bucketName: String,
                     region: Regions = Regions.EU_CENTRAL_1) = {
    val fixedS3Path = s3Path.contains(s"s3a://$bucketName") match {
      case true =>
        s3Path.replace(s"s3a://$bucketName/", "")
      case false =>
        s3Path
    }

    println(s"Download from s3 [$bucketName]: $fixedS3Path")

    val amazonS3Client =
      AmazonS3ClientBuilder.standard().withRegion(region).build()

    amazonS3Client.getObject(new GetObjectRequest(bucketName, fixedS3Path),
                             new File(localFilePath))

  }

  def getFilelistFromFolder(bucketName: String, folderKey: String) = {
    val s3Client = AmazonS3ClientBuilder.standard().build()
    val listObjectsRequest = new ListObjectsRequest()
      .withBucketName(bucketName)
      .withPrefix(folderKey + "/")
    val keys = new java.util.ArrayList[String]
    var objects = s3Client.listObjects(listObjectsRequest)
    var loop = true
    while ({
      loop
    }) {
      val summaries = objects.getObjectSummaries.asScala
      if (summaries.size > 0) {
        summaries.foreach { s: S3ObjectSummary =>
          keys.add(s.getKey)
        }
        objects = s3Client.listNextBatchOfObjects(objects)
      } else {
        loop = false
      }
    }
    keys.asScala.toSeq
  }

  def getFileCreationDatelistFromFolder(bucketName: String,
                                        folderKey: String) = {
    val s3Client = AmazonS3ClientBuilder.standard().build()
    val listing = s3Client.listObjects(bucketName, folderKey)
    val lastModified = listing.getObjectSummaries.toSeq.foldLeft(0L) {
      (acc, x) =>
        x.getLastModified.getTime > acc match {
          case true => x.getLastModified.getTime
          case _    => acc
        }
    }
    val df = new SimpleDateFormat("yyyy-MM-dd")
    val date: String = df.format(lastModified)
    date
  }

  def listSubdirsInDirectory(bucketName: String,
                             prefix: String,
                             region: Regions = Regions.EU_CENTRAL_1) = {
    val amazonS3Client =
      AmazonS3ClientBuilder.standard().withRegion(region).build()
    var prefixVar = prefix.replace(s"s3a://$bucketName/", "")
    val delimiter = "/"
    if (!prefixVar.endsWith(delimiter)) prefixVar += delimiter
    val listObjectsRequest = new ListObjectsRequest()
      .withBucketName(bucketName)
      .withPrefix(prefixVar)
      .withDelimiter(delimiter)
    val objects = amazonS3Client.listObjects(listObjectsRequest)
    val result =
      objects.getCommonPrefixes.toSeq.map(path => s"s3a://$bucketName/$path")
    println(
      s"""Listing subdirs of s3://$bucketName/$prefix: \n ${result.mkString(
        ", ")}""")
    result
  }

  def deleteS3FolderRecursively(s3Path: String,
                                bucketName: String,
                                region: Regions = Regions.EU_CENTRAL_1) = {
    println(s"trying to delete data in s3://$bucketName/$s3Path")
    val amazonS3Client =
      AmazonS3ClientBuilder.standard().withRegion(region).build()

    var list = amazonS3Client.listObjects(bucketName, s3Path)
    while (list.getObjectSummaries().asScala.size > 0) {
      list.getObjectSummaries().asScala.foreach { x =>
        amazonS3Client.deleteObject(bucketName, x.getKey())
      }

      list = amazonS3Client.listObjects(bucketName, s3Path)
    }

    Thread.sleep(10000) //sleep again for the same reasons
  }

}
