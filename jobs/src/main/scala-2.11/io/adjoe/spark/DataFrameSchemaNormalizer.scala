package io.adjoe.spark

import java.util.UUID.randomUUID
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{
  array,
  col,
  collect_list,
  explode_outer,
  lit,
  struct,
  when
}
import org.apache.spark.sql.types.{
  ArrayType,
  BooleanType,
  DataType,
  DateType,
  DoubleType,
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType
}

class DataFrameSchemaNormalizer(jobName: String = "work_done",
                                workingFolder: String = "tmp",
                                maxNestingDepth: Int = 1)
    extends DataTransformer(jobName, workingFolder) {

  case class ConversionColumn(name: String,
                              datatype: String,
                              children: Seq[ConversionColumn],
                              curExecIdx: Int = 0)

  val STRUCT_SEP = "___"
  val S3_PROTOCOL = "s3a"

  /*******************
    * This function normalizes a dataframe.
    * @param df
    * @param s3Bucket
    * @param spark
    * @return the normalized dataframe
    */
  def normalizeSchema(df: DataFrame, s3Bucket: String)(
      implicit spark: SparkSession) = {
    val uuid = randomUUID

    var nestedDepth = 0
    df.columns.size > 0 match {
      case true =>
        val exploded = (1 to maxNestingDepth).foldLeft(df) { (acc, idx) =>
          isAtomic(acc) match {
            case true => acc
            case false =>
              nestedDepth = nestedDepth + 1
              unfoldNestedColumns(acc)
          }
        }

        exploded.write
          .mode("overwrite")
          .parquet(
            s"$S3_PROTOCOL://$s3Bucket/$TEMPORARY_WORKING_FOLDER/NORMALIZE_SCHEMA/1_UNFOLDED/$uuid")
        val exploded_loaded = spark.read.parquet(
          s"$S3_PROTOCOL://$s3Bucket/$TEMPORARY_WORKING_FOLDER/NORMALIZE_SCHEMA/1_UNFOLDED/$uuid")

        val normalized = makeLowercaseAndUnify(exploded_loaded).distinct()
        normalized.write
          .mode("overwrite")
          .parquet(
            s"$S3_PROTOCOL://$s3Bucket/$TEMPORARY_WORKING_FOLDER/NORMALIZE_SCHEMA/2_LOWERCASED_MERGED_SORTED_COL/$uuid")
        val normalized_loaded = spark.read.parquet(
          s"$S3_PROTOCOL://$s3Bucket/$TEMPORARY_WORKING_FOLDER/NORMALIZE_SCHEMA/2_LOWERCASED_MERGED_SORTED_COL/$uuid")

        val imploded = (1 to Math.min(nestedDepth, maxNestingDepth)).foldLeft(normalized_loaded) {
          (acc, idx) =>
            implodeAndFoldDF(acc)
        }

        deleteComments(imploded).write
          .mode("overwrite")
          .parquet(
            s"$S3_PROTOCOL://$s3Bucket/$TEMPORARY_WORKING_FOLDER/NORMALIZE_SCHEMA/3_FOLDED/$uuid")
        val imploded_loaded = spark.read.parquet(
          s"$S3_PROTOCOL://$s3Bucket/$TEMPORARY_WORKING_FOLDER/NORMALIZE_SCHEMA/3_FOLDED/$uuid")

        imploded_loaded
      case _ => df
    }

  }

  def getSchemaPreservingTuple(df: DataFrame, s3Bucket: String)(
      implicit spark: SparkSession) = {
    val uuid = randomUUID

    val schemaPreservingTuple_1 = (1 to maxNestingDepth).foldLeft(df.limit(1)) {
      (acc, idx) =>
        unfoldNestedColumns(acc, true)
    }

    val schemaPreservingTuple_2 = schemaPreservingTuple_1.na
      .fill("___NOVALUE___")
      .na
      .fill(Int.MinValue)
      .na
      .fill(Double.MinValue)
      .na
      .fill(false)

    val schemaPreservingTuple_3 = spark.sqlContext.createDataFrame(
      schemaPreservingTuple_2.rdd,
      schemaPreservingTuple_1.schema)

    schemaPreservingTuple_3.show(10, false)

    schemaPreservingTuple_3.write
      .mode("overwrite")
      .parquet(
        s"$S3_PROTOCOL://$s3Bucket/$TEMPORARY_WORKING_FOLDER/NORMALIZE_SCHEMA_PRESERVING_TUPLE/1_UNFOLDED/$uuid")

    val schemaPreservingTuple_loaded = spark.read.parquet(
      s"$S3_PROTOCOL://$s3Bucket/$TEMPORARY_WORKING_FOLDER/NORMALIZE_SCHEMA_PRESERVING_TUPLE/1_UNFOLDED/$uuid")

    val schemaPreservingTuple_normalized = makeLowercaseAndUnify(
      schemaPreservingTuple_loaded)

    schemaPreservingTuple_normalized.write
      .mode("overwrite")
      .parquet(
        s"$S3_PROTOCOL://$s3Bucket/$TEMPORARY_WORKING_FOLDER/NORMALIZE_SCHEMA_PRESERVING_TUPLE/2_LOWERCASED_MERGED_SORTED_COL/$uuid")

    val schemaPreservingTuple_normalized_loaded = spark.read.parquet(
      s"$S3_PROTOCOL://$s3Bucket/$TEMPORARY_WORKING_FOLDER/NORMALIZE_SCHEMA_PRESERVING_TUPLE/2_LOWERCASED_MERGED_SORTED_COL/$uuid")

    val schemaPreservingTuple_imploded =
      (1 to maxNestingDepth).foldLeft(schemaPreservingTuple_normalized_loaded) {
        (acc, idx) =>
          implodeAndFoldDF(acc)
      }

    deleteComments(schemaPreservingTuple_imploded).write
      .mode("overwrite")
      .parquet(
        s"$S3_PROTOCOL://$s3Bucket/$TEMPORARY_WORKING_FOLDER/NORMALIZE_SCHEMA_PRESERVING_TUPLE/3_FOLDED/$uuid")
    val schemaPreservingTuple_imploded_loaded = spark.read.parquet(
      s"$S3_PROTOCOL://$s3Bucket/$TEMPORARY_WORKING_FOLDER/NORMALIZE_SCHEMA_PRESERVING_TUPLE/3_FOLDED/$uuid")

    schemaPreservingTuple_imploded_loaded
  }

  /***************************
    * Recursive function to create a structure which contains the columns and order of unnesting
    * @param schema
    * @param curExecIdx
    * @return
    */
  def createEasySchema(schema: ArrayType,
                       curExecIdx: Int): (Seq[ConversionColumn], Int) = {
    var execIdx = curExecIdx
    //derive the children
    val containedCols = schema.elementType match {
      case str: StructType =>
        val colName = "element"
        val colType = "struct"
        val newColName = colName.toLowerCase
        val recursiveResult = createEasySchema(str, execIdx)
        val result =
          ConversionColumn(colName, "struct", recursiveResult._1, execIdx)
        execIdx = recursiveResult._2
        result

      case other: DataType =>
        val colName = "element"
        val colType = other.simpleString
        val newColName = colName.toLowerCase
        ConversionColumn(colName, colType, Seq.empty[ConversionColumn], execIdx)

    }
    (Seq(containedCols), execIdx)
  }

  /******************
    * Recursive function to create a structure which contains the columns and order of unnesting
    * @param schema
    * @param curExecIdx
    * @return
    */
  def createEasySchema(schema: StructType,
                       curExecIdx: Int = 0): (Seq[ConversionColumn], Int) = {
    var execIdx = curExecIdx

    //derive the children: start with normal atomar columns, then structs and finally arrays
    val containedCols = schema.fields
      .sortWith(orderStructFields)
      .foldLeft(Seq.empty[ConversionColumn]) { (acc, x) =>
        val colName = x.name
        val columnField = x
        val columnComment = x.getComment

        val newColName = colName.toLowerCase

        val newElement = columnField match {
          case StructField(name, atype: StructType, _, _) =>
            execIdx = columnComment match {
              case None =>
                execIdx + 1
              case Some(oldIdx) =>
                oldIdx.split(",").reverse.head.toInt
            }
            val recursiveResult = createEasySchema(atype, execIdx)
            val result =
              ConversionColumn(colName, "struct", recursiveResult._1, execIdx)
            execIdx = recursiveResult._2
            result

          case StructField(name, atype: ArrayType, _, _) =>
            execIdx = columnComment match {
              case None =>
                atype.elementType match {
                  case str: StructType => execIdx + 1
                  case _ =>
                    execIdx //do no count atomsr data types as arrays and keep them as is
                }
              case Some(oldIdx) =>
                oldIdx.split(",").reverse.head.toInt
            }
            val recursiveResult = createEasySchema(atype, execIdx)
            val result =
              ConversionColumn(colName, "array", recursiveResult._1, execIdx)
            execIdx = recursiveResult._2
            result

          case StructField(name, atype: DataType, _, _) =>
            execIdx = columnComment match {
              case None =>
                execIdx
              case Some(oldIdx) =>
                oldIdx.split(",").reverse.head.toInt
            }
            ConversionColumn(colName,
                             atype.simpleString,
                             Seq.empty[ConversionColumn],
                             execIdx)

        }

        acc :+ newElement

      }
    (containedCols, execIdx)
  }

  def isAtomic(df: DataFrame) = {
    var schema =
      ConversionColumn("root", "struct", createEasySchema(df.schema)._1)
    //must only contain atomic arrays
    val arrayColumns = schema.children
      .filter(_.datatype == "array")
    //and no structs
    val structColumns = schema.children
      .filter(_.datatype == "struct")

    val isAtomic = (structColumns.size == 0) & arrayColumns.foldLeft(true) {
      (acc, column) =>
        acc & ("struct" != column.children.head.datatype)
    }

    isAtomic
  }

  /*****************
    * Step 1 of Schema normalisation and deduplication: results in all nested properties to be flattened
    * @param df
    * @param replaceNullsWithEmptyArraysForAtomarArrays
    * @param spark
    * @return flattened df
    */
  def unfoldNestedColumns(df: DataFrame,
                          replaceNullsWithEmptyArraysForAtomarArrays: Boolean =
                            false)(implicit spark: SparkSession) = {
    var schema =
      ConversionColumn("root", "struct", createEasySchema(df.schema)._1)
    //first get array_types
    val arrayColumns = schema.children
      .filter(_.datatype == "array")
      .sortWith((x, y) => x.curExecIdx < y.curExecIdx)
    val dfExplodedColumns = arrayColumns.foldLeft(df) { (acc, column) =>
      "struct" == column.children.head.datatype match {
        case true =>
          val oldComment = acc.schema(column.name).getComment match {
            case None =>
              logger.debug(s"struct col ${column.name} has NO comment.")
              ""

            case Some(com) =>
              logger.debug(s"struct col ${column.name} has comment: $com")
              com + ","

          }

          val tmpDF = acc
            .withColumn(s"${column.name}_exploded",
                        explode_outer(col(s"${column.name}")))
            .drop(s"${column.name}")

          setComment(tmpDF,
                     s"${column.name}_exploded",
                     oldComment + column.curExecIdx.toString)

        case false =>
          replaceNullsWithEmptyArraysForAtomarArrays match {
            case false => acc
            case _ =>
              acc.schema(column.name).getComment match {
                case None =>
                  logger.info(
                    s"Atomar array col ${column.name} has NO comment.")
                  val arrayElementType = acc
                    .schema(column.name)
                    .dataType
                    .asInstanceOf[ArrayType]
                    .elementType
                    .typeName
                  val dummyElements = arrayElementType match {
                    case x if x == StringType.typeName =>
                      array(lit("dummy1"), lit("dummy2"))
                    case x if x == BooleanType.typeName =>
                      array(lit(true), lit(false))
                    case x if x == IntegerType.typeName =>
                      array(lit(Int.MaxValue), lit(1))
                    case x if x == LongType.typeName =>
                      array(lit(Long.MaxValue), lit(1))
                    case x if x == DoubleType.typeName =>
                      array(lit(0.1), lit(1.1))
                    case x if x == DateType.typeName =>
                      array(lit("2000-01-01"), lit("2000-01-02"))
                  }
                  val tmpDF = acc
                    .withColumn("____tmp____",
                                when(col(column.name).isNull, dummyElements)
                                  .otherwise(col(column.name)))
                    .drop(column.name)
                    .withColumnRenamed("____tmp____", column.name)

                  tmpDF

                case Some(com) =>
                  val oldComment = com
                  logger.info(s"Atomar array ${column.name} has comment: $com")

                  val arrayElementType = acc
                    .schema(column.name)
                    .dataType
                    .asInstanceOf[ArrayType]
                    .elementType
                    .typeName
                  val dummyElements = arrayElementType match {
                    case x if x == StringType.typeName =>
                      array(lit("dummy1"), lit("dummy2"))
                    case x if x == BooleanType.typeName =>
                      array(lit(true), lit(false))
                    case x if x == IntegerType.typeName =>
                      array(lit(Int.MaxValue), lit(1))
                    case x if x == LongType.typeName =>
                      array(lit(Long.MaxValue), lit(1))
                    case x if x == DoubleType.typeName =>
                      array(lit(0.1), lit(1.1))
                    case x if x == DateType.typeName =>
                      array(lit("2000-01-01"), lit("2000-01-02"))
                  }
                  val tmpDF = acc
                    .withColumn("____tmp____",
                                when(col(column.name).isNull, dummyElements)
                                  .otherwise(col(column.name)))
                    .drop(column.name)
                    .withColumnRenamed("____tmp____", column.name)

                  setComment(tmpDF, s"${column.name}", oldComment)

              }

          }

      }
    }

    schema = ConversionColumn("root",
                              "struct",
                              createEasySchema(dfExplodedColumns.schema)._1)

    val structColumns = schema.children.filter(_.datatype == "struct")
    val dfUnfoldedColumns = structColumns
      .foldLeft(dfExplodedColumns) { (acc, structColumn) =>
        structColumn.children.foldLeft(acc) { (acc2, column) =>
          //explode the column and drop the original to keep data small as possible
          val tmpDF =
            acc2.withColumn(s"${structColumn.name}${STRUCT_SEP}${column.name}",
                            col(structColumn.name)(column.name))
          val oldComment = tmpDF.schema(structColumn.name).getComment match {
            case None =>
              logger.info(s"struct col ${structColumn.name} has NO comment.")
              ""

            case Some(com) =>
              logger.info(s"struct col ${structColumn.name} has comment: $com")
              com + ","

          }
          val res =
            setComment(tmpDF,
                       s"${structColumn.name}${STRUCT_SEP}${column.name}",
                       oldComment + column.curExecIdx.toString)

          res

        }

      }
      .drop(structColumns.map(_.name): _*)

    dfUnfoldedColumns.createOrReplaceTempView("dfUnfoldedColumns")
    logger.info("result of unnest function application:")
    spark.sql("describe dfUnfoldedColumns").show(200, false)
    logger.isDebugEnabled match {
      case true =>
        spark.sql("describe dfUnfoldedColumns").show(200, false)
      case _ =>
    }

    dfUnfoldedColumns
  }

  /***************
    * This is step 2 of the schema normalization: makes all columns lowercase (merges them) and sorts them alphabetically
    * @param df
    * @param spark
    * @return lowercased and sorted columns
    */
  def makeLowercaseAndUnify(df: DataFrame)(implicit spark: SparkSession) = {
    val explodedSchema =
      ConversionColumn("root", "struct", createEasySchema(df.schema)._1)
    val nameCollisions = explodedSchema.children.foldLeft(
      Map.empty[String, Seq[(String, String, String)]]) { (acc, x) =>
      val newName = x.name.toLowerCase()
      val comment = df.schema(x.name).getComment() match {
        case None    => "0"
        case Some(x) => x
      }

      acc.contains(newName) match {
        case true =>
          val oldList = acc(newName)
          acc - newName + (newName -> (oldList :+ (x.name, x.datatype, comment)))
        case _ =>
          acc + (newName -> (Seq
            .empty[(String, String, String)] :+ (x.name, x.datatype, comment)))

      }
    }

    //TODO: HERE WE CAN MERGE DIFFERENT NAME COLUMNS BY RULES THAT WE DEFINE
    //TODO: CONSIDER UNIVERSAL TYPE CAST RULES FOR SCHEMA UNIFICATION
    val merged = nameCollisions.foldLeft(df) { (acc, column) =>
      val alias = column._1
      val cols2merge = column._2
      val comments = cols2merge.map(_._3)
      //check for nulls and take the other available value that exists
      val selects = cols2merge.tail.foldLeft(col(cols2merge.head._1)) {
        (acc2, aCol) =>
          when(col(aCol._1).isNull, acc2).otherwise(col(aCol._1))
      }
      //take the comment with the higher prio to collapse them first in the implode step
      val mergedComments = comments.sortWith((x, y) => x.head > y.head).head
      val tmpDF = acc
        .withColumn(alias, selects)
        .drop(cols2merge.map(_._1).filter(_ != alias): _*)
      setComment(tmpDF, alias, mergedComments)
    }
    merged.createOrReplaceTempView("merged")

    logger.info("result of lowercase-and-merge function application:")
    spark.sql("describe merged").show(200, false)
    logger.isDebugEnabled match {
      case true =>
        spark.sql("describe merged").show(200, false)
      case _ =>
    }
    merged

  }

  /****************
    * This is step 3 of the schema normalization: collapses the unfolded and deduplicated data
    * @param df
    * @param spark
    * @return
    */
  def implodeAndFoldDF(df: DataFrame)(implicit spark: SparkSession) = {
    val collapses = df.columns
      .map { colName =>
        val nestedElements = colName.split(STRUCT_SEP).reverse.tail
        val comment =
          df.schema(colName).getComment.get.split(",").map(_.toInt).reverse
        (nestedElements.mkString(","), comment.mkString(","))
      }
      //.distinct
      .groupBy(_._1)
      .toSeq
      .map(x => (x._1, x._2.max._2)) //per column take the max execIdx (in case of name collisions AND new properties can differ)
      .sortWith((x, y) =>
        x._2.split(",").head.toInt > y._2.split(",").head.toInt)
      .filter(_._1 != "")
      .map { x =>
        (x._1.split(","), x._2.split(","))
      }
      .filter(_._1.size > 0)
    //.map(_._1)

    val result = collapses.foldLeft(df) { (acc, x) =>
      val releventColSegments = x._1
      val relComment = x._2.mkString(",")

      logger.info(s"releventColSegments: $releventColSegments")
      logger.info(s"relComment: $relComment")

      releventColSegments(0).endsWith("_exploded") match {

        case false => //this is a struct
          val currentColNamePrefix =
            releventColSegments.reverse.mkString(STRUCT_SEP)

          logger.info("columns: " + acc.columns.mkString(", "))
          logger.info("currentColNamePrefix: " + currentColNamePrefix)

          val selectedColsForStruct =
            acc.columns.filter(_.startsWith(currentColNamePrefix)).sorted.map {
              y =>
                col(y) as y.replaceFirst(currentColNamePrefix + STRUCT_SEP, "")
            }

          logger.info(
            "IMPLODE: " + currentColNamePrefix + "->" + selectedColsForStruct
              .mkString(", "))

          val newAcc = acc
            .withColumn(
              currentColNamePrefix,
              struct(
                selectedColsForStruct: _*
              ) as currentColNamePrefix
            )
            .drop(
              acc.columns.filter(
                _.startsWith(currentColNamePrefix + STRUCT_SEP)): _*
            )
          setComment(newAcc, currentColNamePrefix, relComment)


        case _ => //this is an array
          val currentColNamePrefix =
            releventColSegments.reverse.mkString(STRUCT_SEP)

          logger.info("columns: " + acc.columns.mkString(", "))
          logger.info("currentColNamePrefix: " + currentColNamePrefix)

          val selectedColsForStruct =
            acc.columns.filter(_.startsWith(currentColNamePrefix)).sorted.map {
              y =>
                col(y) as y.replaceFirst(currentColNamePrefix + STRUCT_SEP, "")
            }

          logger.info(
            "IMPLODE: " + currentColNamePrefix + "->" + selectedColsForStruct
              .mkString(", "))

          val isEmptyArray = acc.columns
            .filter(_.startsWith(currentColNamePrefix))
            .foldLeft(lit(true)) { (acc, z) =>
              acc and col(z).isNull
            }

          assert(
            selectedColsForStruct.size > 0,
            "Error in implodeAndFoldDF: array must contain at least 1 field")

          selectedColsForStruct.size match {
            case 1 =>
              val toDrop =
                acc.columns.filter(_.startsWith(currentColNamePrefix)).head
              val groupByCols =
                acc.columns.filter(!_.startsWith(currentColNamePrefix))

              val newAcc = acc
                .groupBy(
                  groupByCols.head,
                  groupByCols.tail: _*
                )
                .agg(
                  collect_list(selectedColsForStruct.head) as currentColNamePrefix
                    .reverse//following lines are the same as replace last "_exploded"
                    .replaceFirst("dedolpxe_", "")
                    .reverse
                )//.replaceFirst("_exploded", ""))

                .drop(toDrop)

              setComment(newAcc,
                         currentColNamePrefix
                           .reverse//following lines are the same as replace last "_exploded"
                           .replaceFirst("dedolpxe_", "")
                           .reverse,
                         relComment)

            case _ =>
              //assemble array content
              val result_ungrouped = acc
                .withColumn(
                  currentColNamePrefix,
                  struct(
                    selectedColsForStruct: _*
                  ) as currentColNamePrefix
                )
                .withColumn("isEmptyArray", isEmptyArray)
                .drop(
                  acc.columns.filter(
                    _.startsWith(currentColNamePrefix + STRUCT_SEP)): _*
                )
              //collect the arrays and keep them empty if all elements are all-null (generally created during exploding the dataframe)
              val groupByCols =
                result_ungrouped.columns.filter(_ != currentColNamePrefix)

              val newAcc = result_ungrouped
                .groupBy(
                  groupByCols.head,
                  groupByCols.tail: _*
                )
                .agg(
                  collect_list(
                    when(!col("isEmptyArray"), col(currentColNamePrefix)) //if all columns are null, do not gather the values
                  ) as currentColNamePrefix
                    .reverse//following lines are the same as replace last "_exploded"
                    .replaceFirst("dedolpxe_", "")
                    .reverse
                )
                .drop(currentColNamePrefix)
                .drop("isEmptyArray")

              setComment(newAcc,
                         currentColNamePrefix
                           .reverse//following lines are the same as replace last "_exploded"
                           .replaceFirst("dedolpxe_", "")
                           .reverse,
                         relComment)
          }
      }
    }
    //order also the most outer columns
    result.createOrReplaceTempView("implodeddf")
    logger.info("result of nesting function application:")
    spark.sql("""describe implodeddf""").show(200, false)
    logger.isDebugEnabled match {
      case true =>
        spark.sql("""describe implodeddf""").show(200, false)
      case _ =>
    }

    result.select(result.columns.sorted.head, result.columns.sorted.tail: _*)
  }

  def orderStructFields(sf1: StructField, sf2: StructField) = {
    sf1.getComment.nonEmpty & sf2.getComment.nonEmpty match {

      case true =>
        (sf1.getComment.get, sf2.getComment.get) match {
          case (a, b) if a == b =>
            sf1 match {
              case StructField(_, atype: StructType, _, _) => {
                sf2 match {
                  case StructField(_, atype: StructType, _, _) => false
                  case StructField(_, atype: ArrayType, _, _)  => true
                  case _                                       => false
                }
              }
              case StructField(_, atype: ArrayType, _, _) => {
                sf2 match {
                  case StructField(_, atype: StructType, _, _) => false
                  case StructField(_, atype: ArrayType, _, _)  => false
                  case _                                       => false
                }
              }
              case _ => true
            }
          case (a, b) if a != b =>
            a < b
        }

      case false =>
        sf1.getComment.nonEmpty match {
          case true => false
          case false =>
            sf2.getComment.nonEmpty match {
              case true => true
              case false =>
                sf1 match {
                  case StructField(_, atype: StructType, _, _) => {
                    sf2 match {
                      case StructField(_, atype: StructType, _, _) => false
                      case StructField(_, atype: ArrayType, _, _)  => true
                      case _                                       => false
                    }
                  }
                  case StructField(_, atype: ArrayType, _, _) => {
                    sf2 match {
                      case StructField(_, atype: StructType, _, _) => false
                      case StructField(_, atype: ArrayType, _, _)  => false
                      case _                                       => false
                    }
                  }
                  case _ => true
                }
            }

        }
    }
  }

  def needsRecalculation(oldDF: DataFrame, newDF: DataFrame)(
      implicit spark: SparkSession) = {
    logger.info(
      "######## Calc if a table needs complete recalculation because of having new properties.")
    val df1_exploded = makeLowercaseAndUnify(
      unfoldNestedColumns(unfoldNestedColumns(oldDF.limit(1))))
    df1_exploded.createOrReplaceTempView("df1")

    val df2_exploded = makeLowercaseAndUnify(
      unfoldNestedColumns(unfoldNestedColumns(newDF.limit(1))))
    df2_exploded.createOrReplaceTempView("df2")

    val df1AttrCount = spark.sql("""DESCRIBE df1 """).drop("comment").count

    logger.info("old df schema:")
    spark.sql("""DESCRIBE df1 """).show(200, false)
    logger.isDebugEnabled match {
      case true =>
        spark.sql("""DESCRIBE df1 """).show(200, false)
      case _ =>
    }

    logger.info("new df schema:")
    spark.sql("""DESCRIBE df2 """).show(200, false)
    logger.isDebugEnabled match {
      case true =>
        spark.sql("""DESCRIBE df2 """).show(200, false)
      case _ =>
    }

    val newCount_leftOuter = spark
      .sql("""DESCRIBE df1 """)
      .drop("comment")
      .join(spark.sql("""DESCRIBE df2 """).drop("comment"),
            Seq("col_name", "data_type"),
            "leftouter")
      .count
    val newCount_rightOuter = spark
      .sql("""DESCRIBE df1 """)
      .drop("comment")
      .join(spark.sql("""DESCRIBE df2 """).drop("comment"),
            Seq("col_name", "data_type"),
            "rightouter")
      .count

    !((df1AttrCount == newCount_leftOuter) & (newCount_leftOuter == newCount_rightOuter))

  }
}
