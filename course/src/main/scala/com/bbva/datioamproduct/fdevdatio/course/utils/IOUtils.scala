package com.bbva.datioamproduct.fdevdatio.course.utils

import com.bbva.datioamproduct.fdevdatio.course.common.ConfigConstants._
import com.datio.dataproc.sdk.datiosparksession.DatioSparkSession
import com.datio.dataproc.sdk.io.output.DatioDataFrameWriter
import com.datio.dataproc.sdk.schema.DatioSchema
import com.typesafe.config._
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.net.URI
import java.util.Optional
import scala.collection.convert.ImplicitConversions.{`map AsJavaMap`, `set asScala`}
import scala.language.implicitConversions

trait IOUtils {
  lazy val datioSparkSession: DatioSparkSession = DatioSparkSession.getOrCreate()

  private def getSchema(config: Config): Optional[DatioSchema] = Optional.ofNullable {
    if (config.hasPath(SchemaPath)) {
      val schemaPath: String = config.getString(SchemaPath)
      val icludeMetadata: Boolean = config.getBoolean(IncludeMetadataFields)
      val icludeDeletedFields: Boolean = config.getBoolean(IncludeDeletedFields)

      DatioSchema.getBuilder.fromURI(URI.create(schemaPath))
        .withMetadataFields(icludeMetadata)
        .withDeletedFields(icludeDeletedFields)
        .build()
    } else {
      None.orNull
    }
  }

  private def getOptions(config: Config): Map[String, String] = {
    if (config.hasPath(Options)) {
      config
        .getObject(Options).keySet()
        .map((key: String) => {
          (key, config.getString(s"$Options.$key"))
        })
        .toMap
    } else {
      None.orNull
    }
  }

  private def getFilter(inputConfig: Config): String = {
    if (inputConfig.hasPath(FilterTag)) {
      inputConfig.getString(FilterTag)
    } else {
      String.valueOf(true)
    }
  }
  def read(inputConfig: Config): DataFrame = {
    val schema: DatioSchema = getSchema(inputConfig).orElse(None.orNull)
    val filterExpr: String = getFilter(inputConfig)

    inputConfig.getString(Type) match {
      case "table" =>

        datioSparkSession.read()
          .datioSchema(schema)
          .table(inputConfig.getString(Table))
          .filter(filterExpr)

        // METASTORE de HIVE


      case "parquet" =>

        datioSparkSession.read()
          .options(getOptions(inputConfig))
          .datioSchema(schema)
          .parquet(inputConfig.getString(Path))
          .filter(filterExpr)

      case "csv" =>

        datioSparkSession.read()
          .options(getOptions(inputConfig))
          .datioSchema(schema)
          .csv(inputConfig.getString(Path))
          .filter(filterExpr)

      case _@inputType => throw new Exception(s"Formato de archivo no soportado: $inputType")
    }
  }

  def write(df: DataFrame, outputConfig: Config): Unit = {
    val mode: SaveMode = outputConfig.getString(Mode) match {
      case "overwrite" => SaveMode.Overwrite
      case "append" => SaveMode.Append
      case _@saveMode => throw new Exception(s"Modo de escritura no soportado: $saveMode")
    }

    val schema: DatioSchema = getSchema(outputConfig).orElse(None.orNull)

    val partitions: Array[String] = outputConfig.getStringList(Partitions).toArray.map(_.toString)
    val options: Map[String, String] = getOptions(outputConfig)

    val writer: DatioDataFrameWriter = datioSparkSession
      .write()
      .mode(mode)
      .options(options)
      .datioSchema(schema)
      .partitionBy(partitions: _*)

    outputConfig.getString(Type) match {
      case "table" => writer.table(df, outputConfig.getString(Table))
      case "parquet" => writer.parquet(df, outputConfig.getString(Path))
      case "csv" => writer.csv(df, outputConfig.getString(Path))
      case "avro" => writer.avro(df, outputConfig.getString(Path))
      case _@outputType => throw new Exception(s"Formato de escritura no soportado: $outputType")
    }
  }
}
