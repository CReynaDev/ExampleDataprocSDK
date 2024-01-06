package com.bbva.datioamproduct.fdevdatio.course

import com.bbva.datioamproduct.fdevdatio.course.common.ConfigConstants.{CutOffDateTag, DevNameTag, FifaUpdateDateTag, InputTag}
import com.bbva.datioamproduct.fdevdatio.course.fields.Field
import com.typesafe.config.Config
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, Dataset, Row}

import scala.collection.convert.ImplicitConversions.`set asScala`
import scala.language.implicitConversions

package object utils {

  case class Params(devName: String, fifaUpdateDate: String, cutOffDate: String)

  implicit class SuperConfig(config:Config) extends IOUtils{

    def readInputs: Map[String, Dataset[Row]] = {
      config.getObject(InputTag).keySet()
        .map(key=>{
          val inputConfig: Config = config.getConfig(s"$InputTag.$key")
          val ds: Dataset[Row] = read(inputConfig)
          key -> ds
        }).toMap
    }

    def getParams: Params = Params(
      devName = config.getString(DevNameTag),
      fifaUpdateDate = config.getString(FifaUpdateDateTag),
      cutOffDate = config.getString(CutOffDateTag)
    )

    }


  implicit def fieldToColumn(field: Field): Column = field.column
}
