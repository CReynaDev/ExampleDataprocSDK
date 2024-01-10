package com.bbva.datioamproduct.fdevdatio.course

import com.bbva.datioamproduct.fdevdatio.course.common.ConfigConstants._
import com.bbva.datioamproduct.fdevdatio.course.common.ConfigConstants.Inputs._
import com.bbva.datioamproduct.fdevdatio.course.fields._
import com.bbva.datioamproduct.fdevdatio.course.utils.{SuperConfig, fieldToColumn}
import com.bbva.datioamproduct.fdevdatio.course.transformations._
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.typesafe.config.Config
import org.apache.spark.sql._
import org.slf4j.{Logger, LoggerFactory}


class CourseJob extends SparkProcess {
  private val logger:Logger = LoggerFactory.getLogger(this.getClass)

  override def getProcessId: String = "CourseJob"

  override def runProcess(runtimeContext: RuntimeContext): Int = {
    val config: Config = runtimeContext.getConfig
    val mapDs: Map[String, Dataset[Row]] = config.readInputs

    val devName: String = config.getString(DevNameTag)
    logger.info(s"Hola $devName desde mi nuevo modulo en el arquetipo desde $getProcessId :D")

    val clubPlayersDs: Dataset[Row] = mapDs(ClubPlayersTag)
    val clubTeamsDs: Dataset[Row] = mapDs(ClubTeamsTag)
    val nationalPlayersDs: Dataset[Row] = mapDs(NationalPlayersTag)
    val nationalTeamsDs: Dataset[Row] = mapDs(NationalTeamsTag)
    val playersDs: Dataset[Row] = mapDs(PlayersTag)
    val nationalitiesDs: Dataset[Row] = mapDs(NationalitiesTag)

    val fifaDs: Dataset[Row] = config
      .readInputs
      .getJoin

   fifaDs
     .select(ShortName, ClubName, Overall, Age)
     .addColumn(CatAge())
     .addColumn(SumOverall())
     .filter(ClubName === "Necaxa")
     .show(100, false)



//     .select(FifaUpdateDate, LongName, NationalityName, ClubTeamId, ClubName, LeagueName, Age, Overall)
//     .getUniqueClubTeams
//     .show(100, false)

//     .select(FifaVersion, FifaUpdateDate, LongName, NationalityName, ClubName, LeagueName, Age, Overall)
//     .addColumn(CatAge())
//     .addColumn(ZScore())
//     .orderBy(ZScore.desc)
//     .filter(NationalityName === MexicoTag)
//     .filter(LeagueName === LigaMx)
//     .show(100, false)







     0
  }
}
