package com.bbva.datioamproduct.fdevdatio.course

import com.bbva.datioamproduct.fdevdatio.course.common.ConfigConstants.JoinTypes._
import com.bbva.datioamproduct.fdevdatio.course.common.ConfigConstants.Inputs._
import com.bbva.datioamproduct.fdevdatio.course.fields._
import org.apache.spark.sql.{Dataset, Row}

package object joins {

  implicit class Join(mapDs: Map[String, Dataset[Row]]) {

    def getJoin: Dataset[Row] = {
      val nationalPlayersKeys: Seq[String] = Seq(FifaVersion.name, FifaUpdateDate.name, NationTeamId.name, PlayerId.name)
      val clubPlayersKeys: Seq[String] = Seq(FifaVersion.name, FifaUpdateDate.name, PlayerId.name, ClubTeamId.name)
      val clubTeamsKeys: Seq[String] = Seq(FifaVersion.name, FifaUpdateDate.name, ClubTeamId.name)
      val nationalTeamsKeys: Seq[String] = Seq(NationTeamId.name, NationalityName.name)
      val nationalitiesKeys: Seq[String] = Seq(NationalityId.name)
      val clubPlayersDs: Dataset[Row] = mapDs(ClubPlayersTag)
      val clubTeamsDs: Dataset[Row] = mapDs(ClubTeamsTag)
      val nationalPlayersDs: Dataset[Row] = mapDs(NationalPlayersTag)
      val nationalTeamsDs: Dataset[Row] = mapDs(NationalTeamsTag)
      val playersDs: Dataset[Row] = mapDs(PlayersTag)
      val nationalitiesDs: Dataset[Row] = mapDs(NationalitiesTag)

      playersDs
        .join(nationalitiesDs, nationalitiesKeys, Left)
        .join(nationalPlayersDs, nationalPlayersKeys, Left)
        .join(clubPlayersDs, clubPlayersKeys, Left)
        .join(nationalTeamsDs, nationalTeamsKeys, Left)
        .join(clubTeamsDs, clubTeamsKeys, Left)
    }
  }
}
