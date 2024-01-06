package com.bbva.datioamproduct.fdevdatio.course


import com.bbva.datioamproduct.fdevdatio.course.common.ConfigConstants.Leagues._
import com.bbva.datioamproduct.fdevdatio.course.common.ConfigConstants.Positions._
import com.bbva.datioamproduct.fdevdatio.course.common.ConfigConstants._
import com.bbva.datioamproduct.fdevdatio.course.fields._
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.functions.{avg, col, length, lit, max, rank, row_number, when}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import com.bbva.datioamproduct.fdevdatio.course.utils.fieldToColumn
import org.apache.spark.sql.expressions.{Window, WindowSpec}

import java.util.Date

package object transformations {


  implicit class Transformations(ds:Dataset[Row]) {
    def addColumn(column: Column): Dataset[Row] = {
      ds.select(
        ds.columns.map(col) :+ column: _*
      )
    }

    def getMaxUpdateDate: Date = {
      ds
        .rdd
        .map(_.getAs[Date](FifaUpdateDate.name))
        .reduce((date1, date2) => if (date1.after(date2)) date1 else date2)
    }

    def filterUpdateDate(date: String): Dataset[Row] = {
      ds
        .filter(FifaUpdateDate.filter(date))
    }

    def magicMethod: DataFrame = {
      ds.select(
        ds.columns.map {
          case name: String if name == "club_jersey_number" => lit(0).alias(name)
          case _@name => col(name)
        }: _*
      )
    }

    def replaceColumn(field: Column): DataFrame = {
      val columnName: String = field.expr.asInstanceOf[NamedExpression].name
      val columns: Array[Column] = ds.columns.map {
        case column if column == columnName => field
        case other => col(other)
      }
      ds.select(columns: _*)
    }

    def avgOverallByNationalTeams: Dataset[Row] = {
      ds
        .filter(NationalityName isin(MexicoTag, SpainTag))
        .groupBy(NationalityName)
        .pivot(NationPosition)
        .agg(AvgOverall())
    }

    def groupByExplodePlayerPositions: Dataset[Row] = {
      ds
        .groupBy(ExplodePlayerPositions.name)
        .agg(CountByPlayerPositions())
    }

    def avgOverallByPremiereLeaguePlayers: Dataset[Row] = {
      ds
        .filter(LeagueName === PremierLeague)
        .filter(ExplodePlayerPositions === Striker)
        .groupBy(ExplodePlayerPositions)
        .agg(avg(Overall).alias(AvgOverall.name))
    }

    def avgOverallByMexicanClubGKPlayers: Dataset[Row] = {
      ds
        .filter(LeagueName === LigaMx && ExplodePlayerPositions === GoalKeeper)
        .groupBy(ClubName)
        .pivot(ExplodePlayerPositions)
        .agg(avg(Overall).alias(AvgOverall.name))
    }

      def getUniqueClubTeams: Dataset[Row] = {
        val window = Window.partitionBy(ClubTeamId.column).orderBy(length(ClubName.column).desc)
        val rankColumn = rank().over(window).alias(Rank)
        val allColumns = ds.columns.map(col) :+ rankColumn
        val subquery = ds.select(allColumns: _*)
        subquery.filter(col(Rank) === 1).drop(Rank)
    }
  }
}
