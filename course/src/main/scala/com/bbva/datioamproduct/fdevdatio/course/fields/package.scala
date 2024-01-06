package com.bbva.datioamproduct.fdevdatio.course

import com.bbva.datioamproduct.fdevdatio.course.common.ConfigConstants.Categorys._
import com.bbva.datioamproduct.fdevdatio.course.common.ConfigConstants.Numbers._
import com.bbva.datioamproduct.fdevdatio.course.common.ConfigConstants._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import com.bbva.datioamproduct.fdevdatio.course.utils.fieldToColumn
import org.apache.spark.sql.expressions.{Window, WindowSpec}

package object fields {

  trait Field {
    val name: String
    lazy val column: Column = col(name)
  }



  case object FifaUpdateDate extends Field{
    override val name: String = "fifa_update_date"

    def filter(date:String): Column = column === date
  }

  case object PlayerId extends Field{
    override val name: String = "player_id"
  }

  case object ClubTeamId extends Field{
    override val name: String = "club_team_id"
  }

  case object LeagueName extends Field{
    override val name: String = "league_name"
  }

  case object ClubName extends Field{
    override val name: String = "club_name"
  }

  case object NationTeamId extends Field{
    override val name: String = "nation_team_id"
  }

  case object NationalityId extends Field{
    override val name: String = "nationality_id"
  }

  case object FifaVersion extends Field{
    override val name: String = "fifa_version"
  }

  case object Overall extends Field{
    override val name: String = "overall"
  }

  case object Age extends Field{
    override val name: String = "age"
  }

  case object NationalityName extends Field{
    override val name: String = "nationality_name"
  }

  case object NationPosition extends Field{
    override val name: String = "nation_position"
  }

  case object AvgOverall extends Field{
    override val name: String = "avg_overall"

    def apply(): Column = {
      avg(Overall).alias(name)
    }
  }

  case object CategoryPlayer extends Field{
    val name: String = "category_player"

    /**
     * category_player = A para overal>85 o age < 23
     * category_player = B para overal>80 o age < 27
     * category_player = C para overal>70 o age < 30
     * category_player = D para el resto
     *
     * @return
     */

    def apply(): Column = {
      when(Overall > 85 || Age < 23, "A")
        .when(Overall > 80 || Age < 27, "B")
        .when(Overall > 70 || Age < 30, "C")
        .otherwise("D")
        .alias(name)
    }
  }

  case object HeightCm extends Field {
    val name: String = "height_cm"
  }

  case object CatHeight extends Field{
    val name: String = "cat_height"

    /**
     * Si el campo height_cm es mayor a 200 el valor será A
     * Si el campo height_cm es mayor o igual a 185 el valor será B
     * Si el campo height_cm es mayor a 175 el valor es C
     * Si el campo height_cm es mayor o igual a 165 el valor es D
     * Para el resto el valor debe ser E
     * @return
     */

    def apply(): Column = {
      when(HeightCm > 200, "A")
        .when(HeightCm >= 185, "B")
        .when(HeightCm > 175, "C")
        .when(HeightCm >= 165, "D")
        .otherwise("D")
        .alias(name)
    }
  }

  case object LongName extends Field {
    override val name: String = "long_name"
  }

  case object CutoffDate extends Field {
    val name: String = "cutoff_date"

    def column(cutoffDate: String): Column = {
      lit(cutoffDate).as(name)
    }
  }

  case object ShortName extends Field {
    override val name:String = "short_name"
    def apply():Column = lit("nuevo valor").as(name)
  }

  case object PlayerPositions extends Field {
    override val name: String = "player_positions"

    def apply(): Column = {
      split(regexp_replace(column, " ", ""), Comma) alias name
    }
  }

  case object ExplodePlayerPositions extends Field {
    override val name: String = "explode_player_positions"

    def apply(): Column = {
      explode(PlayerPositions.column) alias name
    }
  }

  case object CountByPlayerPositions extends Field {
    override val name: String = "count_by_player_positions"

    def apply(): Column = {
      count(ExplodePlayerPositions.column) alias name
    }
  }

  case object Lead extends Field {
    override val name: String = "lead_overall"

    def apply(): Column = {
      val window: WindowSpec = Window.partitionBy(NationalityName).orderBy(Overall)
      lead(Overall, 1, 100).over(window) alias name
    }
  }

  case object PercentRank extends Field {
    override val name: String = "percent_rank"

    def apply(): Column = {
      val window: WindowSpec = Window.partitionBy(NationalityName).orderBy(Overall)
      percent_rank().over(window) alias name
    }
  }

  case object CumeDist extends Field {
    override val name: String = "cume_dist"

    def apply(): Column = {
      val window: WindowSpec = Window.partitionBy(NationalityName).orderBy(Overall)
      cume_dist().over(window) alias name
    }
  }

  case object CatAge extends Field {
    override val name: String = "cat_age_overall"

    def apply(): Column = {
      when(Age <= Twenty || Overall > 80, A)
        .when(Age <= TwentyThree || Overall > 70, B)
        .when(Age <= Thirty, C)
        .otherwise(D)
        .alias(name)
    }
  }

  case object ZScore extends Field {
    override val name: String = "z_score"

    def apply(): Column = {
      val window: WindowSpec = Window.partitionBy(NationalityName.column, CatAge.column)
      val avgOverall = avg(Overall).over(window)
      val stddevOverall = stddev(Overall).over(window)
      ((Overall - avgOverall) / stddevOverall).alias(name)
    }
  }

  case object SumOverall extends Field {
    override val name: String = "sum_overall"

    def apply(): Column = {
      val window: WindowSpec = Window.partitionBy(ClubName, CatAge)//.orderBy(Overall)
      sum(Overall) over window alias name
    }
  }
}
