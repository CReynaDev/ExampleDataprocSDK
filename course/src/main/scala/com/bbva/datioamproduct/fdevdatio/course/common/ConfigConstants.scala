package com.bbva.datioamproduct.fdevdatio.course.common

object ConfigConstants {
  val RootTag: String = "courseJob"
  val InputTag: String = s"$RootTag.input"
  val FilterTag: String = "filter"

  val Options: String = "options"
  val OverrideSchema: String = s"$Options.overrideSchema"
  val MergeSchema: String = s"$Options.mergeSchema"
  val PartitionOverwriteMode: String = s"$Options.partitionOverwriteMode"
  val CoalesceNumber: String = s"$Options.coalesce"
  val Delimiter: String = s"$Options.delimiter"
  val Header: String = s"$Options.header"

  val Schema: String = "schema"
  val SchemaPath: String = s"$Schema.path"
  val IncludeMetadataFields: String = s"$Schema.includeMetadataFields"
  val IncludeDeletedFields: String = s"$Schema.includeDeletedFields"

  val Path: String = "path"
  val Table: String = "table"
  val Type: String = "type"

  val PartitionOverwriteModeString: String = s"$Options.partitionOverwriteMode"
  val Partitions: String = "partitions"
  val Mode: String = "mode"

  val DelimiterOption: String = "delimiter"
  val HeaderOption: String = "header"
  val OverrideSchemaOption: String = "overrideSchema"
  val MergeSchemaOption: String = "mergeSchema"
  val DevNameTag: String = s"$RootTag.params.devName"
  val FifaUpdateDateTag: String = s"$RootTag.params.fifaUpdateDate"
  val CutOffDateTag: String = s"$RootTag.params.cutOffDate"
  case object Inputs{
    val ClubTeamsTag: String = "clubTeams"
    val ClubPlayersTag: String = "clubPlayers"
    val NationalPlayersTag: String = "nationalPlayers"
    val NationalTeamsTag: String = "nationalTeams"
    val NationalitiesTag: String = "nationalities"
    val PlayersTag: String = "players"
  }
  val MexicoIdTag: String = "83"
  val MexicoTag: String = "Mexico"
  val SpainTag: String = "Spain"
  val ArgentinaTag: String = "Argentina"
  val Comma: String = ","
  val Rank: String = "rank"

  case object JoinTypes  {
    val Left: String = "left"
    val Right: String = "right"
    val Inner: String = "inner"
    val LeftAnti: String = "left_anti"
    val LeftSemi: String = "left_semi"
    val Cross: String = "cross"
    val FullOuter: String = "full"
  }

  case object Categorys {
    val A: String = "A"
    val B: String = "B"
    val C: String = "C"
    val D: String = "D"
  }

  case object Numbers {
    val Zero: Int = 0
    val Twenty: Int = 20
    val TwentyThree: Int = 23
    val Thirty: Int = 30
    val Eighty: Int = 80
  }

  case object Leagues {
    val PremierLeague: String = "Premier League"
    val LigaMx: String = "Liga MX"
  }

  case object Positions {
    val GoalKeeper: String = "GK"
    val Defender: String = "DF"
    val Midfielder: String = "MF"
    val Forward: String = "FW"
    val Striker: String = "ST"
  }
}
