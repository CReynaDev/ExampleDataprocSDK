package com.bbva.datioamproduct.fdevdatio.simpsons

import com.bbva.datioamproduct.fdevdatio.simpsons.utils.IOUtils
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.slf4j.{Logger, LoggerFactory}


class SimpsonsJob extends SparkProcess with IOUtils{
  private val logger:Logger = LoggerFactory.getLogger(this.getClass)

  override def getProcessId: String = "SimpsonsJob"

  override def runProcess(runtimeContext: RuntimeContext): Int = {

    val config: Config = runtimeContext.getConfig
    val devName: String = config.getString("simpsonsJob.params.devName")
    logger.info(s"Hola mundo $devName")
    val simpsonsDS = read(config.getConfig("simpsonsJob.input.simpsonstab"))

    def cleanDataFrame(df: DataFrame, columns: Seq[String]): DataFrame = {
      df.na.drop(columns)
    }
    val simpsonsClean = cleanDataFrame(simpsonsDS, Seq("rating", "votes", "viewers_in_millions"))

    val dfWithTotalRating = simpsonsClean.withColumn("total_rating",
      sum("rating").over(Window.partitionBy("season"))
    )

    val bestSeason = dfWithTotalRating
      .select("season", "total_rating")
      .orderBy(desc("total_rating"))
      .limit(1)

    val bestYear = simpsonsClean
      .groupBy("original_air_year")
      .agg(sum("viewers_in_millions").alias("viewers_in_millions_year"))
      .orderBy(desc("viewers_in_millions_year"))
      .limit(1)

    val dfWithScore = simpsonsClean.withColumn("score", col("rating") * col("viewers_in_millions"))


    val bestEpisode = dfWithScore
      .select("title", "score")
      .orderBy(desc("score"))
      .limit(1)

    val filteredSeasons = dfWithScore
      .groupBy("season")
      .agg(count("title").alias("num_episodes"))
      .filter(col("num_episodes") >= 3)
      .select("season")


    val dfFiltered = dfWithScore.join(filteredSeasons, "season")


    val topEpisodesBySeason = dfFiltered
      .withColumn("rank", dense_rank().over(Window.partitionBy("season").orderBy(desc("score"))))
      .filter(col("rank") <= 3)


    val outputPath = "src/test/resources/data/output"
    val outputSchemaPath = "src/test/resources/schema/t_fdev_winners.output.schema"


    topEpisodesBySeason
      .write
      .partitionBy("season")
      .parquet(outputPath)



    bestSeason.show()
    bestYear.show()
    bestEpisode.show()

    0
  }

}
