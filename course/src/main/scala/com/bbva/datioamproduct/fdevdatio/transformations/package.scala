package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.common.StaticVals.CourseConfigConstants._
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.Joins.{LeftAntiJoin, LeftJoin}
import com.bbva.datioamproduct.fdevdatio.common.fields._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, functions}

package object transformations {
  implicit class TransformationsUtils(dataFrame: DataFrame) {

    def addColumn(newColumn: Column): DataFrame = {
      val columns: Array[Column] = dataFrame.columns.map(col) :+ newColumn
      dataFrame.select(columns: _*) // : _*, te regresa todos los elementos separados de una estructura
    }


    def aggregateExample: DataFrame = {
      dataFrame.groupBy(NationalityName.column)
        .agg(OverallByNationality(), PlayersByNationality())
        .orderBy(PlayersByNationality.column.desc)
    }

    def aggregatePivotExample: DataFrame = {
      dataFrame.groupBy(ClubName.column, ExplodePlayerPositions.column)
        .agg(count("*") alias "Cantidad")
    }

    def aggregatePivotExampleTwo: DataFrame = {
      dataFrame.groupBy(ClubName.column)
        .pivot(ExplodePlayerPositions.column, Seq("GK", "ST"))
        //.agg(count("*") alias "Cantidad")
        .agg(
          max(Overall.column) alias "max",
          min(Overall.column) alias "min",
          avg(Overall.column) alias "avg"
        )
    }

    def aggregatePivotPlayersTrait: DataFrame = {
      dataFrame.groupBy(LeagueName.column)
        .pivot(ExplodePlayerTraits.column,
          Seq("Leadership", "Early Crosser", "Power Header", "Long Shot Taker (AI)"))
        .agg(count(ExplodePlayerTraits.column) alias "count")
    }

    def overallAvgByLeague: DataFrame = {
      dataFrame.groupBy(LeagueName.column)
        .pivot(ExplodePlayerTraits.column)
        .agg(avg(Overall.column) alias "avg")
    }

    def groupByExplodePlayerPositions: DataFrame = {
      dataFrame.groupBy(ExplodePlayerPositions.column)
        .agg(CountByPlayerPositions())
    }

    def overallAvgEnglishPremierLeagueSts: DataFrame = {
      dataFrame
        .where(LeagueName.column === "English Premier League")
        .groupBy(ExplodePlayerPositions.column)
        .agg(CountByPlayerPositions(), OverallPlayersPositions())
        .where(ExplodePlayerPositions.column === "ST")

    }

    def overallAvgMexicalLigaMxLb: DataFrame = {
      dataFrame
        .where(LeagueName.column === "Mexican Liga MX")
        .groupBy(ClubName.column, ExplodePlayerPositions.column)
        .agg(CountByPlayerPositions(), OverallPlayersPositions())
        .where(ExplodePlayerPositions.column === "LB")
        .orderBy(OverallPlayersPositions.column.desc)

    }

    def filterLeague(leagueName: String): DataFrame = {
      dataFrame.filter(LeagueName.column === leagueName)
    }


    def magicMethod: DataFrame = {
      dataFrame.select(
        dataFrame.columns.map {
          case name: String if name == "club_jersey_number" => lit(0).alias(name)
          case _@name =>
            col(name)
        }: _*
      )
    }
  }

  @throws[JoinException]
  implicit class MapToDataFrame(dfMap: Map[String, DataFrame]) {
    def getFullDf: DataFrame = {

      // Captura de datos
      val playersKeys: Set[String] = Set(SofifaId.name, ClubTeamId.name, NationTeamId.name)
      val clubTeamsKeys: Set[String] = Set(SofifaId.name, ClubTeamId.name)
      val nationalPlayersKeys: Set[String] = Set(SofifaId.name, NationTeamId.name)
      val nationalTeamsKeys: Set[String] = Set(NationTeamId.name)
      val nationalitiesKeys: Set[String] = Set(NationalityId.name)
      // validaciones
      if (!playersKeys.subsetOf(dfMap(PlayersTag).columns.toSet)) throw
        JoinException(playersKeys.toArray, dfMap(PlayersTag).columns)
      if (!clubTeamsKeys.subsetOf(dfMap(ClubPlayersTag).columns.toSet)) throw
        JoinException(clubTeamsKeys.toArray, dfMap(ClubTeamsTag).columns)
      if (!nationalPlayersKeys.subsetOf(dfMap(NationalPlayersTag).columns.toSet)) throw
        JoinException(nationalPlayersKeys.toArray, dfMap(NationalPlayersTag).columns)
      if (!nationalTeamsKeys.subsetOf(dfMap(NationalTeamsTag).columns.toSet)) throw
        JoinException(nationalTeamsKeys.toArray, dfMap(NationalTeamsTag).columns, message = "hola")
      if (!nationalitiesKeys.subsetOf(dfMap(NationalitiesTag).columns.toSet)) throw
        JoinException(nationalTeamsKeys.toArray, dfMap(NationalTeamsTag).columns, message = "hola")

      // join
      dfMap(PlayersTag)
        .join(dfMap(ClubPlayersTag), Seq(SofifaId.name, ClubTeamId.name), LeftJoin)
        .join(dfMap(ClubTeamsTag), Seq(ClubTeamId.name), LeftJoin)
        .join(dfMap(NationalPlayersTag), Seq(NationTeamId.name, SofifaId.name), LeftJoin)
        .join(dfMap(NationalitiesTag), Seq(NationalityId.name), LeftJoin)
    }

  }

  case class ReplaceColumnException(message: String, columnName: String, columnas: Array[String])
    extends Exception(message)

  case class JoinException(
                            expectedKeys: Array[String],
                            columns: Array[String],
                            location: String = "com.bbva.datioamproduct.fdevdatio.transformations.MapToDataFrame.getFullData",
                            message: String = "Sucedió un error al intentar hacer el Join, las primary key no son las adecuadas"
                          ) extends Exception(message)


}
